/* SnapTree - (c) 2009 Stanford University - PPL */

// SnapFixedList
package edu.stanford.ppl.concurrent;

import java.util.AbstractList;
import java.util.concurrent.atomic.AtomicReferenceArray;

/** Implements a concurrent fixed-size <code>List</code> with fast clone and
 *  consistent iteration.  Reads and writes have volatile semantics, and an
 *  iterator will enumerate an atomic snapshot of the list as it existed when
 *  the iterator was created.  No provision is provided to grow or shrink the
 *  list.
 */
public class SnapFixedList<E> extends AbstractList<E> implements Cloneable {

    private static final int LOG_BF = 5;
    private static final int BF = 1 << LOG_BF;
    private static final int BF_MASK = BF - 1;

    // Internally this is implemented as an external tree with branching factor
    // BF.  The leaves of the tree are the elements E.  Nodes are considered to
    // be unshared if they have the same generation as the root node.

    private static class Generation {
    }

    private static class Node extends AtomicReferenceArray<Object> {
        // never modified after initialization of the SnapFixedList, but
        // convenient to be non-final
        Generation gen;

        Node(final Generation gen, final int size, final Object initialValue) {
            super(size);
            this.gen = gen;
            if (initialValue != null) {
                for (int i = 0; i < size; ++i) {
                    lazySet(i, initialValue);
                }
            }
        }

        Node(final Generation gen, final Node src) {
            super(src.length());
            this.gen = gen;
            for (int i = 0; i < src.length(); ++i) {
                lazySet(i, src.get(i));
            }
        }
    }

    private static class COWMgr extends CopyOnWriteManager<Node> {
        private COWMgr(final Node initialValue) {
            super(initialValue, 0);
        }

        protected Node freezeAndClone(final Node value) {
            return new Node(new Generation(), value);
        }
    }

    /** 0 if _size == 0, otherwise the smallest positive int such that
     *  (1L << (LOG_BF * _height)) >= _size.
     */
    private final int _height;

    private final int _size;

    private CopyOnWriteManager<Node> _rootRef;

    public SnapFixedList(final int size) {
        this(size, null);
    }

    public SnapFixedList(final int size, final E element) {
        int height = 0;
        Node partial = null;

        if (size > 0) {
            // We will insert the gen into all of the partials (since they
            // are used exactly once).  We reuse the fulls, so we will give
            // them a null gen that will cause them to be copied before any
            // actual writes.
            final Generation gen = new Generation();

            Object full = element;

            do {
                ++height;

                // This is the number of nodes required at this level.  They
                // are either all full, or all but one full and one partial.
                int levelSize = ((size - 1) >> (LOG_BF * (height - 1))) + 1;

                // Partial is only present if this level doesn't evenly divide into
                // pieces of size BF, or if a lower level didn't divide evenly.
                Node newP = null;
                if (partial != null || (levelSize & BF_MASK) != 0) {
                    final int partialBF = ((levelSize - 1) & BF_MASK) + 1;
                    newP = new Node(gen, partialBF, full);
                    if (partial != null) {
                        newP.set(partialBF - 1, partial);
                    }
                    assert(partial != null || partialBF < BF);
                }

                Node newF = null;
                if (levelSize > BF || newP == null) {
                    newF = new Node(null, BF, full);
                }

                if (levelSize <= BF) {
                    // we're done
                    if (newP == null) {
                        // top level is a full, which isn't duplicated
                        newF.gen = gen;
                        partial = newF;
                    }
                    else {
                        // Top level is a partial.  If it uses exactly one
                        // full child, then we can mark that as unshared.
                        if (newP.length() == 2 && newP.get(0) != newP.get(1)) {
                            ((Node) newP.get(0)).gen = gen;
                        }
                        partial = newP;
                    }
                    full = null;
                }
                else {
                    partial = newP;
                    full = newF;
                    assert(full != null);
                }
            } while (full != null);
        }

        _height = height;
        _size = size;
        _rootRef = new COWMgr(partial);
    }

    public SnapFixedList<E> clone() {
        final SnapFixedList<E> copy;
        try {
            copy = (SnapFixedList<E>) super.clone();
        }
        catch (final CloneNotSupportedException xx) {
            throw new Error("unexpected", xx);
        }
        // height and size are done properly by the magic Cloneable.clone()
        copy._rootRef = new COWMgr(_rootRef.frozen());
        return copy;
    }

    @Override
    public int size() {
        return _size;
    }

    @SuppressWarnings("unchecked")
    @Override
    public E get(final int index) {
        if (index < 0 || index >= _size) {
            throw new IndexOutOfBoundsException();
        }

        Node cur = _rootRef.read();
        for (int h = _height - 1; h >= 1; --h) {
            cur = (Node) cur.get((index >> (LOG_BF * h)) & BF_MASK);
        }
        return (E) cur.get(index & BF_MASK);
    }

    @Override
    public E set(final int index, final E newValue) {
        if (index < 0 || index >= _size) {
            throw new IndexOutOfBoundsException();
        }

        final Epoch.Ticket ticket = _rootRef.beginMutation();
        try {
            return setImpl(_rootRef.mutable(), index, newValue);
        }
        finally {
            ticket.leave(0);
        }
    }

    @SuppressWarnings("unchecked")
    private E setImpl(final Node root, final int index, final E newValue) {
        return (E) mutableLeaf(root, index).getAndSet(index & BF_MASK, newValue);
    }

    private Node mutableLeaf(final Node root, final int index) {
        final Generation gen = root.gen;
        Node cur = root;
        for (int h = _height - 1; h >= 1; --h) {
            final int i = (index >> (LOG_BF * h)) & BF_MASK;
            final Node child = (Node) cur.get(i);
            if (child.gen == gen) {
                // easy case
                cur = child;
                continue;
            }

            final Node repl = new Node(gen, child);

            // reread before CAS
            Node newChild = (Node) cur.get(i);
            if (newChild == child) {
                cur.compareAndSet(i, child, repl);
                newChild = (Node) cur.get(i);
            }
            assert(newChild.gen == gen);
            cur = newChild;
        }
        return cur;
    }
}
