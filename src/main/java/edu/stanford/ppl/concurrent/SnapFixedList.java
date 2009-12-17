/* CCSTM - (c) 2009 Stanford University - PPL */

// SnapFixedList
package edu.stanford.ppl.concurrent;

import sun.misc.Unsafe;

import java.util.AbstractList;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
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
    // BF.  The leaves of the tree are the elements E.

    private static class Epoch extends ClosableRefCount {
        Epoch queued;
        private final CountDownLatch _closed = new CountDownLatch(1);

        Epoch() {
        }

        Epoch(final Epoch queued) {
            this.queued = queued;
        }

        protected void onClose() {
            queued.queued = new Epoch();
            _closed.countDown();
        }

        public void awaitClosed() {
            boolean interrupted = false;
            while (true) {
                try {
                    _closed.await();
                    break;
                }
                catch (final InterruptedException xx) {
                    interrupted = true;
                }
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static class Node extends AtomicReferenceArray<Object> {
        /** This is not volatile, because it is not changed after the initial
         *  publication of the node.
         */
        Epoch epoch;

        Node(Epoch epoch, int size, Object initialValue) {
            super(size);
            this.epoch = epoch;
            if (initialValue != null) {
                for (int i = 0; i < size; ++i) {
                    lazySet(i, initialValue);
                }
            }
        }

        Node(Epoch epoch, Node src) {
            super(src.length());
            this.epoch = epoch;
            for (int i = 0; i < src.length(); ++i) {
                lazySet(i, src.get(i));
            }
        }
    }

    /** 0 if _size == 0, otherwise the smallest positive int such that
     *  (1L << (LOG_BF * _height)) >= _size.
     */
    private final int _height;

    private final int _size;

    private final AtomicReference<Node> _rootRef;

    public SnapFixedList(final int size) {
        this(size, null);
    }

    public SnapFixedList(final int size, final E element) {
        int height = 0;
        Node partial = null;

        if (size > 0) {
            // We will insert the epoch into all of the partials (since they
            // are used exactly once).  We reuse the fulls, so we will give
            // them a null Epoch that will cause them to be copied before any
            // actual writes.
            final Epoch epoch = new Epoch();

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
                    newP = new Node(epoch, partialBF, full);
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
                        newF.epoch = epoch;
                        partial = newF;
                    }
                    else {
                        // Top level is a partial.  If it uses exactly one
                        // full child, then we can mark that as unshared.
                        if (newP.length() == 2 && newP.get(0) != newP.get(1)) {
                            ((Node) newP.get(0)).epoch = epoch;
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
        _rootRef = new AtomicReference<Node>(partial);
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

        Node cur = _rootRef.get();
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

        while (true) {
            final Node root = _rootRef.get();
            final ClosableRefCount t0 = root.epoch.attemptIncr();
            if (t0 != null) {
                // entered the current epoch
                try {
                    return setImpl(root, index, newValue);
                }
                finally {
                    t0.decr();
                }
            }

            final ClosableRefCount t1 = root.epoch.queued.attemptIncr();
            if (t1 != null) {
                // Entered the queued epoch.  This guarantees us a seat at the
                // next table.
                try {
                    // This epoch is either closing or closed.  Wait for the latter.
                    root.epoch.awaitClosed();

                    Node newRoot = _rootRef.get();
                    if (newRoot == root) {
                        // no one has yet installed a new root, try to do it
                        final Node repl = new Node(root.epoch.queued, root);
                        newRoot = _rootRef.get();
                        if (newRoot == root) {
                            _rootRef.compareAndSet(root, repl);
                            newRoot = _rootRef.get();
                        }
                    }

                    assert(newRoot != root && newRoot.epoch == root.epoch.queued);

                    return setImpl(newRoot, index, newValue);
                }
                finally {
                    t1.decr();
                }
            }

            // our read of root must be stale, try again
            assert(_rootRef.get() != root);
        }
    }

    @SuppressWarnings("unchecked")
    private E setImpl(final Node root, final int index, final E newValue) {
        final Epoch epoch = root.epoch;
        Node cur = root;
        for (int h = _height - 1; h >= 1; --h) {
            final int i = (index >> (LOG_BF * h)) & BF_MASK;
            final Node child = (Node) cur.get(i);
            if (child.epoch == epoch) {
                // easy case
                cur = child;
                continue;
            }

            final Node repl = new Node(epoch, child);

            // reread before CAS
            Node newChild = (Node) cur.get(i);
            if (newChild == child) {
                cur.compareAndSet(i, child, repl);
                newChild = (Node) cur.get(i);
            }
            assert(newChild.epoch == epoch);
            cur = newChild;
        }
        return (E) cur.get(index & BF_MASK);
    }
}
