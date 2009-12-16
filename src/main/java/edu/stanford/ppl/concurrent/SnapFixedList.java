/* CCSTM - (c) 2009 Stanford University - PPL */

// SnapFixedList
package edu.stanford.ppl.concurrent;

import sun.misc.Unsafe;

import java.util.AbstractList;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;

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
    // BF.  The leaves of the tree are the elements E.  To avoid extra
    // indirection, branches are represented using an Object[BF+1], where the
    // zero-th element indicates the current Epoch.  Only the elements of the
    // bottom-most nodes change during their lifetime, so volatile loads and
    // stores are performed only there (using sun.misc.Unsafe).

    private static final Unsafe unsafe = Unsafe.getUnsafe();
    private static final int base = unsafe.arrayBaseOffset(Object[].class);
    private static final int scale = unsafe.arrayIndexScale(Object[].class);

    /** 0 if _size == 0, otherwise the smallest positive int such that
     *  (1L << (LOG_BF * _height)) >= _size.
     */
    private final int _height;

    private final int _size;

    private final AtomicReference<Object[]> _rootRef;

    public SnapFixedList(final int size) {
        this(size, null);
    }

    public SnapFixedList(final int size, final E element) {
        int height = 0;
        Object[] partial = null;

        if (size > 0) {
            // We will insert the epoch into all of the partials (since they
            // are used exactly once).  We reuse the fulls, so we will give
            // them a null Epoch that will cause them to be copied before any
            // actual writes.
            final Epoch epoch = new Epoch();

            Object full = element;

            do {
                ++height;

                // this is the number of nodes required at this level
                int levelSize = ((size - 1) >> (LOG_BF * (height - 1))) + 1;

                // partial is only present if this level doesn't evenly divide into
                // pieces of size BF, or if a lower level didn't divide evenly
                Object[] newP = null;
                if (partial != null || (levelSize & BF_MASK) != 0) {
                    newP = new Object[1 + ((levelSize - 1) & BF_MASK) + 1];
                    newP[0] = epoch;
                    Arrays.fill(newP, 1, newP.length, full);
                    if (partial != null) {
                        newP[newP.length - 1] = partial;
                    }
                    assert(partial != null || newP.length != 1 + BF);
                }

                Object[] newF = null;
                if (levelSize > BF || newP == null) {
                    newF = new Object[1 + BF];
                    Arrays.fill(newF, 1, newF.length, full);
                }

                if (levelSize <= BF) {
                    // we're done
                    if (newP == null) {
                        // top level is a full, which isn't duplicated
                        newF[0] = epoch;
                        partial = newF;
                    }
                    else {
                        // Top level is a partial.  If it uses exactly one
                        // full, then we can mark it as unshared.
                        if (newP.length == 3 && newP[1] != newP[2]) {
                            ((Object[]) newP[1])[0] = epoch;
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
        _rootRef = new AtomicReference<Object[]>(partial);
    }

    @Override
    public int size() {
        return _size;
    }

    private long rawOffset(final int index) {
        return base + index * scale;
    }

    @Override
    public E get(final int index) {
        if (index < 0 || index >= _size) {
            throw new IndexOutOfBoundsException();
        }

        Object[] cur = _rootRef.get();
        for (int h = _height - 1; h >= 1; --h) {
            cur = (Object[]) cur[1 + ((index >> (LOG_BF * h)) & BF_MASK)];
        }
        return (E) unsafe.getObjectVolatile(cur, rawOffset(1 + (index & BF_MASK)));
    }
}
