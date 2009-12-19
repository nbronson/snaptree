/* SnapTree - (c) 2009 Stanford University - PPL */

// CopyOnWriteManager
package edu.stanford.ppl.concurrent;

import java.util.concurrent.CountDownLatch;

/** Manages copy-on-write behavior for a concurrent tree structure.  It is
 *  assumed that the managed structure allows concurrent mutation, but that no
 *  mutating operations may be active when a copy-on-write snapshot of tree is
 *  taken.  Because it is difficult to update the size of data structure in a
 *  highly concurrent fashion, the <code>CopyOnWriteManager</code> also manages
 *  a running total that represents the size of the contained tree.
 */
abstract public class CopyOnWriteManager<E> {
    private class Root extends EpochNode {
        /** The value used by this epoch. */
        E value;

        /** True iff value was cloned at the beginning of this epoch. */
        boolean prevIsFrozen;

        /** The computed size of <code>value</code>, as of the beginning of
         *  this epoch.
         */
        int initialSize;

        /** If true during onClosed(), the successor epoch will have prevIsFrozen == true. */
        boolean closeShouldClone;

        /** The previous epoch, unless an arrival has been attempted in this
         *  epoch.
         */
        volatile Root cleanPrev = null;

        /** The epoch that will follow this one.  All of the fields added by
         *  <code>Root</code> may be uninitialized.
         */
        Root queued;

        /** A latch that will be triggered when this epoch has completed its
         *  shutdown and installed <code>queued</code> as the active epoch.
         */
        private CountDownLatch _closed;

        private Root(final Root cleanPrev) {
            this.cleanPrev = cleanPrev;
        }

        public Root(final E value, final int initialSize) {
            this.value = value;
            this.initialSize = initialSize;
            this.queued = new Root(this);
            this._closed = new CountDownLatch(1);
        }

        @Override
        public EpochNode attemptArrive() {
            if (cleanPrev != null) {
                cleanPrev = null;
            }
            return super.attemptArrive();
        }

        protected void onClosed(final int dataSum) {
            queued.queued = new Root(queued);
            if (closeShouldClone) {
                queued.value = freezeAndClone(value, false);
                queued.prevIsFrozen = true;
            }
            else {
                // must just be a size() request
                queued.value = value;
            }
            queued.initialSize = initialSize + dataSum;
            queued._closed = new CountDownLatch(1);
            cleanPrev = null;
            _active = queued;
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

    private volatile Root _active;

    public CopyOnWriteManager(final E initialValue, final int initialSize) {
        _active = new Root(initialValue, initialSize);
    }

    abstract protected E freezeAndClone(final E value, final boolean alreadyFrozen);

    public Epoch.Ticket beginMutation() {
        while (true) {
            final Root a = _active;
            final Epoch.Ticket t0 = a.attemptArrive();
            if (t0 != null) {
                // success
                return t0;
            }

            final Epoch.Ticket t1 = a.queued.attemptArrive();
            if (t1 != null) {
                // we are guaranteed a seat at the table *next* epoch
                a.awaitClosed();
                return t1;
            }

            // our read of _active must be stale, try again
            assert(_active != a);
        }        
    }

    public E read() {
        return _active.value;
    }

    public E mutable() {
        return _active.value;
    }

    public E frozen() {
        final Root a = _active;
        final Root cp = a.cleanPrev;
        if (cp != null && a.prevIsFrozen) {
            return cp.value;
        }
        else {
            a.closeShouldClone = true;
            a.beginClose();
            a.awaitClosed();
            return a.value;
        }
    }

    public E cloned() {
        return freezeAndClone(frozen(), true);
    }

    public boolean isEmpty() {
        // for a different internal implementation (such as a C-SNZI) we might
        // be able to do better than this
        return size() == 0;
    }

    public int size() {
        final Root a = _active;
        final Integer delta = a.attemptDataSum();
        if (delta != null) {
            return a.initialSize + delta;
        }
        else {
            // no use in checking cleanPrev, because we would have been able
            // to read the data sum without closing in that case
            a.beginClose();
            a.awaitClosed();
            // if _active is newer than a.queued, then we just linearize at
            // the close of _active's predecessor
            return _active.initialSize;
        }
    }
}
