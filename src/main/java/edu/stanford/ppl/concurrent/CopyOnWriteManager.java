/* SnapTree - (c) 2009 Stanford University - PPL */

// CopyOnWriteManager
package edu.stanford.ppl.concurrent;

import java.util.concurrent.locks.AbstractQueuedSynchronizer;

/** Manages copy-on-write behavior for a concurrent tree structure.  It is
 *  assumed that the managed structure allows concurrent mutation, but that no
 *  mutating operations may be active when a copy-on-write snapshot of tree is
 *  taken.  Because it is difficult to update the size of data structure in a
 *  highly concurrent fashion, the <code>CopyOnWriteManager</code> also manages
 *  a running total that represents the size of the contained tree structure.
 *  <p>
 *  Users should implement the {@link #freezeAndClone(Object)} method.
 */
abstract public class CopyOnWriteManager<E> {
    /** This is basically a stripped-down CountDownLatch.  Implementing our own
     *  reduces the object count by one, and it gives us access to the
     *  uninterruptable acquires.
     */
    private class Latch extends AbstractQueuedSynchronizer {
        Latch() {
            setState(1);
        }

        public int tryAcquireShared(final int acquires) {
            // 1 = success, and followers may also succeed
            // -1 = failure
            return getState() == 0 ? 1 : -1;
        }

        public boolean tryReleaseShared(final int releases) {
            // Before, state is either 0 or 1.  After, state is always 0.
            return compareAndSetState(1, 0);
        }
    }

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
        private Latch _closed;

        private Root(final Root cleanPrev) {
            this.cleanPrev = cleanPrev;
        }

        public Root(final E value, final int initialSize) {
            this.value = value;
            this.initialSize = initialSize;
            this.queued = new Root(this);
            this._closed = new Latch();
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
                queued.value = freezeAndClone(value);
                queued.prevIsFrozen = true;
            }
            else {
                // must just be a size() request
                queued.value = value;
            }
            queued.initialSize = initialSize + dataSum;
            queued._closed = new Latch();
            cleanPrev = null;
            _active = queued;
            _closed.releaseShared(1);
        }

        public void awaitClosed() {
            _closed.acquireShared(1);
        }
    }

    private volatile Root _active;

    /** Creates a new {@link CopyOnWriteManager} holding
     *  <code>initialValue</code>, with an assumed size of
     *  <code>initialSize</code>.
     */
    public CopyOnWriteManager(final E initialValue, final int initialSize) {
        _active = new Root(initialValue, initialSize);
    }

    /** The implementing method must mark <code>value</code> as shared, and
     *  return a new object to use in its place.  Hopefully, the majority of
     *  the work of the clone can be deferred by copy-on-write. 
     */
    abstract protected E freezeAndClone(final E value);

    /** Returns a reference to the tree structure suitable for a read
     *  operation.  The returned structure may be mutated by operations that
     *  have the permission of this {@link CopyOnWriteManager}, but they will
     *  not observe changes managed by other instances.
     */
    public E read() {
        return _active.value;
    }

    /** Obtains permission to mutate the copy-on-write value held by this
     *  instance, perhaps blocking while a concurrent snapshot is being
     *  performed.  {@link Epoch.Ticket#leave} must be called exactly once on
     *  the object returned from this method, after the mutation has been
     *  completed.  The change in size reflected by the mutation should be
     *  passed as the parameter to <code>leave</code>.
     */
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

    /** Returns a reference to the tree structure suitable for a mutating
     *  operation.  This method may only be called under the protection of a
     *  ticket returned from {@link #beginMutation}.
     */
    public E mutable() {
        return _active.value;
    }

    /** Returns a reference to a snapshot of this instance's tree structure
     *  that may be read, but not written.  This is accomplished by suspending
     *  mutation, replacing the mutable root of this manager with the result of
     *  <code>freezeAndClone(root, false)</code>, and then returning a
     *  reference to the old root.  Successive calls to this method may return
     *  the same instance.
     */
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

    /** Returns true if the computed {@link #size} is zero. */
    public boolean isEmpty() {
        // for a different internal implementation (such as a C-SNZI) we might
        // be able to do better than this
        return size() == 0;
    }

    /** Returns the sum of the <code>initialSize</code> parameter passed to the
     *  constructor, and the size deltas passed to {@link Epoch.Ticket#leave}
     *  for all of the mutation tickets.  The result returned is linearizable
     *  with mutations, which requires mutation to be quiesced.  No tree freeze
     *  is required, however.
     */
    public int size() {
        final Root a = _active;
        final Integer delta = a.attemptDataSum();
        if (delta != null) {
            return a.initialSize + delta;
        }
        else {
            // no use in checking cleanPrev, because we would have been able
            // to read the data sum without closing in that case
            a.closeShouldClone = true;
            a.beginClose();
            a.awaitClosed();
            // if _active is newer than a.queued, then we just linearize at
            // the close of _active's predecessor
            return _active.initialSize;
        }
    }
}
