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
 *  Users should implement the {@link #freezeAndClone(Object)} and
 *  {@link #cloneFrozen(Object)} methods.
 */
abstract public class CopyOnWriteManager<E> implements Cloneable {
    
    /** This is basically a stripped-down CountDownLatch.  Implementing our own
     *  reduces the object count by one, and it gives us access to the
     *  uninterruptable acquireShared.
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

        /** The computed size of <code>value</code>, as of the beginning of
         *  this epoch.
         */
        int initialSize;

        /** True if this epoch is being closed to generate a frozen value. */
        boolean closeShouldFreeze;

        /** A frozen E equal to <code>value</code>, if not <code>dirty</code>. */
        private volatile E _frozenValue;

        /** True if any mutations have been performed on <code>value</code>. */
        volatile boolean dirty;

        /** The epoch that will follow this one.  All of the fields added by
         *  <code>Root</code> may be uninitialized.
         */
        Root queued;

        /** A latch that will be triggered when this epoch has completed its
         *  shutdown and installed <code>queued</code> as the active epoch.
         */
        private Latch _closed;

        private Root() {
        }

        public Root(final E value, final int initialSize) {
            this.value = value;
            this.initialSize = initialSize;
            this.queued = new Root();
            this._closed = new Latch();
            // no frozenValue, so we are considered dirty
            this.dirty = true;
        }

        @Override
        public EpochNode attemptArrive() {
            final EpochNode ticket = super.attemptArrive();
            if (ticket != null && !dirty) {
                dirty = true;
                _frozenValue = null;
            }
            return ticket;
        }

        private void setFrozenValue(final E v) {
            if (!dirty) {
                _frozenValue = v;
                if (dirty) {
                    _frozenValue = null;
                }
            }
        }

        private E getFrozenValue() {
            final E v = _frozenValue;
            return dirty ? null : v;
        }

        protected void onClosed(final int dataSum) {
            assert(dataSum == 0 || dirty);

            queued.queued = new Root();
            if (closeShouldFreeze) {
                queued.value = freezeAndClone(value);
                queued.setFrozenValue(value);
            }
            else {
                queued.value = value;

                // Since we're not actually copying, any mutations in this
                // epoch dirty the next one.
                if (dirty) {
                    queued.dirty = true;
                }
                else {
                    queued.setFrozenValue(_frozenValue);
                }
            }
            queued.initialSize = initialSize + dataSum;
            queued._closed = new Latch();

            assert(!dirty || _frozenValue == null);

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

    /** Returns a clone of a frozen E. */
    abstract protected E cloneFrozen(E frozenValue);

    public CopyOnWriteManager<E> clone() {
        final CopyOnWriteManager<E> copy;
        try {
            copy = (CopyOnWriteManager<E>) super.clone();
        }
        catch (final CloneNotSupportedException xx) {
            throw new Error("unexpected", xx);
        }

        final Root a = _active;
        E f = a.getFrozenValue();
        if (f == null) {
            a.closeShouldFreeze = true;
            a.beginClose();
            a.awaitClosed();
            f = a.value;
        }

        copy._active = new Root(cloneFrozen(f), a.initialSize);
        return copy;
    }

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
        final E f = a.getFrozenValue();
        if (f != null) {
            return f;
        }
        else {
            a.closeShouldFreeze = true;
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
            a.closeShouldFreeze = true;
            a.beginClose();
            a.awaitClosed();
            return a.queued.initialSize;
        }
    }
}
