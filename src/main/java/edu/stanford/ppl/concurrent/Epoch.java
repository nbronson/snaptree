/* CCSTM - (c) 2009 Stanford University - PPL */

// Epoch
package edu.stanford.ppl.concurrent;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/** A <code>Epoch</code> has a lifecycle consisting of three phases: active,
 *  closing, and closed.  During the active phase partipants may arrive and
 *  leave the epoch.  Once a close has been requested, new participants are not
 *  allowed, only leaving is possible.  Once close has been requested and all
 *  participants have left, the epoch is transitioned to the closed state.
 *  <p>
 *  Entry is performed with {@link #attemptArrive}, which returns a non-null
 *  ticket on success or null if {@link #beginClose} has already been called.
 *  Each successful call to <code>attemptArrive</code> must be paired by a call
 *  to {@link Ticket#leave} on the returned ticket.
 *  <p>
 *  The abstract method {@link #onClosed} will be invoked exactly once after
 *  the epoch becomes closed.  It will be passed the sum of the values passed
 *  to {@link Ticket#leave}.  There is no way to query the current participant
 *  count or state of the epoch without changing it.
 *  <p>
 *  Internally the epoch responds to contention by increasing its size,
 *  striping the participant count across multiple objects (and hopefully
 *  multiple cache lines).  Once close has begun, the epoch converts itself to
 *  a single-shot hierarchical barrier, that also performs a hierarchical
 *  reduction of the leave parameters.
 */
abstract public class Epoch {

    /** Represents a single successful arrival to an {@link Epoch}. */ 
    public interface Ticket {
        /** Informs the epoch that returned this ticket that the participant
         *  has left.  This method should be called exactly once per ticket.
         *  The sum of the <code>data</code> values for all tickets will be
         *  computed and passed to {@link Epoch#onClosed}. 
         */
        void leave(int data);
    }

    private final Root _root = new Root();

    /** Returns a {@link Ticket} indicating a successful arrival, if no call to
     *  {@link #beginClose} has been made for this epoch, or returns null if
     *  close has already begun.  {@link Ticket#leave} must be called exactly
     *  once on any returned ticket.
     */
    public Ticket attemptArrive() {
        return _root.attemptArrive();
    }

    /** Prevents new arrivals from succeeding, then returns immediately.
     *  {@link #onClosed} will be called after all outstanding tickets have
     *  been returned.  To block until close is complete, add some sort of
     *  synchronization logic to the user-defined implementation of {@link
     *  #onClosed}.
     */
    public void beginClose() {
        _root.beginClose();
    }

    /** Override this method to provide user-defined behavior.
     *  <code>dataSum</code> will be the sum of the <code>data</code> values
     *  passed to {@link Ticket#leave} for all tickets in this epoch.
     *  <p>
     *  As a simple example, a blocking close operation may be defined by:<pre>
     *    class BlockingEpoch extends Epoch {
     *        private final CountDownLatch _closed = new CountDownLatch(1);
     *
     *        public void blockingClose() throws InterruptedException {
     *            beginClose();
     *            _closed.await();
     *        }
     *
     *        protected void onClosed(int dataSum) {
     *            _closed.countDown(1);
     *        }
     *    }
     *  </pre>
     */
    abstract protected void onClosed(int dataSum);

    //////////////// internal implementation

    private static class Child extends Node {
        private Child(final Node parent, final int whichInParent) {
            super(parent, whichInParent);
        }

        protected void onClosed(final int dataSum) {
            throw new Error();
        }
    }

    private class Root extends Node {
        protected void onClosed(final int dataSum) {
            Epoch.this.onClosed(dataSum);
        }
    }

    private abstract static class Node extends AtomicLong implements Ticket {
    
        private static final int TRIES_BEFORE_SUBTREE = 3;
        private static final int CLOSER_HEAD_START = 1000;

        /** This includes the root.  3 or fewer procs gets 2, 15 or fewer gets
         *  3, 63 or fewer 4, 255 or fewer 5, ...
         *  TODO: evaluate the best choice here
         */ 
        private static final int MAX_LEVELS = 2 + log4(Runtime.getRuntime().availableProcessors());

        /** Returns floor(log_base_4(value)). */
        private static int log4(final int value) {
            return (31 - Integer.numberOfLeadingZeros(value)) / 2;
        }

        //////////////// branching factor

        private static final int LOG_BF = 2;
        private static final int BF = 1 << LOG_BF;
        private static final int BF_MASK = BF - 1;

        //////////////// bit packing

        private static final int ENTRY_COUNT_BITS = 20;
        private static final int ENTRY_COUNT_MASK = (1 << ENTRY_COUNT_BITS) - 1;

        private static int entryCount(long state) { return ((int) state) & ENTRY_COUNT_MASK; }
        private static boolean willOverflow(long state) { return entryCount(state) == ENTRY_COUNT_MASK; }

        private static final int DATA_SUM_SHIFT = 32;
        private static int dataSum(long state) { return (int)(state >> DATA_SUM_SHIFT); }
        private static long withDataDelta(long state, int delta) { return state + (((long) delta) << DATA_SUM_SHIFT); }

        private static final long CLOSING = (1L << 21);
        private static boolean isClosing(long state) { return (state & CLOSING) != 0L; }
        private static long withClosing(long state) { return state | CLOSING; }

        private static final long CLOSED = (1L << 22);
        private static boolean isClosed(long state) { return (state & CLOSED) != 0L; }
        private static long withClosed(long state) { return state | CLOSED; }

        private static final long EMPTY = (1L << 23);
        private static boolean isEmpty(long state) { return (state & EMPTY) != 0L; }
        private static long withEmpty(long state) { return state | EMPTY; }

        private static final int CHILD_PRESENT_SHIFT = 24;
        private static final long ANY_CHILD_PRESENT = ((long) BF_MASK) << CHILD_PRESENT_SHIFT;
        private static long childPresentBit(int which) { return 1L << (CHILD_PRESENT_SHIFT + which); }
        private static boolean isAnyChildPresent(long state) { return (state & ANY_CHILD_PRESENT) != 0; }
        private static boolean isChildPresent(long state, int which) { return (state & childPresentBit(which)) != 0; }
        private static long withChildPresent(long state, int which) { return state | childPresentBit(which); }

        private static final int CHILD_EMPTY_SHIFT = 28;
        private static long ANY_CHILD_EMPTY = ((long) BF_MASK) << CHILD_EMPTY_SHIFT;
        private static long childEmptyBit(int which) { return 1L << (CHILD_EMPTY_SHIFT + which); }
        private static boolean isChildEmpty(long state, int which) { return (state & childEmptyBit(which)) != 0; }
        private static long withChildEmpty(long state, int which, long childState) {
            assert(!isChildEmpty(state, which));
            return withDataDelta(state | childEmptyBit(which), dataSum(childState));
        }
        private static long withAllChildrenEmpty(long state) { return state | ANY_CHILD_EMPTY; }

        private static final long READY_FOR_EMPTY_MASK = ANY_CHILD_EMPTY | CLOSED | ENTRY_COUNT_MASK;
        private static final long READY_FOR_EMPTY_EXPECTED = CLOSED;
        private static boolean readyForEmpty(long state) { return (state & READY_FOR_EMPTY_MASK) == READY_FOR_EMPTY_EXPECTED; }
        private static long recomputeEmpty(long state) { return readyForEmpty(state) ? withEmpty(state) : state; }

        private static final long ENTRY_FAST_PATH_MASK = ANY_CHILD_EMPTY | CLOSING | (1L << (ENTRY_COUNT_BITS - 1));

        /** Not closed, no children, and no overflow possible. */
        private static boolean isEntryFastPath(long state) { return (state & ENTRY_FAST_PATH_MASK) == 0L; }

        //////////////// instance state

        private static final AtomicReferenceFieldUpdater[] childrenUpdaters = {
            AtomicReferenceFieldUpdater.newUpdater(Node.class, Node.class, "child0"),
            AtomicReferenceFieldUpdater.newUpdater(Node.class, Node.class, "child1"),
            AtomicReferenceFieldUpdater.newUpdater(Node.class, Node.class, "child2"),
            AtomicReferenceFieldUpdater.newUpdater(Node.class, Node.class, "child3")
        };

        private final Node _parent;
        private final int _whichInParent;

        // It would be cleaner to use an array of children, but we want to force
        // all of the bulk into the same object as the AtomicLong.value.

        // To avoid races between creating a child and marking a node as closed,
        // we add a bit to the state for each child that records whether it
        // *should* exist.  If we find that the bit is set but a child is missing,
        // we can create it ourself.

        private volatile Node _child0;
        private volatile Node _child1;
        private volatile Node _child2;
        private volatile Node _child3;

        Node() {
            _parent = null;
            _whichInParent = 0;
        }

        Node(final Node parent, final int whichInParent) {
            _parent = parent;
            _whichInParent = whichInParent;
        }

        //////////////// provided by the caller

        abstract protected void onClosed(int dataSum);

        //////////////// child management

        private Node getChildFromField(final int which) {
            switch (which) {
                case 0: return _child0;
                case 1: return _child1;
                case 2: return _child2;
                default: return _child3;
            }
        }

        private Node getChild(final long state, final int which) {
            if (!isChildPresent(state, which)) {
                return null;
            }
            final Node existing = getChildFromField(which);
            return (existing != null) ? existing : createChild(which);
        }

        @SuppressWarnings("unchecked")
        private Node createChild(final int which) {
            final Node n = new Child(this, which);
            return childrenUpdaters[which].compareAndSet(this, null, n) ? n : getChildFromField(which);
        }

        private Node getOrCreateChild(final int which) {
            while (true) {
                final long state = get();
                final Node existing = getChild(state, which);
                if (existing != null || isClosing(state)) {
                    // either we're happy with what we've got, or we're stuck with it
                    return existing;
                }
                if (compareAndSet(state, withChildPresent(state, which))) {
                    // the child now should exist, but we must still actually
                    // construct and link in the instance
                    return createChild(which);
                }
            }
        }

        /** Returns the <code>Node</code> to decr on success, null if
         *  {@link #beginClose} has already been called on this instance.
         */
        public Node attemptArrive() {
            final long state = get();
            if (isEntryFastPath(state) && compareAndSet(state, state + 1)) {
                return this;
            }
            else {
                return attemptArrive(0, 1);
            }
        }

        private int getIdentity() {
            final int h = System.identityHashCode(Thread.currentThread());

            // Multiply by -127, as suggested by java.util.IdentityHashMap.
            // We also set an bit we don't use, to make sure it is never zero.
            return (h - (h << 7)) | (1 << 31);
        }

        /** level 1 is the root. */
        private Node attemptArrive(int id, final int level) {
            int tries = 0;
            while (true) {
                final long state = get();
                if (isClosing(state)) {
                    return null;
                }
                if (isAnyChildPresent(state) ||
                        (tries >= TRIES_BEFORE_SUBTREE && level < MAX_LEVELS)) {
                    // Go deeper if we have previously detected contention, or if
                    // we are currently detecting it.  Lazy computation of our
                    // current identity.
                    if (id == 0) {
                        id = getIdentity();
                    }
                    final Node child = getOrCreateChild(id & BF_MASK);
                    if (child == null) {
                        return null;
                    }
                    return child.attemptArrive(id >> LOG_BF, level + 1);
                }
                if (willOverflow(state)) {
                    throw new IllegalStateException("maximum ref count of " + ENTRY_COUNT_MASK + " exceeded");
                }
                if (compareAndSet(state, state + 1)) {
                    // success
                    return this;
                }

                ++tries;
            }
        }

        /** Should be called on every non-null return value from attemptArrive. */
        public void leave(final int dataDelta) {
            while (true) {
                final long state = get();
                if (entryCount(state) == 0) {
                    throw new IllegalStateException("incorrect call to Node.decr()");
                }
                final long after = recomputeEmpty(withDataDelta(state - 1, dataDelta));
                if (compareAndSet(state, after)) {
                    if (isEmpty(after)) {
                        newlyEmpty(dataSum(state));
                    }
                    return;
                }
            }
        }

        private void newlyEmpty(final long state) {
            if (_parent != null) {
                // propogate
                _parent.childIsNowEmpty(_whichInParent, state);
            }
            else {
                // report
                onClosed(dataSum(state));
            }
        }

        private void childIsNowEmpty(final int which, final long childState) {
            while (true) {
                final long state = get();
                if (isChildEmpty(state, which)) {
                    // not our problem
                    return;
                }
                final long after = recomputeEmpty(withChildEmpty(state, which, childState));
                if (compareAndSet(state, after)) {
                    if (isEmpty(after)) {
                        newlyEmpty(after);
                    }
                    return;
                }
            }
        }

        /** Prevents subsequent calls to {@link #attemptArrive} from succeeding. */
        public void beginClose() {
            int attempts = 0;
            long state;
            while (true) {
                ++attempts;

                state = get();
                if (isClosed(state)) {
                    return;
                }

                if (isClosing(state)) {
                    // give the thread that actually performed this transition a
                    // bit of a head start
                    if (attempts < CLOSER_HEAD_START) {
                        continue;
                    }
                    break;
                }

                if (!isAnyChildPresent(state)) {
                    if (entryCount(state) == 0) {
                        // we can transition directly to the empty state
                        final long after = withEmpty(withClosed(withAllChildrenEmpty(withClosing(state))));
                        if (compareAndSet(state, after)) {
                            if (_parent == null) {
                                onClosed(dataSum(after));
                            }
                            return;
                        }
                    }
                    else {
                        // we can transition directly to the closed state
                        final long after = withClosed(withAllChildrenEmpty(withClosing(state)));
                        if (compareAndSet(state, after)) {
                            return;
                        }
                    }
                }

                // closing is needed while we go mark the children
                if (compareAndSet(state, withClosing(state))) {
                    break;
                }
            }

            // no new child bits can be set after closing, so this will be
            // exhaustive
            for (int which = 0; which < BF; ++which) {
                final Node child = getChild(state, which);
                if (child != null) {
                    child.beginClose();
                }
            }

            while (true) {
                final long before = get();
                if (isClosed(before)) {
                    return;
                }

                // We know all of the children are closed because we did it
                // ourself.  Check each one for empty, and if so set the
                // corresponding bit in our state while we merge in the dataSum.
                long after = withClosed(before);
                for (int which = 0; which < BF; ++which) {
                    if (!isChildEmpty(before, which)) {
                        if (isChildPresent(before, which)) {
                            final long childState = getChildFromField(which).get();
                            if (isEmpty(childState)) {
                                after = withChildEmpty(after, which, childState);
                            }
                        }
                        else {
                            // non-existent children are empty
                            after = withChildEmpty(after, which, 0L);
                        }
                    }
                }
                after = recomputeEmpty(after);
                
                if (compareAndSet(before, after)) {
                    if (_parent == null && isEmpty(after)) {
                        onClosed(dataSum(after));
                    }
                    return;
                }
            }
        }
    }
}
