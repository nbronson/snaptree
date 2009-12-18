/* SnapTree - (c) 2009 Stanford University - PPL */

// EpochNode
package edu.stanford.ppl.concurrent;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/** Provides an implementation of the behavior of an {@link Epoch}. */
abstract class EpochNode extends AtomicLong implements Epoch.Ticket {

    private static final int TRIES_BEFORE_SUBTREE = 2;
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

    //////////////// subclasses

    private static class Child extends EpochNode {
        private Child(final EpochNode parent, final int whichInParent) {
            super(parent, whichInParent);
        }

        protected void onClosed(final int dataSum) {
            throw new Error();
        }
    }

    //////////////// instance state

    private static final AtomicReferenceFieldUpdater[] childrenUpdaters = {
        AtomicReferenceFieldUpdater.newUpdater(EpochNode.class, EpochNode.class, "_child0"),
        AtomicReferenceFieldUpdater.newUpdater(EpochNode.class, EpochNode.class, "_child1"),
        AtomicReferenceFieldUpdater.newUpdater(EpochNode.class, EpochNode.class, "_child2"),
        AtomicReferenceFieldUpdater.newUpdater(EpochNode.class, EpochNode.class, "_child3")
    };

    private final EpochNode _parent;
    private final int _whichInParent;

    // It would be cleaner to use an array of children, but we want to force
    // all of the bulk into the same object as the AtomicLong.value.

    // To avoid races between creating a child and marking a node as closed,
    // we add a bit to the state for each child that records whether it
    // *should* exist.  If we find that the bit is set but a child is missing,
    // we can create it ourself.

    private volatile EpochNode _child0;
    private volatile EpochNode _child1;
    private volatile EpochNode _child2;
    private volatile EpochNode _child3;

    EpochNode() {
        _parent = null;
        _whichInParent = 0;
    }

    private EpochNode(final EpochNode parent, final int whichInParent) {
        _parent = parent;
        _whichInParent = whichInParent;
    }

    //////////////// provided by the caller

    abstract protected void onClosed(int dataSum);

    //////////////// child management

    private EpochNode getChildFromField(final int which) {
        switch (which) {
            case 0: return _child0;
            case 1: return _child1;
            case 2: return _child2;
            default: return _child3;
        }
    }

    private EpochNode getChild(final long state, final int which) {
        if (!isChildPresent(state, which)) {
            return null;
        }
        final EpochNode existing = getChildFromField(which);
        return (existing != null) ? existing : createChild(which);
    }

    @SuppressWarnings("unchecked")
    private EpochNode createChild(final int which) {
        final EpochNode n = new Child(this, which);
        return childrenUpdaters[which].compareAndSet(this, null, n) ? n : getChildFromField(which);
    }

    private EpochNode getOrCreateChild(final int which) {
        while (true) {
            final long state = get();
            final EpochNode existing = getChild(state, which);
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
    public EpochNode attemptArrive() {
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
    private EpochNode attemptArrive(int id, final int level) {
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
                final EpochNode child = getOrCreateChild(id & BF_MASK);
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
            final EpochNode child = getChild(state, which);
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

    /** For debugging purposes. */
    int computeSpread() {
        final long state = get();
        if (isAnyChildPresent(state)) {
            int sum = 0;
            for (int which = 0; which < BF; ++which) {
                final EpochNode child = getChild(state, which);
                if (child != null) {
                    sum += child.computeSpread();
                }
                else {
                    // child would be created for arrive, so count it
                    sum += 1;
                }
            }
            return sum;
        }
        else {
            return 1;
        }
    }
}
