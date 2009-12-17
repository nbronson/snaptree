/* SnapTree - (c) 2009 Stanford University - PPL */

// TreeEpoch
package edu.stanford.ppl.concurrent;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;


/** A <code>ClosableRefCount</code> has three phases of its lifecycle: active,
 *  closing, and closed.  During the active phase the count may be incremented
 *  and decremented.  Transition to the closing phase is initiated by a call to
 *  {@link #beginClose}.  In this phase only decrements are allowed, calls to
 *  {@link #attemptIncr} will report failure.  Once every increment has been
 *  matched with a decrement, the state will be transitioned to closed.  If
 *  {@link #beginClose} is called while the reference count is zero, then the
 *  state will immediately transition to closed.
 *  <p>
 *  There are no provisions for reading the current reference count or the
 *  current state.  The method {@link #onClosed} will be invoked on exactly one
 *  thread after the transition to closed is performed.  The thread on which it
 *  is invoked may be the last to execute {@link #decr}, or it may be a thread
 *  that is executing {@link #beginClose}.
 *  <p>
 *  <code>AtomicLong</code> is subclassed directly to control object layout,
 *  please don't call its methods directly.  (By subclassing directly, the
 *  value which must be CAS-ed is part of an object that occupies at least a
 *  large portion of a cache line.)
 */
abstract class ClosableRefCount extends AtomicLong {

    private static class Child extends ClosableRefCount {
        private Child(final ClosableRefCount parent, final int whichInParent) {
            super(parent, whichInParent);
        }

        protected void onClose() {
            throw new Error();
        }
    }

    // TODO: enforce a maximum depth

    private static int TRIES_BEFORE_SUBTREE = 3;
    private static int CLOSER_HEAD_START = 1000;

//    private static long CLOSING_MASK = (1L << 32);
//    private static long CLOSED_MASK = (1L << 33);
//    private static long EMPTY_MASK = (1L << 34);
//
//    private static long childPresentMask(int which) { return (1L << (48 + which)); }
//    private static long ANY_CHILD_PRESENT_MASK = (0xfL << 48);
//    private static long childClosedAndEmptyMask(int which) { return (1L << (56 + which)); }
//    private static long PRE_EMPTY_MASK = CLOSING_MASK | CLOSED_MASK | (0xfL << 56) | 0xffffffffL;
//    private static long PRE_EMPTY_VALUE = CLOSING_MASK | CLOSED_MASK | (0xfL << 56);

    //////////////// branching factor

    private static int LOG_BF = 2;
    private static int BF = 1 << LOG_BF;
    private static int BF_MASK = BF - 1;

    //////////////// bit packing

    private static int ENTRY_COUNT_BITS = 20;
    private static int ENTRY_COUNT_MASK = (1 << ENTRY_COUNT_BITS) - 1;

    private static int entryCount(long state) { return ((int) state) & ENTRY_COUNT_MASK; }
    private static boolean willOverflow(long state) { return entryCount(state) == ENTRY_COUNT_MASK; }

    private static long CLOSING = (1L << 21);
    private static boolean isClosing(long state) { return (state & CLOSING) != 0L; }
    private static long withClosing(long state) { return state | CLOSING; }

    private static long CLOSED = (1L << 22);
    private static boolean isClosed(long state) { return (state & CLOSED) != 0L; }
    private static long withClosed(long state) { return state | CLOSED; }

    private static long EMPTY = (1L << 23);
    private static boolean isEmpty(long state) { return (state & EMPTY) != 0L; }
    private static long withEmpty(long state) { return state | EMPTY; }

    private static int CHILD_PRESENT_SHIFT = 24;
    private static long ANY_CHILD_PRESENT = ((long) BF_MASK) << CHILD_PRESENT_SHIFT;
    private static long childPresentBit(int which) { return 1L << (CHILD_PRESENT_SHIFT + which); }
    private static boolean isAnyChildPresent(long state) { return (state & ANY_CHILD_PRESENT) != 0; }
    private static boolean isChildPresent(long state, int which) { return (state & childPresentBit(which)) != 0; }
    private static long withChildPresent(long state, int which) { return state | childPresentBit(which); }

    private static int CHILD_EMPTY_SHIFT = 28;
    private static long ANY_CHILD_EMPTY = ((long) BF_MASK) << CHILD_EMPTY_SHIFT;
    private static long childEmptyBit(int which) { return 1L << (CHILD_EMPTY_SHIFT + which); }
    private static boolean isChildEmpty(long state, int which) { return (state & childEmptyBit(which)) != 0; }
    private static long withChildEmpty(long state, int which) { return state | childEmptyBit(which); }

    private static long READY_FOR_EMPTY_MASK = ANY_CHILD_EMPTY | CLOSED | ENTRY_COUNT_MASK;
    private static long READY_FOR_EMPTY_EXPECTED = CLOSED;
    private static boolean readyForEmpty(long state) { return (state & READY_FOR_EMPTY_MASK) == READY_FOR_EMPTY_EXPECTED; }
    private static long recomputeEmpty(long state) { return readyForEmpty(state) ? withEmpty(state) : state; }

    private static long ENTRY_FAST_PATH_MASK = ANY_CHILD_EMPTY | CLOSING | (1L << (ENTRY_COUNT_BITS - 1));

    /** Not closed, no children, and no overflow possible. */
    private static boolean isEntryFastPath(long state) { return (state & ENTRY_FAST_PATH_MASK) == 0L; }

    //////////////// instance state

    private static final AtomicReferenceFieldUpdater[] childrenUpdaters = {
        AtomicReferenceFieldUpdater.newUpdater(ClosableRefCount.class, ClosableRefCount.class, "child0"),
        AtomicReferenceFieldUpdater.newUpdater(ClosableRefCount.class, ClosableRefCount.class, "child1"),
        AtomicReferenceFieldUpdater.newUpdater(ClosableRefCount.class, ClosableRefCount.class, "child2"),
        AtomicReferenceFieldUpdater.newUpdater(ClosableRefCount.class, ClosableRefCount.class, "child3")
    };

    private final ClosableRefCount _parent;
    private final int _whichInParent;

    // It would be cleaner to use an array of children, but we want to force
    // all of the bulk into the same object as the AtomicLong.value.

    // To avoid races between creating a child and marking a node as closed,
    // we add a bit to the state for each child that records whether it
    // *should* exist.  If we find that the bit is set but a child is missing,
    // we can create it ourself.

    private volatile ClosableRefCount _child0;
    private volatile ClosableRefCount _child1;
    private volatile ClosableRefCount _child2;
    private volatile ClosableRefCount _child3;

    public ClosableRefCount() {
        _parent = null;
        _whichInParent = 0;
    }

    ClosableRefCount(final ClosableRefCount parent, final int whichInParent) {
        _parent = parent;
        _whichInParent = whichInParent;
    }

    //////////////// provided by the caller

    abstract protected void onClose();

    //////////////// child management

    private ClosableRefCount getChildFromField(final int which) {
        switch (which) {
            case 0: return _child0;
            case 1: return _child1;
            case 2: return _child2;
            default: return _child3;
        }
    }

    private ClosableRefCount getChild(final long state, final int which) {
        if (!isChildPresent(state, which)) {
            return null;
        }
        final ClosableRefCount existing = getChildFromField(which);
        if (existing != null) {
            return existing;
        }
        final ClosableRefCount fresh = new Child(this, which);
        if (childrenUpdaters[which].compareAndSet(this, null, fresh)) {
            // success
            return fresh;
        }
        else {
            return getChildFromField(which);
        }
    }

    private ClosableRefCount getOrCreateChild(final int which) {
        while (true) {
            final long state = get();
            if (isChildPresent(state, which)) {
                final ClosableRefCount existing = getChildFromField(which);
                if (existing != null) {
                    return existing;
                }
                // we must create it
                break;
            }
            if (isClosing(state)) {
                // can't create any new children
                return null;
            }
            if (compareAndSet(state, withChildPresent(state, which))) {
                // the child now should exist, but we must still actually
                // construct and link in the instance
                break;
            }
        }

        final ClosableRefCount fresh = new Child(this, which);
        if (childrenUpdaters[which].compareAndSet(this, null, fresh)) {
            // success
            return fresh;
        }
        else {
            return getChildFromField(which);
        }
    }

    /** Returns the <code>ClosableRefCount</code> to decr on success, null if
     *  {@link #beginClose} has already been called on this instance.
     */
    public ClosableRefCount attemptIncr() {
        final long state = get();
        if (isEntryFastPath(state) && compareAndSet(state, state + 1)) {
            return this;
        }
        else {
            return attemptIncr(0);
        }
    }

    private ClosableRefCount attemptIncr(int id) {
        int tries = 0;
        while (true) {
            final long state = get();
            if (isClosing(state)) {
                return null;
            }
            if (!isAnyChildPresent(state) || tries >= TRIES_BEFORE_SUBTREE) {
                // Go deeper if we have previously detected contention, or if
                // we are currently detecting it.  Lazy computation of our
                // current identity.
                if (id == 0) {
                    id = System.identityHashCode(Thread.currentThread()) | Integer.MAX_VALUE;
                }
                final ClosableRefCount child = getOrCreateChild(id & BF_MASK);
                if (child == null) {
                    return null;
                }
                return child.attemptIncr(id >> LOG_BF);
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

    /** Should be called on every non-null return value from attemptIncr. */
    public void decr() {
        while (true) {
            final long state = get();
            if (entryCount(state) == 0) {
                throw new IllegalStateException("incorrect call to ClosableRefCount.decr()");
            }
            final long after = recomputeEmpty(state - 1);
            if (compareAndSet(state, after)) {
                if (isEmpty(after)) {
                    newlyEmpty();
                }
                return;
            }
        }
    }

    private void newlyEmpty() {
        if (_parent != null) {
            // propogate
            _parent.childIsNowEmpty(_whichInParent);
        }
        else {
            // report
            onClose();
        }
    }

    private void childIsNowEmpty(final int which) {
        while (true) {
            final long state = get();
            final long after = recomputeEmpty(withChildEmpty(state, which));
            if (compareAndSet(state, after)) {
                if (isEmpty(after)) {
                    newlyEmpty();
                }
                return;
            }
        }
    }

    /** Prevents subsequent calls to {@link #attemptIncr} from succeeding. */
    public void beginClose() {
        markClosed();
    }

    /** Returns true if already empty. */
    private boolean markClosed() {
        int attempts = 0;
        long state;
        do {
            ++attempts;

            state = get();
            if (isClosed(state)) {
                return isEmpty(state);
            }

            if (isClosing(state)) {
                // give the thread that actually performed this transition a
                // bit of a head start
                if (attempts < CLOSER_HEAD_START) {
                    continue;
                }
                break;
            }
        } while (!compareAndSet(state, withClosing(state)));

        // no new child bits can be set after closing, so this will be
        // exhaustive
        long childCloseResults = 0L;
        for (int which = 0; which < BF; ++which) {
            final ClosableRefCount child = getChild(state, which);
            if (child == null || child.markClosed()) {
                childCloseResults = withChildEmpty(childCloseResults, which);
            }
        }

        while (true) {
            final long before = get();
            if (isClosed(before)) {
                return isEmpty(before);
            }

            final long after = recomputeEmpty(withClosed(before) | childCloseResults);
            if (compareAndSet(before, after)) {
                if (_parent == null && isEmpty(after)) {
                    onClose();
                }
                return isEmpty(after);
            }
        }
    }
}