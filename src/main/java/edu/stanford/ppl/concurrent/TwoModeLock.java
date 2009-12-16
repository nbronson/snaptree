/* CCSTM - (c) 2009 Stanford University - PPL */

// TwoModeLock
package edu.stanford.ppl.concurrent;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;


/** At each moment, the lock is in state { A-active, A-closing, B-active, or B-closing }.
 *  Waiters may join the existing waiters of their type.
 */
class TwoModeLock extends AtomicReference<TwoModeLock.State> {

    private static class Epoch extends AtomicLong {
        static int TRIES_BEFORE_TREE = 4;
        static int TRIES_BEFORE_SUBTREE = 2;
        static int CLOSER_HEAD_START = 1000;

        static long CLOSING_MASK = (1L << 32);
        static long CLOSED_MASK = (1L << 33);
        static long EMPTY_MASK = (1L << 34);

        static long childPresentMask(int which) { return (1L << (48 + which)); }
        static long ANY_CHILD_PRESENT_MASK = (0xfL << 48);
        static long childClosedAndEmptyMask(int which) { return (1L << (56 + which)); }
        static long PRE_EMPTY_MASK = CLOSING_MASK | CLOSED_MASK | (0xfL << 56) | 0xffffffffL;
        static long PRE_EMPTY_VALUE = CLOSING_MASK | CLOSED_MASK | (0xfL << 56);

        private final Epoch _parent;
        private final int _whichInParent;
        private final CountDownLatch _closed;

        private Epoch _child0;
        private Epoch _child1;
        private Epoch _child2;
        private Epoch _child3;

        Epoch() {
            _parent = null;
            _whichInParent = 0;
            _closed = new CountDownLatch(1);
        }

        Epoch(final Epoch parent, final int whichInParent) {
            _parent = parent;
            _whichInParent = whichInParent;
            _closed = null;
        }

        Epoch getChild(final int which) {
            if ((get() & childPresentMask(which)) != 0L) {
                // the bit in the state is the canonical version of whether
                // or not the node is present, not the actual field
                switch (which) {
                    case 0:
                        if (_child0 == null) {
                            synchronized (this) {
                                if (_child0 == null) _child0 = new Epoch(this, 0);
                            }
                        }
                        return _child0;
                    case 1:
                        if (_child1 == null) {
                            synchronized (this) {
                                if (_child1 == null) _child1 = new Epoch(this, 1);
                            }
                        }
                        return _child1;
                    case 2:
                        if (_child2 == null) {
                            synchronized (this) {
                                if (_child2 == null) _child2 = new Epoch(this, 2);
                            }
                        }
                        return _child2;
                    default:
                        assert(which == 3);
                        if (_child3 == null) {
                            synchronized (this) {
                                if (_child3 == null) _child3 = new Epoch(this, 3);
                            }
                        }
                        return _child3;
                }
            }
            else {
                return null;
            }
        }

        Epoch getOrCreateChild(final int which) {
            final long mask = childPresentMask(which);
            while (true) {
                final long state = get();
                if ((state & mask) != 0L) {
                    return getChild(which);
                }
                if ((state & CLOSING_MASK) != 0L) {
                    // can't create any new children
                    return null;
                }
                if (compareAndSet(state, state | mask)) {
                    return getChild(which);
                }
            }
        }

        Epoch attemptEnter() {
            int tries = 0;
            while (true) {
                ++tries;
                final long state = get();
                if ((state & CLOSING_MASK) != 0L) {
                    return null;
                }
                if ((state & ANY_CHILD_PRESENT_MASK) != 0L || tries > TRIES_BEFORE_TREE) {
                    // forward to the tree-based one
                    return attemptEnter(System.identityHashCode(Thread.currentThread()));
                }
                if (compareAndSet(state, state + 1)) {
                    // entry was recorded successfully right here
                    return this;
                }
            }
        }

        Epoch attemptEnter(final int id) {
            for (int attempts = 0; attempts < TRIES_BEFORE_SUBTREE; ++attempts) {
                final long state = get();
                if ((state & CLOSING_MASK) != 0L) {
                    return null;
                }
                if ((state & childPresentMask(id & 3)) != 0L) {
                    // recurse
                    return getChild(id & 3).attemptEnter(id >> 2);
                }
                if (compareAndSet(state, state + 1)) {
                    // entry was recorded successfully right here
                    return this;
                }
            }

            // contention detected, let's go down
            final Epoch child = getOrCreateChild(id & 3);
            return (child == null) ? null : child.attemptEnter(id >> 2);
        }

        void exit() {
            while (true) {
                final long state = get();
                long after = state - 1;
                if ((after & PRE_EMPTY_MASK) == PRE_EMPTY_VALUE) {
                    // transition from closed -> empty
                    after |= EMPTY_MASK;
                }
                if (compareAndSet(state, after)) {
                    if ((after & EMPTY_MASK) != 0L) {
                        // propogate the emptiness
                        if (_parent != null) {
                            _parent.childIsNowEmpty(_whichInParent);
                        }
                        else {
                            _closed.countDown();
                        }
                    }

                    // success
                    return;
                }
            }
        }

        void childIsNowEmpty(final int which) {
            while (true) {
                final long state = get();
                long after = state | childClosedAndEmptyMask(which);
                if ((after & PRE_EMPTY_MASK) == PRE_EMPTY_VALUE) {
                    // transition from closed -> empty
                    after |= EMPTY_MASK;
                }
                if (compareAndSet(state, after)) {
                    if ((after & EMPTY_MASK) != 0L) {
                        // propogate the emptiness
                        if (_parent != null) {
                            _parent.childIsNowEmpty(_whichInParent);
                        }
                        else {
                            _closed.countDown();
                        }
                    }

                    // success
                    return;
                }
            }
        }

        void blockingClose() {
            if (!markClosed()) {
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

        /** Returns true if already empty. */
        boolean markClosed() {
            int attempts = 0;
            while (true) {
                ++attempts;

                final long state = get();
                if ((state & CLOSED_MASK) != 0L) {
                    // Somebody else has already finished closing this
                    // node.  Either it has been marked empty, or there is
                    // a pending or concurrent exit().
                    return (state & EMPTY_MASK) != 0L;
                }

                if ((state & CLOSING_MASK) != 0L) {
                    // only take over after we have given the thread that
                    // set CLOSING_MASK a bit of a head start
                    if (attempts < CLOSER_HEAD_START) {
                        continue;
                    }
                    break;
                }

                if (compareAndSet(state, state | CLOSING_MASK)) {
                    // successfully entered the closing state, get to work
                    break;
                }
            }

            // no new child bits can be set after closing, so this will be
            // exhaustive
            long childCloseResults = 0L;
            for (int which = 0; which < 4; ++which) {
                final Epoch child = getChild(which);
                if (child == null || child.markClosed()) {
                    childCloseResults |= childClosedAndEmptyMask(which);
                }
            }

            while (true) {
                final long state = get();
                if ((state & CLOSED_MASK) != 0L) {
                    // somebody else already closed
                    return (state & EMPTY_MASK) != 0L;
                }

                long after = state | CLOSED_MASK | childCloseResults;
                if ((after & PRE_EMPTY_MASK) == PRE_EMPTY_VALUE) {
                    after |= EMPTY_MASK;
                }
                if (compareAndSet(state, after)) {
                    // success
                    return (after & EMPTY_MASK) != 0L;
                }
            }
        }
    }

    static class State {
        final char bias;
        final Epoch active;
        final Epoch aQueue;
        final Epoch bQueue;

        private State(final char bias,
                      final Epoch active,
                      final Epoch aQueue,
                      final Epoch bQueue) {
            this.bias = bias;
            this.active = active;
            this.aQueue = aQueue;
            this.bQueue = bQueue;
        }
    }

    TwoModeLock() {
        super(new State('A', new Epoch(), new Epoch(), new Epoch()));
    }

    Object lockA() {
        OUTER: while (true) {
            final State s = get();
            if (s.bias == 'A') {
                final Epoch aa = s.active.attemptEnter();
                if (aa != null) {
                    // success!
                    return aa;
                }
                final Epoch aq = s.aQueue.attemptEnter();
                if (aq == null) {
                    // state has changed out from underneath us
                    continue OUTER;
                }
                final State sB = transitionAToB(s);
                transitionBToA(sB);
                return aq;
            }
            else {
                final Epoch aq = s.aQueue.attemptEnter();
                if (aq == null) {
                    continue OUTER;
                }
                transitionBToA(s);
                return aq;
            }
        }
    }

    void unlockA(final Object lockResult) {
        ((Epoch) lockResult).exit();
    }

    private State transitionAToB(final State sA) {
        sA.active.blockingClose();
        final State s0 = get();
        if (s0 != sA) {
            return s0;
        } else {
            final State sB = new State('B', sA.bQueue, sA.aQueue, new Epoch());
            final State s1 = get();
            if (s1 != sA) {
                return s1;
            } else {
                compareAndSet(sA, sB);
                return sB;
            }
        }
    }

    private State transitionBToA(final State sB) {
        sB.active.blockingClose();
        final State s0 = get();
        if (s0 != sB) {
            return s0;
        } else {
            final State sA = new State('A', sB.aQueue, new Epoch(), sB.bQueue);
            final State s1 = get();
            if (s1 != sB) {
                return s1;
            } else {
                compareAndSet(sB, sA);
                return sA;
            }
        }
    }
}
