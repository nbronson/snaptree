/* CCSTM - (c) 2009 Stanford University - PPL */

// TwoModeLock
package edu.stanford.ppl.concurrent;

import java.util.concurrent.atomic.AtomicReference;


/** At each moment, the lock is in state { A-active, A-closing, B-active, or B-closing }.
 *  Waiters may join the existing waiters of their type.
 */
class TwoModeLock extends AtomicReference<TwoModeLock.State> {

    static class State {
        final char bias;
        final TreeEpoch active;
        final TreeEpoch sameBiasQueue;
        final TreeEpoch otherBiasQueue;

        private State(final char bias,
                      final TreeEpoch active,
                      final TreeEpoch sameBiasQueue,
                      final TreeEpoch otherBiasQueue) {
            this.bias = bias;
            this.active = active;
            this.sameBiasQueue = sameBiasQueue;
            this.otherBiasQueue = otherBiasQueue;
        }
    }

    TwoModeLock() {
        super(new State('A', new TreeEpoch(), new TreeEpoch(), new TreeEpoch()));
    }

    Object lock(final char us) {
        OUTER: while (true) {
            final State s = get();
            if (s.bias == us) {
                final Object z0 = s.active.attemptEnter();
                if (z0 != null) {
                    // success!
                    return z0;
                }
                final Object z1 = s.sameBiasQueue.attemptEnter();
                if (z1 == null) {
                    // state has changed out from underneath us
                    continue OUTER;
                }
                // TODO: this is not correct
                transition(transition(s));
                return z1;
            }
            else {
                final TreeEpoch aq = s.sameBiasQueue.attemptEnter();
                if (aq == null) {
                    continue OUTER;
                }
                transition(s);
                return aq;
            }
        }
    }

    void unlockA(final Object lockResult) {
        ((TreeEpoch) lockResult).exit();
    }

    private State transition(final State s) {
        s.active.close();
        final State s0 = get();
        if (s0 != s) {
            return s0;
        }

        final char other = (char) (s.bias ^ 3);
        final State sOther = new State(other, s.otherBiasQueue, new TreeEpoch(), s.sameBiasQueue);

        final State s1 = get();
        if (s1 != s) {
            return s1;
        }

        compareAndSet(s, sOther);
        return get();
    }
}
