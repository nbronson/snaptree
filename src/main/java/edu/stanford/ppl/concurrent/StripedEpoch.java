/* SnapTree - (c) 2009 Stanford University - PPL */

// StripedSizedEpoch

package edu.stanford.ppl.concurrent;

import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

class StripedEpoch {
    /** NumStripes*sizeof(int) should cross multiple cache lines. */
    static final int NumStripes = nextPowerOfTwo(Integer.valueOf(System.getProperty("epoch.stripes", "64")));

    private static int nextPowerOfTwo(final int n) {
        return 1 << (32 - Integer.numberOfLeadingZeros(n - 1));
    }

    static final AtomicReferenceFieldUpdater<StripedEpoch,Object> WakeupUpdater
            = AtomicReferenceFieldUpdater.newUpdater(StripedEpoch.class, Object.class, "wakeup");
    static final AtomicLongFieldUpdater<StripedEpoch> CleanupAcquiredUpdater
            = AtomicLongFieldUpdater.newUpdater(StripedEpoch.class, "cleanupAcquired");


    private final AtomicIntegerArray entryCounts = new AtomicIntegerArray(NumStripes);

    private volatile Object wakeup;
    private volatile long cleanupAcquired;
    private volatile boolean completed;

    // TODO: fix the problem of an unfortunate thread hash code collision causing long-term false sharing

    public StripedEpoch() {
    }

    /** Returns true if entry was successful, false if shutdown has already begun on this StripedEpoch. */
    public boolean enter(final int id) {
        entryCounts.getAndIncrement(id & (NumStripes - 1));
        if (wakeup != null) {
            exit(id);
            return false;
        } else {
            return true;
        }
    }

    public void exit(final int id) {
        if (entryCounts.addAndGet(id & (NumStripes - 1), -1) == 0) {
            final Object w = wakeup;
            if (w != null) {
                synchronized(w) {
                    w.notifyAll();
                }
            }
        }
    }

    /** Triggers shutdown, but does not accept cleanup responsibility. */
    public void shutdown() {
        awaitShutdown(triggerShutdown(), false);
    }

    /** Returns the wakeup object. */
    private Object triggerShutdown() {
        Object w = wakeup;
        if (w == null) {
            WakeupUpdater.compareAndSet(this, null, new Object());
            w = wakeup;
        }
        return w;
    }

    /** Awaits a pending shutdown, then returns true if the caller has been
     *  assigned cleanup responsibility.  The caller will only be assigned
     *  cleanup responsibility if they have requested it.
     */
    public boolean awaitShutdown(final boolean canAcceptCleanup) {
        return awaitShutdown(wakeup, canAcceptCleanup);
    }

    private boolean awaitShutdown(final Object w, final boolean canAcceptCleanup) {
        boolean done = false;
        boolean interrupted = false;
        while (!done && isPending()) {
            synchronized(w) {
                if (isPending()) {
                    try {
                        w.wait();
                    } catch (final InterruptedException xx) {
                        interrupted = true;
                    }
                } else {
                    done = true;
                }
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
        return canAcceptCleanup &&
                cleanupAcquired == 0 &&
                CleanupAcquiredUpdater.compareAndSet(this, 0L, 1L);
    }

    private boolean isPending() {
        if (completed) {
            return false;
        }

        for (int i = 0; i < NumStripes; ++i) {
            if (entryCounts.get(i) != 0) {
                return true;
            }
        }

        completed = true;
        return false;
    }
}