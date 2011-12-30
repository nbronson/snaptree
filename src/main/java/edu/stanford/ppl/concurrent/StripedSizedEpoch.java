/*
 * Copyright (c) 2009 Stanford University, unless otherwise specified.
 * All rights reserved.
 *
 * This software was developed by the Pervasive Parallelism Laboratory of
 * Stanford University, California, USA.
 *
 * Permission to use, copy, modify, and distribute this software in source
 * or binary form for any purpose with or without fee is hereby granted,
 * provided that the following conditions are met:
 *
 *    1. Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *
 *    2. Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *
 *    3. Neither the name of Stanford University nor the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE REGENTS AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE REGENTS OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

package edu.stanford.ppl.concurrent;

import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

class StripedSizedEpoch {
    /** NumStripes*sizeof(long) should cross multiple cache lines. */
    static final int NumStripes = nextPowerOfTwo(Integer.valueOf(System.getProperty("epoch.stripes", "64")));

    private static int nextPowerOfTwo(final int n) {
        return 1 << (32 - Integer.numberOfLeadingZeros(n - 1));
    }

    static final AtomicReferenceFieldUpdater<StripedSizedEpoch,Object> WakeupUpdater
            = AtomicReferenceFieldUpdater.newUpdater(StripedSizedEpoch.class, Object.class, "wakeup");
    static final AtomicLongFieldUpdater<StripedSizedEpoch> CleanupAcquiredUpdater
            = AtomicLongFieldUpdater.newUpdater(StripedSizedEpoch.class, "cleanupAcquired");


    /** Size is in the high int (&lt;&lt; 32), entryCount in the low int. */
    private final AtomicLongArray entryCountsAndSizes = new AtomicLongArray(NumStripes);

    private volatile Object wakeup;
    private volatile int completedSize = -1;
    private volatile long cleanupAcquired;

    // TODO: fix the problem of an unfortunate thread hash code collision causing long-term false sharing

    public StripedSizedEpoch(final int initialSize) {
        entryCountsAndSizes.set(0, ((long) initialSize) << 32);
    }

    /** Returns true if entry was successful, false if shutdown has already begun on this Epoch. */
    public boolean enter(final int id) {
        entryCountsAndSizes.getAndIncrement(id & (NumStripes - 1));
        if (wakeup != null) {
            exit(id, 0);
            return false;
        } else {
            return true;
        }
    }

    public void exit(final int id, final int sizeDelta) {
        final long ecasDelta = (((long) sizeDelta) << 32) - 1L;
        final long after = entryCountsAndSizes.addAndGet(id & (NumStripes - 1), ecasDelta);
        if (((int) after) == 0) {
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
        if (completedSize != -1) {
            return false;
        }

        int size = 0;
        for (int i = 0; i < NumStripes; ++i) {
            final long ecas = entryCountsAndSizes.get(i);
            if (((int) ecas) != 0) {
                return true;
            }
            size += (int)(ecas >> 32);
        }
        if (completedSize == -1) {
            completedSize = size;
        }
        return false;
    }

    public int size() {
        final int z = completedSize;
        if (z == -1) {
            throw new IllegalStateException("can't read size prior to shutdown");
        }
        return z;
    }
}
