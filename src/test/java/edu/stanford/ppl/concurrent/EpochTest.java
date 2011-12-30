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

import junit.framework.TestCase;

public class EpochTest extends TestCase {
    public void testImmediateClose() {
        final boolean[] closed = { false };
        final Epoch e = new Epoch() {
            protected void onClosed(final int dataSum) {
                closed[0] = true;
                assertEquals(0, dataSum);
            }
        };
        e.beginClose();
        assertTrue(closed[0]);
        assertNull(e.attemptArrive());
    }

    public void testSimple() {
        final boolean[] closed = { false };
        final Epoch e = new Epoch() {
            protected void onClosed(final int dataSum) {
                closed[0] = true;
                assertEquals(1, dataSum);
            }
        };
        final Epoch.Ticket t0 = e.attemptArrive();
        assertNotNull(t0);
        t0.leave(1);
        assertTrue(!closed[0]);
        e.beginClose();
        assertTrue(closed[0]);
        assertNull(e.attemptArrive());
    }

    public void testPending() {
        final boolean[] closed = { false };
        final Epoch e = new Epoch() {
            protected void onClosed(final int dataSum) {
                closed[0] = true;
                assertEquals(1, dataSum);
            }
        };
        final Epoch.Ticket t0 = e.attemptArrive();
        assertNotNull(t0);
        e.beginClose();
        assertTrue(!closed[0]);
        t0.leave(1);
        assertTrue(closed[0]);
        assertNull(e.attemptArrive());
    }

    public void testParallelCutoff() {
        final int numThreads = 32;
        final int arrivalsPerThread = 1000000;
        final boolean[] closed = { false };
        final Epoch e = new Epoch() {
            protected void onClosed(final int dataSum) {
                closed[0] = true;
            }
        };
        ParUtil.parallel(numThreads, new ParUtil.Block() {
            public void call(final int index) {
                for (int i = 0; i < arrivalsPerThread; ++i) {
                    final Epoch.Ticket t = e.attemptArrive();
                    if (t == null) {
                        //System.out.print("thread " + index + " got to " + i + "\n");
                        return;
                    }
                    t.leave(1 + index);
                    if (index == numThreads - 1 && i == arrivalsPerThread / 2) {
                        e.beginClose();
                    }
                }
            }
        });
        assertTrue(closed[0]);
    }

    public void testParallelPerformance() {
        final int arrivalsPerThread = 1000000;
        for (int i = 0; i < 3; ++i) {
            for (int t = 1; t <= Runtime.getRuntime().availableProcessors(); t *= 2) {
                runNoClosePerf(t, arrivalsPerThread);
            }
            for (int f = 2; f <= 4; f *= 2) {
                runNoClosePerf(Runtime.getRuntime().availableProcessors() * f, arrivalsPerThread / f);
            }
        }
    }

    private void runNoClosePerf(final int numThreads, final int arrivalsPerThread) {
        final boolean[] closed = { false };
        final Epoch e = new Epoch() {
            protected void onClosed(final int dataSum) {
                closed[0] = true;
                assertEquals(numThreads * (numThreads + 1L) * arrivalsPerThread / 2, dataSum);
            }
        };
        final long elapsed = ParUtil.timeParallel(numThreads, new ParUtil.Block() {
            public void call(final int index) {
                for (int i = 0; i < arrivalsPerThread; ++i) {
                    final Epoch.Ticket t = e.attemptArrive();
                    assertNotNull(t);
                    t.leave(1 + index);
                }
            }
        });
        assertTrue(!closed[0]);
        e.beginClose();
        assertTrue(closed[0]);
        assertNull(e.attemptArrive());

        final long arrivalsPerSec = numThreads * 1000L * arrivalsPerThread / elapsed;
        System.out.println("numThreads " + numThreads + "    arrivalsPerThread " + arrivalsPerThread + "    " +
                "elapsedMillis " + elapsed + "    arrivalsPerSec " + arrivalsPerSec + "    " +
                "spread " + e.computeSpread());
    }
}
