/* CCSTM - (c) 2009 Stanford University - PPL */

// EpochTest
package edu.stanford.ppl.concurrent;

import junit.framework.TestCase;

public class EpochTest extends TestCase {
    public void testImmediateClose() {
        final boolean[] closed = { false };
        final Epoch e = new Epoch() {
            protected void onClosed(final int dataSum) {
                closed[0] = true;
                assert(dataSum == 0);
            }
        };
        e.beginClose();
        assert(closed[0]);
        assert(e.attemptArrive() == null);
    }

    public void testSimple() {
        final boolean[] closed = { false };
        final Epoch e = new Epoch() {
            protected void onClosed(final int dataSum) {
                closed[0] = true;
                assert(dataSum == 1);
            }
        };
        final Epoch.Ticket t0 = e.attemptArrive();
        assert(t0 != null);
        t0.leave(1);
        assert(!closed[0]);
        e.beginClose();
        assert(closed[0]);
        assert(e.attemptArrive() == null);
    }

    public void testPending() {
        final boolean[] closed = { false };
        final Epoch e = new Epoch() {
            protected void onClosed(final int dataSum) {
                closed[0] = true;
                assert(dataSum == 1);
            }
        };
        final Epoch.Ticket t0 = e.attemptArrive();
        assert(t0 != null);
        e.beginClose();
        assert(!closed[0]);
        t0.leave(1);
        assert(closed[0]);
        assert(e.attemptArrive() == null);
    }

    public void _testParallelCutoff() {
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
                        System.out.print("thread " + index + " got to " + i + "\n");
                        return;
                    }
                    t.leave(1 + index);
                    if (index == numThreads - 1 && i == arrivalsPerThread / 2) {
                        e.beginClose();
                    }
                }
            }
        });
        assert(closed[0]);
    }

    public void testParallelPerformance() {
        final int arrivalsPerThread = 1000000;
        for (int i = 0; i < 10; ++i) {
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
                assert(dataSum == numThreads * (numThreads + 1L) * arrivalsPerThread / 2);
            }
        };
        final long elapsed = ParUtil.timeParallel(numThreads, new ParUtil.Block() {
            public void call(final int index) {
                for (int i = 0; i < arrivalsPerThread; ++i) {
                    final Epoch.Ticket t = e.attemptArrive();
                    assert(t != null);
                    t.leave(1 + index);
                }
            }
        });
        assert(!closed[0]);
        e.beginClose();
        assert(closed[0]);
        assert(e.attemptArrive() == null);

        final long arrivalsPerSec = numThreads * 1000L * arrivalsPerThread / elapsed;
        System.out.println("numThreads " + numThreads + "    arrivalsPerThread " + arrivalsPerThread + "    " +
                "elapsedMillis " + elapsed + "    arrivalsPerSec " + arrivalsPerSec + "    " +
                "spread " + e.computeSpread());
    }
}
