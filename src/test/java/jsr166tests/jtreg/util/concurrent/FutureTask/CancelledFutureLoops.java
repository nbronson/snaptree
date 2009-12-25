// from gee.cs.oswego.edu/home/jsr166/jsr166

package jsr166tests.jtreg.util.concurrent.FutureTask;

/*
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */

/*
 * @test
 * @bug 4486658
 * @compile -source 1.5 CancelledFutureLoops.java
 * @run main/timeout=2000 CancelledFutureLoops
 * @summary Checks for responsiveness of futures to cancellation.
 * Runs under the assumption that ITERS computations require more than
 * TIMEOUT msecs to complete.
 */

import java.util.concurrent.*;
import java.util.concurrent.locks.*;
import java.util.*;

public final class CancelledFutureLoops {
    static final ExecutorService pool = Executors.newCachedThreadPool();
    static final LoopHelpers.SimpleRandom rng = new LoopHelpers.SimpleRandom();
    static boolean print = false;
    static final int ITERS = 1000000;
    static final long TIMEOUT = 100;

    public static void main(String[] args) throws Exception {
        int maxThreads = 5;
        if (args.length > 0)
            maxThreads = Integer.parseInt(args[0]);

        print = true;

        for (int i = 2; i <= maxThreads; i += (i+1) >>> 1) {
            System.out.print("Threads: " + i);
            try {
                new FutureLoop(i).test();
            }
            catch(BrokenBarrierException bb) {
                // OK; ignore
            }
            catch(ExecutionException ee) {
                // OK; ignore
            }
            Thread.sleep(TIMEOUT);
        }
        pool.shutdown();
        if (! pool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS))
            throw new Error();
    }

    static final class FutureLoop implements Callable {
        private int v = rng.next();
        private final ReentrantLock lock = new ReentrantLock();
        private final LoopHelpers.BarrierTimer timer = new LoopHelpers.BarrierTimer();
        private final CyclicBarrier barrier;
        private final int nthreads;
        FutureLoop(int nthreads) {
            this.nthreads = nthreads;
            barrier = new CyclicBarrier(nthreads+1, timer);
        }

        final void test() throws Exception {
            Future[] futures = new Future[nthreads];
            for (int i = 0; i < nthreads; ++i)
                futures[i] = pool.submit(this);

            barrier.await();
            Thread.sleep(TIMEOUT);
            boolean tooLate = false;
            for (int i = 1; i < nthreads; ++i) {
                if (!futures[i].cancel(true))
                    tooLate = true;
                // Unbunch some of the cancels
                if ( (i & 3) == 0)
                    Thread.sleep(1 + rng.next() % 10);
            }

            Object f0 = futures[0].get();
            if (!tooLate) {
                for (int i = 1; i < nthreads; ++i) {
                    if (!futures[i].isDone() || !futures[i].isCancelled())
                        throw new Error("Only one thread should complete");
                }
            }
            else
                System.out.print("(cancelled too late) ");

            long endTime = System.nanoTime();
            long time = endTime - timer.startTime;
            if (print) {
                double secs = (double)(time) / 1000000000.0;
                System.out.println("\t " + secs + "s run time");
            }

        }

        public final Object call() throws Exception {
            barrier.await();
            int sum = v;
            int x = 0;
            int n = ITERS;
            while (n-- > 0) {
                lock.lockInterruptibly();
                try {
                    v = x = LoopHelpers.compute1(v);
                }
                finally {
                    lock.unlock();
                }
                sum += LoopHelpers.compute2(LoopHelpers.compute2(x));
            }
            return new Integer(sum);
        }
    }

}
