// from gee.cs.oswego.edu/home/jsr166/jsr166

package jsr166tests.loops;

/*
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */
/*
 * @test
 * Checks for responsiveness of locks to timeouts. Runs under the
 * assumption that ITERS computations require more than TIMEOUT msecs
 * to complete, which seems to be a safe assumption for another
 * decade.
 */

import java.util.concurrent.*;
import java.util.concurrent.locks.*;
import java.util.*;

public final class TimeoutMutexLoops {
    static final ExecutorService pool = Executors.newCachedThreadPool();
    static final LoopHelpers.SimpleRandom rng = new LoopHelpers.SimpleRandom();
    static boolean print = false;
    static final int ITERS = Integer.MAX_VALUE;
    static final long TIMEOUT = 100;

    public static void main(String[] args) throws Exception {
        int maxThreads = 100;
        if (args.length > 0)
            maxThreads = Integer.parseInt(args[0]);

        print = true;

        for (int i = 1; i <= maxThreads; i += (i+1) >>> 1) {
            System.out.print("Threads: " + i);
            new MutexLoop(i).test();
            Thread.sleep(TIMEOUT);
        }
        pool.shutdown();
    }

    static final class MutexLoop implements Runnable {
        private int v = rng.next();
        private volatile boolean completed;
        private volatile int result = 17;
        private final Mutex lock = new Mutex();
        private final LoopHelpers.BarrierTimer timer = new LoopHelpers.BarrierTimer();
        private final CyclicBarrier barrier;
        private final int nthreads;
        MutexLoop(int nthreads) {
            this.nthreads = nthreads;
            barrier = new CyclicBarrier(nthreads+1, timer);
        }

        final void test() throws Exception {
            for (int i = 0; i < nthreads; ++i)
                pool.execute(this);
            barrier.await();
            Thread.sleep(TIMEOUT);
            lock.lock();
            barrier.await();
            if (print) {
                long time = timer.getTime();
                double secs = (double) time / 1000000000.0;
                System.out.println("\t " + secs + "s run time");
            }

            if (completed)
                throw new Error("Some thread completed instead of timing out");
            int r = result;
            if (r == 0) // avoid overoptimization
                System.out.println("useless result: " + r);
        }

        public final void run() {
            try {
                barrier.await();
                int sum = v;
                int x = 0;
                int n = ITERS;
                do {
                    if (!lock.tryLock(TIMEOUT, TimeUnit.MILLISECONDS))
                        break;
                    try {
                        v = x = LoopHelpers.compute1(v);
                    }
                    finally {
                        lock.unlock();
                    }
                    sum += LoopHelpers.compute2(x);
                } while (n-- > 0);
                if (n <= 0)
                    completed = true;
                barrier.await();
                result += sum;
            }
            catch (Exception ie) {
                return;
            }
        }
    }

}
