// from gee.cs.oswego.edu/home/jsr166/jsr166

package jsr166tests.loops;

/*
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */

import java.util.concurrent.*;
import java.util.concurrent.locks.*;
import java.util.*;

public final class CancelledLockLoops {
    static final Random rng = new Random();
    static boolean print = false;
    static final int ITERS = 10000000;
    static final long TIMEOUT = 100;

    public static void main(String[] args) throws Exception {
        int maxThreads = 100;
        if (args.length > 0)
            maxThreads = Integer.parseInt(args[0]);

        print = true;

        for (int i = 2; i <= maxThreads; i += (i+1) >>> 1) {
            System.out.print("Threads: " + i);
            try {
                new ReentrantLockLoop(i).test();
            }
            catch (BrokenBarrierException bb) {
                // OK, ignore
            }
        }
    }

    static final class ReentrantLockLoop implements Runnable {
        private int v = rng.nextInt();
        private int completed;
        private volatile int result = 17;
        private final ReentrantLock lock = new ReentrantLock();
        private final LoopHelpers.BarrierTimer timer = new LoopHelpers.BarrierTimer();
        private final CyclicBarrier barrier;
        private final int nthreads;
        ReentrantLockLoop(int nthreads) {
            this.nthreads = nthreads;
            barrier = new CyclicBarrier(nthreads+1, timer);
        }

        final void test() throws Exception {
            Thread[] threads = new Thread[nthreads];
            for (int i = 0; i < threads.length; ++i)
                threads[i] = new Thread(this);
            for (int i = 0; i < threads.length; ++i)
                threads[i].start();
            Thread[] cancels = threads.clone();
            Collections.shuffle(Arrays.asList(cancels), rng);
            barrier.await();
            Thread.sleep(TIMEOUT);
            for (int i = 0; i < cancels.length-2; ++i) {
                cancels[i].interrupt();
                // make sure all OK even when cancellations spaced out
                if ( (i & 3) == 0)
                    Thread.sleep(1 + rng.nextInt(10));
            }
            barrier.await();
            if (print) {
                long time = timer.getTime();
                double secs = (double) time / 1000000000.0;
                System.out.println("\t " + secs + "s run time");
            }

            int c;
            lock.lock();
            try {
                c = completed;
            }
            finally {
                lock.unlock();
            }
            if (c != 2)
                throw new Error("Completed != 2");
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
                boolean done = false;
                do {
                    try {
                        lock.lockInterruptibly();
                    }
                    catch (InterruptedException ie) {
                        break;
                    }
                    try {
                        v = x = LoopHelpers.compute1(v);
                    }
                    finally {
                        lock.unlock();
                    }
                    sum += LoopHelpers.compute2(x);
                } while (n-- > 0);
                if (n <= 0) {
                    lock.lock();
                    try {
                        ++completed;
                    }
                    finally {
                        lock.unlock();
                    }
                }
                barrier.await();
                result += sum;
            }
            catch (Exception ex) {
                //                ex.printStackTrace();
                return;
            }
        }
    }

}
