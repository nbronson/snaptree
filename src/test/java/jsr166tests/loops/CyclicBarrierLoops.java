// from gee.cs.oswego.edu/home/jsr166/jsr166

package jsr166tests.loops;

/*
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */

import java.util.*;
import java.util.concurrent.*;
//import jsr166y.*;

/*
 * Based loosely on Java Grande Forum barrierBench
 */

public class CyclicBarrierLoops {
    static final int NCPUS = Runtime.getRuntime().availableProcessors();
    static final ExecutorService pool = Executors.newCachedThreadPool();
    static final int FIRST_SIZE = 10000;
    static final int LAST_SIZE = 1000000;
    /** for time conversion */
    static final long NPS = (1000L * 1000 * 1000);

    static final class CyclicBarrierAction implements Runnable {
        final int id;
        final int size;
        final CyclicBarrier barrier;
        public CyclicBarrierAction(int id, CyclicBarrier b, int size) {
            this.id = id;
            this.barrier = b;
            this.size = size;
        }


        public void run() {
            try {
                int n = size;
                CyclicBarrier b = barrier;
                for (int i = 0; i < n; ++i)
                    b.await();
            }
            catch (Exception ex) {
                throw new Error(ex);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        int nthreads = NCPUS;

        if (args.length > 0)
            nthreads = Integer.parseInt(args[0]);

        System.out.printf("max %d Threads\n", nthreads);

        for (int k = 2; k <= nthreads; k *= 2) {
            for (int size = FIRST_SIZE; size <= LAST_SIZE; size *= 10) {
                long startTime = System.nanoTime();

                CyclicBarrier barrier = new CyclicBarrier(k);
                CyclicBarrierAction[] actions = new CyclicBarrierAction[k];
                for (int i = 0; i < k; ++i) {
                    actions[i] = new CyclicBarrierAction(i, barrier, size);
                }

                Future[] futures = new Future[k];
                for (int i = 0; i < k; ++i) {
                    futures[i] = pool.submit(actions[i]);
                }
                for (int i = 0; i < k; ++i) {
                    futures[i].get();
                }
                long elapsed = System.nanoTime() - startTime;
                long bs = (NPS * size) / elapsed;
                System.out.printf("%4d Threads %8d iters: %11d barriers/sec\n",
                                  k, size, bs);
            }
        }
        pool.shutdown();
    }

}
