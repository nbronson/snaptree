// from gee.cs.oswego.edu/home/jsr166/jsr166

package jsr166tests.loops;

/*
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;
import java.util.concurrent.atomic.*;

public class ConcurrentQueueLoops {
    static final ExecutorService pool = Executors.newCachedThreadPool();
    static boolean print = false;
    static final Integer zero = new Integer(0);
    static final Integer one = new Integer(1);
    static int workMask;
    static final long RUN_TIME_NANOS = 5 * 1000L * 1000L * 1000L;
    static final int BATCH_SIZE = 8;

    public static void main(String[] args) throws Exception {
        int maxStages = 100;
        int work = 1024;
        Class klass = null;
        if (args.length > 0) {
            try {
                klass = Class.forName(args[0]);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("Class " + args[0] + " not found.");
            }
        }

        if (args.length > 1)
            maxStages = Integer.parseInt(args[1]);

        if (args.length > 2)
            work = Integer.parseInt(args[2]);

        workMask = work - 1;
        System.out.print("Class: " + klass.getName());
        System.out.print(" stages: " + maxStages);
        System.out.println(" work: " + work);

        print = false;
        System.out.println("Warmup...");
        //        oneRun(klass, 4);
        //
        Thread.sleep(100);
        oneRun(klass, 1);
        Thread.sleep(100);
        print = true;

        int k = 1;
        for (int i = 1; i <= maxStages;) {
            oneRun(klass, i);
            if (i == k) {
                k = i << 1;
                i = i + (i >>> 1);
            }
            else
                i = k;
        }
        pool.shutdown();
   }

    static final class Stage implements Callable<Integer> {
        final Queue<Integer> queue;
        final CyclicBarrier barrier;
        final int nthreads;
        Stage (Queue<Integer> q, CyclicBarrier b, int nthreads) {
            queue = q;
            barrier = b;
            this.nthreads = nthreads;
        }

        static int compute(int l) {
            if (l == 0)
                return (int) System.nanoTime();
            int nn =  (l >>> 7) & workMask;
            while (nn-- > 0)
                l = LoopHelpers.compute6(l);
            return l;
        }

        public Integer call() {
            try {
                barrier.await();
                long now = System.nanoTime();
                long stopTime = now + RUN_TIME_NANOS;
                int l = (int) now;
                int takes = 0;
                int misses = 0;
                int lmask = 1;
                for (;;) {
                    l = compute(l);
                    Integer item = queue.poll();
                    if (item != null) {
                        ++takes;
                        if (item == one)
                            l = LoopHelpers.compute6(l);
                    } else if ((misses++ & 255) == 0 &&
                               System.nanoTime() >= stopTime) {
                        return Integer.valueOf(takes);
                    } else {
                        for (int i = 0; i < BATCH_SIZE; ++i) {
                            queue.offer(((l & lmask)== 0) ? zero : one);
                            if ((lmask <<= 1) == 0) lmask = 1;
                            if (i != 0) l = compute(l);
                        }
                    }
                }
            }
            catch (Exception ie) {
                ie.printStackTrace();
                throw new Error("Call loop failed");
            }
        }
    }

    static void oneRun(Class klass, int n) throws Exception {
        Queue<Integer> q = (Queue<Integer>) klass.newInstance();
        LoopHelpers.BarrierTimer timer = new LoopHelpers.BarrierTimer();
        CyclicBarrier barrier = new CyclicBarrier(n + 1, timer);
        ArrayList<Future<Integer>> results = new ArrayList<Future<Integer>>(n);
        for (int i = 0; i < n; ++i)
            results.add(pool.submit(new Stage(q, barrier, n)));

        if (print)
            System.out.print("Threads: " + n + "\t:");
        barrier.await();
        int total = 0;
        for (int i = 0; i < n; ++i) {
            Future<Integer> f = results.get(i);
            Integer r = f.get();
            total += r.intValue();
        }
        long endTime = System.nanoTime();
        long time = endTime - timer.startTime;
        long ips = 1000000000L * total / time;

        if (print)
            System.out.print(LoopHelpers.rightJustify(ips) + " items per sec");
        if (print)
            System.out.println();
    }

}
