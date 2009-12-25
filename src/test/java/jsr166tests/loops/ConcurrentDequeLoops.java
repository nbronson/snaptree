// from gee.cs.oswego.edu/home/jsr166/jsr166

package jsr166tests.loops;

/*
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;

public class ConcurrentDequeLoops {
    static final ExecutorService pool = Executors.newCachedThreadPool();
    static AtomicInteger totalItems;
    static boolean print = false;

    public static void main(String[] args) throws Exception {
        int maxStages = 8;
        int items = 1000000;

        Class klass = null;
        if (args.length > 0) {
            try {
                klass = Class.forName(args[0]);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("Class " + args[0] + " not found.");
            }
        }
        else
            throw new Error();

        if (args.length > 1)
            maxStages = Integer.parseInt(args[1]);

        System.out.print("Class: " + klass.getName());
        System.out.println(" stages: " + maxStages);

        print = false;
        System.out.println("Warmup...");
        oneRun(klass, 1, items);
        Thread.sleep(100);
        oneRun(klass, 1, items);
        Thread.sleep(100);
        print = true;

        int k = 1;
        for (int i = 1; i <= maxStages;) {
            oneRun(klass, i, items);
            if (i == k) {
                k = i << 1;
                i = i + (i >>> 1);
            }
            else
                i = k;
        }
        pool.shutdown();
   }

    static class Stage implements Callable<Integer> {
        final Deque<Integer> queue;
        final CyclicBarrier barrier;
        final LoopHelpers.SimpleRandom rng = new LoopHelpers.SimpleRandom();
        int items;
        Stage (Deque<Integer> q, CyclicBarrier b, int items) {
            queue = q;
            barrier = b;
            this.items = items;
        }

        public Integer call() {
            // Repeatedly take something from queue if possible,
            // transform it, and put back in.
            try {
                barrier.await();
                int l = (int) System.nanoTime();
                int takes = 0;
                for (;;) {
                    Integer item;
                    int rnd = rng.next();
                    if ((rnd & 1) == 0)
                        item = queue.pollFirst();
                    else
                        item = queue.pollLast();
                    if (item != null) {
                        ++takes;
                        l += LoopHelpers.compute2(item.intValue());
                    }
                    else if (takes != 0) {
                        totalItems.getAndAdd(-takes);
                        takes = 0;
                    }
                    else if (totalItems.get() <= 0)
                        break;
                    l = LoopHelpers.compute1(l);
                    if (items > 0) {
                        --items;
                        Integer res = new Integer(l);
                        if ((rnd & 16) == 0)
                            queue.addFirst(res);
                        else
                            queue.addLast(res);
                    }
                    else { // spinwait
                        for (int k = 1 + (l & 15); k != 0; --k)
                            l = LoopHelpers.compute1(LoopHelpers.compute2(l));
                        if ((l & 3) == 3) {
                            Thread.sleep(1);
                        }
                    }
                }
                return new Integer(l);
            }
            catch (Exception ie) {
                ie.printStackTrace();
                throw new Error("Call loop failed");
            }
        }
    }

    static void oneRun(Class klass, int n, int items) throws Exception {
        Deque<Integer> q = (Deque<Integer>) klass.newInstance();
        LoopHelpers.BarrierTimer timer = new LoopHelpers.BarrierTimer();
        CyclicBarrier barrier = new CyclicBarrier(n + 1, timer);
        totalItems = new AtomicInteger(n * items);
        ArrayList<Future<Integer>> results = new ArrayList<Future<Integer>>(n);
        for (int i = 0; i < n; ++i)
            results.add(pool.submit(new Stage(q, barrier, items)));

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
        if (print)
            System.out.println(LoopHelpers.rightJustify(time / (items * n)) + " ns per item");
        if (total == 0) // avoid overoptimization
            System.out.println("useless result: " + total);

    }
}
