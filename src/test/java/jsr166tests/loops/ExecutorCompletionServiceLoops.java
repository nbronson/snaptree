// from gee.cs.oswego.edu/home/jsr166/jsr166

package jsr166tests.loops;

/*
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */

import java.util.concurrent.*;

public class ExecutorCompletionServiceLoops {
    static final int POOLSIZE =      100;
    static final ExecutorService pool =
        Executors.newFixedThreadPool(POOLSIZE);
    static final ExecutorCompletionService<Integer> ecs =
        new ExecutorCompletionService<Integer>(pool);
    static boolean print = false;

    public static void main(String[] args) throws Exception {
        int max = 8;
        int base = 10000;

        if (args.length > 0)
            max = Integer.parseInt(args[0]);

        System.out.println("Warmup...");
        oneTest( base );
        Thread.sleep(100);
        print = true;

        for (int i = 1; i <= max; i += (i+1) >>> 1) {
            System.out.print("n: " + i * base);
            oneTest(i * base );
            Thread.sleep(100);
        }
        pool.shutdown();
   }

    static class Task implements Callable<Integer> {
        public Integer call() {
            int l = System.identityHashCode(this);
            l = LoopHelpers.compute2(l);
            int s = LoopHelpers.compute1(l);
            l = LoopHelpers.compute2(l);
            s += LoopHelpers.compute1(l);
            return new Integer(s);
        }
    }

    static class Producer implements Runnable {
        final ExecutorCompletionService cs;
        final int iters;
        Producer(ExecutorCompletionService ecs, int i) {
            cs = ecs;
            iters = i;
        }
        public void run() {
            for (int i = 0; i < iters; ++i)
                ecs.submit(new Task());
        }
    }

    static void oneTest(int iters) throws Exception {
        long startTime = System.nanoTime();
        new Thread(new Producer(ecs, iters)).start();

        int r = 0;
        for (int i = 0; i < iters; ++i)
            r += ecs.take().get().intValue();

        long elapsed = System.nanoTime() - startTime;
        long tpi = elapsed/ iters;

        if (print)
            System.out.println("\t: " + LoopHelpers.rightJustify(tpi) + " ns per task");

        if (r == 0) // avoid overoptimization
            System.out.println("useless result: " + r);


    }

}
