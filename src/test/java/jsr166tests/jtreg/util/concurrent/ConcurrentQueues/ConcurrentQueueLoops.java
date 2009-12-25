// from gee.cs.oswego.edu/home/jsr166/jsr166

package jsr166tests.jtreg.util.concurrent.ConcurrentQueues;

/*
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */

/*
 * @test
 * @bug 4486658 6785442
 * @run main ConcurrentQueueLoops 8 123456
 * @summary Checks that a set of threads can repeatedly get and modify items
 */

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public class ConcurrentQueueLoops {
    ExecutorService pool;
    AtomicInteger totalItems;
    boolean print;

    // Suitable for benchmarking.  Overriden by args[0] for testing.
    int maxStages = 20;

    // Suitable for benchmarking.  Overriden by args[1] for testing.
    int items = 1024 * 1024;

    Collection<Queue<Integer>> concurrentQueues() {
        List<Queue<Integer>> queues = new ArrayList<Queue<Integer>>();
        queues.add(new ConcurrentLinkedQueue<Integer>());
        queues.add(new ArrayBlockingQueue<Integer>(items, false));
        //queues.add(new ArrayBlockingQueue<Integer>(count, true));
        queues.add(new LinkedBlockingQueue<Integer>());
        queues.add(new LinkedBlockingDeque<Integer>());
        //queues.add(new LinkedTransferQueue<Integer>());

        // Following additional implementations are available from:
        // http://gee.cs.oswego.edu/dl/concurrency-interest/index.html
        // queues.add(new SynchronizedLinkedListQueue<Integer>());

        // Avoid "first fast, second slow" benchmark effect.
        Collections.shuffle(queues);
        return queues;
    }

    void test(String[] args) throws Throwable {
        if (args.length > 0)
            maxStages = Integer.parseInt(args[0]);
        if (args.length > 1)
            items = Integer.parseInt(args[1]);

        for (Queue<Integer> queue : concurrentQueues())
            test(queue);
    }

    void test(final Queue<Integer> q) throws Throwable {
        System.out.println(q.getClass().getSimpleName());
        pool = Executors.newCachedThreadPool();
        print = false;

        print = false;
        System.out.println("Warmup...");
        oneRun(1, items, q);
        //Thread.sleep(100);
        oneRun(3, items, q);
        Thread.sleep(100);
        print = true;

        for (int i = 1; i <= maxStages; i += (i+1) >>> 1) {
            oneRun(i, items, q);
        }
        pool.shutdown();
        check(pool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS));
   }

    class Stage implements Callable<Integer> {
        final Queue<Integer> queue;
        final CyclicBarrier barrier;
        int items;
        Stage (Queue<Integer> q, CyclicBarrier b, int items) {
            queue = q;
            barrier = b;
            this.items = items;
        }

        public Integer call() {
            // Repeatedly take something from queue if possible,
            // transform it, and put back in.
            try {
                barrier.await();
                int l = 4321;
                int takes = 0;
                for (;;) {
                    Integer item = queue.poll();
                    if (item != null) {
                        ++takes;
                        l = LoopHelpers.compute2(item.intValue());
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
                        queue.offer(new Integer(l));
                    }
                    else if ( (l & (3 << 5)) == 0) // spinwait
                        Thread.sleep(1);
                }
                return new Integer(l);
            }
            catch (Throwable t) { unexpected(t); return null; }
        }
    }

    void oneRun(int n, int items, final Queue<Integer> q) throws Exception {
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

    //--------------------- Infrastructure ---------------------------
    volatile int passed = 0, failed = 0;
    void pass() {passed++;}
    void fail() {failed++; Thread.dumpStack();}
    void fail(String msg) {System.err.println(msg); fail();}
    void unexpected(Throwable t) {failed++; t.printStackTrace();}
    void check(boolean cond) {if (cond) pass(); else fail();}
    void equal(Object x, Object y) {
        if (x == null ? y == null : x.equals(y)) pass();
        else fail(x + " not equal to " + y);}
    public static void main(String[] args) throws Throwable {
        new ConcurrentQueueLoops().instanceMain(args);}
    public void instanceMain(String[] args) throws Throwable {
        try {test(args);} catch (Throwable t) {unexpected(t);}
        System.out.printf("%nPassed = %d, failed = %d%n%n", passed, failed);
        if (failed > 0) throw new AssertionError("Some tests failed");}
}
