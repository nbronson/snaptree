// from gee.cs.oswego.edu/home/jsr166/jsr166

package jsr166tests.jtreg.util.concurrent.BlockingQueue;

/*
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */

/*
 * @test
 * @bug 6805775 6815766
 * @run main OfferDrainToLoops 300
 * @summary Test concurrent offer vs. drainTo
 */

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

@SuppressWarnings({"unchecked", "rawtypes", "deprecation"})
public class OfferDrainToLoops {
    final long testDurationMillisDefault = 10L * 1000L;
    final long testDurationMillis;

    OfferDrainToLoops(String[] args) {
        testDurationMillis = (args.length > 0) ?
            Long.valueOf(args[0]) : testDurationMillisDefault;
    }

    void checkNotContainsNull(Iterable it) {
        for (Object x : it)
            check(x != null);
    }

    void test(String[] args) throws Throwable {
        test(new LinkedBlockingQueue());
        test(new LinkedBlockingQueue(2000));
        test(new LinkedBlockingDeque());
        test(new LinkedBlockingDeque(2000));
        test(new ArrayBlockingQueue(2000));
        //test(new LinkedTransferQueue());
    }

    private static Random seeder = new Random();
    private static ThreadLocal<Random> local = new ThreadLocal<Random>() {
        protected Random initialValue() {
            return new Random(seeder.nextLong());
        }
    };

    Random getRandom() {
        //return ThreadLocalRandom.current();
        return local.get();
    }

    void test(final BlockingQueue q) throws Throwable {
        System.out.println(q.getClass().getSimpleName());
        final long testDurationNanos = testDurationMillis * 1000L * 1000L;
        final long quittingTimeNanos = System.nanoTime() + testDurationNanos;
        final long timeoutMillis = 10L * 1000L;

        /** Poor man's bounded buffer. */
        final AtomicLong approximateCount = new AtomicLong(0L);

        abstract class CheckedThread extends Thread {
            CheckedThread(String name) {
                super(name);
                setDaemon(true);
                start();
            }
            /** Polls for quitting time. */
            protected boolean quittingTime() {
                return System.nanoTime() - quittingTimeNanos > 0;
            }
            /** Polls occasionally for quitting time. */
            protected boolean quittingTime(long i) {
                return (i % 1024) == 0 && quittingTime();
            }
            abstract protected void realRun();
            public void run() {
                try { realRun(); } catch (Throwable t) { unexpected(t); }
            }
        }

        Thread offerer = new CheckedThread("offerer") {
            protected void realRun() {
                long c = 0;
                for (long i = 0; ! quittingTime(i); i++) {
                    if (q.offer(c)) {
                        if ((++c % 1024) == 0) {
                            approximateCount.getAndAdd(1024);
                            while (approximateCount.get() > 10000)
                                Thread.yield();
                        }
                    } else {
                        Thread.yield();
                    }}}};

        Thread drainer = new CheckedThread("drainer") {
            protected void realRun() {
                final Random rnd = getRandom();
                while (! quittingTime()) {
                    List list = new ArrayList();
                    int n = rnd.nextBoolean() ?
                        q.drainTo(list) :
                        q.drainTo(list, 100);
                    approximateCount.getAndAdd(-n);
                    equal(list.size(), n);
                    for (int j = 0; j < n - 1; j++)
                        equal((Long) list.get(j) + 1L, list.get(j + 1));
                    Thread.yield();
                }
                q.clear();
                approximateCount.set(0); // Releases waiting offerer thread
            }};

        Thread scanner = new CheckedThread("scanner") {
            protected void realRun() {
                final Random rnd = getRandom();
                while (! quittingTime()) {
                    switch (rnd.nextInt(3)) {
                    case 0: checkNotContainsNull(q); break;
                    case 1: q.size(); break;
                    case 2:
                        Long[] a = (Long[]) q.toArray(new Long[0]);
                        int n = a.length;
                        for (int j = 0; j < n - 1; j++) {
                            check(a[j] < a[j+1]);
                            check(a[j] != null);
                        }
                        break;
                    }
                    Thread.yield();
                }}};

        for (Thread thread : new Thread[] { offerer, drainer, scanner }) {
            thread.join(timeoutMillis + testDurationMillis);
            if (thread.isAlive()) {
                System.err.printf("Hung thread: %s%n", thread.getName());
                failed++;
                for (StackTraceElement e : thread.getStackTrace())
                    System.err.println(e);
                // Kludge alert
                thread.stop();
                thread.join(timeoutMillis);
            }
        }
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
        new OfferDrainToLoops(args).instanceMain(args);}
    public void instanceMain(String[] args) throws Throwable {
        try {test(args);} catch (Throwable t) {unexpected(t);}
        System.out.printf("%nPassed = %d, failed = %d%n%n", passed, failed);
        if (failed > 0) throw new AssertionError("Some tests failed");}
}
