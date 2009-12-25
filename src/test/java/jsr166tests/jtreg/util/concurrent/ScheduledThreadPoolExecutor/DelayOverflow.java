// from gee.cs.oswego.edu/home/jsr166/jsr166

package jsr166tests.jtreg.util.concurrent.ScheduledThreadPoolExecutor;

/*
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */

/*
 * @test
 * @bug 6725789
 * @summary Check for long overflow in task time comparison.
 */

import java.util.concurrent.*;

public class DelayOverflow {
    static void waitForNanoTimeTick() {
        for (long t0 = System.nanoTime(); t0 == System.nanoTime(); )
            ;
    }

    void scheduleNow(ScheduledThreadPoolExecutor pool,
                     Runnable r, int how) {
        switch (how) {
        case 0:
            pool.schedule(r, 0, TimeUnit.MILLISECONDS);
            break;
        case 1:
            pool.schedule(Executors.callable(r), 0, TimeUnit.DAYS);
            break;
        case 2:
            pool.scheduleWithFixedDelay(r, 0, 1000, TimeUnit.NANOSECONDS);
            break;
        case 3:
            pool.scheduleAtFixedRate(r, 0, 1000, TimeUnit.MILLISECONDS);
            break;
        default:
            fail(String.valueOf(how));
        }
    }

    void scheduleAtTheEndOfTime(ScheduledThreadPoolExecutor pool,
                                Runnable r, int how) {
        switch (how) {
        case 0:
            pool.schedule(r, Long.MAX_VALUE, TimeUnit.MILLISECONDS);
            break;
        case 1:
            pool.schedule(Executors.callable(r), Long.MAX_VALUE, TimeUnit.DAYS);
            break;
        case 2:
            pool.scheduleWithFixedDelay(r, Long.MAX_VALUE, 1000, TimeUnit.NANOSECONDS);
            break;
        case 3:
            pool.scheduleAtFixedRate(r, Long.MAX_VALUE, 1000, TimeUnit.MILLISECONDS);
            break;
        default:
            fail(String.valueOf(how));
        }
    }

    /**
     * Attempts to test exhaustively and deterministically, all 20
     * possible ways that one task can be scheduled in the maximal
     * distant future, while at the same time an existing tasks's time
     * has already expired.
     */
    void test(String[] args) throws Throwable {
        for (int nowHow = 0; nowHow < 4; nowHow++) {
            for (int thenHow = 0; thenHow < 4; thenHow++) {

                final ScheduledThreadPoolExecutor pool
                    = new ScheduledThreadPoolExecutor(1);
                final CountDownLatch runLatch     = new CountDownLatch(1);
                final CountDownLatch busyLatch    = new CountDownLatch(1);
                final CountDownLatch proceedLatch = new CountDownLatch(1);
                final Runnable notifier = new Runnable() {
                        public void run() { runLatch.countDown(); }};
                final Runnable neverRuns = new Runnable() {
                        public void run() { fail(); }};
                final Runnable keepPoolBusy = new Runnable() {
                        public void run() {
                            try {
                                busyLatch.countDown();
                                proceedLatch.await();
                            } catch (Throwable t) { unexpected(t); }
                        }};
                pool.schedule(keepPoolBusy, 0, TimeUnit.SECONDS);
                busyLatch.await();
                scheduleNow(pool, notifier, nowHow);
                waitForNanoTimeTick();
                scheduleAtTheEndOfTime(pool, neverRuns, thenHow);
                proceedLatch.countDown();

                check(runLatch.await(10L, TimeUnit.SECONDS));
                equal(runLatch.getCount(), 0L);

                pool.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
                pool.shutdown();
            }

            final int nowHowCopy = nowHow;
            final ScheduledThreadPoolExecutor pool
                = new ScheduledThreadPoolExecutor(1);
            final CountDownLatch runLatch = new CountDownLatch(1);
            final Runnable notifier = new Runnable() {
                    public void run() { runLatch.countDown(); }};
            final Runnable scheduleNowScheduler = new Runnable() {
                    public void run() {
                        try {
                            scheduleNow(pool, notifier, nowHowCopy);
                            waitForNanoTimeTick();
                        } catch (Throwable t) { unexpected(t); }
                    }};
            pool.scheduleWithFixedDelay(scheduleNowScheduler,
                                        0, Long.MAX_VALUE,
                                        TimeUnit.NANOSECONDS);

            check(runLatch.await(10L, TimeUnit.SECONDS));
            equal(runLatch.getCount(), 0L);

            pool.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
            pool.shutdown();
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
        Class<?> k = new Object(){}.getClass().getEnclosingClass();
        try {k.getMethod("instanceMain",String[].class)
                .invoke( k.newInstance(), (Object) args);}
        catch (Throwable e) {throw e.getCause();}}
    public void instanceMain(String[] args) throws Throwable {
        try {test(args);} catch (Throwable t) {unexpected(t);}
        System.out.printf("%nPassed = %d, failed = %d%n%n", passed, failed);
        if (failed > 0) throw new AssertionError("Some tests failed");}
}
