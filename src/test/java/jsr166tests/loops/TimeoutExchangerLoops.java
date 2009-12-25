// from gee.cs.oswego.edu/home/jsr166/jsr166

package jsr166tests.loops;

/*
 * Written by Bill Scherer and Doug Lea with assistance from members
 * of JCP JSR-166 Expert Group and released to the public domain, as
 * explained at http://creativecommons.org/licenses/publicdomain
 */

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;

public class TimeoutExchangerLoops {
    static final int NCPUS = Runtime.getRuntime().availableProcessors();

    static final int  DEFAULT_THREADS = NCPUS + 2;
    static final long DEFAULT_PATIENCE_NANOS = 500000;
    static final long DEFAULT_TRIAL_MILLIS   = 10000;

    public static void main(String[] args) throws Exception {
        int maxThreads = DEFAULT_THREADS;
        long trialMillis = DEFAULT_TRIAL_MILLIS;
        long patienceNanos = DEFAULT_PATIENCE_NANOS;
        int nReps = 3;

        // Parse and check args
        int argc = 0;
        while (argc < args.length) {
            String option = args[argc++];
            if (option.equals("-t"))
                trialMillis = Integer.parseInt(args[argc]);
            else if (option.equals("-p"))
                patienceNanos = Long.parseLong(args[argc]);
            else if (option.equals("-r"))
                nReps = Integer.parseInt(args[argc]);
            else
                maxThreads = Integer.parseInt(option);
            argc++;
        }

	// Display runtime parameters
	System.out.print("TimeoutExchangerTest");
	System.out.print(" -t " + trialMillis);
	System.out.print(" -p " + patienceNanos);
        System.out.print(" -r " + nReps);
	System.out.print(" max threads " + maxThreads);
	System.out.println();

        System.out.println("Warmups..");
        long warmupTime = 1000;
        long sleepTime = 500;
        if (false) {
            for (int k = 0; k < 10; ++k) {
                for (int j = 0; j < 10; ++j) {
                    oneRun(2, (j + 1) * 1000, patienceNanos);
                    Thread.sleep(sleepTime);
                }
            }
        }

        oneRun(3, warmupTime, patienceNanos);
        Thread.sleep(sleepTime);

        for (int i = maxThreads; i >= 2; i -= 1) {
            oneRun(i, warmupTime, patienceNanos);
            Thread.sleep(sleepTime);
        }

        for (int j = 0; j < nReps; ++j) {
            System.out.println("Replication " + j);
            for (int i = 2; i <= maxThreads; i += 2) {
                oneRun(i, trialMillis, patienceNanos);
                Thread.sleep(sleepTime);
            }
        }
    }

    static void oneRun(int nThreads, long trialMillis, long patienceNanos)
        throws Exception {
        System.out.printf("%4d threads", nThreads);
        System.out.printf("%9dms", trialMillis);
        final CountDownLatch start = new CountDownLatch(1);
        Exchanger x = new Exchanger();
        Runner[] runners = new Runner[nThreads];
        Thread[] threads = new Thread[nThreads];
        for (int i = 0; i < nThreads; ++i) {
            runners[i] = new Runner(x, patienceNanos, start);
            threads[i] = new Thread(runners[i]);
            threads[i].start();
        }
        long startTime = System.nanoTime();
        start.countDown();
        Thread.sleep(trialMillis);
        for (int i = 0; i < nThreads; ++i)
            threads[i].interrupt();
        long elapsed = System.nanoTime() - startTime;
        for (int i = 0; i < nThreads; ++i)
            threads[i].join();
        int iters = 0;
        long fails = 0;
        for (int i = 0; i < nThreads; ++i) {
            iters += runners[i].iters;
            fails += runners[i].failures;
        }
        if (iters <= 0) iters = 1;
        long rate = iters * 1000L * 1000L * 1000L / elapsed;
        long npt = elapsed / iters;
        double failRate = (fails * 100.0) / (double) iters;
        System.out.printf("%9d it/s ", rate);
        System.out.printf("%9d ns/it", npt);
        System.out.printf("%9.5f%% fails", failRate);
        System.out.println();
        //        x.printStats();
    }


    static final class Runner implements Runnable {
        final Exchanger exchanger;
        final CountDownLatch start;
        final long patience;
        volatile int iters;
        volatile int failures;
        Runner(Exchanger x, long patience, CountDownLatch start) {
            this.exchanger = x;
            this.patience = patience;
            this.start = start;
        }

        public void run() {
            int i = 0;
            try {
                Exchanger x = exchanger;
                Object m = new Integer(17);
                long p = patience;
                start.await();
                for (;;) {
                    try {
                        Object e = x.exchange(m, p, TimeUnit.NANOSECONDS);
                        if (e == null || e == m)
                            throw new Error();
                        m = e;
                        ++i;
                    } catch (TimeoutException to) {
                        if (Thread.interrupted()) {
                            iters = i;
                            return;
                        }
                        ++i;
                        ++failures;
                    }
                }
            } catch (InterruptedException ie) {
                iters = i;
            }
        }
    }
}
