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

public class ExchangeLoops {
    static final int NCPUS = Runtime.getRuntime().availableProcessors();

    static final int  DEFAULT_THREADS = NCPUS + 2;
    static final long DEFAULT_TRIAL_MILLIS   = 10000;

    public static void main(String[] args) throws Exception {
        int maxThreads = DEFAULT_THREADS;
        long trialMillis = DEFAULT_TRIAL_MILLIS;
        int nReps = 3;

        // Parse and check args
        int argc = 0;
        while (argc < args.length) {
            String option = args[argc++];
            if (option.equals("-t"))
                trialMillis = Integer.parseInt(args[argc]);
            else if (option.equals("-r"))
                nReps = Integer.parseInt(args[argc]);
            else
                maxThreads = Integer.parseInt(option);
            argc++;
        }

	// Display runtime parameters
	System.out.print("ExchangeTest");
	System.out.print(" -t " + trialMillis);
        System.out.print(" -r " + nReps);
	System.out.print(" max threads " + maxThreads);
	System.out.println();
        long warmupTime = 2000;
        long sleepTime = 100;
        int nw = (maxThreads >= 3) ? 3 : 2;

        System.out.println("Warmups..");
        oneRun(3, warmupTime);
        Thread.sleep(sleepTime);

        for (int i = maxThreads; i >= 2; i -= 1) {
            oneRun(i, warmupTime++);
            //            System.gc();
            Thread.sleep(sleepTime);
        }

        /*
        for (int i = maxThreads; i >= 2; i -= 1) {
            oneRun(i, warmupTime++);
        }
        */

        for (int j = 0; j < nReps; ++j) {
            System.out.println("Trial: " + j);
            for (int i = 2; i <= maxThreads; i += 2) {
                oneRun(i, trialMillis);
                //                System.gc();
                Thread.sleep(sleepTime);
            }
            for (int i = maxThreads; i >= 2; i -= 2) {
                oneRun(i, trialMillis);
                //                System.gc();
                Thread.sleep(sleepTime);
            }
            Thread.sleep(sleepTime);
        }


    }

    static void oneRun(int nThreads, long trialMillis) throws Exception {
        System.out.printf("%4d threads", nThreads);
        Exchanger x = new Exchanger();
        Runner[] runners = new Runner[nThreads];
        Thread[] threads = new Thread[nThreads];
        for (int i = 0; i < nThreads; ++i) {
            runners[i] = new Runner(x);
            threads[i] = new Thread(runners[i]);
            //            int h = System.identityHashCode(threads[i]);
            //            h ^= h << 1;
            //            h ^= h >>> 3;
            //            h ^= h << 10;
            //            System.out.printf("%10x\n", h);
        }

        long startTime = System.nanoTime();
        for (int i = 0; i < nThreads; ++i) {
            threads[i].start();
        }
        Thread.sleep(trialMillis);
        for (int i = 0; i < nThreads; ++i)
            threads[i].interrupt();
        long elapsed = System.nanoTime() - startTime;
        for (int i = 0; i < nThreads; ++i)
            threads[i].join();
        int iters = 1;
        //        System.out.println();
        for (int i = 0; i < nThreads; ++i) {
            int ipr = runners[i].iters;
            //            System.out.println(ipr);
            iters += ipr;
        }
        long rate = iters * 1000L * 1000L * 1000L / elapsed;
        long npt = elapsed / iters;
        System.out.printf("%9dms", elapsed / (1000L * 1000L));
        System.out.printf("%9d it/s ", rate);
        System.out.printf("%9d ns/it", npt);
        System.out.println();
        //        x.printStats();
    }

    static final class Runner implements Runnable {
        final Exchanger exchanger;
        final Object mine = new Integer(2688);
        volatile int iters;
        Runner(Exchanger x) { this.exchanger = x; }

        public void run() {
            Exchanger x = exchanger;
            Object m = mine;
            int i = 0;
            try {
                for (;;) {
                    Object e = x.exchange(m);
                    if (e == null || e == m)
                        throw new Error();
                    m = e;
                    ++i;
                }
            } catch (InterruptedException ie) {
                iters = i;
            }
        }
    }
}
