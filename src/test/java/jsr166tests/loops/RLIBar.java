// from gee.cs.oswego.edu/home/jsr166/jsr166

package jsr166tests.loops;

/*
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */
// Adapted from code that was in turn
// Derived from SocketPerformanceTest.java - BugID: 4763450
//
//

import java.io.*;
import java.net.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;

public class RLIBar {

    static int batchLimit ;
    static int mseq ;
    static int nReady ;
    static int ExThreads ;
    static int ASum ;
    static final ReentrantLock Gate = new ReentrantLock () ;
    static final Condition GateCond = Gate.newCondition () ;

    static final ReentrantLock HoldQ = new ReentrantLock () ;
    static final Condition HoldQCond = HoldQ.newCondition() ;
    static boolean Hold = false ;
    static int HoldPop ;
    static int HoldLimit ;

    static private boolean HoldCheck () {
        try {
            HoldQ.lock();
            try {
                if (!Hold) return false;
                else {
                    ++HoldPop ;
                    if (HoldPop >= HoldLimit) {
                        System.out.print ("Holding ") ;
                        Thread.sleep (1000) ;
                        System.out.println () ;
                        Hold = false ;
                        HoldQCond.signalAll () ;
                    }
                    else
                        while (Hold)
                            HoldQCond.await() ;

                    if (--HoldPop == 0) HoldQCond.signalAll () ;
                    return true;
                }
            }
            finally {
                HoldQ.unlock();
            }
        } catch (Exception Ex) {
            System.out.println ("Unexpected exception in Hold: " + Ex) ;
            return false;
        }
    }

    private static class Server {
        private int nClients;
        final ReentrantLock thisLock = new ReentrantLock();
        final Condition thisCond = thisLock.newCondition();

        Server (int nClients) {
            this.nClients = nClients;
            try {
                for (int i = 0; i < nClients; ++i) {
                    final int fix = i ;
                    new Thread() { public void run () { runServer(fix); }}.start();
                }
            } catch (Exception e) {
                System.err.println(e) ;
            }
        }

        // the total number of messages received by all server threads
        // on this server
        int msgsReceived = 0;

        // incremented each time we get a complete batch of requests
        private int currentBatch = 0;

        // the number of requests received since the last time currentBatch
        // was incremented
        private int currentBatchSize = 0;

        private void runServer (int id) {
            int msg ;
            boolean held = false;
            final ReentrantLock thisLock = this.thisLock;
            final Condition thisCond = this.thisCond;

            try {

                // Startup barrier - rendezvous - wait for all threads.
                // Forces all threads to park on their LWPs, ensuring
                // proper provisioning on T1.
                // Alternately, use THR_BOUND threads
                Gate.lock(); try {
                    ++nReady ;
                    if (nReady == ExThreads ) {
                        GateCond.signalAll () ;
                    }
                    while (nReady != ExThreads )
                        GateCond.await() ;
                } finally { Gate.unlock(); }

                for (;;) {
                    //                    if (!held && currentBatchSize == 0) held = HoldCheck () ;
                    msg = (++ mseq) ^ id ;
                    thisLock.lock();
                    try {
                        ASum += msg ;
                        ++msgsReceived;
                        int myBatch = currentBatch;
                        if (++currentBatchSize >= batchLimit) {
                            // this batch is full, start a new one ...
                            ++currentBatch;
                            currentBatchSize = 0;
                            // and wake up everyone in this one
                            thisCond.signalAll () ;
                        }
                        // Wait until our batch is complete
                        while (myBatch == currentBatch)
                            thisCond.await();
                    }
                    finally {
                        thisLock.unlock();
                    }
                }
            } catch (Exception e) {
                System.err.println("Server thread: exception "  + e) ;
                e.printStackTrace();
            }
        }


    }

    public static void main (String[] args) throws Exception {
        int nServers = 10 ;
        int nClients = 10 ;
        int samplePeriod = 10000;
        int nSamples = 5;

        int nextArg = 0;
        while (nextArg < args.length) {
            String arg = args[nextArg++];
            if (arg.equals("-nc"))
                nClients = Integer.parseInt(args[nextArg++]);
            else if (arg.equals("-ns"))
                nServers = Integer.parseInt(args[nextArg++]);
            else if (arg.equals("-batch"))
                batchLimit = Integer.parseInt(args[nextArg++]);
            else if (arg.equals("-sample"))
                samplePeriod = Integer.parseInt(args[nextArg++]);
            else if (arg.equals("-np"))
                nSamples = Integer.parseInt(args[nextArg++]);
            else {
                System.err.println ("Argument error:" + arg) ;
                System.exit (1) ;
            }
        }
        if (nClients <= 0 || nServers <= 0 || samplePeriod <= 0 || batchLimit > nClients) {
            System.err.println ("Argument error") ;
            System.exit (1) ;
        }

        // default batch size is 2/3 the number of clients
        // (for no particular reason)
        if (false && batchLimit <= 0)
            batchLimit = (2 * nClients + 1) / 3;

        ExThreads = nServers * nClients ; 	// expected # of threads
        HoldLimit = ExThreads ;

        // start up all threads
        Server[] servers = new Server[nServers];
        for (int i = 0; i < nServers; ++i) {
            servers[i] = new Server(nClients);
        }

        // Wait for consensus
        try {
            Gate.lock(); try {
                while (nReady != ExThreads ) GateCond.await() ;
            } finally { Gate.unlock(); }
        } catch (Exception ex) {
            System.out.println (ex);
        }
        System.out.println (
                            nReady + " Ready: nc=" + nClients + " ns=" + nServers + " batch=" + batchLimit) ;

        // Start sampling ...
        // Methodological problem: all the mutator threads
        // can starve the compiler threads, resulting in skewed scores.
        // In theory, over time, the scores will improve as the compiler
        // threads are granted CPU cycles, but in practice a "warm up" phase
        // might be good idea to help C2.  For this reason I've implemented
        // the "Hold" facility.

        long lastNumMsgs = 0;
        long sampleStart = System.currentTimeMillis();
        for (int j = 0; j < nSamples; ++j) {
            // when this sample period is supposed to end
            long sampleEnd = sampleStart + samplePeriod;
            for (;;) {
                long now = System.currentTimeMillis();
                if (now >= sampleEnd) {
                    // when it really did end
                    sampleEnd = now;
                    break;
                }
                Thread.sleep(sampleEnd - now);
            }

            if (false && j == 2) {
                System.out.print ("Hold activated ...") ;
                HoldQ.lock();
                try  {
                    Hold = true ;
                    while (Hold) HoldQCond.await() ;
                }
                finally {
                    HoldQ.unlock();
                }
            }



            // there's no synchronization here, so the total i get is
            // approximate, but that's OK since any i miss for this
            // sample will get credited to the next sample, and on average
            // we'll be right
            long numMsgs = 0;
            for (int i = 0; i < nServers; ++i)
                numMsgs += servers[i].msgsReceived;
            long deltaMsgs = numMsgs - lastNumMsgs;
            long deltaT = sampleEnd - sampleStart;
            if (true || j != 2) { 	// Don't report results if we issued a hold ...
                System.out.print(
                                 "Sample period = " + deltaT + " ms; "
                                 + "New msgs rcvd = " + deltaMsgs + "; "
                                 + "Throughput = " + (deltaMsgs*1000 / deltaT) + " msg/sec\n");
                //                for (int i = 0; i < nServers; ++i)
                //                    servers[i].thisLock.dump();
            }
            sampleStart = sampleEnd;
            lastNumMsgs = numMsgs;
        }
        System.exit(0);
    }
}
