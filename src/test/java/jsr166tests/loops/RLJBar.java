// from gee.cs.oswego.edu/home/jsr166/jsr166

package jsr166tests.loops;

/*
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */

// Yet another contended object monitor throughput test
// adapted from bug reports

import java.util.*;
import java.lang.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;

class Producer extends Thread
{
    //  private static Hashtable buddiesOnline = new Hashtable();
  private static Map buddiesOnline = new ConcurrentHashMap();
  public Producer (String name) { super(name); }

  public void run()
    {
      Object key = null ;
      final ReentrantLock dr = RLJBar.DeathRow;
      final ReentrantLock bar = RLJBar.bar;
      final ReentrantLock end = RLJBar.End;
      final Condition endCondition = RLJBar.EndCondition;
      if (RLJBar.OneKey) key = new Integer(0) ; 	// per-thread v. per iteration

      // The barrier has a number of interesting effects:
      // 1.	It enforces full LWP provisioning on T1.
      //		(nearly all workers park concurrently).
      // 2.	It gives the C2 compiler thread(s) a chance to run.
      //		By transiently quiescing the workings the C2 threads
      //		might avoid starvation.
      //

      try {
          bar.lock();
          try {
              ++RLJBar.nUp ;
              if (RLJBar.nUp == RLJBar.nThreads) {
                  if (RLJBar.quiesce != 0) {
                      RLJBar.barCondition.awaitNanos(RLJBar.quiesce * 1000000) ;
                  }
                  RLJBar.epoch = System.currentTimeMillis () ;
                  RLJBar.barCondition.signalAll () ;
                  //                  System.out.print ("Consensus ") ;
              }
              if (RLJBar.UseBar) {
                  while (RLJBar.nUp != RLJBar.nThreads) {
                      RLJBar.barCondition.await () ;
                  }
              }
          }
          finally {
              bar.unlock();
          }
      } catch (Exception ex) {
        System.out.println ("Exception in barrier: " + ex) ;
      }

      // Main execution time ... the code being timed ...
      // HashTable.get() is highly contended (serial).
      for (int loop = 1; loop < 100000 ;loop++) {
        if (!RLJBar.OneKey) key = new Integer(0) ;
        buddiesOnline.get(key);
      }

      // Mutator epilog:
      // The following code determines if the test will/wont include (measure)
      // thread death time.

      end.lock();
      try {
        ++RLJBar.nDead ;
        if (RLJBar.nDead == RLJBar.nUp) {
            //          System.out.print((System.currentTimeMillis()-RLJBar.epoch) + " ms") ;
          endCondition.signalAll() ;
        }
      }
      finally {
          end.unlock();
      }
      dr.lock();
      dr.unlock();
    }
}


public class RLJBar				// ProdConsTest
{

    public static final int ITERS = 10;
  public static boolean OneKey = false ; 			// alloc once or once per iteration

  public static boolean UseBar = false ;
  public static int nThreads = 100 ;
  public static int nUp = 0 ;
  public static int nDead = 0 ;
  public static ReentrantLock bar = new ReentrantLock() ;
  public static Condition barCondition = bar.newCondition() ;
  public static long epoch ;
  public static ReentrantLock DeathRow = new ReentrantLock () ;
  public static ReentrantLock End = new ReentrantLock () ;
  public static int quiesce = 0 ;
  public static Condition EndCondition = End.newCondition();

  public static void main (String[] args)    {
      int argix = 0 ;
      if (argix < args.length && args[argix].equals("-o")) {
          ++argix ;
          OneKey = true ;
          System.out.println ("OneKey") ;
      }
      if (argix < args.length && args[argix].equals ("-b")) {
          ++argix ;
          UseBar = true ;
          System.out.println ("UseBar") ;
      }
      if (argix < args.length && args[argix].equals ("-q")) {
          ++argix ;
          if (argix < args.length) {
              quiesce = Integer.parseInt (args[argix++]) ;
              System.out.println ("Quiesce " + quiesce + " msecs") ;
          }
      }
      for (int k = 0; k < ITERS; ++k)
          oneRun();
  }

    public static void oneRun() {
        DeathRow = new ReentrantLock () ;
        End = new ReentrantLock () ;
        EndCondition = End.newCondition();

        nDead = nUp = 0 ;
        long cyBase = System.currentTimeMillis () ;
        DeathRow.lock();
        try {
            for (int i = 1; i <= nThreads ; i++) {
                new Producer("Producer" + i).start();
            }
            try {
                End.lock();
                try {
                    while (nDead != nThreads)
                        EndCondition.await() ;
                }
                finally {
                    End.unlock();
                }
            } catch (Exception ex) {
                System.out.println ("Exception in End: " + ex) ;
            }
        }
        finally {
            DeathRow.unlock();
        }
        System.out.println ("Outer time: " + (System.currentTimeMillis()-cyBase)) ;

        // Let workers quiesce/exit.
        try { Thread.sleep (1000) ; } catch (Exception ex) {} ;
    }
}
