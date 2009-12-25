// from gee.cs.oswego.edu/home/jsr166/jsr166

package jsr166tests.loops;

/*
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */

import java.util.concurrent.*;
import java.util.concurrent.locks.*;
import java.util.concurrent.atomic.*;
import java.util.*;

public final class ContextSwitchTest {
    final static int iters = 1000000;
    static AtomicReference turn = new AtomicReference();
    public static void main(String[] args) throws Exception {
        test();
        test();
        test();
    }

    static void test() throws Exception {
        MyThread a = new MyThread();
        MyThread b = new MyThread();
        a.other = b;
        b.other = a;
        turn.set(a);
        long startTime = System.nanoTime();
        a.start();
        b.start();
        a.join();
        b.join();
        long endTime = System.nanoTime();
        int np = a.nparks + b.nparks;
        System.out.println("Average time: " +
                           ((endTime - startTime) / np) +
                           "ns");
    }

    static final class MyThread extends Thread {
        volatile Thread other;
        volatile int nparks;

        public void run() {
            final AtomicReference t = turn;
            final Thread other = this.other;
            if (turn == null || other == null)
                throw new NullPointerException();
            int p = 0;
            for (int i = 0; i < iters; ++i) {
                while (!t.compareAndSet(other, this)) {
                    LockSupport.park();
                    ++p;
                }
                LockSupport.unpark(other);
            }
            LockSupport.unpark(other);
            nparks = p;
            System.out.println("parks: " + p);

        }
    }
}
