// from gee.cs.oswego.edu/home/jsr166/jsr166

package jsr166tests.loops;

/*
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;

class Mutex100M {
    public static void main(String[] args) throws Exception {
        int x = loop((int) System.nanoTime(), 100000);
        x = loop(x, 100000);
        x = loop(x, 100000);
        long start = System.nanoTime();
        x = loop(x, 100000000);
        if (x == 0) System.out.print(" ");
        long time = System.nanoTime() - start;
        double secs = (double) time / 1000000000.0;
        System.out.println("time: " + secs);
        start = System.nanoTime();
        x = loop(x, 100000000);
        if (x == 0) System.out.print(" ");
        time = System.nanoTime() - start;
        secs = (double) time / 1000000000.0;
        System.out.println("time: " + secs);

    }

    static final Mutex100M.Mutex lock = new Mutex100M.Mutex();

    static int loop(int x, int iters) {
        final Mutex100M.Mutex l = lock;
        for (int i = iters; i > 0; --i) {
            l.lock();
            x = x * 134775813 + 1;
            l.unlock();
        }
        return x;
    }


    static final class Mutex extends AbstractQueuedSynchronizer {
        public boolean isHeldExclusively() { return getState() == 1; }

        public boolean tryAcquire(int acquires) {
            return getState() == 0 && compareAndSetState(0, 1);
        }

        public boolean tryRelease(int releases) {
            setState(0);
            return true;
        }
        public Condition newCondition() { return new ConditionObject(); }

        public void lock() {
            if (!compareAndSetState(0, 1))
                acquire(1);
        }
        public boolean tryLock() {
            return tryAcquire(1);
        }
        public void lockInterruptibly() throws InterruptedException {
            acquireInterruptibly(1);
        }
        public boolean tryLock(long timeout, TimeUnit unit) throws InterruptedException {
            return tryAcquireNanos(1, unit.toNanos(timeout));
        }
        public void unlock() { release(1); }
    }

}
