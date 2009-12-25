// from gee.cs.oswego.edu/home/jsr166/jsr166

package jsr166tests.loops;

/*
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;
import java.util.concurrent.atomic.*;
import java.io.*;

/**
 * A sample user extension of AbstractQueuedSynchronizer.
 */
public final class Mutex extends AbstractQueuedSynchronizer implements Lock, java.io.Serializable {
    public boolean isHeldExclusively() { return getState() == 1; }

    public boolean tryAcquire(int acquires) {
        return compareAndSetState(0, 1);
    }

    public boolean tryRelease(int releases) {
        setState(0);
        return true;
    }
    public Condition newCondition() { return new ConditionObject(); }

    private void readObject(ObjectInputStream s) throws IOException, ClassNotFoundException {
        s.defaultReadObject();
        setState(0); // reset to unlocked state
    }

    public void lock() {
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
