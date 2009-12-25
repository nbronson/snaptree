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
 * A sample user extension of AbstractQueuedLongSynchronizer.
 */
public final class LongMutex extends AbstractQueuedLongSynchronizer implements Lock, java.io.Serializable {
    static final long LOCKED = -1L;
    public boolean isHeldExclusively() { return getState() == LOCKED; }

    public boolean tryAcquire(long acquires) {
        return compareAndSetState(0, LOCKED);
    }

    public boolean tryRelease(long releases) {
        setState(0);
        return true;
    }
    public Condition newCondition() { return new ConditionObject(); }

    private void readObject(ObjectInputStream s) throws IOException, ClassNotFoundException {
        s.defaultReadObject();
        setState(0); // reset to unlocked state
    }

    public void lock() {
        acquire(LOCKED);
    }
    public boolean tryLock() {
        return tryAcquire(LOCKED);
    }
    public void lockInterruptibly() throws InterruptedException {
        acquireInterruptibly(LOCKED);
    }
    public boolean tryLock(long timeout, TimeUnit unit) throws InterruptedException {
        return tryAcquireNanos(LOCKED, unit.toNanos(timeout));
    }
    public void unlock() { release(LOCKED); }
}
