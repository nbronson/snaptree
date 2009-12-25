// from gee.cs.oswego.edu/home/jsr166/jsr166

package jsr166tests.loops;

/*
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */
import java.util.concurrent.locks.*;

class ReadHoldingWriteLock {

    public static void main(String[] args) throws Exception {
        ReadHoldingWriteLock t = new ReadHoldingWriteLock();
        t.testReadAfterWriteLock();
        t.testReadHoldingWriteLock();
        t.testReadHoldingWriteLock2();
        t.testReadHoldingWriteLockFair();
        t.testReadHoldingWriteLockFair2();
    }

    static final long SHORT_DELAY_MS = 50;
    static final long MEDIUM_DELAY_MS = 200;

    void assertTrue(boolean b) {
        if (!b) throw new Error();
    }

    /**
     * Readlocks succeed after a writing thread unlocks
     */
    public void testReadAfterWriteLock() throws Exception {
	final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
	lock.writeLock().lock();
	Thread t1 = new Thread(new Runnable() {
                public void run() {
                    lock.readLock().lock();
                    lock.readLock().unlock();
		}
	    });
	Thread t2 = new Thread(new Runnable() {
                public void run() {
                    lock.readLock().lock();
                    lock.readLock().unlock();
		}
	    });

        t1.start();
        t2.start();
        Thread.sleep(SHORT_DELAY_MS);
        lock.writeLock().unlock();
        t1.join(MEDIUM_DELAY_MS);
        t2.join(MEDIUM_DELAY_MS);
        assertTrue(!t1.isAlive());
        assertTrue(!t2.isAlive());

    }

    /**
     * Read trylock succeeds if write locked by current thread
     */
    public void testReadHoldingWriteLock()throws Exception {
	final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
	lock.writeLock().lock();
        assertTrue(lock.readLock().tryLock());
        lock.readLock().unlock();
        lock.writeLock().unlock();
    }

    /**
     * Read lock succeeds if write locked by current thread even if
     * other threads are waiting
     */
    public void testReadHoldingWriteLock2() throws Exception{
	final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
	lock.writeLock().lock();
	Thread t1 = new Thread(new Runnable() {
                public void run() {
                    lock.readLock().lock();
                    lock.readLock().unlock();
		}
	    });
	Thread t2 = new Thread(new Runnable() {
                public void run() {
                    lock.readLock().lock();
                    lock.readLock().unlock();
		}
	    });

        t1.start();
        t2.start();
        lock.readLock().lock();
        lock.readLock().unlock();
        Thread.sleep(SHORT_DELAY_MS);
        lock.readLock().lock();
        lock.readLock().unlock();
        lock.writeLock().unlock();
        t1.join(MEDIUM_DELAY_MS);
        t2.join(MEDIUM_DELAY_MS);
        assertTrue(!t1.isAlive());
        assertTrue(!t2.isAlive());
    }

    /**
     * Fair Read trylock succeeds if write locked by current thread
     */
    public void testReadHoldingWriteLockFair() throws Exception{
	final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);
	lock.writeLock().lock();
        assertTrue(lock.readLock().tryLock());
        lock.readLock().unlock();
        lock.writeLock().unlock();
    }

    /**
     * Fair Read lock succeeds if write locked by current thread even if
     * other threads are waiting
     */
    public void testReadHoldingWriteLockFair2() throws Exception {
	final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);
	lock.writeLock().lock();
	Thread t1 = new Thread(new Runnable() {
                public void run() {
                    lock.readLock().lock();
                    lock.readLock().unlock();
		}
	    });
	Thread t2 = new Thread(new Runnable() {
                public void run() {
                    lock.readLock().lock();
                    lock.readLock().unlock();
		}
	    });

        t1.start();
        t2.start();
        lock.readLock().lock();
        lock.readLock().unlock();
        Thread.sleep(SHORT_DELAY_MS);
        lock.readLock().lock();
        lock.readLock().unlock();
        lock.writeLock().unlock();
        t1.join(MEDIUM_DELAY_MS);
        t2.join(MEDIUM_DELAY_MS);
        assertTrue(!t1.isAlive());
        assertTrue(!t2.isAlive());


    }
}
