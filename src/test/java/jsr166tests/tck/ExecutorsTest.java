// from gee.cs.oswego.edu/home/jsr166/jsr166

package jsr166tests.tck;

/*
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 * Other contributors include Andrew Wright, Jeffrey Hayes,
 * Pat Fisher, Mike Judd.
 */


import junit.framework.*;
import java.util.*;
import java.util.concurrent.*;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import java.math.BigInteger;
import java.security.*;

public class ExecutorsTest extends JSR166TestCase {
    public static void main(String[] args) {
        junit.textui.TestRunner.run (suite());
    }
    public static Test suite() {
        return new TestSuite(ExecutorsTest.class);
    }

    /**
     * A newCachedThreadPool can execute runnables
     */
    public void testNewCachedThreadPool1() {
        ExecutorService e = Executors.newCachedThreadPool();
        e.execute(new NoOpRunnable());
        e.execute(new NoOpRunnable());
        e.execute(new NoOpRunnable());
        joinPool(e);
    }

    /**
     * A newCachedThreadPool with given ThreadFactory can execute runnables
     */
    public void testNewCachedThreadPool2() {
        ExecutorService e = Executors.newCachedThreadPool(new SimpleThreadFactory());
        e.execute(new NoOpRunnable());
        e.execute(new NoOpRunnable());
        e.execute(new NoOpRunnable());
        joinPool(e);
    }

    /**
     * A newCachedThreadPool with null ThreadFactory throws NPE
     */
    public void testNewCachedThreadPool3() {
        try {
            ExecutorService e = Executors.newCachedThreadPool(null);
            shouldThrow();
        } catch (NullPointerException success) {}
    }


    /**
     * A new SingleThreadExecutor can execute runnables
     */
    public void testNewSingleThreadExecutor1() {
        ExecutorService e = Executors.newSingleThreadExecutor();
        e.execute(new NoOpRunnable());
        e.execute(new NoOpRunnable());
        e.execute(new NoOpRunnable());
        joinPool(e);
    }

    /**
     * A new SingleThreadExecutor with given ThreadFactory can execute runnables
     */
    public void testNewSingleThreadExecutor2() {
        ExecutorService e = Executors.newSingleThreadExecutor(new SimpleThreadFactory());
        e.execute(new NoOpRunnable());
        e.execute(new NoOpRunnable());
        e.execute(new NoOpRunnable());
        joinPool(e);
    }

    /**
     * A new SingleThreadExecutor with null ThreadFactory throws NPE
     */
    public void testNewSingleThreadExecutor3() {
        try {
            ExecutorService e = Executors.newSingleThreadExecutor(null);
            shouldThrow();
        } catch (NullPointerException success) {}
    }

    /**
     * A new SingleThreadExecutor cannot be casted to concrete implementation
     */
    public void testCastNewSingleThreadExecutor() {
        ExecutorService e = Executors.newSingleThreadExecutor();
        try {
            ThreadPoolExecutor tpe = (ThreadPoolExecutor)e;
            shouldThrow();
        } catch (ClassCastException success) {
        } finally {
            joinPool(e);
        }
    }


    /**
     * A new newFixedThreadPool can execute runnables
     */
    public void testNewFixedThreadPool1() {
        ExecutorService e = Executors.newFixedThreadPool(2);
        e.execute(new NoOpRunnable());
        e.execute(new NoOpRunnable());
        e.execute(new NoOpRunnable());
        joinPool(e);
    }

    /**
     * A new newFixedThreadPool with given ThreadFactory can execute runnables
     */
    public void testNewFixedThreadPool2() {
        ExecutorService e = Executors.newFixedThreadPool(2, new SimpleThreadFactory());
        e.execute(new NoOpRunnable());
        e.execute(new NoOpRunnable());
        e.execute(new NoOpRunnable());
        joinPool(e);
    }

    /**
     * A new newFixedThreadPool with null ThreadFactory throws NPE
     */
    public void testNewFixedThreadPool3() {
        try {
            ExecutorService e = Executors.newFixedThreadPool(2, null);
            shouldThrow();
        } catch (NullPointerException success) {}
    }

    /**
     * A new newFixedThreadPool with 0 threads throws IAE
     */
    public void testNewFixedThreadPool4() {
        try {
            ExecutorService e = Executors.newFixedThreadPool(0);
            shouldThrow();
        } catch (IllegalArgumentException success) {}
    }


    /**
     * An unconfigurable newFixedThreadPool can execute runnables
     */
    public void testunconfigurableExecutorService() {
        ExecutorService e = Executors.unconfigurableExecutorService(Executors.newFixedThreadPool(2));
        e.execute(new NoOpRunnable());
        e.execute(new NoOpRunnable());
        e.execute(new NoOpRunnable());
        joinPool(e);
    }

    /**
     * unconfigurableExecutorService(null) throws NPE
     */
    public void testunconfigurableExecutorServiceNPE() {
        try {
            ExecutorService e = Executors.unconfigurableExecutorService(null);
            shouldThrow();
        } catch (NullPointerException success) {}
    }

    /**
     * unconfigurableScheduledExecutorService(null) throws NPE
     */
    public void testunconfigurableScheduledExecutorServiceNPE() {
        try {
            ExecutorService e = Executors.unconfigurableScheduledExecutorService(null);
            shouldThrow();
        } catch (NullPointerException success) {}
    }


    /**
     * a newSingleThreadScheduledExecutor successfully runs delayed task
     */
    public void testNewSingleThreadScheduledExecutor() throws Exception {
        TrackedCallable callable = new TrackedCallable();
        ScheduledExecutorService p1 = Executors.newSingleThreadScheduledExecutor();
        Future f = p1.schedule(callable, SHORT_DELAY_MS, MILLISECONDS);
        assertFalse(callable.done);
        Thread.sleep(MEDIUM_DELAY_MS);
        assertTrue(callable.done);
        assertEquals(Boolean.TRUE, f.get());
        joinPool(p1);
    }

    /**
     * a newScheduledThreadPool successfully runs delayed task
     */
    public void testnewScheduledThreadPool() throws Exception {
        TrackedCallable callable = new TrackedCallable();
        ScheduledExecutorService p1 = Executors.newScheduledThreadPool(2);
        Future f = p1.schedule(callable, SHORT_DELAY_MS, MILLISECONDS);
        assertFalse(callable.done);
        Thread.sleep(MEDIUM_DELAY_MS);
        assertTrue(callable.done);
        assertEquals(Boolean.TRUE, f.get());
        joinPool(p1);
    }

    /**
     * an unconfigurable newScheduledThreadPool successfully runs delayed task
     */
    public void testunconfigurableScheduledExecutorService() throws Exception {
        TrackedCallable callable = new TrackedCallable();
        ScheduledExecutorService p1 = Executors.unconfigurableScheduledExecutorService(Executors.newScheduledThreadPool(2));
        Future f = p1.schedule(callable, SHORT_DELAY_MS, MILLISECONDS);
        assertFalse(callable.done);
        Thread.sleep(MEDIUM_DELAY_MS);
        assertTrue(callable.done);
        assertEquals(Boolean.TRUE, f.get());
        joinPool(p1);
    }

    /**
     *  Future.get on submitted tasks will time out if they compute too long.
     */
    public void testTimedCallable() throws Exception {
        final Runnable sleeper =
            new RunnableShouldThrow(InterruptedException.class) {
                public void realRun() throws InterruptedException {
                    Thread.sleep(LONG_DELAY_MS);
                }};
        for (ExecutorService executor :
                 new ExecutorService[] {
                     Executors.newSingleThreadExecutor(),
                     Executors.newCachedThreadPool(),
                     Executors.newFixedThreadPool(2),
                     Executors.newScheduledThreadPool(2),
                 }) {
            try {
                Future future = executor.submit(sleeper);
                try {
                    future.get(SHORT_DELAY_MS, MILLISECONDS);
                    shouldThrow();
                } catch (TimeoutException success) {
                } finally {
                    future.cancel(true);
                }
            }
            finally {
                joinPool(executor);
            }
        }
    }


    /**
     * ThreadPoolExecutor using defaultThreadFactory has
     * specified group, priority, daemon status, and name
     */
    public void testDefaultThreadFactory() throws Exception {
        final ThreadGroup egroup = Thread.currentThread().getThreadGroup();
        Runnable r = new Runnable() {
                public void run() {
                    try {
                        Thread current = Thread.currentThread();
                        threadAssertTrue(!current.isDaemon());
                        threadAssertTrue(current.getPriority() <= Thread.NORM_PRIORITY);
                        ThreadGroup g = current.getThreadGroup();
                        SecurityManager s = System.getSecurityManager();
                        if (s != null)
                            threadAssertTrue(g == s.getThreadGroup());
                        else
                            threadAssertTrue(g == egroup);
                        String name = current.getName();
                        threadAssertTrue(name.endsWith("thread-1"));
                    } catch (SecurityException ok) {
                        // Also pass if not allowed to change setting
                    }
                }
            };
        ExecutorService e = Executors.newSingleThreadExecutor(Executors.defaultThreadFactory());

        e.execute(r);
        try {
            e.shutdown();
        } catch (SecurityException ok) {
        }

        try {
            Thread.sleep(SHORT_DELAY_MS);
        } finally {
            joinPool(e);
        }
    }

    /**
     * ThreadPoolExecutor using privilegedThreadFactory has
     * specified group, priority, daemon status, name,
     * access control context and context class loader
     */
    public void testPrivilegedThreadFactory() throws Exception {
        Policy savedPolicy = null;
        try {
            savedPolicy = Policy.getPolicy();
            AdjustablePolicy policy = new AdjustablePolicy();
            policy.addPermission(new RuntimePermission("getContextClassLoader"));
            policy.addPermission(new RuntimePermission("setContextClassLoader"));
            Policy.setPolicy(policy);
        } catch (AccessControlException ok) {
            return;
        }
        final ThreadGroup egroup = Thread.currentThread().getThreadGroup();
        final ClassLoader thisccl = Thread.currentThread().getContextClassLoader();
        final AccessControlContext thisacc = AccessController.getContext();
        Runnable r = new Runnable() {
                public void run() {
                    try {
                        Thread current = Thread.currentThread();
                        threadAssertTrue(!current.isDaemon());
                        threadAssertTrue(current.getPriority() <= Thread.NORM_PRIORITY);
                        ThreadGroup g = current.getThreadGroup();
                        SecurityManager s = System.getSecurityManager();
                        if (s != null)
                            threadAssertTrue(g == s.getThreadGroup());
                        else
                            threadAssertTrue(g == egroup);
                        String name = current.getName();
                        threadAssertTrue(name.endsWith("thread-1"));
                        threadAssertTrue(thisccl == current.getContextClassLoader());
                        threadAssertTrue(thisacc.equals(AccessController.getContext()));
                    } catch (SecurityException ok) {
                        // Also pass if not allowed to change settings
                    }
                }
            };
        ExecutorService e = Executors.newSingleThreadExecutor(Executors.privilegedThreadFactory());

        Policy.setPolicy(savedPolicy);
        e.execute(r);
        try {
            e.shutdown();
        } catch (SecurityException ok) {
        }
        try {
            Thread.sleep(SHORT_DELAY_MS);
        } finally {
            joinPool(e);
        }
    }

    void checkCCL() {
        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            sm.checkPermission(new RuntimePermission("setContextClassLoader"));
            sm.checkPermission(new RuntimePermission("getClassLoader"));
        }
    }

    class CheckCCL implements Callable<Object> {
        public Object call() {
            checkCCL();
            return null;
        }
    }


    /**
     * Without class loader permissions, creating
     * privilegedCallableUsingCurrentClassLoader throws ACE
     */
    public void testCreatePrivilegedCallableUsingCCLWithNoPrivs() {
        Policy savedPolicy = null;
        try {
            savedPolicy = Policy.getPolicy();
            AdjustablePolicy policy = new AdjustablePolicy();
            Policy.setPolicy(policy);
        } catch (AccessControlException ok) {
            return;
        }

        // Check if program still has too many permissions to run test
        try {
            checkCCL();
            // too many privileges to test; so return
            Policy.setPolicy(savedPolicy);
            return;
        } catch (AccessControlException ok) {
        }

        try {
            Callable task = Executors.privilegedCallableUsingCurrentClassLoader(new NoOpCallable());
            shouldThrow();
        } catch (AccessControlException success) {
        } finally {
            Policy.setPolicy(savedPolicy);
        }
    }

    /**
     * With class loader permissions, calling
     * privilegedCallableUsingCurrentClassLoader does not throw ACE
     */
    public void testprivilegedCallableUsingCCLWithPrivs() throws Exception {
        Policy savedPolicy = null;
        try {
            savedPolicy = Policy.getPolicy();
            AdjustablePolicy policy = new AdjustablePolicy();
            policy.addPermission(new RuntimePermission("getContextClassLoader"));
            policy.addPermission(new RuntimePermission("setContextClassLoader"));
            Policy.setPolicy(policy);
        } catch (AccessControlException ok) {
            return;
        }

        try {
            Callable task = Executors.privilegedCallableUsingCurrentClassLoader(new NoOpCallable());
            task.call();
        }
        finally {
            Policy.setPolicy(savedPolicy);
        }
    }

    /**
     * Without permissions, calling privilegedCallable throws ACE
     */
    public void testprivilegedCallableWithNoPrivs() throws Exception {
        Callable task;
        Policy savedPolicy = null;
        AdjustablePolicy policy = null;
        AccessControlContext noprivAcc = null;
        try {
            savedPolicy = Policy.getPolicy();
            policy = new AdjustablePolicy();
            Policy.setPolicy(policy);
            noprivAcc = AccessController.getContext();
            task = Executors.privilegedCallable(new CheckCCL());
            Policy.setPolicy(savedPolicy);
        } catch (AccessControlException ok) {
            return; // program has too few permissions to set up test
        }

        // Make sure that program doesn't have too many permissions
        try {
            AccessController.doPrivileged(new PrivilegedAction() {
                    public Object run() {
                        checkCCL();
                        return null;
                    }}, noprivAcc);
            // too many permssions; skip test
            return;
        } catch (AccessControlException ok) {
        }

        try {
            task.call();
            shouldThrow();
        } catch (AccessControlException success) {}
    }

    /**
     * With permissions, calling privilegedCallable succeeds
     */
    public void testprivilegedCallableWithPrivs() throws Exception {
        Policy savedPolicy = null;
        try {
            savedPolicy = Policy.getPolicy();
            AdjustablePolicy policy = new AdjustablePolicy();
            policy.addPermission(new RuntimePermission("getContextClassLoader"));
            policy.addPermission(new RuntimePermission("setContextClassLoader"));
            Policy.setPolicy(policy);
        } catch (AccessControlException ok) {
            return;
        }

        Callable task = Executors.privilegedCallable(new CheckCCL());
        try {
            task.call();
        } finally {
            Policy.setPolicy(savedPolicy);
        }
    }

    /**
     * callable(Runnable) returns null when called
     */
    public void testCallable1() throws Exception {
        Callable c = Executors.callable(new NoOpRunnable());
        assertNull(c.call());
    }

    /**
     * callable(Runnable, result) returns result when called
     */
    public void testCallable2() throws Exception {
        Callable c = Executors.callable(new NoOpRunnable(), one);
        assertSame(one, c.call());
    }

    /**
     * callable(PrivilegedAction) returns its result when called
     */
    public void testCallable3() throws Exception {
        Callable c = Executors.callable(new PrivilegedAction() {
                public Object run() { return one; }});
        assertSame(one, c.call());
    }

    /**
     * callable(PrivilegedExceptionAction) returns its result when called
     */
    public void testCallable4() throws Exception {
        Callable c = Executors.callable(new PrivilegedExceptionAction() {
                public Object run() { return one; }});
        assertSame(one, c.call());
    }


    /**
     * callable(null Runnable) throws NPE
     */
    public void testCallableNPE1() {
        try {
            Callable c = Executors.callable((Runnable) null);
            shouldThrow();
        } catch (NullPointerException success) {}
    }

    /**
     * callable(null, result) throws NPE
     */
    public void testCallableNPE2() {
        try {
            Callable c = Executors.callable((Runnable) null, one);
            shouldThrow();
        } catch (NullPointerException success) {}
    }

    /**
     * callable(null PrivilegedAction) throws NPE
     */
    public void testCallableNPE3() {
        try {
            Callable c = Executors.callable((PrivilegedAction) null);
            shouldThrow();
        } catch (NullPointerException success) {}
    }

    /**
     * callable(null PrivilegedExceptionAction) throws NPE
     */
    public void testCallableNPE4() {
        try {
            Callable c = Executors.callable((PrivilegedExceptionAction) null);
            shouldThrow();
        } catch (NullPointerException success) {}
    }


}
