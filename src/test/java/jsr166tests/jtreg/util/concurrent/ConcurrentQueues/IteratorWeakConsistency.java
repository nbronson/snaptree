// from gee.cs.oswego.edu/home/jsr166/jsr166

package jsr166tests.jtreg.util.concurrent.ConcurrentQueues;

/*
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */

import java.util.*;
import java.util.concurrent.*;

/*
 * @test
 * @bug 6805775 6815766
 * @summary Check weak consistency of concurrent queue iterators
 */

@SuppressWarnings({"unchecked", "rawtypes"})
public class IteratorWeakConsistency {

    void test(String[] args) throws Throwable {
        test(new LinkedBlockingQueue());
        test(new LinkedBlockingQueue(20));
        test(new LinkedBlockingDeque());
        test(new LinkedBlockingDeque(20));
        test(new ConcurrentLinkedQueue());
        //test(new LinkedTransferQueue());
        // Other concurrent queues (e.g. ArrayBlockingQueue) do not
        // currently have weakly consistent iterators.
        // test(new ArrayBlockingQueue(20));
    }

    void test(Queue q) throws Throwable {
        // TODO: make this more general
        for (int i = 0; i < 10; i++)
            q.add(i);
        Iterator it = q.iterator();
        q.poll();
        q.poll();
        q.poll();
        q.remove(7);
        List list = new ArrayList();
        while (it.hasNext())
            list.add(it.next());
        equal(list, Arrays.asList(0, 3, 4, 5, 6, 8, 9));
        check(! list.contains(null));
        System.out.printf("%s: %s%n",
                          q.getClass().getSimpleName(),
                          list);
    }

    //--------------------- Infrastructure ---------------------------
    volatile int passed = 0, failed = 0;
    void pass() {passed++;}
    void fail() {failed++; Thread.dumpStack();}
    void fail(String msg) {System.err.println(msg); fail();}
    void unexpected(Throwable t) {failed++; t.printStackTrace();}
    void check(boolean cond) {if (cond) pass(); else fail();}
    void equal(Object x, Object y) {
        if (x == null ? y == null : x.equals(y)) pass();
        else fail(x + " not equal to " + y);}
    static Class<?> thisClass = new Object(){}.getClass().getEnclosingClass();
    public static void main(String[] args) throws Throwable {
        new IteratorWeakConsistency().instanceMain(args);}
    public void instanceMain(String[] args) throws Throwable {
        try {test(args);} catch (Throwable t) {unexpected(t);}
        System.out.printf("%nPassed = %d, failed = %d%n%n", passed, failed);
        if (failed > 0) throw new AssertionError("Some tests failed");}
}
