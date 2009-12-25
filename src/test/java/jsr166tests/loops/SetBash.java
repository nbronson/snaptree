// from gee.cs.oswego.edu/home/jsr166/jsr166

package jsr166tests.loops;

/*
 * Written by Doug Lea and Josh Bloch with assistance from members of
 * JCP JSR-166 Expert Group and released to the public domain, as
 * explained at http://creativecommons.org/licenses/publicdomain
 */
import java.util.*;

public class SetBash {
    static Random rnd = new Random();

    public static void main(String[] args) {
	int numItr = Integer.parseInt(args[1]);
	int setSize = Integer.parseInt(args[2]);
	Class cl = null;

	try {
	    cl = Class.forName(args[0]);
	} catch (ClassNotFoundException e) {
	    fail("Class " + args[0] + " not found.");
	}

        boolean synch = (args.length>3);

	for (int i=0; i<numItr; i++) {
	    Set s1 = newSet(cl, synch);
	    AddRandoms(s1, setSize);

	    Set s2 = newSet(cl, synch);
	    AddRandoms(s2, setSize);

	    Set intersection = clone(s1, cl, synch);
            intersection.retainAll(s2);
	    Set diff1 = clone(s1, cl, synch); diff1.removeAll(s2);
	    Set diff2 = clone(s2, cl, synch); diff2.removeAll(s1);
	    Set union = clone(s1, cl, synch); union.addAll(s2);

	    if (diff1.removeAll(diff2))
		fail("Set algebra identity 2 failed");
	    if (diff1.removeAll(intersection))
		fail("Set algebra identity 3 failed");
	    if (diff2.removeAll(diff1))
		fail("Set algebra identity 4 failed");
	    if (diff2.removeAll(intersection))
		fail("Set algebra identity 5 failed");
	    if (intersection.removeAll(diff1))
		fail("Set algebra identity 6 failed");
	    if (intersection.removeAll(diff1))
		fail("Set algebra identity 7 failed");

	    intersection.addAll(diff1); intersection.addAll(diff2);
	    if (!intersection.equals(union))
		fail("Set algebra identity 1 failed");

	    Iterator e = union.iterator();
	    while (e.hasNext())
		if (!intersection.remove(e.next()))
		    fail("Couldn't remove element from copy.");
	    if (!intersection.isEmpty())
		fail("Copy nonempty after deleting all elements.");

	    e = union.iterator();
	    while (e.hasNext()) {
		Object o = e.next();
		if (!union.contains(o))
		    fail("Set doesn't contain one of its elements.");
		e.remove();
		if (union.contains(o))
		    fail("Set contains element after deletion.");
	    }
	    if (!union.isEmpty())
		fail("Set nonempty after deleting all elements.");

	    s1.clear();
	    if (!s1.isEmpty())
		fail("Set nonempty after clear.");
	}
	System.out.println("Success.");
    }

    // Done inefficiently so as to exercise toArray
    static Set clone(Set s, Class cl, boolean synch) {
	Set clone = newSet(cl, synch);
	clone.addAll(Arrays.asList(s.toArray()));
	if (!s.equals(clone))
	    fail("Set not equal to copy.");
	if (!s.containsAll(clone))
	    fail("Set does not contain copy.");
	if (!clone.containsAll(s))
	    fail("Copy does not contain set.");
	return clone;
    }

    static Set newSet(Class cl, boolean synch) {
	try {
	    Set s = (Set) cl.newInstance();
            if (synch)
                s = Collections.synchronizedSet(s);
	    if (!s.isEmpty())
		fail("New instance non empty.");
	    return s;
	} catch (Throwable t) {
	    fail("Can't instantiate " + cl + ": " + t);
	}
	return null; //Shut up compiler.
    }

    static void AddRandoms(Set s, int n) {
	for (int i=0; i<n; i++) {
	    int r = rnd.nextInt() % n;
	    Integer e = new Integer(r < 0 ? -r : r);

	    int preSize = s.size();
	    boolean prePresent = s.contains(e);
	    boolean added = s.add(e);
	    if (!s.contains(e))
		fail ("Element not present after addition.");
	    if (added == prePresent)
		fail ("added == alreadyPresent");
	    int postSize = s.size();
	    if (added && preSize == postSize)
		fail ("Add returned true, but size didn't change.");
	    if (!added && preSize != postSize)
		fail ("Add returned false, but size changed.");
	}
    }

    static void fail(String s) {
	System.out.println(s);
	System.exit(1);
    }
}
