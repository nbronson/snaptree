// from gee.cs.oswego.edu/home/jsr166/jsr166

package jsr166tests.loops;

/*
 * Written by Josh Bloch and Doug Lea with assistance from members of
 * JCP JSR-166 Expert Group and released to the public domain, as
 * explained at http://creativecommons.org/licenses/publicdomain
 */

import java.util.*;

public class ListBash {
    static boolean canRemove = true;
    static final Random rnd = new Random();
    static int numItr;
    static int listSize;
    static boolean synch;
    static Class cl;

    public static void main(String[] args) {
        numItr = Integer.parseInt(args[1]);
        listSize = Integer.parseInt(args[2]);
        cl = null;

	try {
	    cl = Class.forName(args[0]);
	} catch (ClassNotFoundException e) {
	    fail("Class " + args[0] + " not found.");
	}

        synch = (args.length>3);
        oneRun();
        oneRun();
        oneRun();
    }

    static void oneRun() {
        long startTime = System.nanoTime();
	for (int i=0; i<numItr; i++) {
            elementLoop();
	}
	List<Integer> s = newList(cl, synch);
	for (int i=0; i<listSize; i++)
	    s.add(new Integer(i));
	if (s.size() != listSize)
	    fail("Size of [0..n-1] != n");
        evenOdd(s);
        sublists(s);
        arrays();
        long elapsed = System.nanoTime() - startTime;
        System.out.println("Time: " + (elapsed/1000000000.0) + "s");
    }



    static void elementLoop() {
        List<Integer> s1 = newList(cl, synch);
        AddRandoms(s1, listSize);

        List<Integer> s2 = newList(cl, synch);
        AddRandoms(s2, listSize);

        sets(s1, s2);

        s1.clear();
        if (s1.size() != 0)
            fail("Clear didn't reduce size to zero.");

        s1.addAll(0, s2);
        if (!(s1.equals(s2) && s2.equals(s1)))
            fail("addAll(int, Collection) doesn't work.");
        // Reverse List
        for (int j=0, n=s1.size(); j<n; j++)
            s1.set(j, s1.set(n-j-1, s1.get(j)));
        // Reverse it again
        for (int j=0, n=s1.size(); j<n; j++)
            s1.set(j, s1.set(n-j-1, s1.get(j)));
        if (!(s1.equals(s2) && s2.equals(s1)))
            fail("set(int, Object) doesn't work");
        sums(s1, s2);
    }

    static void sums(List<Integer> s1, List<Integer> s2) {
        int sum = 0;
        for (int k = 0; k < listSize; ++k) {
            sum += (s1.get(k)).intValue();
            sum -= (s2.get(k)).intValue();
        }
        for (int k = 0; k < listSize; ++k) {
            sum += (s1.get(k)).intValue();
            s1.set(k, sum);
            sum -= (s2.get(k)).intValue();
            s1.set(k, -sum);
        }
        for (int k = 0; k < listSize; ++k) {
            sum += (s1.get(k)).intValue();
            sum -= (s2.get(k)).intValue();
        }
        if (sum == 0) System.out.print(" ");
    }

    static void sets(List<Integer> s1, List<Integer> s2) {
        List<Integer> intersection = clone(s1, cl,synch);intersection.retainAll(s2);
        List<Integer> diff1 = clone(s1, cl, synch); diff1.removeAll(s2);
        List<Integer> diff2 = clone(s2, cl, synch); diff2.removeAll(s1);
        List<Integer> union = clone(s1, cl, synch); union.addAll(s2);

        if (diff1.removeAll(diff2))
            fail("List algebra identity 2 failed");
        if (diff1.removeAll(intersection))
            fail("List algebra identity 3 failed");
        if (diff2.removeAll(diff1))
            fail("List algebra identity 4 failed");
        if (diff2.removeAll(intersection))
            fail("List algebra identity 5 failed");
        if (intersection.removeAll(diff1))
            fail("List algebra identity 6 failed");
        if (intersection.removeAll(diff1))
            fail("List algebra identity 7 failed");

        intersection.addAll(diff1); intersection.addAll(diff2);
        if (!(intersection.containsAll(union) &&
              union.containsAll(intersection)))
            fail("List algebra identity 1 failed");

        Iterator e = union.iterator();
        while (e.hasNext())
            intersection.remove(e.next());
        if (!intersection.isEmpty())
            fail("Copy nonempty after deleting all elements.");

        e = union.iterator();
        while (e.hasNext()) {
            Object o = e.next();
            if (!union.contains(o))
                fail("List doesn't contain one of its elements.");
            if (canRemove) {
                try { e.remove();
                } catch (UnsupportedOperationException uoe) {
                    canRemove = false;
                }
            }
        }
        if (canRemove && !union.isEmpty())
            fail("List nonempty after deleting all elements.");
    }

    static void evenOdd(List<Integer> s) {
        List<Integer> even = clone(s, cl, synch);
        List<Integer> odd = clone(s, cl, synch);
        List<Integer> all;
        Iterator<Integer> it;

        if (!canRemove)
            all = clone(s, cl, synch);
        else {
            it = even.iterator();
            while (it.hasNext())
                if ((it.next()).intValue() % 2 == 1)
                    it.remove();
            it = even.iterator();
            while (it.hasNext())
                if ((it.next()).intValue() % 2 == 1)
                    fail("Failed to remove all odd nubmers.");

            for (int i=0; i<(listSize/2); i++)
                odd.remove(i);
            for (int i=0; i<(listSize/2); i++) {
                int ii = (odd.get(i)).intValue();
                if (ii % 2 != 1)
                    fail("Failed to remove all even nubmers. " + ii);
            }

            all = clone(odd, cl, synch);
            for (int i=0; i<(listSize/2); i++)
                all.add(2*i, even.get(i));
            if (!all.equals(s))
                fail("Failed to reconstruct ints from odds and evens.");

            all = clone(odd,  cl, synch);
            ListIterator<Integer> itAll = all.listIterator(all.size());
            ListIterator<Integer> itEven = even.listIterator(even.size());
            while (itEven.hasPrevious()) {
                itAll.previous();
                itAll.add(itEven.previous());
                itAll.previous(); // ???
            }
            itAll = all.listIterator();
            while (itAll.hasNext()) {
                Integer i = itAll.next();
                itAll.set(new Integer(i.intValue()));
            }
            itAll = all.listIterator();
            it = s.iterator();
            while (it.hasNext())
                if (it.next()==itAll.next())
                    fail("Iterator.set failed to change value.");
        }
        if (!all.equals(s))
            fail("Failed to reconstruct ints with ListIterator.");
    }

    static void sublists(List<Integer> s) {
        List<Integer> all = clone(s, cl, synch);
        Iterator it = all.listIterator();
        int i=0;
        while (it.hasNext()) {
            Object o = it.next();
            if (all.indexOf(o) != all.lastIndexOf(o))
                fail("Apparent duplicate detected.");
            if (all.subList(i,   all.size()).indexOf(o) != 0) {
                System.out.println("s0: " + all.subList(i,   all.size()).indexOf(o));
                fail("subList/indexOf is screwy.");
            }
            if (all.subList(i+1, all.size()).indexOf(o) != -1) {
                System.out.println("s-1: " + all.subList(i+1, all.size()).indexOf(o));
                fail("subList/indexOf is screwy.");
            }
            if (all.subList(0,i+1).lastIndexOf(o) != i) {
                System.out.println("si" + all.subList(0,i+1).lastIndexOf(o));
                fail("subList/lastIndexOf is screwy.");
            }
            i++;
        }
    }

    static void arrays() {
        List<Integer> l = newList(cl, synch);
        AddRandoms(l, listSize);
        Integer[] ia = l.toArray(new Integer[0]);
        if (!l.equals(Arrays.asList(ia)))
            fail("toArray(Object[]) is hosed (1)");
        ia = new Integer[listSize];
        Integer[] ib = l.toArray(ia);
        if (ia != ib || !l.equals(Arrays.asList(ia)))
            fail("toArray(Object[]) is hosed (2)");
        ia = new Integer[listSize+1];
        ia[listSize] = new Integer(69);
        ib = l.toArray(ia);
        if (ia != ib || ia[listSize] != null
            || !l.equals(Arrays.asList(ia).subList(0, listSize)))
            fail("toArray(Object[]) is hosed (3)");
    }

    // Done inefficiently so as to exercise toArray
    static List<Integer> clone(List s, Class cl, boolean synch) {
        List a = Arrays.asList(s.toArray());
        if (s.hashCode() != a.hashCode())
            fail("Incorrect hashCode computation.");

	List clone = newList(cl, synch);
	clone.addAll(a);
	if (!s.equals(clone))
	    fail("List not equal to copy.");
	if (!s.containsAll(clone))
	    fail("List does not contain copy.");
	if (!clone.containsAll(s))
	    fail("Copy does not contain list.");

	return (List<Integer>) clone;
    }

    static List<Integer> newList(Class cl, boolean synch) {
	try {
	    List<Integer> s = (List<Integer>) cl.newInstance();
            if (synch)
                s = Collections.synchronizedList(s);
	    if (!s.isEmpty())
		fail("New instance non empty.");
	    return s;
	} catch (Throwable t) {
	    fail("Can't instantiate " + cl + ": " + t);
	}
	return null; //Shut up compiler.
    }

    static void AddRandoms(List<Integer> s, int n) {
	for (int i=0; i<n; i++) {
	    int r = rnd.nextInt() % n;
	    Integer e = new Integer(r < 0 ? -r : r);

	    int preSize = s.size();
	    if (!s.add(e))
		fail ("Add failed.");
	    int postSize = s.size();
	    if (postSize-preSize != 1)
		fail ("Add didn't increase size by 1.");
	}
    }

    static void fail(String s) {
	System.out.println(s);
	System.exit(1);
    }
}
