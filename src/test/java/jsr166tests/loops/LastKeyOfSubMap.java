// from gee.cs.oswego.edu/home/jsr166/jsr166

package jsr166tests.loops;

/*
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */

// Adapted from bug report 5018354
import java.util.*;

public class LastKeyOfSubMap {
    private static final Comparator NULL_AT_END = new Comparator() {
            /**
             * Allows for nulls.  Null is greater than anything non-null.
             */
            public int compare(Object pObj1, Object pObj2) {
                if (pObj1 == null && pObj2 == null) return 0;
                if (pObj1 == null && pObj2 != null) return 1;
                if (pObj1 != null && pObj2 == null) return -1;
                return ((Comparable) pObj1).compareTo(pObj2);
            }
	};


    public static void main(String[] pArgs) {
        SortedMap m1 = new TreeMap(NULL_AT_END);
        m1.put("a", "a");
        m1.put("b", "b");
        m1.put("c", "c");
        m1.put(null, "d");

        SortedMap m2 = new TreeMap(m1);

        System.out.println(m1.lastKey());
        System.out.println(m1.get(m1.lastKey()));
        Object m1lk = m1.remove(m1.lastKey());
        if (m1lk == null)
            throw new Error("bad remove of last key");

        m2 = m2.tailMap("b");

        System.out.println(m2.lastKey());
        System.out.println(m2.get(m2.lastKey()));
        Object m2lk = m2.remove(m2.lastKey());
        if (m2lk == null)
            throw new Error("bad remove of last key");
    }
}
