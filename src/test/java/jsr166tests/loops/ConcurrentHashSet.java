// from gee.cs.oswego.edu/home/jsr166/jsr166

package jsr166tests.loops;

/*
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */

// A set wrapper over CHM for testing

import java.util.*;
import java.util.concurrent.*;
import java.io.*;

public class ConcurrentHashSet<E> extends AbstractSet<E>
    implements Set<E>, Serializable {

    private final ConcurrentHashMap<E, Boolean> m;  // The backing map
    private transient Set<E> keySet;  // Its keySet

    public ConcurrentHashSet() {
        m = new ConcurrentHashMap<E, Boolean>();
        keySet = m.keySet();
    }
    public ConcurrentHashSet(int initialCapacity) {
        m = new ConcurrentHashMap<E, Boolean>(initialCapacity);
        keySet = m.keySet();
    }
    public ConcurrentHashSet(int initialCapacity, float loadFactor,
                             int concurrencyLevel) {
        m = new ConcurrentHashMap<E, Boolean>(initialCapacity, loadFactor,
                                              concurrencyLevel);
        keySet = m.keySet();
    }

    public int size()                 { return m.size(); }
    public boolean isEmpty()          { return m.isEmpty(); }
    public boolean contains(Object o) { return m.containsKey(o); }
    public Iterator<E> iterator()     { return keySet.iterator(); }
    public Object[] toArray()         { return keySet.toArray(); }
    public <T> T[] toArray(T[] a)     { return keySet.toArray(a); }
    public boolean add(E e) {
        return m.put(e, Boolean.TRUE) == null;
    }
    public boolean remove(Object o)   { return m.remove(o) != null; }

    public boolean removeAll(Collection<?> c) {
        return keySet.removeAll(c);
    }
    public boolean retainAll(Collection<?> c) {
        return keySet.retainAll(c);
    }
    public void clear()               { m.clear(); }
    public boolean equals(Object o)   { return keySet.equals(o); }
    public int hashCode()             { return keySet.hashCode(); }

    private static final long serialVersionUID = 2454657854757543876L;

    private void readObject(java.io.ObjectInputStream s)
        throws IOException, ClassNotFoundException
    {
        s.defaultReadObject();
        keySet = m.keySet();
    }
}
