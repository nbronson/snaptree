// from gee.cs.oswego.edu/home/jsr166/jsr166

package jsr166tests.loops;


/*
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */

// Stand-alone version of java.util.Collections.synchronizedCollection
import java.util.*;
import java.io.*;

public final class SynchronizedCollection<E> implements Collection<E>, Serializable {
    final Collection<E> c;	   // Backing Collection
    final Object	   mutex;  // Object on which to synchronize

    public SynchronizedCollection(Collection<E> c) {
        if (c==null)
            throw new NullPointerException();
        this.c = c;
        mutex = this;
    }
    public SynchronizedCollection(Collection<E> c, Object mutex) {
        this.c = c;
        this.mutex = mutex;
    }

    public SynchronizedCollection() {
        this(new ArrayList<E>());
    }

    public final int size() {
        synchronized(mutex) {return c.size();}
    }
    public final boolean isEmpty() {
        synchronized(mutex) {return c.isEmpty();}
    }
    public final boolean contains(Object o) {
        synchronized(mutex) {return c.contains(o);}
    }
    public final Object[] toArray() {
        synchronized(mutex) {return c.toArray();}
    }
    public final <T> T[] toArray(T[] a) {
        synchronized(mutex) {return c.toArray(a);}
    }

    public final Iterator<E> iterator() {
        return c.iterator();
    }

    public final boolean add(E e) {
        synchronized(mutex) {return c.add(e);}
    }
    public final boolean remove(Object o) {
        synchronized(mutex) {return c.remove(o);}
    }

    public final boolean containsAll(Collection<?> coll) {
        synchronized(mutex) {return c.containsAll(coll);}
    }
    public final boolean addAll(Collection<? extends E> coll) {
        synchronized(mutex) {return c.addAll(coll);}
    }
    public final boolean removeAll(Collection<?> coll) {
        synchronized(mutex) {return c.removeAll(coll);}
    }
    public final boolean retainAll(Collection<?> coll) {
        synchronized(mutex) {return c.retainAll(coll);}
    }
    public final void clear() {
        synchronized(mutex) {c.clear();}
    }
    public final String toString() {
        synchronized(mutex) {return c.toString();}
    }
    private void writeObject(ObjectOutputStream s) throws IOException {
        synchronized(mutex) {s.defaultWriteObject();}
    }
}
