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

public class SynchronizedLinkedListQueue<E>
    extends AbstractCollection<E> implements Queue<E> {
    private final Queue<E> q = new LinkedList<E>();

    public synchronized Iterator<E> iterator() {
        return q.iterator();
    }

    public synchronized boolean isEmpty() {
        return q.isEmpty();
    }
    public synchronized int size() {
        return q.size();
    }
    public synchronized boolean offer(E o) {
        return q.offer(o);
    }
    public synchronized boolean add(E o) {
        return q.add(o);
    }
    public synchronized E poll() {
        return q.poll();
    }
    public synchronized E remove() {
        return q.remove();
    }
    public synchronized E peek() {
        return q.peek();
    }
    public synchronized E element() {
        return q.element();
    }

    public synchronized boolean contains(Object o) {
        return q.contains(o);
    }
    public synchronized Object[] toArray() {
        return q.toArray();
    }
    public synchronized <T> T[] toArray(T[] a) {
        return q.toArray(a);
    }
    public synchronized boolean remove(Object o) {
        return q.remove(o);
    }

    public synchronized boolean containsAll(Collection<?> coll) {
        return q.containsAll(coll);
    }
    public synchronized boolean addAll(Collection<? extends E> coll) {
        return q.addAll(coll);
    }
    public synchronized boolean removeAll(Collection<?> coll) {
        return q.removeAll(coll);
    }
    public synchronized boolean retainAll(Collection<?> coll) {
        return q.retainAll(coll);
    }
    public synchronized void clear() {
        q.clear();
    }
    public synchronized String toString() {
        return q.toString();
    }

}
