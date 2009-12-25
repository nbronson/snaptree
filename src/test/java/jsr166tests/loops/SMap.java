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


/**
 * This is an incomplete implementation of a wrapper class
 * that places read-write locks around unsynchronized Maps.
 * Exists as a sample input for MapLoops test.
 */

public class SMap implements Map {
    private final Map m;
    public SMap(Map m) {
        if (m == null)
            throw new NullPointerException();
        this.m = m;
    }

    public SMap() {
        this(new TreeMap()); // use TreeMap by default
    }

    public synchronized int size() {
        return m.size();
    }
    public synchronized boolean isEmpty(){
        return m.isEmpty();
    }

    public synchronized Object get(Object key) {
        return m.get(key);
    }

    public synchronized boolean containsKey(Object key) {
        return m.containsKey(key);
    }
    public synchronized boolean containsValue(Object value){
        return m.containsValue(value);
    }


    public synchronized Set keySet() { // Not implemented
        return m.keySet();
    }

    public synchronized Set entrySet() { // Not implemented
        return m.entrySet();
    }

    public synchronized Collection values() { // Not implemented
        return m.values();
    }

    public synchronized boolean equals(Object o) {
        return m.equals(o);
    }
    public synchronized int hashCode() {
        return m.hashCode();
    }
    public synchronized String toString() {
        return m.toString();
    }



    public synchronized Object put(Object key, Object value) {
        return m.put(key, value);
    }
    public synchronized Object remove(Object key) {
        return m.remove(key);
    }
    public synchronized void putAll(Map map) {
        m.putAll(map);
    }
    public synchronized void clear() {
        m.clear();
    }

}
