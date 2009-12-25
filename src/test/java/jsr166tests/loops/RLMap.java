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

public class RLMap implements Map {
    private final Map m;
    private final ReentrantLock rl = new ReentrantLock();

    public RLMap(Map m) {
        if (m == null)
            throw new NullPointerException();
        this.m = m;
    }

    public RLMap() {
        this(new TreeMap()); // use TreeMap by default
    }

    public int size() {
        rl.lock(); try {return m.size();} finally { rl.unlock(); }
    }
    public boolean isEmpty(){
        rl.lock(); try {return m.isEmpty();} finally { rl.unlock(); }
    }

    public Object get(Object key) {
        rl.lock(); try {return m.get(key);} finally { rl.unlock(); }
    }

    public boolean containsKey(Object key) {
        rl.lock(); try {return m.containsKey(key);} finally { rl.unlock(); }
    }
    public boolean containsValue(Object value){
        rl.lock(); try {return m.containsValue(value);} finally { rl.unlock(); }
    }


    public Set keySet() { // Not implemented
        return m.keySet();
    }

    public Set entrySet() { // Not implemented
        return m.entrySet();
    }

    public Collection values() { // Not implemented
        return m.values();
    }

    public boolean equals(Object o) {
        rl.lock(); try {return m.equals(o);} finally { rl.unlock(); }
    }
    public int hashCode() {
        rl.lock(); try {return m.hashCode();} finally { rl.unlock(); }
    }
    public String toString() {
        rl.lock(); try {return m.toString();} finally { rl.unlock(); }
    }



    public Object put(Object key, Object value) {
        rl.lock(); try {return m.put(key, value);} finally { rl.unlock(); }
    }
    public Object remove(Object key) {
        rl.lock(); try {return m.remove(key);} finally { rl.unlock(); }
    }
    public void putAll(Map map) {
        rl.lock(); try {m.putAll(map);} finally { rl.unlock(); }
    }
    public void clear() {
        rl.lock(); try {m.clear();} finally { rl.unlock(); }
    }

}
