/* SnapTree - (c) 2009 Stanford University - PPL */

// LeafMap
package edu.stanford.ppl.concurrent;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.*;

public class SnapHashMap3<K,V> extends AbstractMap<K,V> implements /*ConcurrentMap<K,V>,*/ Cloneable, Serializable {
    private static final long serialVersionUID = 9212449426984968891L;

    private static final int LOG_BF = 5;
    private static final int BF = 1 << LOG_BF;
    private static final int BF_MASK = BF - 1;
    private static final int maxLoad(final int capacity) { return capacity - (capacity >> 2); /* 0.75 */ }
    private static final int minLoad(final int capacity) { return (capacity >> 2); /* 0.25 */ }

    // LEAF_MAX_CAPACITY * 2 / BF should be >= LEAF_MIN_CAPACITY
    private static final int LOG_LEAF_MIN_CAPACITY = 3;
    private static final int LOG_LEAF_MAX_CAPACITY = LOG_LEAF_MIN_CAPACITY + LOG_BF - 1;
    private static final int LEAF_MIN_CAPACITY = 1 << LOG_LEAF_MIN_CAPACITY;
    private static final int LEAF_MAX_CAPACITY = 1 << LOG_LEAF_MAX_CAPACITY;

    private static final int ROOT_SHIFT = 0;

    static class Generation {
    }

    static class HashEntry<K,V> {
        final Object gen;
        final K key;
        final int hash;
        V value;
        final HashEntry<K,V> next;

        HashEntry(final Object gen, final K key, final int hash, final V value, final HashEntry<K,V> next) {
            this.gen = gen;
            this.key = key;
            this.hash = hash;
            this.value = value;
            this.next = next;
        }

        HashEntry<K,V> withRemoved(final Object gen, final HashEntry<K,V> target) {
            if (this == target) {
                return next;
            } else {
                return new HashEntry<K,V>(gen, key, hash, value, next.withRemoved(gen, target));
            }
        }
    }

    static final class Leaf<K,V> {
        final Object gen;
        private HashEntry<K,V>[] table;
        private volatile int uniq;

        /** Creates a new empty leaf. */
        Leaf(final Object gen) {
            this.gen = gen;

            this.table = new HashEntry[LEAF_MIN_CAPACITY];
        }

        /** Creates a copy of <code>src</code>.  The caller is responsible for
         *  locking the source.
         */
        private Leaf(final Object gen, final Leaf<K,V> src) {
            this.gen = gen;
            this.table = (HashEntry<K,V>[]) src.table.clone();
            this.uniq = src.uniq;
        }

        //////// Reads

        @SuppressWarnings("unchecked")
        private HashEntry<K,V> childEntry(final int hash, final int shift) {
            return (HashEntry<K,V>) table[(hash >> shift) & (table.length - 1)];
        }

        boolean containsKey(final Object key, final int hash, final int shift) {
            if (uniq > 0) { // volatile read
                for (HashEntry<K,?> e = childEntry(hash, shift); e != null; e = e.next) {
                    if (e.hash == hash && key.equals(e.key)) {
                        return true;
                    }
                }
            }
            return false;
        }

        private synchronized <U> U lockedReadValue(final HashEntry<?,U> e) {
            return e.value;
        }

        /** This is only valid for a quiesced map. */
        boolean containsValue(final Object value) {
            for (HashEntry<K,V> head : table) {
                for (HashEntry<K,V> e = head; e != null; e = e.next) {
                    Object v = e.value;
                    if (v == null) {
                        v = lockedReadValue(e);
                    }
                    if (value.equals(v)) {
                        return true;
                    }
                }
            }
            return false;
        }

        V get(final Object key, final int hash, final int shift) {
            if (uniq > 0) { // volatile read
                for (HashEntry<K,V> e = childEntry(hash, shift); e != null; e = e.next) {
                    if (e.hash == hash && key.equals(e.key)) {
                        V v = e.value;
                        if (v == null) {
                            v = lockedReadValue(e);
                        }
                        return v;
                    }
                }
            }
            return null;
        }

        //////// Writes

        V put(final K key, final int hash, final V value, final int shift, int u) {
            growIfNecessary(shift, u);

            final int i = (hash >> shift) & (table.length - 1);
            final HashEntry<K,V> head = table[i];
            int uDelta = 1;
            for (HashEntry<K,V> e = head; e != null; e = e.next) {
                if (e.hash == hash) {
                    if (key.equals(e.key)) {
                        // match
                        final V prev = e.value;
                        if (e.gen == gen) {
                            // we have permission to mutate the node
                            e.value = value;
                        } else {
                            // we must replace the node
                            table[i] = new HashEntry<K,V>(gen, key, hash, value, head.withRemoved(gen, e));
                        }
                        uniq = u; // volatile store
                        return prev;
                    }
                    // Hash match, but not a key match.  If we eventually insert,
                    // then we won't modify uniq.
                    uDelta = 0;
                }
            }
            // no match
            table[i] = new HashEntry<K,V>(gen, key, hash, value, head);
            uniq = u + uDelta; // volatile store
            return null;
        }

//        V remove(final K key, final int hash, final int shift) {
//            if (uniq != BRANCH_UNIQ) {
//                synchronized (this) {
//                    int u = uniq;
//                    if (u != BRANCH_UNIQ) {
//                        return leafRemove(key, hash, shift, u);
//                    }
//                }
//            }
//            // forward to child
//            return childNodeForWrite(hash, shift).remove(key, hash, shift + LOG_BF);
//        }
//
//        private V leafRemove(final K key, final int hash, final int shift, int u) {
//            if (u <= 0) {
//                return null;
//            }
//
//            shrinkIfNecessary(shift, u);
//
//            final int i = (hash >> shift) & (table.length - 1);
//            final HashEntry<K,V> head = (HashEntry<K,V>) table[i];
//            int uDelta = -1;
//            for (HashEntry<K,V> e = head; e != null; e = e.next) {
//                if (e.hash == hash) {
//                    if (key.equals(e.key)) {
//                        // match
//                        final HashEntry<K,V> target = e;
//
//                        // continue the loop to get the right uDelta
//                        if (uDelta != 0) {
//                            e = e.next;
//                            while (e != null) {
//                                if (e.hash == hash) {
//                                    uDelta = 0;
//                                    break;
//                                }
//                                e = e.next;
//                            }
//                        }
//
//                        // match
//                        uniq = u + uDelta; // volatile store
//                        table[i] = head.withRemoved(gen, target);
//                        return target.value;
//                    }
//                    // hash match, but not key match
//                    uDelta = 0;
//                }
//            }
//            // no match, no write
//            return null;
//        }

//        //////// CAS-like
//
//        V putIfAbsent(final K key, final int hash, final V value, final int shift) {
//            if (uniq != BRANCH_UNIQ) {
//                synchronized (this) {
//                    int u = uniq;
//                    if (shouldSplit(u)) {
//                        split(shift);
//                    } else if (u != BRANCH_UNIQ) {
//                        return leafPutIfAbsent(key, hash, value, shift, u);
//                    }
//                }
//            }
//            // forward to child
//            return childNodeForWrite(hash, shift).putIfAbsent(key, hash, value, shift + LOG_BF);
//        }
//
//        private V leafPutIfAbsent(final K key, final int hash, final V value, final int shift, int u) {
//            if (u == UNUSED_UNIQ) {
//                table = new Object[LEAF_MIN_CAPACITY];
//                u = 0;
//            } else {
//                growIfNecessary(shift, u);
//            }
//
//            final int i = (hash >> shift) & (table.length - 1);
//            final HashEntry<K,V> head = (HashEntry<K,V>) table[i];
//            int uDelta = 1;
//            for (HashEntry<K,V> e = head; e != null; e = e.next) {
//                if (e.hash == hash) {
//                    if (key.equals(e.key)) {
//                        // match => no change, can't have been part of the unused -> leaf transition
//                        return e.value;
//                    }
//                    // Hash match, but not a key match.  If we eventually insert,
//                    // then we won't modify uniq.
//                    uDelta = 0;
//                }
//            }
//            // no match
//            table[i] = new HashEntry<K,V>(gen, key, hash, value, head);
//            uniq = u + uDelta; // volatile store
//            return null;
//        }
//
//        boolean replace(final K key, final int hash, final V oldValue, final V newValue, final int shift) {
//            if (uniq != BRANCH_UNIQ) {
//                synchronized (this) {
//                    int u = uniq;
//                    if (shouldSplit(u)) {
//                        split(shift);
//                    } else if (u != BRANCH_UNIQ) {
//                        return leafReplace(key, hash, oldValue, newValue, shift, u);
//                    }
//                }
//            }
//            // forward to child
//            return childNodeForWrite(hash, shift).replace(key, hash, oldValue, newValue, shift + LOG_BF);
//        }
//
//        private boolean leafReplace(final K key, final int hash, final V oldValue, final V newValue, final int shift, int u) {
//            if (u <= 0) {
//                return false;
//            }
//
//            final int i = (hash >> shift) & (table.length - 1);
//            final HashEntry<K,V> head = (HashEntry<K,V>) table[i];
//            for (HashEntry<K,V> e = head; e != null; e = e.next) {
//                if (e.hash == hash && key.equals(e.key)) {
//                    // key match
//                    if (oldValue.equals(e.value)) {
//                        // CAS success
//                        if (e.gen == gen) {
//                            // we have permission to mutate the node
//                            e.value = newValue;
//                        } else {
//                            // we must replace the node
//                            table[i] = new HashEntry<K,V>(gen, key, hash, newValue, head.withRemoved(gen, e));
//                        }
//                        uniq = u; // volatile store
//                        return true;
//                    } else {
//                        // CAS failure
//                        return false;
//                    }
//                }
//            }
//            // no match
//            return false;
//        }
//
//        V replace(final K key, final int hash, final V value, final int shift) {
//            if (uniq != BRANCH_UNIQ) {
//                synchronized (this) {
//                    int u = uniq;
//                    if (shouldSplit(u)) {
//                        split(shift);
//                    } else if (u != BRANCH_UNIQ) {
//                        return leafReplace(key, hash, value, shift, u);
//                    }
//                }
//            }
//            // forward to child
//            return childNodeForWrite(hash, shift).replace(key, hash, value, shift + LOG_BF);
//        }
//
//        private V leafReplace(final K key, final int hash, final V value, final int shift, int u) {
//            if (u <= 0) {
//                return null;
//            }
//
//            final int i = (hash >> shift) & (table.length - 1);
//            final HashEntry<K,V> head = (HashEntry<K,V>) table[i];
//            for (HashEntry<K,V> e = head; e != null; e = e.next) {
//                if (e.hash == hash && key.equals(e.key)) {
//                    // match
//                    final V prev = e.value;
//                    if (e.gen == gen) {
//                        // we have permission to mutate the node
//                        e.value = value;
//                    } else {
//                        // we must replace the node
//                        table[i] = new HashEntry<K,V>(gen, key, hash, value, head.withRemoved(gen, e));
//                    }
//                    uniq = u; // volatile store
//                    return prev;
//                }
//            }
//            // no match
//            return null;
//        }
//
//        boolean remove(final K key, final int hash, final V value, final int shift) {
//            if (uniq != BRANCH_UNIQ) {
//                synchronized (this) {
//                    int u = uniq;
//                    if (shouldSplit(u)) {
//                        split(shift);
//                    } else if (u != BRANCH_UNIQ) {
//                        return leafRemove(key, hash, value, shift, u);
//                    }
//                }
//            }
//            // forward to child
//            return childNodeForWrite(hash, shift).remove(key, hash, value, shift + LOG_BF);
//        }
//
//        private boolean leafRemove(final K key, final int hash, final V value, final int shift, int u) {
//            if (u <= 0) {
//                return false;
//            }
//
//            shrinkIfNecessary(shift, u);
//
//            final int i = (hash >> shift) & (table.length - 1);
//            final HashEntry<K,V> head = (HashEntry<K,V>) table[i];
//            int uDelta = -1;
//            HashEntry<K,V> e = head;
//            while (e != null) {
//                if (e.hash == hash) {
//                    if (key.equals(e.key)) {
//                        // key match
//                        if (!value.equals(e.value)) {
//                            // CAS failure
//                            return false;
//                        }
//
//                        final HashEntry<K,V> target = e;
//
//                        // continue the loop to get the right uDelta
//                        if (uDelta != 0) {
//                            e = e.next;
//                            while (e != null) {
//                                if (e.hash == hash) {
//                                    uDelta = 0;
//                                    break;
//                                }
//                                e = e.next;
//                            }
//                        }
//
//                        // match
//                        uniq = u + uDelta; // volatile store
//                        table[i] = head.withRemoved(gen, target);
//                        return true;
//                    }
//                    // hash match, but not key match
//                    uDelta = 0;
//                }
//                e = e.next;
//            }
//            // no match
//            return false;
//        }
//

        //////// Leaf resizing

        private void growIfNecessary(final int shift, final int u) {
            final int n = table.length;
            if (n < LEAF_MAX_CAPACITY && u > maxLoad(n)) {
                resize(shift, n << 1);
            }
        }

        private void shrinkIfNecessary(final int shift, final int u) {
            final int n = table.length;
            if (n > LEAF_MIN_CAPACITY && u < minLoad(n)) {
                resize(shift, n >> 1);
            }
        }

        @SuppressWarnings("unchecked")
        private void resize(final int shift, final int newSize) {
            final HashEntry<K,V>[] prevTable = table;
            table = (HashEntry<K,V>[]) new HashEntry[newSize];
            for (HashEntry<K,V> head : prevTable) {
                reputAll(shift, head);
            }
        }

        private void reputAll(final int shift, final HashEntry<K,V> head) {
            if (head != null) {
                reputAll(shift, head.next);
                reput(shift, head);
            }
        }

        @SuppressWarnings("unchecked")
        private void reput(final int shift, final HashEntry<K,V> e) {
            final int i = (e.hash >> shift) & (table.length - 1);
            final HashEntry<K,V> next = table[i];
            if (e.next == next) {
                // existing entry will work in the new table
                table[i] = e;
            } else {
                // copy required to get the next pointer right
                table[i] = new HashEntry<K,V>(gen, e.key, e.hash, e.value, next);
            }
        }

        //////// Leaf splitting

        private boolean shouldSplit(final int u) {
            return u > maxLoad(LEAF_MAX_CAPACITY);
        }

        /** Caller must have locked the leaf. */
        private Object[] split(final int shift) {
            final Object[] result = emptyBranch(gen);

            // move all of the entries
            for (HashEntry<K,V> head : table) {
                scatterAll(result, shift, head);
            }

            return result;
        }

        @SuppressWarnings("unchecked")
        private void scatterAll(final Object[] branch, final int shift, final HashEntry<K,V> head) {
            if (head != null) {
                scatterAll(branch, shift, head.next);
                final int i = (head.hash >> shift) & BF_MASK;
                ((Leaf<K,V>) branch[1 + i]).putForSplit(shift + LOG_BF, head);
            }
        }

        private void putForSplit(final int shift, final HashEntry<K,V> entry) {
            int u = uniq;
            growIfNecessary(shift, u);

            final int i = (entry.hash >> shift) & (table.length - 1);
            final HashEntry<K,V> head = table[i];

            // is this hash a duplicate?
            int uDelta = 1;
            for (HashEntry<K,V> e = head; e != null; e = e.next) {
                if (e.hash == entry.hash) {
                    uDelta = 0;
                    break;
                }
            }

            if (entry.next == head) {
                // no new entry needed
                table[i] = entry;
            } else {
                table[i] = new HashEntry<K,V>(gen, entry.key, entry.hash, entry.value, head);
            }
            uniq = u + uDelta;
        }
    }


    static class COWMgr extends CopyOnWriteManager<Object[]> {
        COWMgr() {
            super(emptyBranch(new Generation()), 0);
        }

        COWMgr(final Object[] initialValue, final int initialSize) {
            super(initialValue, initialSize);
        }

        protected Object[] freezeAndClone(final Object[] value) {
            final Object[] repl = value.clone();
            repl[0] = new Generation();
            return repl;
        }

        protected Object[] cloneFrozen(final Object[] frozenValue) {
            return freezeAndClone(frozenValue);
        }
    }

    private transient volatile COWMgr rootHolder;

    private static int hash(int h) {
        // taken from ConcurrentHashMap
        h += (h <<  15) ^ 0xffffcd7d;
        h ^= (h >>> 10);
        h += (h <<   3);
        h ^= (h >>>  6);
        h += (h <<   2) + (h << 14);
        return h ^ (h >>> 16);
    }

    private static Object[] emptyBranch(final Object gen) {
        final Object[] result = new Object[1 + BF];
        result[0] = gen;
        for (int i = 0; i < BF; ++i) {
            result[1 + i] = new Leaf(gen);
        }
        return result;
    }

    private static Object[] cloneBranch(final Object gen, final Object[] branch) {
        final Object[] repl = new Object[1 + BF];
        repl[0] = gen;
        System.arraycopy(branch, 1, repl, 1, BF);
        return repl;
    }

    //////// construction and cloning

    public SnapHashMap3() {
        this.rootHolder = new COWMgr();
    }

    public SnapHashMap3(final Map<? extends K, ? extends V> source) {
        this.rootHolder = new COWMgr();
        putAll(source);
    }

    @SuppressWarnings("unchecked")
    public SnapHashMap3(final SortedMap<K,? extends V> source) {
        if (source instanceof SnapHashMap3) {
            final SnapHashMap3<K,V> s = (SnapHashMap3<K,V>) source;
            this.rootHolder = (COWMgr) s.rootHolder.clone();
        }
        else {
            this.rootHolder = new COWMgr();
            putAll(source);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public SnapHashMap3<K,V> clone() {
        final SnapHashMap3<K,V> copy;
        try {
            copy = (SnapHashMap3<K,V>) super.clone();
        } catch (final CloneNotSupportedException xx) {
            throw new InternalError();
        }
        copy.rootHolder = (COWMgr) rootHolder.clone();
        return copy;
    }

    //////// public interface

    public void clear() {
        rootHolder = new COWMgr();
    }

    public boolean isEmpty() {
        return rootHolder.isEmpty();
    }

    public int size() {
        return rootHolder.size();
    }

    public boolean containsKey(final Object key) {
        final int hash = hash(key.hashCode());
        Object[] node = rootHolder.read();
        int shift = ROOT_SHIFT;
        while (true) {
            final int i = 1 + ((hash >> shift) & BF_MASK);
            Object child = node[i];
            if (child instanceof Leaf) {
                return ((Leaf<?,?>) child).containsKey(key, hash, shift + LOG_BF);
            }
            // else recurse
            node = (Object[]) child;
            shift += LOG_BF;
        }
    }

    public boolean containsValue(final Object value) {
        if (value == null) {
            throw new NullPointerException();
        }
        return containsValue(rootHolder.frozen(), value);
    }

    private boolean containsValue(final Object[] node, final Object value) {
        for (int i = 1; i < node.length; ++i) {
            final Object child = node[i];
            if (child != null) {
                if (child instanceof Leaf) {
                    if (((Leaf<?,?>) child).containsValue(value)) {
                        return true;
                    }
                } else {
                    if (containsValue((Object[]) child, value)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    public V get(final Object key) {
        final int hash = hash(key.hashCode());
        Object[] node = rootHolder.read();
        int shift = ROOT_SHIFT;
        while (true) {
            final int i = 1 + ((hash >> shift) & BF_MASK);
            Object child = node[i];
            if (child instanceof Leaf) {
                return ((Leaf<?,V>) child).get(key, hash, shift + LOG_BF);
            }
            // else recurse
            node = (Object[]) child;
            shift += LOG_BF;
        }
    }

    public V put(final K key, final V value) {
        if (value == null) {
            throw new NullPointerException();
        }
        final int h = hash(key.hashCode());
        final Epoch.Ticket ticket = rootHolder.beginMutation();
        int sizeDelta = 0;
        try {
            final V prev = putImpl(key, h, value);
            if (prev == null) {
                sizeDelta = 1;
            }
            return prev;
        } finally {
            ticket.leave(sizeDelta);
        }
    }

    private V putImpl(final K key, final int hash, final V value) {
        Object[] node = rootHolder.mutable();
        int shift = ROOT_SHIFT;
        final Object gen = node[0];
        while (true) {
            final int i = 1 + ((hash >> shift) & BF_MASK);
            Object child = node[i];
            if (child instanceof Leaf) {
                final Leaf<K,V> leaf = (Leaf<K,V>) child;
                synchronized (leaf) {
                    if (leaf.gen != gen) {
                        if (leaf == node[i]) {
                            node[i] = new Leaf<K,V>(gen, leaf);
                        }
                        continue;
                    }
                    final int u = leaf.uniq;
                    if (leaf.shouldSplit(u)) {
                        if (leaf == node[i]) {
                            node[i] = leaf.split(shift);
                        }
                        continue;
                    }
                    return leaf.put(key, hash, value, shift + LOG_BF, u);
                }
            } else {
                Object[] branch = (Object[]) child;
                if (branch[0] != gen) {
                    synchronized (branch) {
                        // TODO: reduce locking here, also, we need a readChildUnderLock
                        if (branch == node[i]) {
                            node[i] = cloneBranch(gen, branch);
                        }
                        continue;
                    }
                }
                // recurse
                node = branch;
                shift += LOG_BF;
            }
        }
    }

    public V remove(final Object key) {
        throw new UnsupportedOperationException();
//        final int h = hash(key.hashCode());
//        final Epoch.Ticket ticket = rootHolder.beginMutation();
//        int sizeDelta = 0;
//        try {
//            final V prev = rootHolder.mutable().remove((K) key, h, ROOT_SHIFT);
//            if (prev != null) {
//                sizeDelta = -1;
//            }
//            return prev;
//        } finally {
//            ticket.leave(sizeDelta);
//        }
    }

//    //////// CAS-like
//
//    public V putIfAbsent(final K key, final V value) {
//        if (value == null) {
//            throw new NullPointerException();
//        }
//        final int h = hash(key.hashCode());
//        final Epoch.Ticket ticket = rootHolder.beginMutation();
//        int sizeDelta = 0;
//        try {
//            final V prev = rootHolder.mutable().putIfAbsent(key, h, value, ROOT_SHIFT);
//            if (prev == null) {
//                sizeDelta = 1;
//            }
//            return prev;
//        } finally {
//            ticket.leave(sizeDelta);
//        }
//    }
//
//    public boolean replace(final K key, final V oldValue, final V newValue) {
//        if (oldValue == null || newValue == null) {
//            throw new NullPointerException();
//        }
//        final int h = hash(key.hashCode());
//        final Epoch.Ticket ticket = rootHolder.beginMutation();
//        try {
//            return rootHolder.mutable().replace(key, h, oldValue, newValue, ROOT_SHIFT);
//        } finally {
//            ticket.leave(0);
//        }
//    }
//
//    public V replace(final K key, final V value) {
//        if (value == null) {
//            throw new NullPointerException();
//        }
//        final int h = hash(key.hashCode());
//        final Epoch.Ticket ticket = rootHolder.beginMutation();
//        try {
//            return rootHolder.mutable().replace(key, h, value, ROOT_SHIFT);
//        } finally {
//            ticket.leave(0);
//        }
//    }
//
//    public boolean remove(final Object key, final Object value) {
//        final int h = hash(key.hashCode());
//        if (value == null) {
//            return false;
//        }
//        final Epoch.Ticket ticket = rootHolder.beginMutation();
//        int sizeDelta = 0;
//        try {
//            final boolean result = rootHolder.mutable().remove((K) key, h, (V) value, ROOT_SHIFT);
//            if (result) {
//                sizeDelta = -1;
//            }
//            return result;
//        } finally {
//            ticket.leave(sizeDelta);
//        }
//    }

    public Set<K> keySet() {
        return new KeySet();
    }

    public Collection<V> values() {
        return new Values();
    }

    public Set<Entry<K,V>> entrySet() {
        return new EntrySet();
    }

    //////// Legacy methods

    public boolean contains(final Object value) {
        return containsValue(value);
    }

    public Enumeration<K> keys() {
        return new KeyIterator(rootHolder.frozen());
    }

    public Enumeration<V> elements() {
        return new ValueIterator(rootHolder.frozen());
    }

    //////// Map support classes

    class KeySet extends AbstractSet<K> {
        public Iterator<K> iterator() {
            return new KeyIterator(rootHolder.frozen());
        }
        public boolean isEmpty() {
            return SnapHashMap3.this.isEmpty();
        }
        public int size() {
            return SnapHashMap3.this.size();
        }
        public boolean contains(Object o) {
            return SnapHashMap3.this.containsKey(o);
        }
        public boolean remove(Object o) {
            return SnapHashMap3.this.remove(o) != null;
        }
        public void clear() {
            SnapHashMap3.this.clear();
        }
    }

    final class Values extends AbstractCollection<V> {
        public Iterator<V> iterator() {
            return new ValueIterator(rootHolder.frozen());
        }
        public boolean isEmpty() {
            return SnapHashMap3.this.isEmpty();
        }
        public int size() {
            return SnapHashMap3.this.size();
        }
        public boolean contains(Object o) {
            return SnapHashMap3.this.containsValue(o);
        }
        public void clear() {
            SnapHashMap3.this.clear();
        }
    }

    final class EntrySet extends AbstractSet<Entry<K,V>> {
        public Iterator<Entry<K,V>> iterator() {
            return new EntryIterator(rootHolder.frozen());
        }
        public boolean contains(Object o) {
            if (!(o instanceof Entry))
                return false;
            Entry<?,?> e = (Entry<?,?>)o;
            V v = SnapHashMap3.this.get(e.getKey());
            return v != null && v.equals(e.getValue());
        }
        public boolean remove(Object o) {
            // TODO: reenable
            throw new UnsupportedOperationException();
//            if (!(o instanceof Entry))
//                return false;
//            Entry<?,?> e = (Entry<?,?>)o;
//            return SnapHashMap.this.remove(e.getKey(), e.getValue());
        }
        public boolean isEmpty() {
            return SnapHashMap3.this.isEmpty();
        }
        public int size() {
            return SnapHashMap3.this.size();
        }
        public void clear() {
            SnapHashMap3.this.clear();
        }
    }

    abstract class AbstractIter {

        private final Object[] root;
        private int currentShift;
        private Leaf<K,V> currentLeaf;
        private HashEntry<K,V> currentEntry;
        private HashEntry<K,V> prevEntry;

        AbstractIter(final Object[] frozenRoot) {
            this.root = frozenRoot;
            pushMin(frozenRoot, ROOT_SHIFT, 0);
        }

        @SuppressWarnings("unchecked")
        private boolean pushMin(final Object[] node, final int shift, final int minIndex) {
            for (int i = minIndex; i < BF; ++i) {
                final Object child = node[1 + i];
                if (child instanceof Leaf) {
                    final Leaf<K,V> leaf = (Leaf<K,V>) child;
                    if (leaf.uniq > 0) {
                        // non-empty
                        for (HashEntry<K,V> head : leaf.table) {
                            if (head != null) {
                                // success
                                currentShift = shift + LOG_BF;
                                currentLeaf = leaf;
                                currentEntry = head;
                                return true;
                            }
                        }
                        throw new Error("logic error");
                    }
                } else {
                    // recurse on branch
                    if (pushMin((Object[]) child, shift + LOG_BF, 0)) {
                        return true;
                    }
                }
            }
            return false;
        }

        private void advance() {
            // advance within a bucket chain
            if (currentEntry.next != null) {
                // easy
                currentEntry = currentEntry.next;
                return;
            }

            // advance to the next non-empty chain
            int i = ((currentEntry.hash >> currentShift) & (currentLeaf.table.length - 1)) + 1;
            while (i < currentLeaf.table.length) {
                if (currentLeaf.table[i] != null) {
                    currentEntry = currentLeaf.table[i];
                    return;
                }
                ++i;
            }

            // now we are moving between leaves
            while (currentShift > ROOT_SHIFT) {
                // search within the parent
                currentShift -= LOG_BF;
                final Object[] parent = findBranch();
                if (pushMin(parent, currentShift, ((currentEntry.hash >> currentShift) & BF_MASK) + 1)) {
                    return;
                }
            }

            // we're done
            currentEntry = null;
        }

        private Object[] findBranch() {
            final int h = currentEntry.hash;
            Object[] node = root;
            for (int s = ROOT_SHIFT; s < currentShift; s += LOG_BF) {
                node = (Object[]) node[1 + ((h >> s) & BF_MASK)];
            }
            return node;
        }

        public boolean hasNext() {
            return currentEntry != null;
        }

        public boolean hasMoreElements() {
            return hasNext();
        }

        HashEntry<K,V> nextEntry() {
            if (currentEntry == null) {
                throw new NoSuchElementException();
            }
            prevEntry = currentEntry;
            advance();
            return prevEntry;
        }

        public void remove() {
            if (prevEntry == null) {
                throw new IllegalStateException();
            }
            SnapHashMap3.this.remove(prevEntry.key);
            prevEntry = null;
        }
    }

    final class KeyIterator extends AbstractIter implements Iterator<K>, Enumeration<K>  {
        KeyIterator(final Object[] frozenRoot) {
            super(frozenRoot);
        }

        public K next() {
            return nextEntry().key;
        }

        public K nextElement() {
            return next();
        }
    }

    final class ValueIterator extends AbstractIter implements Iterator<V>, Enumeration<V> {
        ValueIterator(final Object[] frozenRoot) {
            super(frozenRoot);
        }

        public V next() {
            return nextEntry().value;
        }

        public V nextElement() {
            return next();
        }
    }

    final class WriteThroughEntry extends SimpleEntry<K,V> {
        WriteThroughEntry(final K k, final V v) {
            super(k, v);
        }

	public V setValue(final V value) {
            if (value == null) {
                throw new NullPointerException();
            }
            final V prev = super.setValue(value);
            SnapHashMap3.this.put(getKey(), value);
            return prev;
        }
    }

    final class EntryIterator extends AbstractIter implements Iterator<Entry<K,V>> {
        EntryIterator(final Object[] frozenRoot) {
            super(frozenRoot);
        }

        public Entry<K,V> next() {
            final HashEntry<K,V> e = nextEntry();
            return new WriteThroughEntry(e.key, e.value);
        }
    }

    //////// Serialization

    /** Saves the state of the <code>SnapTreeMap</code> to a stream. */
    private void writeObject(final ObjectOutputStream xo) throws IOException {
        // this handles the comparator, and any subclass stuff
        xo.defaultWriteObject();

        // by cloning the COWMgr, we get a frozen tree plus the size
        final COWMgr h = (COWMgr) rootHolder.clone();

        xo.writeInt(h.size());
        writeEntry(xo, h.frozen());
    }

    private void writeEntry(final ObjectOutputStream xo, final Object[] branch) throws IOException {
        for (Object child : branch) {
            if (child instanceof Leaf) {
                final Leaf<?,?> leaf = (Leaf<?,?>) child;
                for (HashEntry<?,?> head : leaf.table) {
                    for (HashEntry<?,?> e = head; e != null; e = e.next) {
                        xo.writeObject(e.key);
                        xo.writeObject(e.value);
                    }
                }
            } else {
                writeEntry(xo, (Object[]) child);
            }
        }
    }

    /** Reverses {@link #writeObject(java.io.ObjectOutputStream)}. */
    private void readObject(final ObjectInputStream xi) throws IOException, ClassNotFoundException  {
        xi.defaultReadObject();

        rootHolder = new COWMgr();

        final int size = xi.readInt();
        for (int i = 0; i < size; ++i) {
            final K k = (K) xi.readObject();
            final V v = (V) xi.readObject();
            put(k, v);
        }
    }

    public static void main(final String[] args) {
        for (int i = 0; i < 10; ++i) {
            runOne(new SnapHashMap3<Integer,String>());
//            runOne(new SnapHashMap<Integer,String>());
//            runOne(new SnapHashMap<Integer,String>());
//            runOne(new SnapHashMap1<Integer,String>());
//            runOne(new SnapHashMap1<Integer,String>());
//            runOne(new SnapHashMap1<Integer,String>());
//            runOne(new java.util.concurrent.ConcurrentHashMap<Integer,String>());
//            runOne(new java.util.concurrent.ConcurrentHashMap<Integer,String>());
//            runOne(new java.util.concurrent.ConcurrentHashMap<Integer,String>());
//            runOne(new SnapTreeMap<Integer,String>());
//            runOne(new java.util.concurrent.ConcurrentSkipListMap<Integer,String>());
            System.out.println();
        }
    }

    private static void runOne(final Map<Integer,String> m) {
        final long t0 = System.currentTimeMillis();
        for (int p = 0; p < 10; ++p) {
            for (int i = 0; i < 100000; ++i) {
                m.put(i, "data");
            }
        }
        final long t1 = System.currentTimeMillis();
        for (int p = 0; p < 10; ++p) {
            for (int i = 0; i < 100000; ++i) {
                m.get(i);
            }
        }
        final long t2 = System.currentTimeMillis();
        for (int p = 0; p < 10; ++p) {
            for (int i = 0; i < 100000; ++i) {
                m.get(-(i + 1));
            }
        }
        final long t3 = System.currentTimeMillis();
        System.out.println(
                (t1 - t0) + " nanos/put, " +
                (t2 - t1) + " nanos/get hit, " +
                (t3 - t2) + " nanos/get miss : " + m.getClass().getSimpleName());
    }
}