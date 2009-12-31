/* SnapTree - (c) 2009 Stanford University - PPL */

// LeafMap
package edu.stanford.ppl.concurrent;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentMap;

// This implementation currently has a race between the read of Node.uniq and Node.table. 

public class SnapHashMap2<K,V> extends AbstractMap<K,V> implements ConcurrentMap<K,V>, Cloneable, Serializable {
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
        final Generation gen;
        final K key;
        final int hash;
        V value;
        final HashEntry<K,V> next;

        HashEntry(final Generation gen, final K key, final int hash, final V value, final HashEntry<K,V> next) {
            this.gen = gen;
            this.key = key;
            this.hash = hash;
            this.value = value;
            this.next = next;
        }

        HashEntry<K,V> withRemoved(final Generation gen, final HashEntry<K,V> target) {
            if (this == target) {
                return next;
            } else {
                return new HashEntry<K,V>(gen, key, hash, value, next.withRemoved(gen, target));
            }
        }
    }

    /** A node goes through three states: unused -> leaf -> branch.  In the
     *  unused state, the table is null.  In the leaf state, the table holds
     *  HashEntry-s, and updates to the node must be synchronized.  In the
     *  branch state, table is completely populated with Node-s, and no more
     *  changes are possible.
     */
    static final class Node<K,V> {
        private static final int UNUSED_UNIQ = -2;
        private static final int BRANCH_UNIQ = -1;

        final Generation mutableGen;
        private Object[] table;
        private volatile int uniq = UNUSED_UNIQ;

        /** Creates a new root node. */
        Node() {
            this.mutableGen = new Generation();
            this.table = new Object[BF];
            for (int i = 0; i < BF; ++i) {
                this.table[i] = new Node(this.mutableGen);
            }
            this.uniq = BRANCH_UNIQ;
        }

        /** Creates a new unused node. */
        Node(final Generation mutableGen) {
            this.mutableGen = mutableGen;
        }

        /** Creates a copy of <code>src</code>.  The caller is responsible for
         *  locking.
         */
        private Node(final Generation mutableGen, final Node<K,V> src) {
            this.mutableGen = mutableGen;
            if (src.table != null) {
                this.table = (Object[]) src.table.clone();
            }
            this.uniq = src.uniq;
        }

        Node<K,V> cloneForWrite(final Generation gen) {
            if (uniq == BRANCH_UNIQ) {
                // no lock required
                return new Node(gen, this);
            } else {
                synchronized (this) {
                    return new Node(gen, this);
                }
            }
        }

        //////// Reads

        private Object child(final int hash, final int shift) {
            return table[(hash >> shift) & (table.length - 1)];
        }

        @SuppressWarnings("unchecked")
        private Node<K,V> childNode(final int hash, final int shift) {
            return (Node<K,V>) child(hash, shift);
        }

        @SuppressWarnings("unchecked")
        private HashEntry<K,V> childEntry(final int hash, final int shift) {
            return (HashEntry<K,V>) child(hash, shift);
        }

        boolean containsKey(final K key, final int hash, final int shift) {
            final int u = uniq; // volatile read
            if (u == BRANCH_UNIQ) {
                // forward to child
                return childNode(hash, shift).containsKey(key, hash, shift + LOG_BF);
            } else if (u <= 0) {
                // unused, or empty
                return false;
            } else {
                // leaf hash map
                for (HashEntry<K,?> e = childEntry(hash, shift); e != null; e = e.next) {
                    if (e.hash == hash && key.equals(e.key)) {
                        return true;
                    }
                }
                return false;
            }
        }

        private synchronized <U> U lockedReadValue(final HashEntry<?,U> e) {
            return e.value;
        }

        /** This is only valid for a quiesced map. */
        boolean containsValue(final Object value) {
            final int u = uniq;
            if (u == BRANCH_UNIQ) {
                for (Object ch : table) {
                    if (((Node<?,?>) ch).containsValue(value)) {
                        return true;
                    }
                }
                return false;
            } else if (u <= 0) {
                // unused, or empty
                return false;
            } else {
                for (Object head : table) {
                    HashEntry<?,?> e = (HashEntry<?,?>) head;
                    while (e != null) {
                        Object v = e.value;
                        if (v == null) {
                            v = lockedReadValue(e);
                        }
                        if (value.equals(v)) {
                            return true;
                        }
                        e = e.next;
                    }
                }
                return false;
            }
        }

        V get(final K key, final int hash, final int shift) {
            final int u = uniq; // volatile read
            if (u == BRANCH_UNIQ) {
                // forward to child
                return childNode(hash, shift).get(key, hash, shift + LOG_BF);
            } else if (u <= 0) {
                // unused, or empty
                return null;
            } else {
                // leaf hash map
                for (HashEntry<K,V> e = childEntry(hash, shift); e != null; e = e.next) {
                    if (e.hash == hash && key.equals(e.key)) {
                        V v = e.value;
                        if (v == null) {
                            v = lockedReadValue(e);
                        }
                        return v;
                    }
                }
                return null;
            }
        }

        //////// Writes

        private Node<K,V> childNodeForWrite(final int hash, final int shift) {
            final Node<K,V> child = childNode(hash, shift);
            return (child.mutableGen == mutableGen) ? child : cloneChild(hash, shift, child);
        }

        private Node<K,V> cloneChild(final int hash, final int shift, final Node<K,V> child) {
            synchronized (child) {
                // child lock protects update to its incoming link, which must
                // be reread now that we actually have the lock
                final Node<K,V> reread = childNode(hash, shift);
                if (reread != child) {
                    // someone else did the clone for us
                    assert(reread.mutableGen == mutableGen);
                    return reread;
                }

                final Node<K,V> repl = new Node<K,V>(mutableGen, child);
                table[(hash >> shift) & (table.length - 1)] = repl;
                return repl;
            }
        }

        V put(final K key, final int hash, final V value, final int shift) {
            if (uniq != BRANCH_UNIQ) {
                synchronized (this) {
                    int u = uniq;
                    if (shouldSplit(u)) {
                        split(shift);
                    } else if (u != BRANCH_UNIQ) {
                        return leafPut(key, hash, value, shift, u);
                    }
                }
            }
            // forward to child
            return childNodeForWrite(hash, shift).put(key, hash, value, shift + LOG_BF);
        }

        private V leafPut(final K key, final int hash, final V value, final int shift, int u) {
            if (u == UNUSED_UNIQ) {
                table = new Object[LEAF_MIN_CAPACITY];
                u = 0;
            } else {
                growIfNecessary(shift, u);
            }

            final int i = (hash >> shift) & (table.length - 1);
            final HashEntry<K,V> head = (HashEntry<K,V>) table[i];
            int uDelta = 1;
            for (HashEntry<K,V> e = head; e != null; e = e.next) {
                if (e.hash == hash) {
                    if (key.equals(e.key)) {
                        // match
                        final V prev = e.value;
                        if (e.gen == mutableGen) {
                            // we have permission to mutate the node
                            e.value = value;
                        } else {
                            // we must replace the node
                            table[i] = new HashEntry<K,V>(mutableGen, key, hash, value, head.withRemoved(mutableGen, e));
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
            table[i] = new HashEntry<K,V>(mutableGen, key, hash, value, head);
            uniq = u + uDelta; // volatile store
            return null;
        }

        V remove(final K key, final int hash, final int shift) {
            if (uniq != BRANCH_UNIQ) {
                synchronized (this) {
                    int u = uniq;
                    if (u != BRANCH_UNIQ) {
                        return leafRemove(key, hash, shift, u);
                    }
                }
            }
            // forward to child
            return childNodeForWrite(hash, shift).remove(key, hash, shift + LOG_BF);
        }

        private V leafRemove(final K key, final int hash, final int shift, int u) {
            if (u <= 0) {
                return null;
            }

            shrinkIfNecessary(shift, u);

            final int i = (hash >> shift) & (table.length - 1);
            final HashEntry<K,V> head = (HashEntry<K,V>) table[i];
            int uDelta = -1;
            for (HashEntry<K,V> e = head; e != null; e = e.next) {
                if (e.hash == hash) {
                    if (key.equals(e.key)) {
                        // match
                        final HashEntry<K,V> target = e;

                        // continue the loop to get the right uDelta
                        if (uDelta != 0) {
                            e = e.next;
                            while (e != null) {
                                if (e.hash == hash) {
                                    uDelta = 0;
                                    break;
                                }
                                e = e.next;
                            }
                        }

                        // match
                        uniq = u + uDelta; // volatile store
                        table[i] = head.withRemoved(mutableGen, target);
                        return target.value;
                    }
                    // hash match, but not key match
                    uDelta = 0;
                }
            }
            // no match, no write
            return null;
        }

        //////// CAS-like

        V putIfAbsent(final K key, final int hash, final V value, final int shift) {
            if (uniq != BRANCH_UNIQ) {
                synchronized (this) {
                    int u = uniq;
                    if (shouldSplit(u)) {
                        split(shift);
                    } else if (u != BRANCH_UNIQ) {
                        return leafPutIfAbsent(key, hash, value, shift, u);
                    }
                }
            }
            // forward to child
            return childNodeForWrite(hash, shift).putIfAbsent(key, hash, value, shift + LOG_BF);
        }

        private V leafPutIfAbsent(final K key, final int hash, final V value, final int shift, int u) {
            if (u == UNUSED_UNIQ) {
                table = new Object[LEAF_MIN_CAPACITY];
                u = 0;
            } else {
                growIfNecessary(shift, u);
            }

            final int i = (hash >> shift) & (table.length - 1);
            final HashEntry<K,V> head = (HashEntry<K,V>) table[i];
            int uDelta = 1;
            for (HashEntry<K,V> e = head; e != null; e = e.next) {
                if (e.hash == hash) {
                    if (key.equals(e.key)) {
                        // match => no change, can't have been part of the unused -> leaf transition
                        return e.value;
                    }
                    // Hash match, but not a key match.  If we eventually insert,
                    // then we won't modify uniq.
                    uDelta = 0;
                }
            }
            // no match
            table[i] = new HashEntry<K,V>(mutableGen, key, hash, value, head);
            uniq = u + uDelta; // volatile store
            return null;
        }

        boolean replace(final K key, final int hash, final V oldValue, final V newValue, final int shift) {
            if (uniq != BRANCH_UNIQ) {
                synchronized (this) {
                    int u = uniq;
                    if (shouldSplit(u)) {
                        split(shift);
                    } else if (u != BRANCH_UNIQ) {
                        return leafReplace(key, hash, oldValue, newValue, shift, u);
                    }
                }
            }
            // forward to child
            return childNodeForWrite(hash, shift).replace(key, hash, oldValue, newValue, shift + LOG_BF);
        }

        private boolean leafReplace(final K key, final int hash, final V oldValue, final V newValue, final int shift, int u) {
            if (u <= 0) {
                return false;
            }

            final int i = (hash >> shift) & (table.length - 1);
            final HashEntry<K,V> head = (HashEntry<K,V>) table[i];
            for (HashEntry<K,V> e = head; e != null; e = e.next) {
                if (e.hash == hash && key.equals(e.key)) {
                    // key match
                    if (oldValue.equals(e.value)) {
                        // CAS success
                        if (e.gen == mutableGen) {
                            // we have permission to mutate the node
                            e.value = newValue;
                        } else {
                            // we must replace the node
                            table[i] = new HashEntry<K,V>(mutableGen, key, hash, newValue, head.withRemoved(mutableGen, e));
                        }
                        uniq = u; // volatile store
                        return true;
                    } else {
                        // CAS failure
                        return false;
                    }
                }
            }
            // no match
            return false;
        }

        V replace(final K key, final int hash, final V value, final int shift) {
            if (uniq != BRANCH_UNIQ) {
                synchronized (this) {
                    int u = uniq;
                    if (shouldSplit(u)) {
                        split(shift);
                    } else if (u != BRANCH_UNIQ) {
                        return leafReplace(key, hash, value, shift, u);
                    }
                }
            }
            // forward to child
            return childNodeForWrite(hash, shift).replace(key, hash, value, shift + LOG_BF);
        }

        private V leafReplace(final K key, final int hash, final V value, final int shift, int u) {
            if (u <= 0) {
                return null;
            }

            final int i = (hash >> shift) & (table.length - 1);
            final HashEntry<K,V> head = (HashEntry<K,V>) table[i];
            for (HashEntry<K,V> e = head; e != null; e = e.next) {
                if (e.hash == hash && key.equals(e.key)) {
                    // match
                    final V prev = e.value;
                    if (e.gen == mutableGen) {
                        // we have permission to mutate the node
                        e.value = value;
                    } else {
                        // we must replace the node
                        table[i] = new HashEntry<K,V>(mutableGen, key, hash, value, head.withRemoved(mutableGen, e));
                    }
                    uniq = u; // volatile store
                    return prev;
                }
            }
            // no match
            return null;
        }

        boolean remove(final K key, final int hash, final V value, final int shift) {
            if (uniq != BRANCH_UNIQ) {
                synchronized (this) {
                    int u = uniq;
                    if (shouldSplit(u)) {
                        split(shift);
                    } else if (u != BRANCH_UNIQ) {
                        return leafRemove(key, hash, value, shift, u);
                    }
                }
            }
            // forward to child
            return childNodeForWrite(hash, shift).remove(key, hash, value, shift + LOG_BF);
        }

        private boolean leafRemove(final K key, final int hash, final V value, final int shift, int u) {
            if (u <= 0) {
                return false;
            }

            shrinkIfNecessary(shift, u);

            final int i = (hash >> shift) & (table.length - 1);
            final HashEntry<K,V> head = (HashEntry<K,V>) table[i];
            int uDelta = -1;
            HashEntry<K,V> e = head;
            while (e != null) {
                if (e.hash == hash) {
                    if (key.equals(e.key)) {
                        // key match
                        if (!value.equals(e.value)) {
                            // CAS failure
                            return false;
                        }

                        final HashEntry<K,V> target = e;

                        // continue the loop to get the right uDelta
                        if (uDelta != 0) {
                            e = e.next;
                            while (e != null) {
                                if (e.hash == hash) {
                                    uDelta = 0;
                                    break;
                                }
                                e = e.next;
                            }
                        }

                        // match
                        uniq = u + uDelta; // volatile store
                        table[i] = head.withRemoved(mutableGen, target);
                        return true;
                    }
                    // hash match, but not key match
                    uDelta = 0;
                }
                e = e.next;
            }
            // no match
            return false;
        }


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
            final Object[] prevTable = table;
            table = new Object[newSize];
            for (Object head : prevTable) {
                reputAll(shift, (HashEntry<K,V>) head);
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
            final HashEntry<K,V> next = (HashEntry<K,V>) table[i];
            if (e.next == next) {
                // existing entry will work in the new table
                table[i] = e;
            } else {
                // copy required to get the next pointer right
                table[i] = new HashEntry<K,V>(mutableGen, e.key, e.hash, e.value, next);
            }
        }

        //////// Leaf splitting

        private boolean shouldSplit(final int u) {
            return u > maxLoad(LEAF_MAX_CAPACITY);
        }

        /** Caller must have locked the node. */
        private void split(final int shift) {
            final Object[] prevTable = table;

            // completely populate the new table
            table = new Object[BF];
            for (int i = 0; i < BF; ++i) {
                table[i] = new Node<K,V>(mutableGen);
            }

            // move all of the entries
            for (Object head : prevTable) {
                scatterAll(shift, (HashEntry<K,V>) head);
            }

            uniq = BRANCH_UNIQ; // volatile store
        }

        private void scatterAll(final int shift, final HashEntry<K,V> head) {
            if (head != null) {
                scatterAll(shift, head.next);
                childNode(head.hash, shift).putForSplit(shift + LOG_BF, head);
            }
        }

        private void putForSplit(final int shift, final HashEntry<K,V> entry) {
            int u = uniq;

            if (u == UNUSED_UNIQ) {
                // create the table if necessary
                table = new Object[LEAF_MIN_CAPACITY];
                u = 0;
            } else {
                growIfNecessary(shift, u);
            }

            final int i = (entry.hash >> shift) & (table.length - 1);
            final HashEntry<K,V> head = (HashEntry<K,V>) table[i];

            // is this hash a duplicate?
            HashEntry<K,V> e = head;
            while (e != null && e.hash != entry.hash) {
                e = e.next;
            }
            if (e == null) {
                ++u;
            }

            if (entry.next == head) {
                // no new entry needed
                table[i] = entry;
            } else {
                table[i] = new HashEntry<K,V>(mutableGen, entry.key, entry.hash, entry.value, head);
            }
            uniq = u;
        }
    }


    static class COWMgr<K,V> extends CopyOnWriteManager<Node<K,V>> {
        COWMgr() {
            super(new Node<K,V>(), 0);
        }

        COWMgr(final Node<K,V> initialValue, final int initialSize) {
            super(initialValue, initialSize);
        }

        protected Node<K, V> freezeAndClone(final Node<K,V> value) {
            return value.cloneForWrite(new Generation());
        }

        protected Node<K, V> cloneFrozen(final Node<K,V> frozenValue) {
            return frozenValue.cloneForWrite(new Generation());
        }
    }

    private transient volatile COWMgr<K,V> rootHolder;

    private static int hash(int h) {
        // taken from ConcurrentHashMap
        h += (h <<  15) ^ 0xffffcd7d;
        h ^= (h >>> 10);
        h += (h <<   3);
        h ^= (h >>>  6);
        h += (h <<   2) + (h << 14);
        return h ^ (h >>> 16);
    }

    //////// construction and cloning

    public SnapHashMap2() {
        this.rootHolder = new COWMgr<K,V>();
    }

    public SnapHashMap2(final Map<? extends K, ? extends V> source) {
        this.rootHolder = new COWMgr<K,V>();
        putAll(source);
    }

    public SnapHashMap2(final SortedMap<K,? extends V> source) {
        if (source instanceof SnapHashMap2) {
            final SnapHashMap2<K,V> s = (SnapHashMap2<K,V>) source;
            this.rootHolder = (COWMgr<K,V>) s.rootHolder.clone();
        }
        else {
            this.rootHolder = new COWMgr<K,V>();
            putAll(source);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public SnapHashMap2<K,V> clone() {
        final SnapHashMap2<K,V> copy;
        try {
            copy = (SnapHashMap2<K,V>) super.clone();
        } catch (final CloneNotSupportedException xx) {
            throw new InternalError();
        }
        copy.rootHolder = (COWMgr<K,V>) rootHolder.clone();
        return copy;
    }

    //////// public interface

    public void clear() {
        rootHolder = new COWMgr<K,V>();
    }

    public boolean isEmpty() {
        return rootHolder.isEmpty();
    }

    public int size() {
        return rootHolder.size();
    }

    public boolean containsKey(final Object key) {
        return rootHolder.read().containsKey((K) key, hash(key.hashCode()), ROOT_SHIFT);
    }

    public boolean containsValue(final Object value) {
        if (value == null) {
            throw new NullPointerException();
        }
        return rootHolder.frozen().containsValue(value);
    }

    public V get(final Object key) {
        return rootHolder.read().get((K) key, hash(key.hashCode()), ROOT_SHIFT);
    }

    public V put(final K key, final V value) {
        if (value == null) {
            throw new NullPointerException();
        }
        final int h = hash(key.hashCode());
        final Epoch.Ticket ticket = rootHolder.beginMutation();
        int sizeDelta = 0;
        try {
            final V prev = rootHolder.mutable().put(key, h, value, ROOT_SHIFT);
            if (prev == null) {
                sizeDelta = 1;
            }
            return prev;
        } finally {
            ticket.leave(sizeDelta);
        }
    }

    public V remove(final Object key) {
        final int h = hash(key.hashCode());
        final Epoch.Ticket ticket = rootHolder.beginMutation();
        int sizeDelta = 0;
        try {
            final V prev = rootHolder.mutable().remove((K) key, h, ROOT_SHIFT);
            if (prev != null) {
                sizeDelta = -1;
            }
            return prev;
        } finally {
            ticket.leave(sizeDelta);
        }
    }

    //////// CAS-like

    public V putIfAbsent(final K key, final V value) {
        if (value == null) {
            throw new NullPointerException();
        }
        final int h = hash(key.hashCode());
        final Epoch.Ticket ticket = rootHolder.beginMutation();
        int sizeDelta = 0;
        try {
            final V prev = rootHolder.mutable().putIfAbsent(key, h, value, ROOT_SHIFT);
            if (prev == null) {
                sizeDelta = 1;
            }
            return prev;
        } finally {
            ticket.leave(sizeDelta);
        }
    }

    public boolean replace(final K key, final V oldValue, final V newValue) {
        if (oldValue == null || newValue == null) {
            throw new NullPointerException();
        }
        final int h = hash(key.hashCode());
        final Epoch.Ticket ticket = rootHolder.beginMutation();
        try {
            return rootHolder.mutable().replace(key, h, oldValue, newValue, ROOT_SHIFT);
        } finally {
            ticket.leave(0);
        }
    }

    public V replace(final K key, final V value) {
        if (value == null) {
            throw new NullPointerException();
        }
        final int h = hash(key.hashCode());
        final Epoch.Ticket ticket = rootHolder.beginMutation();
        try {
            return rootHolder.mutable().replace(key, h, value, ROOT_SHIFT);
        } finally {
            ticket.leave(0);
        }
    }

    public boolean remove(final Object key, final Object value) {
        final int h = hash(key.hashCode());
        if (value == null) {
            return false;
        }
        final Epoch.Ticket ticket = rootHolder.beginMutation();
        int sizeDelta = 0;
        try {
            final boolean result = rootHolder.mutable().remove((K) key, h, (V) value, ROOT_SHIFT);
            if (result) {
                sizeDelta = -1;
            }
            return result;
        } finally {
            ticket.leave(sizeDelta);
        }
    }

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
            return SnapHashMap2.this.isEmpty();
        }
        public int size() {
            return SnapHashMap2.this.size();
        }
        public boolean contains(Object o) {
            return SnapHashMap2.this.containsKey(o);
        }
        public boolean remove(Object o) {
            return SnapHashMap2.this.remove(o) != null;
        }
        public void clear() {
            SnapHashMap2.this.clear();
        }
    }

    final class Values extends AbstractCollection<V> {
        public Iterator<V> iterator() {
            return new ValueIterator(rootHolder.frozen());
        }
        public boolean isEmpty() {
            return SnapHashMap2.this.isEmpty();
        }
        public int size() {
            return SnapHashMap2.this.size();
        }
        public boolean contains(Object o) {
            return SnapHashMap2.this.containsValue(o);
        }
        public void clear() {
            SnapHashMap2.this.clear();
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
            V v = SnapHashMap2.this.get(e.getKey());
            return v != null && v.equals(e.getValue());
        }
        public boolean remove(Object o) {
            if (!(o instanceof Entry))
                return false;
            Entry<?,?> e = (Entry<?,?>)o;
            return SnapHashMap2.this.remove(e.getKey(), e.getValue());
        }
        public boolean isEmpty() {
            return SnapHashMap2.this.isEmpty();
        }
        public int size() {
            return SnapHashMap2.this.size();
        }
        public void clear() {
            SnapHashMap2.this.clear();
        }
    }

    abstract class AbstractIter {

        private final Node<K,V> root;
        private int currentShift;
        private Node<K,V> currentLeaf;
        private HashEntry<K,V> currentEntry;
        private HashEntry<K,V> prevEntry;

        AbstractIter(final Node<K,V> frozenRoot) {
            this.root = frozenRoot;
            pushMin(frozenRoot, ROOT_SHIFT, 0);
        }

        private boolean pushMin(final Node<K,V> node, final int shift, final int minIndex) {
            for (int i = minIndex; i < BF; ++i) {
                final Node<K,V> child = (Node<K,V>) node.table[i];
                final int u = child.uniq;
                if (u == Node.BRANCH_UNIQ) {
                    // branch
                    if (pushMin(child, shift + LOG_BF, 0)) {
                        return true;
                    }
                } else if (u > 0) {
                    // non-empty leaf
                    for (Object head : child.table) {
                        if (head != null) {
                            // success
                            currentShift = shift + LOG_BF;
                            currentLeaf = child;
                            currentEntry = (HashEntry<K,V>) head;
                            return true;
                        }
                    }
                    throw new Error("logic error");
                }
                // else unused or empty
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
                    currentEntry = (HashEntry<K,V>) currentLeaf.table[i];
                    return;
                }
                ++i;
            }

            // now we are moving between nodes
            while (currentShift > ROOT_SHIFT) {
                // search within the parent
                currentShift -= LOG_BF;
                final Node<K,V> parent = findNode();
                if (pushMin(parent, currentShift, ((currentEntry.hash >> currentShift) & BF_MASK) + 1)) {
                    return;
                }
            }

            // we're done
            currentEntry = null;
        }

        private Node<K,V> findNode() {
            final int h = currentEntry.hash;
            Node<K,V> node = root;
            for (int s = ROOT_SHIFT; s < currentShift; s += LOG_BF) {
                node = node.childNode(h, s);
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
            SnapHashMap2.this.remove(prevEntry.key);
            prevEntry = null;
        }
    }

    final class KeyIterator extends AbstractIter implements Iterator<K>, Enumeration<K>  {
        KeyIterator(final Node<K,V> frozenRoot) {
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
        ValueIterator(final Node<K,V> frozenRoot) {
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
            SnapHashMap2.this.put(getKey(), value);
            return prev;
        }
    }

    final class EntryIterator extends AbstractIter implements Iterator<Entry<K,V>> {
        EntryIterator(final Node<K,V> frozenRoot) {
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
        final COWMgr<K,V> h = (COWMgr<K,V>) rootHolder.clone();

        xo.writeInt(h.size());
        writeEntry(xo, h.frozen());
    }

    private void writeEntry(final ObjectOutputStream xo, final Node<K,V> branch) throws IOException {
        int u = branch.uniq;
        if (u == Node.BRANCH_UNIQ) {
            for (Object child : branch.table) {
                writeEntry(xo, (Node<K,V>) child);
            }
        } else if (u > 0) {
            for (Object head : branch.table) {
                for (HashEntry<K,V> e = (HashEntry<K,V>) head; e != null; e = e.next) {
                    xo.writeObject(e.key);
                    xo.writeObject(e.value);
                }
            }
        }
    }

    /** Reverses {@link #writeObject(java.io.ObjectOutputStream)}. */
    private void readObject(final ObjectInputStream xi) throws IOException, ClassNotFoundException  {
        xi.defaultReadObject();

        final int size = xi.readInt();

        final Node<K,V> root = new Node<K,V>();
        for (int i = 0; i < size; ++i) {
            final K k = (K) xi.readObject();
            final V v = (V) xi.readObject();
            root.put(k, hash(k.hashCode()), v, ROOT_SHIFT);
        }

        rootHolder = new COWMgr<K,V>(root, size);
    }

    public static void main(final String[] args) {
        for (int i = 0; i < 10; ++i) {
            runOne(new SnapHashMap2<Integer,String>());
            runOne(new SnapHashMap2<Integer,String>());
            runOne(new SnapHashMap2<Integer,String>());
            runOne(new SnapHashMap1<Integer,String>());
            runOne(new SnapHashMap1<Integer,String>());
            runOne(new SnapHashMap1<Integer,String>());
            runOne(new java.util.concurrent.ConcurrentHashMap<Integer,String>());
            runOne(new java.util.concurrent.ConcurrentHashMap<Integer,String>());
            runOne(new java.util.concurrent.ConcurrentHashMap<Integer,String>());
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