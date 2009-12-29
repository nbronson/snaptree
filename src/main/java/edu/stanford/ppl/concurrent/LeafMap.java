/* CCSTM - (c) 2009 Stanford University - PPL */

// LeafMap
package edu.stanford.ppl.concurrent;

/** A small hash table. */
class LeafMap<K,V> {
    static class EntryImpl<K,V> {
        final K key;
        final int hash;
        V value;
        final EntryImpl<K,V> next;

        EntryImpl(final K key, final int hash, final V value, final EntryImpl<K,V> next) {
            this.key = key;
            this.hash = hash;
            this.value = value;
            this.next = next;
        }

        private EntryImpl<K,V> withRemoved(final EntryImpl<K,V> target) {
            if (this == target) {
                return next;
            } else {
                return new EntryImpl<K,V>(key, hash, value, next.withRemoved(target));
            }
        }
    }

    /** The number of unique hash codes recorded in this LeafMap.  This is also
     *  used to establish synchronization order, by reading in containsKey and
     *  get and writing in any updating function.  We track unique hash codes
     *  instead of entries because LeafMaps are split into multiple LeafMaps
     *  when they grow too large.  If we used a basic count, then if many keys
     *  were present with the same hash code this splitting operation would not
     *  help to restore the splitting condition.
     */
    volatile int uniq;
    EntryImpl<K,V>[] table;
    int threshold;
    final float loadFactor;

    @SuppressWarnings("unchecked")
    LeafMap(final int initialCapacity, final float loadFactor) {
        this.table = (EntryImpl<K,V>[]) new EntryImpl[initialCapacity];
        this.threshold = (int) (initialCapacity * loadFactor);
        this.loadFactor = loadFactor;
    }

    boolean containsKey(final K key, final int hash) {
        if (uniq == 0) { // volatile read
            return false;
        }
        EntryImpl<K,V> e = table[hash & (table.length - 1)];
        while (e != null) {
            if (e.hash == hash && key.equals(e.key)) {
                return true;
            }
            e = e.next;
        }
        return false;
    }

    private synchronized V lockedReadValue(final EntryImpl<K,V> e) {
        return e.value;
    }

    /** This is only valid for a quiesced map. */
    boolean containsValue(final Object value) {
        final EntryImpl<K,V>[] t = table;
        for (EntryImpl<K,V> head : t) {
            EntryImpl<K,V> e = head;
            while (e != null) {
                V v = e.value;
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

    V get(final K key, final int hash) {
        if (uniq == 0) { // volatile read
            return null;
        }
        EntryImpl<K,V> e = table[hash & (table.length - 1)];
        while (e != null) {
            if (e.hash == hash && key.equals(e.key)) {
                final V v = e.value;
                if (v == null) {
                    return lockedReadValue(e);
                }
                return v;
            }
            e = e.next;
        }
        return null;
    }

    private void grow() {
        rehash(table.length * 2);
    }

    private void rehash(final int newSize) {
        final EntryImpl<K,V>[] t = table;
        final EntryImpl<K,V>[] n = (EntryImpl<K,V>[]) new EntryImpl[newSize];

        for (EntryImpl<K,V> head : t) {
            reputAll(n, head);
        }
        table = n;
        threshold = (int) (loadFactor * newSize);
    }

    private static <K,V> void reputAll(final EntryImpl<K,V>[] newTable, final EntryImpl<K,V> head) {
        if (head != null) {
            reputAll(newTable, head.next);
            reput(newTable, head);
        }
    }

    private static <K,V> void reput(final EntryImpl<K,V>[] newTable, final EntryImpl<K,V> e) {
        final int i = e.hash & (newTable.length - 1);
        final EntryImpl<K,V> next = newTable[i];
        if (e.next == next) {
            // no new entry needed
            newTable[i] = e;
        } else {
            newTable[i] = new EntryImpl<K,V>(e.key, e.hash, e.value, next);
        }
    }

    synchronized V put(final K key, final int hash, final V value) {
        final EntryImpl<K,V>[] t = table;
        final int i = hash & (t.length - 1);
        final EntryImpl<K,V> head = t[i];
        EntryImpl<K,V> e = head;
        int insDelta = 1;
        while (e != null) {
            if (e.hash == hash) {
                if (key.equals(e.key)) {
                    // match
                    final V prev = e.value;
                    e.value = value;
                    uniq = uniq; // volatile store
                    return prev;
                }
                // Hash match, but not a key match.  If we eventually insert,
                // then we won't modify uniq.
                insDelta = 0;
            }
            e = e.next;
        }
        // no match
        t[i] = new EntryImpl<K,V>(key, hash, value, null);
        uniq += insDelta; // volatile store
        return null;
    }

    synchronized V remove(final K key, final int hash) {
        final EntryImpl<K,V>[] t = table;
        final int i = hash & (t.length - 1);
        final EntryImpl<K,V> head = t[i];
        EntryImpl<K,V> e = head;
        int delDelta = -1;
        while (e != null) {
            if (e.hash == hash) {
                if (key.equals(e.key)) {
                    // match
                    final EntryImpl<K, V> target = e;

                    // continue the loop to get the right delDelta
                    if (delDelta != 0) {
                        e = e.next;
                        while (e != null) {
                            if (e.hash == hash) {
                                delDelta = 0;
                                break;
                            }
                            e = e.next;
                        }
                    }

                    // match
                    uniq += delDelta; // volatile store
                    t[i] = head.withRemoved(target);
                    return target.value;
                }
                // hash match, but not key match
                delDelta = 0;
            }
            e = e.next;
        }
        // no match
        return null;
    }

    //////// CAS-like

    synchronized V putIfAbsent(final K key, final int hash, final V value) {
        final EntryImpl<K,V>[] t = table;
        final int i = hash & (t.length - 1);
        final EntryImpl<K,V> head = t[i];
        EntryImpl<K,V> e = head;
        int insDelta = 1;
        while (e != null) {
            if (e.hash == hash) {
                if (key.equals(e.key)) {
                    // match => failure
                    return e.value;
                }
                // Hash match, but not a key match.  If we eventually insert,
                // then we won't modify uniq.
                insDelta = 0;
            }
            e = e.next;
        }
        // no match
        t[i] = new EntryImpl<K,V>(key, hash, value, null);
        uniq += insDelta; // volatile store
        return null;
    }

    synchronized boolean replace(final K key, final int hash, final V oldValue, final V newValue) {
        final EntryImpl<K,V>[] t = table;
        final int i = hash & (t.length - 1);
        final EntryImpl<K,V> head = t[i];
        EntryImpl<K,V> e = head;
        while (e != null) {
            if (e.hash == hash && key.equals(e.key)) {
                // key match
                if (oldValue.equals(e.value)) {
                    // CAS success
                    e.value = newValue;
                    uniq = uniq; // volatile store
                    return true;
                } else {
                    // CAS failure
                    return false;
                }
            }
            e = e.next;
        }
        // no match
        return false;
    }

    synchronized V replace(final K key, final int hash, final V value) {
        final EntryImpl<K,V>[] t = table;
        final int i = hash & (t.length - 1);
        final EntryImpl<K,V> head = t[i];
        EntryImpl<K,V> e = head;
        while (e != null) {
            if (e.hash == hash && key.equals(e.key)) {
                // match
                final V prev = e.value;
                e.value = value;
                uniq = uniq; // volatile store
                return prev;
            }
            e = e.next;
        }
        // no match
        return null;
    }

    synchronized boolean remove(final K key, final int hash, final V value) {
        final EntryImpl<K,V>[] t = table;
        final int i = hash & (t.length - 1);
        final EntryImpl<K,V> head = t[i];
        EntryImpl<K,V> e = head;
        int delDelta = -1;
        while (e != null) {
            if (e.hash == hash) {
                if (key.equals(e.key)) {
                    // key match
                    if (!value.equals(e.value)) {
                        // CAS failure
                        return false;
                    }

                    final EntryImpl<K, V> target = e;

                    // continue the loop to get the right delDelta
                    if (delDelta != 0) {
                        e = e.next;
                        while (e != null) {
                            if (e.hash == hash) {
                                delDelta = 0;
                                break;
                            }
                            e = e.next;
                        }
                    }

                    // match
                    uniq += delDelta; // volatile store
                    t[i] = head.withRemoved(target);
                    return true;
                }
                // hash match, but not key match
                delDelta = 0;
            }
            e = e.next;
        }
        // no match
        return false;
    }

    //////// Leaf splitting

    boolean shouldSplit() {
        return uniq > 100; // TODO SPLIT_THRESHOLD;
    }

    LeafMap<K,V>[] split(final int shift, final int bf) {
        final LeafMap<K,V>[] pieces = (LeafMap<K,V>[]) new LeafMap[bf];
        
    }
}
