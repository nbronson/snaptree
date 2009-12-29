/* CCSTM - (c) 2009 Stanford University - PPL */

// LeafMap
package edu.stanford.ppl.concurrent;

/** A small hash table.  The caller is responsible for synchronization in most
 *  cases.
 */
class LeafMap<K,V> {
    static final int MIN_CAPACITY = 8;
    static final int MAX_CAPACITY = MIN_CAPACITY * 32; // TODO: SnapHashMap.BF;

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
    LeafMap(final float loadFactor) {
        this.table = (EntryImpl<K,V>[]) new EntryImpl[MIN_CAPACITY];
        this.threshold = (int) (MIN_CAPACITY * loadFactor);
        this.loadFactor = loadFactor;
    }

    boolean containsKeyU(final K key, final int hash) {
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
    boolean containsValueQ(final Object value) {
        for (EntryImpl<K,V> head : table) {
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

    V getU(final K key, final int hash) {
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

    private void growIfNecessaryL() {
        if (uniq > threshold && table.length < MAX_CAPACITY) {
            rehashL(table.length * 2);
        }
    }

    private void shrinkIfNecessaryL() {
        if (uniq < (threshold >> 1) && table.length > MIN_CAPACITY) {
            rehashL(table.length / 2);
        }
    }

    @SuppressWarnings("unchecked")
    private void rehashL(final int newSize) {
        threshold = (int) (loadFactor * newSize);
        final EntryImpl<K,V>[] prevTable = table;
        table = (EntryImpl<K,V>[]) new EntryImpl[newSize];
        for (EntryImpl<K,V> head : prevTable) {
            reputAllL(head);
        }
    }

    private void reputAllL(final EntryImpl<K,V> head) {
        if (head != null) {
            reputAllL(head.next);
            reputL(head);
        }
    }

    private void reputL(final EntryImpl<K,V> e) {
        final int i = e.hash & (table.length - 1);
        final EntryImpl<K,V> next = table[i];
        if (e.next == next) {
            // no new entry needed
            table[i] = e;
        } else {
            table[i] = new EntryImpl<K,V>(e.key, e.hash, e.value, next);
        }
    }

    V putL(final K key, final int hash, final V value) {
        growIfNecessaryL();
        final int i = hash & (table.length - 1);
        EntryImpl<K,V> e = table[i];
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
        table[i] = new EntryImpl<K,V>(key, hash, value, null);
        uniq += insDelta; // volatile store
        return null;
    }

    V removeL(final K key, final int hash) {
        shrinkIfNecessaryL();
        final int i = hash & (table.length - 1);
        final EntryImpl<K,V> head = table[i];
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
                    table[i] = head.withRemoved(target);
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

    V putIfAbsentL(final K key, final int hash, final V value) {
        growIfNecessaryL();
        final int i = hash & (table.length - 1);
        EntryImpl<K,V> e = table[i];
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
        table[i] = new EntryImpl<K,V>(key, hash, value, null);
        uniq += insDelta; // volatile store
        return null;
    }

    boolean replaceL(final K key, final int hash, final V oldValue, final V newValue) {
        final int i = hash & (table.length - 1);
        EntryImpl<K,V> e = table[i];
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

    V replaceL(final K key, final int hash, final V value) {
        final int i = hash & (table.length - 1);
        EntryImpl<K,V> e = table[i];
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

    boolean removeL(final K key, final int hash, final V value) {
        shrinkIfNecessaryL();
        final int i = hash & (table.length - 1);
        final EntryImpl<K,V> head = table[i];
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
                    table[i] = head.withRemoved(target);
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

    boolean shouldSplitL() {
        return uniq > threshold && table.length == MAX_CAPACITY;
    }

    @SuppressWarnings("unchecked")
    LeafMap<K,V>[] splitL(final int shift, final int bf) {
        final LeafMap<K,V>[] pieces = (LeafMap<K,V>[]) new LeafMap[bf];
        for (int i = 0; i < pieces.length; ++i) {
            pieces[i] = new LeafMap<K,V>(loadFactor);
        }
        for (EntryImpl<K,V> head : table) {
            scatterAllL(pieces, shift, head);
        }
        return pieces;
    }

    private static <K,V> void scatterAllL(final LeafMap<K,V>[] pieces, final int shift, final EntryImpl<K,V> head) {
        if (head != null) {
            scatterAllL(pieces, shift, head.next);
            pieces[(head.hash >> shift) & (pieces.length - 1)].putL(head);
        }
    }

    private void putL(final EntryImpl<K,V> entry) {
        growIfNecessaryL();
        final int i = entry.hash & (table.length - 1);
        final EntryImpl<K,V> head = table[i];

        // is this hash a duplicate?
        EntryImpl<K,V> e = head;
        while (e != null && e.hash != entry.hash) {
            e = e.next;
        }
        if (e == null) {
            ++uniq;
        }

        if (e.next == head) {
            // no new entry needed
            table[i] = e;
        } else {
            table[i] = new EntryImpl<K,V>(e.key, e.hash, e.value, head);
        }
    }
}
