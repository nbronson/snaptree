/* CCSTM - (c) 2009 Stanford University - PPL */

// LeafMap
package edu.stanford.ppl.concurrent;

public class SnapHashMap<K,V> {

    private static final int LOG_BF = 4;
    private static final int BF = 1 << LOG_BF;
    private static final int BF_MASK = BF - 1;

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

    /** A small hash table.  The caller is responsible for synchronization in most
     *  cases.
     */
    static class LeafMap<K,V> {
        static final int MIN_CAPACITY = 8;
        static final int MAX_CAPACITY = MIN_CAPACITY * BF;

        final Generation gen;

        /** The number of unique hash codes recorded in this LeafMap.  This is also
         *  used to establish synchronization order, by reading in containsKey and
         *  get and writing in any updating function.  We track unique hash codes
         *  instead of entries because LeafMaps are split into multiple LeafMaps
         *  when they grow too large.  If we used a basic count, then if many keys
         *  were present with the same hash code this splitting operation would not
         *  help to restore the splitting condition.
         */
        volatile int uniq;
        HashEntry<K,V>[] table;
        int threshold;
        final float loadFactor;

        @SuppressWarnings("unchecked")
        LeafMap(final Generation gen, final float loadFactor) {
            this.gen = gen;
            this.table = (HashEntry<K,V>[]) new HashEntry[MIN_CAPACITY];
            this.threshold = (int) (MIN_CAPACITY * loadFactor);
            this.loadFactor = loadFactor;
        }

        boolean containsKeyU(final K key, final int hash) {
            if (uniq == 0) { // volatile read
                return false;
            }
            HashEntry<K,V> e = table[hash & (table.length - 1)];
            while (e != null) {
                if (e.hash == hash && key.equals(e.key)) {
                    return true;
                }
                e = e.next;
            }
            return false;
        }

        private synchronized V lockedReadValue(final HashEntry<K,V> e) {
            return e.value;
        }

        /** This is only valid for a quiesced map. */
        boolean containsValueQ(final Object value) {
            for (HashEntry<K,V> head : table) {
                HashEntry<K,V> e = head;
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
            HashEntry<K,V> e = table[hash & (table.length - 1)];
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
            final HashEntry<K,V>[] prevTable = table;
            table = (HashEntry<K,V>[]) new HashEntry[newSize];
            for (HashEntry<K,V> head : prevTable) {
                reputAllL(head);
            }
        }

        private void reputAllL(final HashEntry<K,V> head) {
            if (head != null) {
                reputAllL(head.next);
                reputL(head);
            }
        }

        private void reputL(final HashEntry<K,V> e) {
            final int i = e.hash & (table.length - 1);
            final HashEntry<K,V> next = table[i];
            if (e.next == next) {
                // no new entry needed
                table[i] = e;
            } else {
                table[i] = new HashEntry<K,V>(gen, e.key, e.hash, e.value, next);
            }
        }

        V putL(final K key, final int hash, final V value) {
            growIfNecessaryL();
            final int i = hash & (table.length - 1);
            final HashEntry<K,V> head = table[i];
            HashEntry<K,V> e = head;
            int insDelta = 1;
            while (e != null) {
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
            table[i] = new HashEntry<K,V>(gen, key, hash, value, head);
            uniq += insDelta; // volatile store
            return null;
        }

        V removeL(final K key, final int hash) {
            shrinkIfNecessaryL();
            final int i = hash & (table.length - 1);
            final HashEntry<K,V> head = table[i];
            HashEntry<K,V> e = head;
            int delDelta = -1;
            while (e != null) {
                if (e.hash == hash) {
                    if (key.equals(e.key)) {
                        // match
                        final HashEntry<K,V> target = e;

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
                        table[i] = head.withRemoved(gen, target);
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
            final HashEntry<K,V> head = table[i];
            HashEntry<K,V> e = head;
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
            table[i] = new HashEntry<K,V>(gen, key, hash, value, head);
            uniq += insDelta; // volatile store
            return null;
        }

        boolean replaceL(final K key, final int hash, final V oldValue, final V newValue) {
            final int i = hash & (table.length - 1);
            final HashEntry<K,V> head = table[i];
            HashEntry<K,V> e = head;
            while (e != null) {
                if (e.hash == hash && key.equals(e.key)) {
                    // key match
                    if (oldValue.equals(e.value)) {
                        // CAS success
                        if (e.gen == gen) {
                            // we have permission to mutate the node
                            e.value = newValue;
                        } else {
                            // we must replace the node
                            table[i] = new HashEntry<K,V>(gen, key, hash, newValue, head.withRemoved(gen, e));
                        }
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
            final HashEntry<K,V> head = table[i];
            HashEntry<K,V> e = head;
            while (e != null) {
                if (e.hash == hash && key.equals(e.key)) {
                    // match
                    final V prev = e.value;
                    if (e.gen == gen) {
                        // we have permission to mutate the node
                        e.value = value;
                    } else {
                        // we must replace the node
                        table[i] = new HashEntry<K,V>(gen, key, hash, value, head.withRemoved(gen, e));
                    }
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
            final HashEntry<K,V> head = table[i];
            HashEntry<K,V> e = head;
            int delDelta = -1;
            while (e != null) {
                if (e.hash == hash) {
                    if (key.equals(e.key)) {
                        // key match
                        if (!value.equals(e.value)) {
                            // CAS failure
                            return false;
                        }

                        final HashEntry<K,V> target = e;

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
                        table[i] = head.withRemoved(gen, target);
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
                pieces[i] = new LeafMap<K,V>(gen, loadFactor);
            }
            for (HashEntry<K,V> head : table) {
                scatterAllL(pieces, shift, head);
            }
            return pieces;
        }

        private static <K,V> void scatterAllL(final LeafMap<K,V>[] pieces, final int shift, final HashEntry<K,V> head) {
            if (head != null) {
                scatterAllL(pieces, shift, head.next);
                pieces[(head.hash >> shift) & (pieces.length - 1)].putL(head);
            }
        }

        private void putL(final HashEntry<K,V> entry) {
            growIfNecessaryL();
            final int i = entry.hash & (table.length - 1);
            final HashEntry<K,V> head = table[i];

            // is this hash a duplicate?
            HashEntry<K,V> e = head;
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
                table[i] = new HashEntry<K,V>(gen, e.key, e.hash, e.value, head);
            }
        }
    }
}
