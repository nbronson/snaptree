/* SnapTree - (c) 2009 Stanford University - PPL */

// SnapTreeMap

package edu.stanford.ppl.concurrent;

import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

public class SnapTreeMap<K,V> extends AbstractMap<K,V> implements ConcurrentMap<K,V>, Cloneable {

    /** This is a special value that indicates the presence of a null value,
     *  to differentiate from the absence of a value.
     */
    static final Object SpecialNull = new Object();

    /** This is a special value that indicates that an optimistic read
     *  failed.
     */ 
    static final Object SpecialRetry = new Object();


    /** The number of spins before yielding. */
    static final int SpinCount = Integer.parseInt(System.getProperty("spin", "100"));

    /** The number of yields before blocking. */
    static final int YieldCount = Integer.parseInt(System.getProperty("yield", "0"));

    
    // we encode directions as characters
    static final char Left = 'L';
    static final char Right = 'R';


    /** An <tt>OVL</tt> is a version number and lock used for optimistic
     *  concurrent control of some program invariant.  If  {@link #isShrinking}
     *  then the protected invariant is changing.  If two reads of an OVL are
     *  performed that both see the same non-changing value, the reader may
     *  conclude that no changes to the protected invariant occurred between
     *  the two reads.  The special value UnlinkedOVL is not changing, and is
     *  guaranteed to not result from a normal sequence of beginChange and
     *  endChange operations.
     *  <p>
     *  For convenience <tt>endChange(ovl) == endChange(beginChange(ovl))</tt>.
     */
    static long beginChange(long ovl) { return ovl | 1; }
    static long endChange(long ovl) { return (ovl | 3) + 1; }
    static long UnlinkedOVL = 2;

    static boolean isShrinking(long ovl) { return (ovl & 1) != 0; }
    static boolean isUnlinked(long ovl) { return (ovl & 2) != 0; }
    static boolean isShrinkingOrUnlinked(long ovl) { return (ovl & 3) != 0L; }


    private static class Node<K,V> implements Map.Entry<K,V> {
        final K key;
        volatile int height;

        /** null means this node is conceptually not present in the map.
         *  SpecialNull means the value is null.
         */
        volatile Object vOpt;
        volatile Node<K,V> parent;
        volatile long shrinkOVL;
        volatile Node<K,V> left;
        volatile Node<K,V> right;

        Node(final K key,
              final int height,
              final Object vOpt,
              final Node<K, V> parent,
              final long shrinkOVL,
              final Node<K, V> left,
              final Node<K, V> right)
        {
            this.key = key;
            this.height = height;
            this.vOpt = vOpt;
            this.parent = parent;
            this.shrinkOVL = shrinkOVL;
            this.left = left;
            this.right = right;
        }

        @Override
        public K getKey() { return key; }

        @Override
        @SuppressWarnings("unchecked")
        public V getValue() {
            final Object tmp = vOpt;
            return tmp == SpecialNull ? null : (V)tmp;
        }

        @Override
        public V setValue(final V v) {
            throw new UnsupportedOperationException();
        }

        Node<K,V> child(char dir) { return dir == Left ? left : right; }
        Node<K,V> childSibling(char dir) { return dir == Left ? right : left; }

        void setChild(char dir, Node<K,V> node) {
            if (dir == Left) {
                left = node;
            } else {
                right = node;
            }
        }

        //////// copy-on-write stuff

        private static <K,V> boolean isShared(final Node<K,V> node) {
            return node != null && node.parent == null; 
        }

        static <K,V> Node<K,V> markShared(final Node<K,V> node) {
            if (node != null) {
                node.parent = null;
            }
            return node;
        }

        private Node<K,V> lazyCopy(Node<K,V> newParent) {
            assert (isShared(this));
            assert (!isShrinkingOrUnlinked(shrinkOVL));

            return new Node<K,V>(key, height, vOpt, newParent, 0L, markShared(left), markShared(right));
        }

        Node<K,V> unsharedLeft() {
            final Node<K,V> cl = left;
            if (!isShared(cl)) {
                return cl;
            } else {
                lazyCopyChildren();
                return left;
            }
        }

        Node<K,V> unsharedRight() {
            final Node<K,V> cr = right;
            if (!isShared(cr)) {
                return cr;
            } else {
                lazyCopyChildren();
                return right;
            }
        }

        Node<K,V> unsharedChild(final char dir) {
            return dir == Left ? unsharedLeft() : unsharedRight();
        }

        private synchronized void lazyCopyChildren() {
            final Node<K,V> cl = left;
            if (isShared(cl)) {
                left = cl.lazyCopy(this);
            }
            final Node<K,V> cr = right;
            if (isShared(cr)) {
                right = cr.lazyCopy(this);
            }
        }

        //////// per-node blocking

        private void waitUntilShrinkCompleted(final long ovl) {
            if (!isShrinking(ovl)) {
                return;
            }

            for (int tries = 0; tries < SpinCount; ++tries) {
                if (shrinkOVL != ovl) {
                    return;
                }
            }

            for (int tries = 0; tries < YieldCount; ++tries) {
                Thread.yield();
                if (shrinkOVL != ovl) {
                    return;
                }
            }

            // spin and yield failed, use the nuclear option
            synchronized (this) {
                // we can't have gotten the lock unless the shrink was over
            }
            assert(shrinkOVL != ovl);
        }


    }

    private static class RootHolder<K,V> extends Node<K,V> {
        final Epoch epoch;

        RootHolder() {
            super(null, 1, null, null, 0L, null, null);
            epoch = new Epoch(0);
        }

        RootHolder(final RootHolder<K,V> snapshot) {
            super(null, 1 + snapshot.height, null, null, 0L, null, snapshot.right);
            epoch = new Epoch(snapshot.epoch.size());
        }
    }

    //////// node access functions

    private static int height(final Node<?,?> node) {
        return node == null ? 0 : node.height;
    }

    @SuppressWarnings("unchecked")
    private V decodeNull(final Object vOpt) {
        assert (vOpt != SpecialRetry);
        return vOpt == SpecialNull ? null : (V)vOpt;
    }

    private static Object encodeNull(final Object v) {
        return v == null ? SpecialNull : v;
    }

    //////////////// state

    private Comparator<? super K> comparator;
    private AtomicReference<RootHolder<K,V>> holderRef
            = new AtomicReference<RootHolder<K,V>>(new RootHolder<K,V>());
    private final EntrySet entries = new EntrySet();

    //////////////// public interface

    public SnapTreeMap() {
    }

    public SnapTreeMap(final Comparator<? super K> comparator) {
        this.comparator = comparator;
    }

    @SuppressWarnings("unchecked")
    @Override
    public SnapTreeMap<K,V> clone() {
        final SnapTreeMap<K,V> m;
        try {
            m = (SnapTreeMap<K,V>) super.clone();
        } catch (final CloneNotSupportedException xx) {
            throw new InternalError();
        }
        m.comparator = comparator;
        m.holderRef = new AtomicReference<RootHolder<K,V>>(new RootHolder<K,V>(completedHolder()));
        return m;
    }

    private RootHolder<K,V> completedHolder() {
        final RootHolder<K,V> h = holderRef.get();
        h.epoch.shutdown();
        return h;
    }

    @Override
    public int size() {
        return completedHolder().epoch.size();
    }

    @Override
    public boolean isEmpty() {
        // removed-but-not-unlinked nodes cannot be leaves, so if the tree is
        // truly empty then the root holder has no right child
        return holderRef.get().right == null;
    }

    @Override
    public void clear() {
        // TODO: implement
        throw new UnsupportedOperationException();
    }

    //////// search

    @Override
    public boolean containsKey(final Object key) {
        return getImpl(key) != null;
    }

    @Override
    public V get(final Object key) {
        return decodeNull(getImpl(key));
    }

    @SuppressWarnings("unchecked")
    private Comparable<? super K> comparable(final Object key) {
        if (key == null) {
            throw new NullPointerException();
        }
        if (comparator == null) {
            return (Comparable<? super K>)key;
        }
        return new Comparable<K>() {
            final Comparator<? super K> _cmp = comparator;

            @SuppressWarnings("unchecked")
            public int compareTo(final K rhs) { return _cmp.compare((K)key, rhs); }
        };
    }

    /** Returns either a value or SpecialNull, if present, or null, if absent. */
    private Object getImpl(final Object key) {
        final Comparable<? super K> k = comparable(key);
        
        while (true) {
            final Node<K,V> right = holderRef.get().right;
            if (right == null) {
                return null;
            } else {
                final int rightCmp = k.compareTo(right.key);
                if (rightCmp == 0) {
                    // who cares how we got here
                    return right.vOpt;
                }

                final long ovl = right.shrinkOVL;
                if (isShrinkingOrUnlinked(ovl)) {
                    right.waitUntilShrinkCompleted(ovl);
                    // RETRY
                } else if (right == holderRef.get().right) {
                    // the reread of .right is the one protected by our read of ovl
                    final Object vo = attemptGet(k, right, (rightCmp < 0 ? Left : Right), ovl);
                    if (vo != SpecialRetry) {
                        return vo;
                    }
                    // else RETRY
                }
            }
        }
    }

    private Object attemptGet(final Comparable<? super K> k,
                              final Node<K,V> node,
                              final char dirToC,
                              final long nodeOVL) {
        while (true) {
            final Node<K,V> child = node.child(dirToC);

            if (child == null) {
                if (node.shrinkOVL != nodeOVL) {
                    return SpecialRetry;
                }

                // Note is not present.  Read of node.child occurred while
                // parent.child was valid, so we were not affected by any
                // shrinks.
                return null;
            } else {
                final int childCmp = k.compareTo(child.key);
                if (childCmp == 0) {
                    // how we got here is irrelevant
                    return child.vOpt;
                }

                // child is non-null
                final long childOVL = child.shrinkOVL;
                if (isShrinkingOrUnlinked(childOVL)) {
                    child.waitUntilShrinkCompleted(childOVL);

                    if (node.shrinkOVL != nodeOVL) {
                        return SpecialRetry;
                    }
                    // else RETRY
                } else if (child != node.child(dirToC)) {
                    // this .child is the one that is protected by childOVL
                    if (node.shrinkOVL != nodeOVL) {
                        return SpecialRetry;
                    }
                    // else RETRY
                } else {
                    if (node.shrinkOVL != nodeOVL) {
                        return SpecialRetry;
                    }

                    // At this point we know that the traversal our parent took
                    // to get to node is still valid.  The recursive
                    // implementation will validate the traversal from node to
                    // child, so just prior to the nodeOVL validation both
                    // traversals were definitely okay.  This means that we are
                    // no longer vulnerable to node shrinks, and we don't need
                    // to validate nodeOVL any more.
                    final Object vo = attemptGet(k, child, (childCmp < 0 ? Left : Right), childOVL);
                    if (vo != SpecialRetry) {
                        return vo;
                    }
                    // else RETRY
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    // TODO: @Override
    public K firstKey() {
        return (K) firstImpl(true);
    }

    @SuppressWarnings("unchecked")
    public Map.Entry<K,V> firstEntry() {
        return (Map.Entry<K,V>) firstImpl(false);
    }

    /** Returns a key if returnKey is true, a Map.Entry otherwise. */
    private Object firstImpl(final boolean returnKey) {
        while (true) {
            final Node<K,V> right = holderRef.get().right;
            if (right == null) {
                throw new NoSuchElementException();
            } else {
                final long ovl = right.shrinkOVL;
                if (isShrinkingOrUnlinked(ovl)) {
                    right.waitUntilShrinkCompleted(ovl);
                    // RETRY
                } else if (right == holderRef.get().right) {
                    // the reread of .right is the one protected by our read of ovl
                    final Object vo = attemptFirst(returnKey, right, ovl);
                    if (vo != SpecialRetry) {
                        return vo;
                    }
                    // else RETRY
                }
            }
        }
    }

    private Object attemptFirst(final boolean returnKey, final Node<K,V> node, final long nodeOVL) {
        while (true) {
            final Node<K,V> child = node.left;

            if (child == null) {
                // read of the value must be protected by the OVL, because we
                // must linearize against another thread that inserts a new min
                // key and then changes this key's value
                final Object vo = node.vOpt;

                if (node.shrinkOVL != nodeOVL) {
                    return SpecialRetry;
                }
                
                assert(vo != null);

                return returnKey ? node.key : new SimpleImmutableEntry<K,V>(node.key, decodeNull(vo));
            } else {
                // child is non-null
                final long childOVL = child.shrinkOVL;
                if (isShrinkingOrUnlinked(childOVL)) {
                    child.waitUntilShrinkCompleted(childOVL);

                    if (node.shrinkOVL != nodeOVL) {
                        return SpecialRetry;
                    }
                    // else RETRY
                } else if (child != node.left) {
                    // this .child is the one that is protected by childOVL
                    if (node.shrinkOVL != nodeOVL) {
                        return SpecialRetry;
                    }
                    // else RETRY
                } else {
                    if (node.shrinkOVL != nodeOVL) {
                        return SpecialRetry;
                    }

                    final Object vo = attemptFirst(returnKey, child, childOVL);
                    if (vo != SpecialRetry) {
                        return vo;
                    }
                    // else RETRY
                }
            }
        }
    }

    //////////////// update

    private static abstract class UpdateFunction {
        abstract boolean isDefinedAt(final Object prev, final Object expected);
    }

    private static final UpdateFunction Always = new UpdateFunction() {
        boolean isDefinedAt(final Object prev, final Object expected) {
            return true;
        }
    };
    private static final UpdateFunction IfAbsent = new UpdateFunction() {
        boolean isDefinedAt(final Object prev, final Object expected) {
            return prev == null;
        }
    };
    private static final UpdateFunction IfPresent = new UpdateFunction() {
        boolean isDefinedAt(final Object prev, final Object expected) {
            return prev != null;
        }
    };
    private static final UpdateFunction IfEq = new UpdateFunction() {
        boolean isDefinedAt(final Object prev, final Object expected) {
            return prev == expected;
        }
    };

    @Override
    public V put(final K key, final V value) {
        return decodeNull(update(key, Always, null, encodeNull(value)));
    }

    @Override
    public V putIfAbsent(final K key, final V value) {
        return decodeNull(update(key, IfAbsent, null, encodeNull(value)));
    }

    @Override
    public V replace(final K key, final V value) {
        return decodeNull(update(key, IfPresent, null, encodeNull(value)));
    }

    @Override
    public boolean replace(final K key, final V oldValue, final V newValue) {
        return update(key, IfEq, encodeNull(oldValue), encodeNull(newValue)) == encodeNull(oldValue);
    }

    @Override
    public V remove(final Object key) {
        return decodeNull(update(key, Always, null, null));
    }

    @Override
    public boolean remove(final Object key, final Object value) {
        return update(key, IfEq, encodeNull(value), null) == encodeNull(value);
    }

    // manages the epoch
    private Object update(final Object key,
                          final UpdateFunction f,
                          final Object expected,
                          final Object newValue) {
        final Comparable<? super K> k = comparable(key);
        while (true) {
            final RootHolder<K,V> h = holderRef.get();
            if (h.epoch.enter()) {
                int sizeDelta = 0;
                try {
                    final Object prev = updateUnderRoot(key, k, f, expected, newValue);
                    if (f.isDefinedAt(prev, expected)) {
                        sizeDelta = (prev != null ? -1 : 0) + (newValue != null ? 1 : 0);
                    }
                    return prev;
                } finally {
                    h.epoch.exit(sizeDelta);
                }
            }
            // Someone is shutting down the epoch.  We can't do anything until
            // this one is done.  We can take responsibility for creating a new
            // one, so we pass true as the parameter to awaitShutdown().
            if (h.epoch.awaitShutdown(true)) {
                // We're on cleanup duty.  CAS detects a racing clear().
                Node.markShared(h.right);
                holderRef.compareAndSet(h, new RootHolder<K,V>(h));
            }
        }
    }

    // manages updates to the root holder
    @SuppressWarnings("unchecked")
    private Object updateUnderRoot(final Object key,
                                   final Comparable<? super K> k,
                                   final UpdateFunction f,
                                   final Object expected,
                                   final Object newValue) {
        final RootHolder<K,V> h = holderRef.get();

        while (true) {
            final Node<K,V> right = h.unsharedRight();
            if (right == null) {
                // key is not present
                if (!f.isDefinedAt(null, expected) ||
                        newValue == null ||
                        attemptInsertIntoEmpty(h, (K)key, newValue)) {
                    // nothing needs to be done, or we were successful, prev value is Absent
                    return null;
                }
                // else RETRY
            } else {
                final long ovl = right.shrinkOVL;
                if (isShrinkingOrUnlinked(ovl)) {
                    right.waitUntilShrinkCompleted(ovl);
                    // RETRY
                } else if (right == h.right) {
                    // this is the protected .right
                    final Object vo = attemptUpdate(key, k, f, expected, newValue, h, right, ovl);
                    if (vo != SpecialRetry) {
                        return vo;
                    }
                    // else RETRY
                }
            }
        }
    }

    private boolean attemptInsertIntoEmpty(final RootHolder<K,V> holder,
                                           final K key,
                                           final Object vOpt) {
        synchronized (holder) {
            if (holder.right == null) {
                holder.right = new Node<K,V>(key, 1, vOpt, holder, 0L, null, null);
                holder.height = 2;
                return true;
            } else {
                return false;
            }
        }
    }

    /** If successful returns the non-null previous value, SpecialNull for a
     *  null previous value, or null if not previously in the map.
     *  The caller should retry if this method returns SpecialRetry.
     */
    @SuppressWarnings("unchecked")
    private Object attemptUpdate(final Object key,
                                 final Comparable<? super K> k,
                                 final UpdateFunction f,
                                 final Object expected,
                                 final Object newValue,
                                 final Node<K,V> parent,
                                 final Node<K,V> node,
                                 final long nodeOVL) {
        // As the search progresses there is an implicit min and max assumed for the
        // branch of the tree rooted at node. A left rotation of a node x results in
        // the range of keys in the right branch of x being reduced, so if we are at a
        // node and we wish to traverse to one of the branches we must make sure that
        // the node has not undergone a rotation since arriving from the parent.
        //
        // A rotation of node can't screw us up once we have traversed to node's
        // child, so we don't need to build a huge transaction, just a chain of
        // smaller read-only transactions.

        assert (nodeOVL != UnlinkedOVL);

        final int cmp = k.compareTo(node.key);
        if (cmp == 0) {
            return attemptNodeUpdate(f, expected, newValue, parent, node);
        }

        final char dirToC = cmp < 0 ? Left : Right;

        while (true) {
            final Node<K,V> child = node.unsharedChild(dirToC);

            if (node.shrinkOVL != nodeOVL) {
                return SpecialRetry;
            }

            if (child == null) {
                // key is not present
                if (newValue == null) {
                    // Removal is requested.  Read of node.child occurred
                    // while parent.child was valid, so we were not affected
                    // by any shrinks.
                    return null;
                } else {
                    // Update will be an insert.
                    final boolean success;
                    synchronized (node) {
                        // Validate that we haven't been affected by past
                        // rotations.  We've got the lock on node, so no future
                        // rotations can mess with us.
                        if (node.shrinkOVL != nodeOVL) {
                            return SpecialRetry;
                        }

                        if (node.child(dirToC) != null) {
                            // Lost a race with a concurrent insert.  No need
                            // to back up to the parent, but we must RETRY in
                            // the outer loop of this method.
                            success = false;
                        } else {
                            // We're valid.  Does the user still want to
                            // perform the operation?
                            if (!f.isDefinedAt(null, expected)) {
                                return null;
                            }

                            // Create a new leaf
                            node.setChild(dirToC, new Node<K,V>((K)key, 1, newValue, node, 0L, null, null));
                            success = true;
                        }
                    }
                    if (success) {
                        fixHeightAndRebalance(node);
                        return null;
                    }
                    // else RETRY
                }
            } else {
                // non-null child
                final long childOVL = child.shrinkOVL;
                if (isShrinkingOrUnlinked(childOVL)) {
                    child.waitUntilShrinkCompleted(childOVL);
                    // RETRY
                } else if (child != node.child(dirToC)) {
                    // this second read is important, because it is protected
                    // by childOVL
                    // RETRY
                } else {
                    // validate the read that our caller took to get to node
                    if (node.shrinkOVL != nodeOVL) {
                        return SpecialRetry;
                    }

                    // At this point we know that the traversal our parent took
                    // to get to node is still valid.  The recursive
                    // implementation will validate the traversal from node to
                    // child, so just prior to the nodeOVL validation both
                    // traversals were definitely okay.  This means that we are
                    // no longer vulnerable to node shrinks, and we don't need
                    // to validate nodeOVL any more.
                    final Object vo = attemptUpdate(key, k, f, expected, newValue, node, child, childOVL);
                    if (vo != SpecialRetry) {
                        return vo;
                    }
                    // else RETRY
                }
            }
        }
    }

    /** parent will only be used for unlink, update can proceed even if parent
     *  is stale.
     */
    private Object attemptNodeUpdate(final UpdateFunction f,
                                     final Object expected,
                                     final Object newValue,
                                     final Node<K,V> parent,
                                     final Node<K,V> node) {
        if (newValue == null) {
            // removal
            if (node.vOpt == null) {
                // This node is already removed, nothing to do.
                return null;
            }
        }

        if (newValue == null && (node.left == null || node.right == null)) {
            // potential unlink, get ready by locking the parent
            final Object prev;
            synchronized (parent) {
                if (isUnlinked(parent.shrinkOVL) || node.parent != parent) {
                    return SpecialRetry;
                }

                synchronized (node) {
                    prev = node.vOpt;
                    if (prev == null || !f.isDefinedAt(prev, expected)) {
                        // nothing to do
                        return prev;
                    }
                    if (!attemptUnlink_nl(parent, node)) {
                        return SpecialRetry;
                    }
                }
            }
            fixHeightAndRebalance(parent);
            return prev;
        } else {
            // potential update (including remove-without-unlink)
            synchronized (node) {
                // regular version changes don't bother us
                if (isUnlinked(node.shrinkOVL)) {
                    return SpecialRetry;
                }

                final Object prev = node.vOpt;
                if (!f.isDefinedAt(prev, expected)) {
                    return prev;
                }

                // retry if we now detect that unlink is possible
                if (newValue == null && (node.left == null || node.right == null)) {
                    return SpecialRetry;
                }

                // update in-place
                node.vOpt = newValue;
                return prev;
            }
        }
    }

    /** Does not adjust the size or any heights. */
    private boolean attemptUnlink_nl(final Node<K,V> parent, final Node<K,V> node) {
        // assert (Thread.holdsLock(parent));
        // assert (Thread.holdsLock(node));
        assert (!isUnlinked(parent.shrinkOVL));

        final Node<K,V> parentL = parent.left;
        final Node<K,V>  parentR = parent.right;
        if (parentL != node && parentR != node) {
            // node is no longer a child of parent
            return false;
        }

        assert (!isUnlinked(node.shrinkOVL));
        assert (parent == node.parent);

        final Node<K,V> left = node.unsharedLeft();
        final Node<K,V> right = node.unsharedRight();
        if (left != null && right != null) {
            // splicing is no longer possible
            return false; 
        }
        final Node<K,V> splice = left != null ? left : right;

        if (parentL == node) {
            parent.left = splice; 
        } else {
            parent.right = splice;
        }
        if (splice != null) {
            splice.parent = parent;
        }

        node.shrinkOVL = UnlinkedOVL;
        node.vOpt = null;

        return true;
    }

    public Map.Entry<K,V> pollFirstEntry() {
        return pollExtremeEntry(Left);
    }

    public Map.Entry<K,V> pollLastEntry() {
        return pollExtremeEntry(Right);
    }

    private Map.Entry<K,V> pollExtremeEntry(final char dir) {
        while (true) {
            final RootHolder<K,V> h = holderRef.get();
            if (h.epoch.enter()) {
                int sizeDelta = 0;
                try {
                    final Map.Entry<K,V> prev = pollExtremeEntryUnderRoot(dir);
                    if (prev != null) {
                        sizeDelta = -1;
                    }
                    return prev;
                } finally {
                    h.epoch.exit(sizeDelta);
                }
            }
            // Someone is shutting down the epoch.  We can't do anything until
            // this one is done.  We can take responsibility for creating a new
            // one, so we pass true as the parameter to awaitShutdown().
            if (h.epoch.awaitShutdown(true)) {
                // We're on cleanup duty.  CAS detects a racing clear().
                Node.markShared(h.right);
                holderRef.compareAndSet(h, new RootHolder<K,V>(h));
            }
        }
    }

    private Map.Entry<K,V> pollExtremeEntryUnderRoot(final char dir) {
        final RootHolder<K,V> h = holderRef.get();

        while (true) {
            final Node<K,V> right = h.unsharedRight();
            if (right == null) {
                // tree is empty, nothing to remove
                return null;
            } else {
                final long ovl = right.shrinkOVL;
                if (isShrinkingOrUnlinked(ovl)) {
                    right.waitUntilShrinkCompleted(ovl);
                    // RETRY
                } else if (right == h.right) {
                    // this is the protected .right
                    final Map.Entry<K,V> result = attemptRemoveExtreme(dir, h, right, ovl);
                    if (result != null) {
                        return result;
                    }
                    // else RETRY
                }
            }
        }
    }

    private Map.Entry<K,V> attemptRemoveExtreme(final char dir,
                                                final Node<K,V> parent,
                                                final Node<K,V> node,
                                                final long nodeOVL) {
        assert (nodeOVL != UnlinkedOVL);

        while (true) {
            final Node<K,V> child = node.child(dir);
            if (child == null) {
                // potential unlink, get ready by locking the parent
                final Map.Entry<K,V> result;
                synchronized (parent) {
                    if (isUnlinked(parent.shrinkOVL) || node.parent != parent) {
                        return null;
                    }

                    synchronized (node) {
                        final Object vo = node.vOpt;
                        if (node.child(dir) != null || !attemptUnlink_nl(parent, node)) {
                            return null;
                        }
                        // success!
                        result = new SimpleImmutableEntry<K,V>(node.key, decodeNull(vo));
                    }
                }
                fixHeightAndRebalance(parent);
                return result;
            } else {
                // keep going down
                final long childOVL = child.shrinkOVL;
                if (isShrinkingOrUnlinked(childOVL)) {
                    child.waitUntilShrinkCompleted(childOVL);
                    // RETRY
                } else if (child != node.child(dir)) {
                    // this second read is important, because it is protected
                    // by childOVL
                    // RETRY
                } else {
                    // validate the read that our caller took to get to node
                    if (node.shrinkOVL != nodeOVL) {
                        return null;
                    }

                    final Map.Entry<K,V> result = attemptRemoveExtreme(dir, node, child, childOVL);
                    if (result != null) {
                        return result;
                    }
                    // else RETRY
                }
            }
        }
    }

    private static final int UnlinkRequired = -1;
    private static final int RebalanceRequired = -2;
    private static final int NothingRequired = -3;

    private int nodeCondition(final Node<K,V> node) {
        final Node<K,V> nL = node.left;
        final Node<K,V> nR = node.right;

        // unlink is required? (counter to our optimistic expectation)
        if ((nL == null || nR == null) && node.vOpt == null) {
            return UnlinkRequired;
        }

        final int hN = node.height;
        final int hL0 = height(nL);
        final int hR0 = height(nR);
        final int hNRepl = 1 + Math.max(hL0, hR0);
        final int bal = hL0 - hR0;

        // rebalance is required?
        if (bal < -1 || bal > 1) {
            return RebalanceRequired;
        }

        return hN != hNRepl ? hNRepl : NothingRequired;
    }

    private void fixHeightAndRebalance(Node<K,V> node) {
        while (node.parent != null) {
            final int condition = nodeCondition(node);
            if (condition == NothingRequired || isUnlinked(node.shrinkOVL)) {
                // nothing to do, or no point in fixing this node
                return;
            }

            final Node<K,V> next;
            if (condition != UnlinkRequired && condition != RebalanceRequired) {
                synchronized (node) {
                    next = attemptFixHeight_nl(node);
                }
            } else {
                final Node<K,V> nParent = node.parent;
                synchronized (nParent) {
                    if (nParent != node.parent) {
                        // retry
                        next = null;
                    } else {
                        synchronized (node) {
                            next = attemptFixOrRebalance_nl(nParent, node);
                        }
                    }
                }
            }

            if (next != null) {
                node = next;
            }
            // else RETRY
        }
    }

    /** Attempts to fix node's height under the assumption that it won't have to
     *  be rebalanced.   Returns null on optimistic failure, the parent node if
     *  the height was changed, or rhe if no change occurred.  node must be
     *  locked on entry.
     */
    private Node<K,V> attemptFixHeight_nl(final Node<K,V> node) {
        final int c = nodeCondition(node);
        switch (c) {
            case RebalanceRequired:
            case UnlinkRequired:
                return null;
            case NothingRequired:
                return holderRef.get();
            default:
                node.height = c;
                return node.parent;
        }
    }

    /** nParent and n must be locked on entry.  Returns null on optimistic
     *  failure, the node to fixAndRebalance if the height has changed or a
     *  rebalance was performed, or the rhe if we are done.
     */
    private Node<K,V> attemptFixOrRebalance_nl(final Node<K,V> nParent, final Node<K,V> n) {

      final Node<K,V> nL = n.unsharedLeft();
      final Node<K,V> nR = n.unsharedRight();

      if ((nL == null || nR == null) && n.vOpt == null) {
          return attemptUnlink_nl(nParent, n) ? nParent : null;
      }

      final int hN = n.height;
      final int hL0 = height(nL);
      final int hR0 = height(nR);
      final int hNRepl = 1 + Math.max(hL0, hR0);
      final int bal = hL0 - hR0;

      if (bal > 1) {
          if (!attemptBalanceRight_nl(nParent, n, nL, hR0)) {
              return null;
          }
      } else if(bal < -1) {
          if (!attemptBalanceLeft_nl(nParent, n, nR, hL0)) {
              return null;
          }
      } else if (hNRepl != hN) {
          // we've got more than enough locks to do a height change, no need to
          // trigger a retry
          n.height = hNRepl;
      } else {
          // nothing to do
          return holderRef.get();
      }

      // Change means that we must continue rebalancing higher up.  No change in
      // parent height, no more rebalancing required.
      return fixHeight(nParent) ? nParent : holderRef.get();
    }

    private boolean fixHeight(final Node<K,V> n) {
      final int hL = height(n.left);
      final int hR = height(n.right);
      final int hRepl = 1 + Math.max(hL, hR);
      if (hRepl != n.height) {
        n.height = hRepl;
        return true;
      } else {
        return false;
      }
    }

    /** Returns true on success, false on optimistic failure. */
    private boolean attemptBalanceRight_nl(final Node<K,V> nParent,
                                           final Node<K,V> n,
                                           final Node<K,V> nL,
                                           final int hR0) {
      // L is too large, we will rotate-right.  If L.R is taller
      // than L.L, then we will first rotate-left L.
      synchronized(nL) {
        //require (nL.epoch eq epoch)
        final int hL = nL.height;
        if (hL - hR0 <= 1) {
          return false; // retry
        } else {
          final Node<K,V> nLR = nL.unsharedRight();
          final int hLR0 = height(nLR);
          if (height(nL.left) >= hLR0) {
            // rotate right based on our snapshot of hLR
            rotateRight(nParent, n, nL, hR0, nLR, hLR0);
          } else {
            synchronized(nLR) {
              //require (nLR.epoch eq epoch)
              // if our hLR snapshot is incorrect then we might actually need to do a single rotate-right
              final int hLR = nLR.height;
              if (height(nL.left) >= hLR) {
                rotateRight(nParent, n, nL, hR0, nLR, hLR);
              } else {
                // actually need a rotate right-over-left
                rotateRightOverLeft(nParent, n, nL, hR0, nLR);
              }
            }
          }
          return true;
        }
      }
    }

    private boolean attemptBalanceLeft_nl(final Node<K,V> nParent,
                                          final Node<K,V> n,
                                          final Node<K,V> nR,
                                          final int hL0) {
        synchronized (nR) {
            //require (nR.epoch eq epoch)
        final int hR = nR.height;
        if (hL0 - hR >= -1) {
          return false; // retry
        } else {
          final Node<K,V> nRL = nR.unsharedLeft();
          final int hRL0 = height(nRL);
          if (height(nR.right) >= hRL0) {
            rotateLeft(nParent, n, hL0, nR, nRL, hRL0);
          } else {
            synchronized(nRL) {
              final int hRL = nRL.height;
              if (height(nR.right) >= hRL) {
                rotateLeft(nParent, n, hL0, nR, nRL, hRL);
              } else {
                rotateLeftOverRight(nParent, n, hL0, nR, nRL);
              }
            }
          }
          return true;
        }
      }
    }

    private void rotateRight(final Node<K,V> nParent,
                             final Node<K,V> n,
                             final Node<K,V> nL,
                             final int hR,
                             final Node<K,V> nLR,
                             final int hLR) {
        final long nodeOVL = n.shrinkOVL;
        n.shrinkOVL = beginChange(nodeOVL);

        // fix up n links, careful to be compatible with concurrent traversal for all but n
        n.left = nLR;
        if (nLR != null) {
            nLR.parent = n;
        }

        nL.right = n;
        n.parent = nL;

        if (nParent.left == n) {
            nParent.left = nL;
        } else {
            nParent.right = nL;
        }
        nL.parent = nParent;

        // fix up heights links
        final int hNRepl = 1 + Math.max(hLR, hR);
        n.height = hNRepl;
        nL.height = 1 + Math.max(height(nL.left), hNRepl);

        n.shrinkOVL = endChange(nodeOVL);
    }

    private void rotateLeft(final Node<K,V> nParent,
                            final Node<K,V> n,
                            final int hL,
                            final Node<K,V> nR,
                            final Node<K,V> nRL,
                            final int hRL) {
        final long nodeOVL = n.shrinkOVL;
        n.shrinkOVL = beginChange(nodeOVL);

        // fix up n links, careful to be compatible with concurrent traversal for all but n
        n.right = nRL;
        if (nRL != null) {
            nRL.parent = n;
        }

        nR.left = n;
        n.parent = nR;

        if (nParent.left == n) {
            nParent.left = nR;
        } else {
            nParent.right = nR;
        }
        nR.parent = nParent;

        // fix up heights
        final int  hNRepl = 1 + Math.max(hL, hRL);
        n.height = hNRepl;
        nR.height = 1 + Math.max(hNRepl, height(nR.right));

        n.shrinkOVL = endChange(nodeOVL);
    }

    private void rotateRightOverLeft(final Node<K,V> nParent,
                                     final Node<K,V> n,
                                     final Node<K,V> nL,
                                     final int hR,
                                     final Node<K,V> nLR) {
        final long nodeOVL = n.shrinkOVL;
        final long leftOVL = nL.shrinkOVL;
        n.shrinkOVL = beginChange(nodeOVL);
        nL.shrinkOVL = beginChange(leftOVL);

        final Node<K,V> nLRL = nLR.unsharedLeft();
        final Node<K,V> nLRR = nLR.unsharedRight();

        // fix up n links, careful about the order!
        n.left = nLRR;
        if (nLRR != null) {
            nLRR.parent = n;
        }

        nL.right = nLRL;
        if (nLRL != null) {
            nLRL.parent = nL;
        }

        nLR.left = nL;
        nL.parent = nLR;
        nLR.right = n;
        n.parent = nLR;

        if (nParent.left == n) {
            nParent.left = nLR;
        } else {
            nParent.right = nLR;
        }
        nLR.parent = nParent;

        // fix up heights
        final int hNRepl = 1 + Math.max(height(nLRR), hR);
        n.height = hNRepl;
        final int hLRepl = 1 + Math.max(height(nL.left), height(nLRL));
        nL.height = hLRepl;
        nLR.height = 1 + Math.max(hLRepl, hNRepl);

        n.shrinkOVL = endChange(nodeOVL);
        nL.shrinkOVL = endChange(leftOVL);
    }

    private void rotateLeftOverRight(final Node<K,V> nParent,
                                     final Node<K,V> n,
                                     final int hL,
                                     final Node<K,V> nR,
                                     final Node<K,V> nRL) {
        final long nodeOVL = n.shrinkOVL;
        final long rightOVL = nR.shrinkOVL;
        n.shrinkOVL = beginChange(nodeOVL);
        nR.shrinkOVL = beginChange(rightOVL);

        final Node<K,V> nRLL = nRL.unsharedLeft();
        final Node<K,V> nRLR = nRL.unsharedRight();

        // fix up n links, careful about the order!
        n.right = nRLL;
        if (nRLL != null) {
            nRLL.parent = n;
        }

        nR.left = nRLR;
        if (nRLR != null) {
            nRLR.parent = nR;
        }

        nRL.right = nR;
        nR.parent = nRL;
        nRL.left = n;
        n.parent = nRL;

        if (nParent.left == n) {
            nParent.left = nRL;
        } else {
            nParent.right = nRL;
        }
        nRL.parent = nParent;

        // fix up heights
        final int hNRepl = 1 + Math.max(hL, height(nRLL));
        n.height = hNRepl;
        final int hRRepl = 1 + Math.max(height(nRLR), height(nR.right));
        nR.height = hRRepl;
        nRL.height = 1 + Math.max(hNRepl, hRRepl);

        n.shrinkOVL = endChange(nodeOVL);
        nR.shrinkOVL = endChange(rightOVL);
    }

    //////////////// views

    @Override
    public Set<Entry<K,V>> entrySet() {
        return entries;
    }

    private class EntrySet extends AbstractSet<Map.Entry<K,V>> {

        @Override
        public int size() {
            return SnapTreeMap.this.size();
        }

        @Override
        public boolean isEmpty() {
            return SnapTreeMap.this.isEmpty();
        }

        @Override
        public void clear() {
            SnapTreeMap.this.clear();
        }

        @Override
        public boolean contains(final Object o) {
            if (!(o instanceof Map.Entry<?,?>)) {
                return false;
            }
            final Object k = ((Map.Entry<?,?>)o).getKey();
            final Object v = ((Map.Entry<?,?>)o).getValue();
            final Object actualVo = SnapTreeMap.this.getImpl(k);
            if (actualVo == null) {
                // no associated value
                return false;
            }
            final V actual = decodeNull(actualVo);
            return v == null ? actual == null : v.equals(actual);
        }

        @Override
        public boolean add(final Entry<K,V> e) {
            final Object v = encodeNull(e.getValue());
            return update(e.getKey(), Always, null, v) != v;
        }

        @Override
        public boolean remove(final Object o) {
            if (!(o instanceof Map.Entry<?,?>)) {
                return false;
            }
            final Object k = ((Map.Entry<?,?>)o).getKey();
            final Object v = ((Map.Entry<?,?>)o).getValue();
            return SnapTreeMap.this.remove(k, v);
        }

        @Override
        public Iterator<Entry<K,V>> iterator() {
            return new EntryIter();
        }
    }

    private class EntryIter implements Iterator<Map.Entry<K,V>> {
        private final ArrayList<Node<K,V>> path;
        protected Node<K,V> lastNode;

        EntryIter() {
            final Node<K,V> root = completedHolder().right;
            path = new ArrayList<Node<K,V>>(1 + height(root));
            pushMin(root);
        }

        private void pushMin(Node<K,V> node) {
            while (node != null) {
                path.add(node);
                node = node.left;
            }
        }

        private Node<K,V> top() {
            return path.get(path.size() - 1);
        }

        private void advance() {
            do {
                Node<K,V> cur = top();
                final Node<K,V> right = cur.right;
                if (right != null) {
                    pushMin(right);
                } else {
                    // keep going up until we pop a node that is a left child
                    do {
                        cur = path.remove(path.size() - 1);
                    } while (!path.isEmpty() && cur == top().right);
                }

                // skip removed-but-not-unlinked entries
            } while (!path.isEmpty() && top().vOpt == null);
        }

        @Override
        public boolean hasNext() {
            return !path.isEmpty();
        }

        @Override
        public Node<K,V> next() {
            lastNode = top();
            advance();
            return lastNode;
        }

        @Override
        public void remove() {
            SnapTreeMap.this.remove(lastNode.key);
        }
    }
}
