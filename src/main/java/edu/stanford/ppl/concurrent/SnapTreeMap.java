/* SnapTree - (c) 2009 Stanford University - PPL */

// SnapTreeMap

package edu.stanford.ppl.concurrent;

import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

public class SnapTreeMap<K,V> extends AbstractMap<K,V> implements ConcurrentMap<K,V> {

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
            if (isShrinking(ovl)) {
                for (int tries = 0; tries < SpinCount + YieldCount; ++tries) {
                    if (shrinkOVL != ovl) {
                        // we're done
                        return;
                    }
                    if (tries >= SpinCount) {
                        Thread.yield();
                    }
                }
                // spin and yield failed, use the nuclear option
                synchronized (this) {
                    // we can't have gotten the lock unless the shrink was over
                }
            }
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

    private static <K,V> Node<K,V> left(final Node<K,V> node) {
        return node == null ? null : node.left;
    }

    private static <K,V> Node<K,V> right(final Node<K,V> node) {
        return node == null ? null : node.right;
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
                    final Object prev = updateRoot(key, k, f, expected, newValue);
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
    private Object updateRoot(final Object key,
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
                        attemptInsertIntoEmpty(h, key, newValue)) {
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
                                           final Object key,
                                           final Object vOpt) {
        synchronized (holder) {
            if (holder.right == null) {
                holder.right = new Node<K,V>((K)key, 1, vOpt, holder, 0L, null, null);
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
                            node.setChild(dirToC, new Node(key, 1, newValue, node, 0L, null, null));
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

    private void fixHeightAndRebalance(final Node<K,V> node) {
        
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
