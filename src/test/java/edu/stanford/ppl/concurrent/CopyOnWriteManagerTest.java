/* CCSTM - (c) 2009 Stanford University - PPL */

// CopyOnWriteManagerTest
package edu.stanford.ppl.concurrent;

import junit.framework.TestCase;

import java.util.concurrent.atomic.AtomicLong;

public class CopyOnWriteManagerTest extends TestCase {
    static class Obj {
        private final AtomicLong _state;

        public Obj(final int value) {
            _state = new AtomicLong(((long) value) << 32);
        }

        boolean isShared() { return (_state.get() & 1L) != 0L; }
        void markShared() { _state.getAndAdd(1L); }
        int size() { return (int) (_state.get() >> 32); }
        void incr() { adjust(1); }
        void decr() { adjust(-1); }

        private void adjust(final int delta) {
            while (true) {
                final long s = _state.get();
                assertEquals(0L, s & 1L);
                if (_state.compareAndSet(s, s + (((long) delta) << 32))) {
                    return;
                }
            }
        }
    }

    static class COWM extends CopyOnWriteManager<Obj> {
        COWM(final int initialSize) {
            super(new Obj(initialSize), initialSize);
        }

        protected Obj freezeAndClone(final Obj value) {
            value.markShared();
            return new Obj(value.size());
        }
    }

    public void testRead() {
        final COWM m = new COWM(10);
        assertEquals(10, m.read().size());
    }

    public void testIncr() {
        final COWM m = new COWM(10);
        incr(m);
        assertEquals(11, m.read().size());
    }

    private void incr(final COWM m) {
        final Epoch.Ticket t = m.beginMutation();
        m.mutable().incr();
        t.leave(1);
    }

    private void decr(final COWM m) {
        final Epoch.Ticket t = m.beginMutation();
        m.mutable().decr();
        t.leave(-1);
    }

    public void testSize() {
        final COWM m = new COWM(10);
        assertEquals(10, m.size());
        incr(m);
        assertEquals(11, m.size());
        assertEquals(11, m.read().size());
    }

    public void testSnapshot() {
        final COWM m = new COWM(10);
        final Obj s10 = m.frozen();
        incr(m);
        final Obj s11 = m.frozen();
        incr(m);
        assertEquals(10, s10.size());
        assertEquals(11, s11.size());
        assertEquals(12, m.read().size());
    }
}
