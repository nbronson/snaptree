/* CCSTM - (c) 2009 Stanford University - PPL */

// CopyOnWriteManager
package edu.stanford.ppl.concurrent;

import java.util.concurrent.CountDownLatch;

abstract public class CopyOnWriteManager<E> {
    private class Root extends EpochNode {
        public E value;
        public int initialSize;
        public volatile Root cleanPrev = null;
        public Root queued;
        public CountDownLatch closed;

        private Root() {
        }

        public Root(final E value, final int initialSize) {
            this.value = value;
            this.initialSize = initialSize;
            this.queued = new Root();
            this.closed = new CountDownLatch(1);
        }

        @Override
        public EpochNode attemptArrive() {
            if (cleanPrev != null) {
                cleanPrev = null;
            }
            return super.attemptArrive();
        }

        protected void onClosed(final int dataSum) {
            queued.queued = new Root();
            queued.value = freezeAndClone(value);
            queued.initialSize = initialSize + dataSum;
            queued.closed = new CountDownLatch(1);
            queued.cleanPrev = this; 
            _active = queued;
        }

        public void awaitClosed() {
            boolean interrupted = false;
            while (true) {
                try {
                    closed.await();
                    break;
                }
                catch (final InterruptedException xx) {
                    interrupted = true;
                }
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private volatile Root _active;

    public CopyOnWriteManager(E initialValue, int initialSize) {
        _active = new Root(initialValue, initialSize);
    }

    abstract protected E freezeAndClone(E value);

    public Epoch.Ticket beginMutation() {
        while (true) {
            final Root a = _active;
            final Epoch.Ticket t0 = a.attemptArrive();
            if (t0 != null) {
                // success
                return t0;
            }

            final Epoch.Ticket t1 = a.queued.attemptArrive();
            if (t1 != null) {
                // we are guaranteed a seat at the table *next* epoch
                a.awaitClosed();
                return t1;
            }

            // our read of _active must be stale, try again
            assert(_active != a);
        }        
    }

    public E read() {
        return _active.value;
    }

    public E mutable() {
        return _active.value;
    }

    public E frozen() {
        final Root a = _active;
        if (a.cleanPrev != null) {
            return a.cleanPrev.value;
        }
        else {
            a.beginClose();
            a.awaitClosed();
            return a.value;
        }
    }

    public int size() {
        final Root a = _active;
        if (a.cleanPrev != null) {
            return a.initialSize;
        }
        else {
            a.beginClose();
            a.awaitClosed();
            return _active.initialSize;
        }
    }
}
