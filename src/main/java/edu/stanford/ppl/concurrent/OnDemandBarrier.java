/* SnapTree - (c) 2009 Stanford University - PPL */

// OnDemandBarrier
package edu.stanford.ppl.concurrent;

import java.util.concurrent.CountDownLatch;

/** An <code>OnDemandBarrier</code> coordinates multiple threads, arranging for
 *  times where no work is being performed.  A barrier action can be performed
 *  during that quiesced period.  Unlike a normal barrier, however, each thread
 *  may perform 0, 1, or many pieces of work between barriers.  A barrier is
 *  only initiated when explicitly requested.
 *  <p>
 *  Three operations are provided: {@link #enter}, {@link #exit}, and
 *  {@link #trigger}.  Threads bracket each unit of indivisible work with enter
 *  and exit.  When a call to {@link #trigger} is made, subsequent calls to
 *  {@link #enter} will be blocked, and the thread that triggered the barrier
 *  will be blocked until each entry performed before the trigger has been
 *  balanced with an exit.  At this point the barrier action will be performed,
 *  and work will resume.  If more than one {@link #trigger} is pending, they
 *  will be merged into a single barrier.
 *  <p>
 *  Barrier actions are defined by implementing {@link #barrierAction}.
 */
abstract public class OnDemandBarrier {

    /** Performs the barrier action.  This method will be invoked on exactly
     *  one thread each time a barrier is triggered.  Note that triggers may be
     *  merged if they are concurrent. 
     */
    abstract protected void barrierAction();

    class Epoch extends ClosableRefCount {
        Epoch queued;

        private final CountDownLatch _closed = new CountDownLatch(1);

        Epoch() {
        }

        Epoch(final Epoch queued) {
            this.queued = queued;
        }

        protected void onClose() {
            try {
                barrierAction();
            }
            finally {
                assert(_active == this);
                assert(queued.queued == null);
                queued.queued = new Epoch();
                _active = queued;
                _closed.countDown();
            }
        }

        public void awaitClosed() {
            boolean interrupted = false;
            while (true) {
                try {
                    _closed.await();
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

    private volatile Epoch _active = new Epoch(new Epoch());

    public Object enter() {
        while (true) {
            final Epoch a = _active;

            // attempt to enter the active epoch
            final ClosableRefCount t0 = a.attemptIncr();
            if (t0 != null) {
                // success!
                return t0;
            }

            // active epoch is closed, queue ourself
            final ClosableRefCount t1 = a.queued.attemptIncr();
            if (t1 != null) {
                // we must now wait until queued has become active
                a.awaitClosed();
                assert(_active == a.queued);
                return t1;
            }

            // a must be stale, we can try again immediately
            assert(a != _active);
        }
    }

    public void exit(final Object entryTicket) {
        ((ClosableRefCount) entryTicket).decr();
    }

    public void trigger() {
        final Epoch a = _active;
        a.beginClose();
        a.awaitClosed();
    }
}
