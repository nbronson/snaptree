/*
 * Copyright (c) 2009 Stanford University, unless otherwise specified.
 * All rights reserved.
 *
 * This software was developed by the Pervasive Parallelism Laboratory of
 * Stanford University, California, USA.
 *
 * Permission to use, copy, modify, and distribute this software in source
 * or binary form for any purpose with or without fee is hereby granted,
 * provided that the following conditions are met:
 *
 *    1. Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *
 *    2. Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *
 *    3. Neither the name of Stanford University nor the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE REGENTS AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE REGENTS OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

package edu.stanford.ppl.concurrent;

import java.util.concurrent.CyclicBarrier;


public class ParUtil {
    public interface Block {
        void call(int index);
    }

    private static class RunnableBlock implements Block {
        private final Runnable _task;

        RunnableBlock(final Runnable task) {
            _task = task;
        }

        public void call(final int index) {
            _task.run();
        }
    }

    public static void parallel(final int numThreads, final Runnable block) {
        parallel(numThreads, new RunnableBlock(block));
    }

    public static void parallel(final int numThreads, final Block block) {
        final Thread[] threads = new Thread[numThreads];
        final Throwable[] failure = { null };
        for (int i = 0; i < threads.length; ++i) {
            final int index = i;
            threads[i] = new Thread("worker #" + i) {
                @Override
                public void run() {
                    try {
                        block.call(index);
                    }
                    catch (final Throwable xx) {
                        failure[0] = xx;
                    }
                }
            };
        }
        for (Thread t : threads) {
            t.start();
        }
        for (Thread t : threads) {
            try {
                t.join();
            }
            catch (final InterruptedException xx) {
                throw new RuntimeException("unexpected", xx);
            }
        }

        if (failure[0] instanceof RuntimeException) {
            throw (RuntimeException) failure[0];
        }
        else if (failure[0] instanceof Error) {
            throw (Error) failure[0];
        }
        else {
            assert(failure[0] == null);
        }
    }

    /** Returns the elapsed milliseconds. */
    public static long timeParallel(final int numThreads, final Runnable block) {
        return timeParallel(numThreads, new RunnableBlock(block));
    }

    /** Returns the elapsed milliseconds. */
    public static long timeParallel(final int numThreads, final Block block) {
        final long[] times = new long[2];
        final CyclicBarrier barrier = new CyclicBarrier(numThreads, new Runnable() {
            public void run() {
                times[0] = times[1];
                times[1] = System.currentTimeMillis();
            }
        });
        parallel(numThreads, new Block() {
            public void call(final int index) {
                try {
                    barrier.await();
                }
                catch (final Exception xx) {
                    throw new RuntimeException("unexpected", xx);
                }
                try {
                    block.call(index);
                }
                finally {
                    try {
                        barrier.await();
                    }
                    catch (final Exception xx) {
                        throw new RuntimeException("unexpected", xx);
                    }
                }
            }
        });
        return times[1] - times[0];
    }

}
