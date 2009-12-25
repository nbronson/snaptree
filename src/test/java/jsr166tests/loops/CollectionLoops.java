// from gee.cs.oswego.edu/home/jsr166/jsr166

package jsr166tests.loops;

/*
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */


import java.util.*;
import java.util.concurrent.*;

public class CollectionLoops {
    static int pinsert     = 100;
    static int premove     = 1;
    static int maxThreads  = 48;
    static int removesPerMaxRandom;
    static int insertsPerMaxRandom;
    static volatile int checkSum = 0;
    static boolean print = false;

    static final ExecutorService pool = Executors.newCachedThreadPool();

    public static void main(String[] args) throws Exception {
        int nkeys       = 10000;
        int nops        = 100000;

        Class collectionClass = null;
        if (args.length > 0) {
            try {
                collectionClass = Class.forName(args[0]);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("Class " + args[0] + " not found.");
            }
        }

        if (args.length > 1)
            maxThreads = Integer.parseInt(args[1]);

        if (args.length > 2)
            nkeys = Integer.parseInt(args[2]);

        if (args.length > 3)
            pinsert = Integer.parseInt(args[3]);

        if (args.length > 4)
            premove = Integer.parseInt(args[4]);

        if (args.length > 5)
            nops = Integer.parseInt(args[5]);

        // normalize probabilities wrt random number generator
        removesPerMaxRandom = (int)(((double)premove/100.0 * 0x7FFFFFFFL));
        insertsPerMaxRandom = (int)(((double)pinsert/100.0 * 0x7FFFFFFFL));

        System.out.print("Class: " + collectionClass.getName());
        System.out.print(" threads: " + maxThreads);
        System.out.print(" size: " + nkeys);
        System.out.print(" ins: " + pinsert);
        System.out.print(" rem: " + premove);
        System.out.print(" ops: " + nops);
        System.out.println();

        // warmup
        test(1, 100, 100, collectionClass);
        test(2, 100, 100, collectionClass);
        test(4, 100, 100, collectionClass);
        print = true;

        int k = 1;
        int warmups = 2;
        for (int i = 1; i <= maxThreads;) {
            Thread.sleep(100);
            test(i, nkeys, nops, collectionClass);
            if (warmups > 0)
                --warmups;
            else if (i == k) {
                k = i << 1;
                i = i + (i >>> 1);
            }
            else if (i == 1 && k == 2) {
                i = k;
                warmups = 1;
            }
            else
                i = k;
        }
        pool.shutdown();
    }

    static Integer[] makeKeys(int n) {
        LoopHelpers.SimpleRandom rng = new LoopHelpers.SimpleRandom();
        Integer[] key = new Integer[n];
        for (int i = 0; i < key.length; ++i)
            key[i] = new Integer(rng.next());
        return key;
    }

    static void shuffleKeys(Integer[] key) {
        Random rng = new Random();
        for (int i = key.length; i > 1; --i) {
            int j = rng.nextInt(i);
            Integer tmp = key[j];
            key[j] = key[i-1];
            key[i-1] = tmp;
        }
    }

    static void test(int i, int nk, int nops, Class collectionClass) throws Exception {
        if (print)
            System.out.print("Threads: " + i + "\t:");
        Collection<Integer> collection = (Collection<Integer>)collectionClass.newInstance();
        Integer[] key = makeKeys(nk);
        // Uncomment to start with a non-empty table
        for (int j = 0; j < nk; j += 2)
            collection.add(key[j]);
        shuffleKeys(key);
        LoopHelpers.BarrierTimer timer = new LoopHelpers.BarrierTimer();
        CyclicBarrier barrier = new CyclicBarrier(i+1, timer);
        for (int t = 0; t < i; ++t)
            pool.execute(new Runner(t, collection, key, barrier, nops));
        barrier.await();
        barrier.await();
        long time = timer.getTime();
        long tpo = time / (i * (long) nops);
        if (print)
            System.out.print(LoopHelpers.rightJustify(tpo) + " ns per op");
        double secs = (double) time / 1000000000.0;
        if (print)
            System.out.print("\t " + secs + "s run time");
        if (checkSum == 0) System.out.print(" ");
        if (print)
            System.out.println();
        collection.clear();
    }

    static class Runner implements Runnable {
        final Collection<Integer> collection;
        final Integer[] key;
        final LoopHelpers.SimpleRandom rng;
        final CyclicBarrier barrier;
        int position;
        int total;
        int nops;

        Runner(int id, Collection<Integer> collection, Integer[] key,  CyclicBarrier barrier, int nops) {
            this.collection = collection;
            this.key = key;
            this.barrier = barrier;
            this.nops = nops;
            position = key.length / (id + 1);
            rng = new LoopHelpers.SimpleRandom((id + 1) * 8862213513L);
            rng.next();
        }

        public void run() {
            try {
                barrier.await();
                int p = position;
                int ops = nops;
                Collection<Integer> c = collection;
                while (ops > 0) {
                    int r = rng.next();
                    p += (r & 7) - 3;
                    while (p >= key.length) p -= key.length;
                    while (p < 0) p += key.length;

                    Integer k = key[p];
                    if (c.contains(k)) {
                        if (r < removesPerMaxRandom) {
                            if (c.remove(k)) {
                                p = Math.abs(total % key.length);
                                ops -= 2;
                                continue;
                            }
                        }
                    }
                    else if (r < insertsPerMaxRandom) {
                        ++p;
                        ops -= 2;
                        c.add(k);
                        continue;
                    }

                    total += LoopHelpers.compute6(k.intValue());
                    --ops;
                }
                checkSum += total;
                barrier.await();
            }
            catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
    }
}
