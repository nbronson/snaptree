// from gee.cs.oswego.edu/home/jsr166/jsr166

package jsr166tests.loops;

/*
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */

import java.util.*;
import java.io.*;
import java.math.*;

/**
 * A micro-benchmark with key types and operation mixes roughly
 * corresponding to some real programs.
 *
 * The main results are a table of approximate nanoseconds per
 * element-operation (averaged across get, put etc) for each type,
 * across a range of map sizes. It also includes category "Mixed"
 * that includes elements of multiple types including those with
 * identical hash codes.
 *
 * The program includes a bunch of microbenchmarking safeguards that
 * might underestimate typical performance. For example, by using many
 * different key types and exercising them in warmups it disables most
 * dynamic type specialization.  Some test classes, like Float and
 * BigDecimal are included not because they are commonly used as keys,
 * but because they can be problematic for some map implementations.
 *
 * By default, it creates and inserts in order dense numerical keys
 * and searches for keys in scrambled order. Use "s" as second arg to
 * instead insert and search in unscrambled order.
 *
 * For String keys, the program tries to use file "testwords.txt", which
 * is best used with real words.  We can't check in this file, but you
 * can create one from a real dictionary (1 line per word) and then run
 * linux "shuf" to randomize entries. If no file exists, it uses
 * String.valueOf(i) for element i.
 */
public class MapMicroBenchmark {
    static final String wordFile = "testwords.txt";

    static Class mapClass;
    static boolean randomSearches = true;

    // Nanoseconds per run
    static final long NANOS_PER_JOB = 6L * 1000L*1000L*1000L;
    static final long NANOS_PER_WARMUP = 100L*1000L*1000L;

    // map operations per item per iteration -- change if job.work changed
    static final int OPS_PER_ITER = 11;
    static final int MIN_ITERS_PER_TEST = 3; // must be > 1
    static final int MAX_ITERS_PER_TEST = 1000000; // avoid runaway

    // sizes are at halfway points for HashMap default resizes
    static final int firstSize = 9;
    static final int sizeStep = 4; // each size 4X last
    static final int nsizes = 9;
    static final int[] sizes = new int[nsizes];

    public static void main(String[] args) throws Throwable {
        if (args.length == 0) {
            System.out.println("Usage: java MapMicroBenchmark className [r|s]keys [r|s]searches");
            return;
        }

        mapClass = Class.forName(args[0]);

        if (args.length > 1) {
            if (args[1].startsWith("s"))
                randomSearches = false;
            else if (args[1].startsWith("r"))
                randomSearches = true;
        }

        System.out.print("Class " + mapClass.getName());
        if (randomSearches)
            System.out.print(" randomized searches");
        else
            System.out.print(" sequential searches");

        System.out.println();

        int n = firstSize;
        for (int i = 0; i < nsizes - 1; ++i) {
            sizes[i] = n;
            n *= sizeStep;
        }
        sizes[nsizes - 1] = n;

        int njobs = 10;
        Job[] jobs = new Job[njobs];

        Object[] os = new Object[n];
        for (int i = 0; i < n; i++) os[i] = new Object();
        jobs[0] = new Job("Object    ", os, Object.class);

        Object[] ss = new Object[n];
        initStringKeys(ss, n);
        jobs[1] = new Job("String    ", ss, String.class);

        Object[] is = new Object[n];
        for (int i = 0; i < n; i++) is[i] = Integer.valueOf(i);
        jobs[2] = new Job("Integer   ", is, Integer.class);

        Object[] ls = new Object[n];
        for (int i = 0; i < n; i++) ls[i] = Long.valueOf((long) i);
        jobs[3] = new Job("Long      ", ls, Long.class);

        Object[] fs = new Object[n];
        for (int i = 0; i < n; i++) fs[i] = Float.valueOf((float) i);
        jobs[4] = new Job("Float     ", fs, Float.class);

        Object[] ds = new Object[n];
        for (int i = 0; i < n; i++) ds[i] = Double.valueOf((double) i);
        jobs[5] = new Job("Double    ", ds, Double.class);

        Object[] bs = new Object[n];
        long b = -n; // include some negatives
        for (int i = 0; i < n; i++) bs[i] = BigInteger.valueOf(b += 3);
        jobs[6] = new Job("BigInteger", bs, BigInteger.class);

        Object[] es = new Object[n];
        long d = Integer.MAX_VALUE; // include crummy codes
        for (int i = 0; i < n; i++) es[i] = BigDecimal.valueOf(d += 65536);
        jobs[7] = new Job("BigDecimal", es, BigDecimal.class);

        Object[] rs = new Object[n];
        for (int i = 0; i < n; i++) rs[i] = new RandomInt();
        jobs[8] = new Job("RandomInt ", rs, RandomInt.class);

        Object[] ms = new Object[n];
        for (int i = 0; i < n; i += 2) {
            int r = rng.nextInt(njobs - 1);
            ms[i] = jobs[r].items[i];
            // include some that will have same hash but not .equal
            if (++r >= njobs - 1) r = 0;
            ms[i+1] = jobs[r].items[i];
        }
        jobs[9] = new Job("Mixed     ", ms, Object.class);
        Job mixed = jobs[9];

        warmup1(mixed);
        warmup2(jobs);
        warmup1(mixed);
        warmup3(jobs);
        Thread.sleep(500);
	time(jobs);
    }

    static void runWork(Job[] jobs, int minIters, int maxIters, long timeLimit) throws Throwable {
        for (int k = 0; k <  nsizes; ++k) {
            int len = sizes[k];
            for (int i = 0; i < jobs.length; i++) {
                Thread.sleep(50);
                jobs[i].nanos[k] = jobs[i].work(len, minIters, maxIters, timeLimit);
                System.out.print(".");
            }
        }
        System.out.println();
    }

    // First warmup -- run only mixed job to discourage type specialization
    static void warmup1(Job job) throws Throwable {
        for (int k = 0; k <  nsizes; ++k)
            job.work(sizes[k], 1, 1, 0);
    }

    // Second, run each once
    static void warmup2(Job[] jobs) throws Throwable {
        System.out.print("warm up");
        runWork(jobs, 1, 1, 0);
        long ck = jobs[0].checkSum;
        for (int i = 1; i < jobs.length - 1; i++) {
            if (jobs[i].checkSum != ck)
                throw new Error("CheckSum");
        }
    }

    // Third: short timed runs
    static void warmup3(Job[] jobs) throws Throwable {
        System.out.print("warm up");
        runWork(jobs, 1, MAX_ITERS_PER_TEST, NANOS_PER_WARMUP);
    }

    static void time(Job[] jobs) throws Throwable {
        System.out.print("running");
        runWork(jobs, MIN_ITERS_PER_TEST, MAX_ITERS_PER_TEST, NANOS_PER_JOB);

        System.out.print("Type/Size:");
        for (int k = 0; k < nsizes; ++k)
            System.out.printf("%7d", sizes[k]);
        System.out.println();

        long[] aves = new long[nsizes];
        int njobs = jobs.length;

	for (int i = 0; i < njobs; i++) {
            System.out.print(jobs[i].name);
            for (int k = 0; k < nsizes; ++k) {
                long nanos = jobs[i].nanos[k];
                System.out.printf("%7d", nanos);
                aves[k] += nanos;
            }
            System.out.println();
        }

        System.out.println();
        System.out.print("average   ");
        for (int k = 0; k < nsizes; ++k)
            System.out.printf("%7d", (aves[k] / njobs));
        System.out.println("\n");
    }


    static final class Job {
	final String name;
        final Class elementClass;
        long[] nanos = new long[nsizes];
        final Object[] items;
        Object[] searches;
        volatile long checkSum;
        volatile int lastSum;
        Job(String name, Object[] items, Class elementClass) {
            this.name = name;
            this.items = items;
            this.elementClass = elementClass;
            if (randomSearches) {
                scramble(items);
                this.searches = new Object[items.length];
                System.arraycopy(items, 0, searches, 0, items.length);
                scramble(searches);
            }
            else
                this.searches = items;
        }

        public long work(int len, int minIters, int maxIters, long timeLimit) {
            Map m;
            try {
                m = (Map) mapClass.newInstance();
            } catch (Exception e) {
                throw new RuntimeException("Can't instantiate " + mapClass + ": " + e);
            }
            Object[] ins = items;
            Object[] keys = searches;

            if (ins.length < len || keys.length < len)
                throw new Error(name);
            int half = len / 2;
            int quarter = half / 2;
            int sum = lastSum;
            long startTime = System.nanoTime();
            long elapsed;
            int j = 0;
            for (;;) {
                for (int i = 0; i < half; ++i) {
                    Object x = ins[i];
                    if (m.put(x, x) == null)
                        ++sum;
                }
                checkSum += sum ^ (sum << 1); // help avoid loop merging
                sum += len - half;
                for (int i = 0; i < len; ++i) {
                    Object x = keys[i];
                    Object v = m.get(x);
                    if (elementClass.isInstance(v)) // touch v
                        ++sum;
                }
                checkSum += sum ^ (sum << 2);
                for (int i = half; i < len; ++i) {
                    Object x = ins[i];
                    if (m.put(x, x) == null)
                        ++sum;
                }
                checkSum += sum ^ (sum << 3);
                for (Object e : m.keySet()) {
                    if (elementClass.isInstance(e))
                        ++sum;
                }
                checkSum += sum ^ (sum << 4);
                for (Object e : m.values()) {
                    if (elementClass.isInstance(e))
                        ++sum;
                }
                checkSum += sum ^ (sum << 5);
                for (int i = len - 1; i >= 0; --i) {
                    Object x = keys[i];
                    Object v = m.get(x);
                    if (elementClass.isInstance(v))
                        ++sum;
                }
                checkSum += sum ^ (sum << 6);
                for (int i = 0; i < len; ++i) {
                    Object x = ins[i];
                    Object v = m.get(x);
                    if (elementClass.isInstance(v))
                        ++sum;
                }
                checkSum += sum ^ (sum << 7);
                for (int i = 0; i < len; ++i) {
                    Object x = keys[i];
                    Object v = ins[i];
                    if (m.put(x, v) == x)
                        ++sum;
                }
                checkSum += sum ^ (sum << 8);
                for (int i = 0; i < len; ++i) {
                    Object x = keys[i];
                    Object v = ins[i];
                    if (v == m.get(x))
                        ++sum;
                }
                checkSum += sum ^ (sum << 9);
                for (int i = len - 1; i >= 0; --i) {
                    Object x = ins[i];
                    Object v = m.get(x);
                    if (elementClass.isInstance(v))
                        ++sum;
                }
                checkSum += sum ^ (sum << 10);
                for (int i = len - 1; i >= 0; --i) {
                    Object x = keys[i];
                    Object v = ins[i];
                    if (v == m.get(x))
                        ++sum;
                }
                checkSum += sum ^ (sum << 11);
                for (int i = 0; i < quarter; ++i) {
                    Object x = keys[i];
                    if (m.remove(x) != null)
                        ++sum;
                }
                for (int i = 0; i < quarter; ++i) {
                    Object x = keys[i];
                    if (m.put(x, x) == null)
                        ++sum;
                }
                m.clear();
                sum += len - (quarter * 2);
                checkSum += sum ^ (sum << 12);

                if (j == 0 && sum != lastSum + len * OPS_PER_ITER)
                    throw new Error(name);

                elapsed = System.nanoTime() - startTime;
                ++j;
                if (j >= minIters &&
                    (j >= maxIters || elapsed >= timeLimit))
                    break;
                // non-warmup - swap some keys for next insert
                if (minIters != 1 && randomSearches)
                    shuffleSome(ins, len, len >>> 3);
            }
            long ops = ((long) j) * len * OPS_PER_ITER;
            lastSum = sum;
            return elapsed / ops;
        }

    }


    static final Random rng = new Random(3122688);

    // Shuffle the subarrays for each size. This doesn't fully
    // randomize, but the remaining partial locality is arguably a bit
    // more realistic
    static void scramble(Object[] a) {
        for (int k = 0; k < sizes.length; ++k) {
            int origin = (k == 0) ? 0 : sizes[k-1];
            for (int i = sizes[k]; i > origin + 1; i--) {
                Object t = a[i-1];
                int r = rng.nextInt(i - origin) + origin;
                a[i-1] = a[r];
                a[r] = t;
            }
        }
    }

    // plain array shuffle
    static void shuffle(Object[] a, int size) {
        for (int i= size; i>1; i--) {
            Object t = a[i-1];
            int r = rng.nextInt(i);
            a[i-1] = a[r];
            a[r] = t;
        }
    }

    // swap nswaps elements
    static void shuffleSome(Object[] a, int size, int nswaps) {
        for (int s = 0; s < nswaps; ++s) {
            int i = rng.nextInt(size);
            int r = rng.nextInt(size);
            Object t = a[i];
            a[i] = a[r];
            a[r] = t;
        }
    }

    // Integer-like class with random hash codes
    static final class RandomInt {
        static int seed = 3122688;
        static int next() { // a non-xorshift, 2^32-period RNG
            int x = seed;
            int lo = 16807 * (x & 0xFFFF);
            int hi = 16807 * (x >>> 16);
            lo += (hi & 0x7FFF) << 16;
            if ((lo & 0x80000000) != 0) {
                lo &= 0x7fffffff;
                ++lo;
            }
            lo += hi >>> 15;
            if (lo == 0 || (lo & 0x80000000) != 0) {
                lo &= 0x7fffffff;
                ++lo;
            }
            seed = lo;
            return x;
        }
        final int value;
        RandomInt() { value = next(); }
        public int hashCode() { return value; }
        public boolean equals(Object x) {
            return (x instanceof RandomInt) && ((RandomInt)x).value == value;
        }
    }

    // Read in String keys from file if possible
    static void initStringKeys(Object[] keys, int n) throws Exception {
        FileInputStream fr = null;
        try {
            fr = new FileInputStream(wordFile);
        } catch (IOException ex) {
            System.out.println("No word file. Using String.valueOf(i)");
            for (int i = 0; i < n; i++)
                keys[i] = String.valueOf(i);
            return;
        }

        BufferedInputStream in = new BufferedInputStream(fr);
        int k = 0;
        outer:while (k < n) {
            StringBuffer sb = new StringBuffer();
            for (;;) {
                int c = in.read();
                if (c < 0)
                    break outer;
                char ch = (char) c;
                if (ch == '\n') {
                    keys[k++] = sb.toString();
                    break;
                }
                if (!Character.isWhitespace(ch))
                    sb.append(ch);
            }
        }
        in.close();

        // fill up remaining keys with path-like compounds of previous pairs
        int j = 0;
        while (k < n)
            keys[k++] = (String) keys[j++] + "/" + (String) keys[j];
    }

}
