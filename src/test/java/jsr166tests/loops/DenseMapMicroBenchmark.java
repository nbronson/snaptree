// from gee.cs.oswego.edu/home/jsr166/jsr166

package jsr166tests.loops;

/*
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */

import java.util.*;

public class DenseMapMicroBenchmark {
    static final int ITERS_PER_TEST = 4;
    static final long NANOS_PER_JOB = 4L * 1000L*1000L*1000L; // 4 sec
    static final long NANOS_PER_WARMUP = 500L*1000L*1000L; // 0.5 sec
    // map operations per iteration -- change if hasher.work changed
    static final int OPS_PER_ITER = 4; // put + get + iter + remove

    static int SIZE = 50000; // may be replaced by program arg

    abstract static class Job {
	final String name;
        long nanos;
        int runs;
	public Job(String name) { this.name = name; }
	public String name() { return name; }
	public abstract void work() throws Throwable;
    }

    /**
     * Runs each job for at least NANOS_PER_JOB seconds.
     * Returns array of average times per job per run.
     */
    static void time0(long nanos, Job ... jobs) throws Throwable {
	for (int i = 0; i < jobs.length; i++) {
            Thread.sleep(50);
	    long t0 = System.nanoTime();
	    long t;
	    int j = 0;
	    do {
                j++;
                jobs[i].work();
            } while ((t = System.nanoTime() - t0) < nanos);
            jobs[i].nanos = t / j;
            jobs[i].runs = j;
	}
    }

    static void time(Job ... jobs) throws Throwable {
        time0(NANOS_PER_JOB, jobs);

	final String nameHeader = "Method";
	int nameWidth  = nameHeader.length();
	for (Job job : jobs)
	    nameWidth = Math.max(nameWidth, job.name().length());

        final int itemsPerTest = SIZE * OPS_PER_ITER * ITERS_PER_TEST;
	final String timeHeader = "Nanos/item";
	int timeWidth  = timeHeader.length();
	final String ratioHeader = "Ratio";
	int ratioWidth = ratioHeader.length();
	String format = String.format("%%-%ds %%%dd %%.3f%%n",
				      nameWidth, timeWidth);
	String headerFormat = String.format("%%-%ds %%-%ds %%-%ds%%n",
					    nameWidth, timeWidth, ratioWidth);
	System.out.printf(headerFormat, "Method", "Nanos/item", "Ratio");

	// Print out absolute and relative times, calibrated against first job
	for (int i = 0; i < jobs.length; i++) {
	    long time = jobs[i].nanos/itemsPerTest;
	    double ratio = (double) jobs[i].nanos / (double) jobs[0].nanos;
	    System.out.printf(format, jobs[i].name(), time, ratio);
	}
    }


    static Long[] toLongs(Integer[] ints) {
	Long[] longs = new Long[ints.length];
	for (int i = 0; i < ints.length; i++)
	    longs[i] = ints[i].longValue();
	return longs;
    }

    static String[] toStrings(Integer[] ints) {
	String[] strings = new String[ints.length];
	for (int i = 0; i < ints.length; i++)
            strings[i] = ints[i].toString();
        //            strings[i] = String.valueOf(ints[i].doubleValue());
	return strings;
    }

    static Float[] toFloats(Integer[] ints) {
	Float[] floats = new Float[ints.length];
	for (int i = 0; i < ints.length; i++)
	    floats[i] = ints[i].floatValue();
	return floats;
    }

    static Double[] toDoubles(Integer[] ints) {
	Double[] doubles = new Double[ints.length];
	for (int i = 0; i < ints.length; i++)
	    doubles[i] = ints[i].doubleValue();
	return doubles;
    }


    static final class Hasher extends Job {
        final Object[] elts;
        final Class mapClass;
        volatile int matches;
        Hasher(String name, Object[] elts, Class mapClass) {
            super(name);
            this.elts = elts;
            this.mapClass = mapClass;
        }
        public void work() {
            Map m = null;
            try {
                m = (Map) mapClass.newInstance();
            } catch (Exception e) {
                throw new RuntimeException("Can't instantiate " + mapClass + ": " + e);
            }
            final int len = elts.length;
            for (int j = 0; j < ITERS_PER_TEST; j++) {
                for (Object x : elts) {
                    if (m.put(x, x) != null)
                        throw new Error();
                }
                if (m.size() != len)
                    throw new Error();
                int ng = 0;
                for (Object x : elts) {
                    if (m.get(x) == x)
                        ++ng;
                }
                matches += ng;
                for (Object e : m.keySet()) {
                    if (m.get(e) == e)
                        --ng;
                }
                if (ng != 0)
                    throw new Error();
                for (Object x : elts) {
                    if (m.remove(x) != x)
                        throw new Error();
                }
                if (!m.isEmpty())
                    throw new Error();
            }
            if (matches != len * ITERS_PER_TEST)
                throw new Error();
            matches = 0;
        }
    }

    public static void main(String[] args) throws Throwable {
        Class mc = java.util.HashMap.class;
        if (args.length > 0)
            mc = Class.forName(args[0]);
        if (args.length > 1)
            SIZE = Integer.parseInt(args[1]);

        System.out.print("Class " + mc.getName());
        System.out.print(" size " + SIZE);
        System.out.println();

	final Integer[] seq = new Integer[SIZE];
        for (int i = 0; i < SIZE; i++)
            seq[i] = new Integer(i);
	final Integer[] shf = seq.clone();
	Collections.shuffle(Arrays.asList(shf));
	List<Hasher> hashers = new ArrayList<Hasher>();
        hashers.add(new Hasher("Integer sequential", seq, mc));
        hashers.add(new Hasher("Integer shuffled", shf, mc));

        hashers.add(new Hasher("Long    sequential", toLongs(seq), mc));
        hashers.add(new Hasher("Long    shuffled", toLongs(shf), mc));

        hashers.add(new Hasher("Float   sequential", toFloats(seq), mc));
        hashers.add(new Hasher("Float   shuffled", toFloats(shf), mc));

        hashers.add(new Hasher("Double  sequential", toDoubles(seq), mc));
        hashers.add(new Hasher("Double  shuffled", toDoubles(shf), mc));

        hashers.add(new Hasher("String  sequential", toStrings(seq), mc));
        hashers.add(new Hasher("String  shuffled", toStrings(shf), mc));

        Hasher[] jobs = hashers.toArray(new Hasher[0]);
        System.out.print("warmup...");
	time0(NANOS_PER_WARMUP, jobs); // Warm up run
	time0(NANOS_PER_WARMUP, jobs); // Warm up run
        for (int i = 0; i < 2; i++) {
            System.gc();
            Thread.sleep(50);
            System.runFinalization();
            Thread.sleep(50);
        }
        System.out.println("starting");
	time(jobs);
    }

}
