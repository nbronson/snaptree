// from gee.cs.oswego.edu/home/jsr166/jsr166

package jsr166tests.loops;

/*
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */
/**
 * @test
 * @synopsis Times and checks basic map operations
 */
import java.util.*;
import java.io.*;

public class NavigableMapCheck {

    static int absentSize;
    static int absentMask;
    static Integer[] absent;

    static final Integer MISSING = new Integer(Integer.MIN_VALUE);

    static TestTimer timer = new TestTimer();

    static void reallyAssert(boolean b) {
        if (!b) throw new Error("Failed Assertion");
    }

    public static void main(String[] args) throws Exception {
        Class mapClass = null;
        int numTests = 50;
        int size = 50000;

        if (args.length > 0) {
            try {
                mapClass = Class.forName(args[0]);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("Class " + args[0] + " not found.");
            }
        }


        if (args.length > 1)
            numTests = Integer.parseInt(args[1]);

        if (args.length > 2)
            size = Integer.parseInt(args[2]);

        System.out.println("Testing " + mapClass.getName() + " trials: " + numTests + " size: " + size);


        absentSize = 8;
        while (absentSize < size) absentSize <<= 1;
        absentMask = absentSize - 1;
        absent = new Integer[absentSize];

        for (int i = 0; i < absentSize; ++i)
            absent[i] = new Integer(i * 2);

        Integer[] key = new Integer[size];
        for (int i = 0; i < size; ++i)
            key[i] = new Integer(i * 2 + 1);


        for (int rep = 0; rep < numTests; ++rep) {
            runTest(newMap(mapClass), key);
        }

        TestTimer.printStats();

    }

    static NavigableMap newMap(Class cl) {
        try {
            NavigableMap m = (NavigableMap) cl.newInstance();
            return m;
        } catch (Exception e) {
            throw new RuntimeException("Can't instantiate " + cl + ": " + e);
        }
    }


    static void runTest(NavigableMap s, Integer[] key) {
        shuffle(key);
        int size = key.length;
        long startTime = System.currentTimeMillis();
        test(s, key);
        long time = System.currentTimeMillis() - startTime;
    }

    static void t1(String nm, int n, NavigableMap s, Integer[] key, int expect) {
        int sum = 0;
        int iters = 4;
        timer.start(nm, n * iters);
        for (int j = 0; j < iters; ++j) {
            for (int i = 0; i < n; i++) {
                if (s.get(key[i]) != null) ++sum;
            }
        }
        timer.finish();
        reallyAssert (sum == expect * iters);
    }

    static void t2(String nm, int n, NavigableMap s, Integer[] key, int expect) {
        int sum = 0;
        timer.start(nm, n);
        for (int i = 0; i < n; i++) {
            if (s.remove(key[i]) != null) ++sum;
        }
        timer.finish();
        reallyAssert (sum == expect);
    }

    static void t3(String nm, int n, NavigableMap s, Integer[] key, int expect) {
        int sum = 0;
        timer.start(nm, n);
        for (int i = 0; i < n; i++) {
            if (s.put(key[i], absent[i & absentMask]) == null) ++sum;
        }
        timer.finish();
        reallyAssert (sum == expect);
    }

    static void t4(String nm, int n, NavigableMap s, Integer[] key, int expect) {
        int sum = 0;
        timer.start(nm, n);
        for (int i = 0; i < n; i++) {
            if (s.containsKey(key[i])) ++sum;
        }
        timer.finish();
        reallyAssert (sum == expect);
    }

    static void t5(String nm, int n, NavigableMap s, Integer[] key, int expect) {
        int sum = 0;
        timer.start(nm, n/2);
        for (int i = n-2; i >= 0; i-=2) {
            if (s.remove(key[i]) != null) ++sum;
        }
        timer.finish();
        reallyAssert (sum == expect);
    }

    static void t6(String nm, int n, NavigableMap s, Integer[] k1, Integer[] k2) {
        int sum = 0;
        timer.start(nm, n * 2);
        for (int i = 0; i < n; i++) {
            if (s.get(k1[i]) != null) ++sum;
            if (s.get(k2[i & absentMask]) != null) ++sum;
        }
        timer.finish();
        reallyAssert (sum == n);
    }

    static void t7(String nm, int n, NavigableMap s, Integer[] k1, Integer[] k2) {
        int sum = 0;
        timer.start(nm, n * 2);
        for (int i = 0; i < n; i++) {
            if (s.containsKey(k1[i])) ++sum;
            if (s.containsKey(k2[i & absentMask])) ++sum;
        }
        timer.finish();
        reallyAssert (sum == n);
    }

    static void t8(String nm, int n, NavigableMap s, Integer[] key, int expect) {
        int sum = 0;
        timer.start(nm, n);
        for (int i = 0; i < n; i++) {
            if (s.get(key[i]) != null) ++sum;
        }
        timer.finish();
        reallyAssert (sum == expect);
    }


    static void t9(NavigableMap s) {
        int sum = 0;
        int iters = 20;
        timer.start("ContainsValue (/n)     ", iters * s.size());
        int step = absentSize / iters;
        for (int i = 0; i < absentSize; i += step)
            if (s.containsValue(absent[i])) ++sum;
        timer.finish();
        reallyAssert (sum != 0);
    }

    static void higherTest(NavigableMap s) {
        int sum = 0;
        int iters = s.size();
        timer.start("Higher                 ", iters);
        Map.Entry e = s.firstEntry();
        while (e != null) {
            ++sum;
            e = s.higherEntry(e.getKey());
        }
        timer.finish();
        reallyAssert (sum == iters);
    }

    static void lowerTest(NavigableMap s) {
        int sum = 0;
        int iters = s.size();
        timer.start("Lower                  ", iters);
        Map.Entry e = s.firstEntry();
        while (e != null) {
            ++sum;
            e = s.higherEntry(e.getKey());
        }
        timer.finish();
        reallyAssert (sum == iters);
    }

    static void ceilingTest(NavigableMap s) {
        int sum = 0;
        int iters = s.size();
        if (iters > absentSize) iters = absentSize;
        timer.start("Ceiling                ", iters);
        for (int i = 0; i < iters; ++i) {
            Map.Entry e = s.ceilingEntry(absent[i]);
            if (e != null)
                ++sum;
        }
        timer.finish();
        reallyAssert (sum == iters);
    }

    static void floorTest(NavigableMap s) {
        int sum = 0;
        int iters = s.size();
        if (iters > absentSize) iters = absentSize;
        timer.start("Floor                  ", iters);
        for (int i = 1; i < iters; ++i) {
            Map.Entry e = s.floorEntry(absent[i]);
            if (e != null)
                ++sum;
        }
        timer.finish();
        reallyAssert (sum == iters-1);
    }


    static void ktest(NavigableMap s, int size, Integer[] key) {
        timer.start("ContainsKey            ", size);
        Set ks = s.keySet();
        int sum = 0;
        for (int i = 0; i < size; i++) {
            if (ks.contains(key[i])) ++sum;
        }
        timer.finish();
        reallyAssert (sum == size);
    }


    static void ittest1(NavigableMap s, int size) {
        int sum = 0;
        timer.start("Iter Key               ", size);
        for (Iterator it = s.keySet().iterator(); it.hasNext(); ) {
            if (it.next() != MISSING)
                ++sum;
        }
        timer.finish();
        reallyAssert (sum == size);
    }

    static void ittest2(NavigableMap s, int size) {
        int sum = 0;
        timer.start("Iter Value             ", size);
        for (Iterator it = s.values().iterator(); it.hasNext(); ) {
            if (it.next() != MISSING)
                ++sum;
        }
        timer.finish();
        reallyAssert (sum == size);
    }
    static void ittest3(NavigableMap s, int size) {
        int sum = 0;
        timer.start("Iter Entry             ", size);
        for (Iterator it = s.entrySet().iterator(); it.hasNext(); ) {
            if (it.next() != MISSING)
                ++sum;
        }
        timer.finish();
        reallyAssert (sum == size);
    }

    static void ittest(NavigableMap s, int size) {
        ittest1(s, size);
        ittest2(s, size);
        ittest3(s, size);
    }

    static void rittest1(NavigableMap s, int size) {
        int sum = 0;
        timer.start("Desc Iter Key          ", size);
        for (Iterator it = s.descendingKeySet().iterator(); it.hasNext(); ) {
            if (it.next() != MISSING)
                ++sum;
        }
        timer.finish();
        reallyAssert (sum == size);
    }


    static void rittest(NavigableMap s, int size) {
        rittest1(s, size);
        //        rittest2(s, size);
    }


    static void rtest(NavigableMap s, int size) {
        timer.start("Remove (iterator)      ", size);
        for (Iterator it = s.keySet().iterator(); it.hasNext(); ) {
            it.next();
            it.remove();
        }
        timer.finish();
    }

    static void rvtest(NavigableMap s, int size) {
        timer.start("Remove (iterator)      ", size);
        for (Iterator it = s.values().iterator(); it.hasNext(); ) {
            it.next();
            it.remove();
        }
        timer.finish();
    }


    static void dtest(NavigableMap s, int size, Integer[] key) {
        timer.start("Put (putAll)           ", size * 2);
        NavigableMap s2 = null;
        try {
            s2 = (NavigableMap) (s.getClass().newInstance());
            s2.putAll(s);
        }
        catch (Exception e) { e.printStackTrace(); return; }
        timer.finish();

        timer.start("Iter Equals            ", size * 2);
        boolean eqt = s2.equals(s) && s.equals(s2);
        reallyAssert (eqt);
        timer.finish();

        timer.start("Iter HashCode          ", size * 2);
        int shc = s.hashCode();
        int s2hc = s2.hashCode();
        reallyAssert (shc == s2hc);
        timer.finish();

        timer.start("Put (present)          ", size);
        s2.putAll(s);
        timer.finish();

        timer.start("Iter EntrySet contains ", size * 2);
        Set es2 = s2.entrySet();
        int sum = 0;
        for (Iterator i1 = s.entrySet().iterator(); i1.hasNext(); ) {
            Object entry = i1.next();
            if (es2.contains(entry)) ++sum;
        }
        timer.finish();
        reallyAssert (sum == size);

        t6("Get                    ", size, s2, key, absent);

        Object hold = s2.get(key[size-1]);
        s2.put(key[size-1], absent[0]);
        timer.start("Iter Equals            ", size * 2);
        eqt = s2.equals(s) && s.equals(s2);
        reallyAssert (!eqt);
        timer.finish();

        timer.start("Iter HashCode          ", size * 2);
        int s1h = s.hashCode();
        int s2h = s2.hashCode();
        reallyAssert (s1h != s2h);
        timer.finish();

        s2.put(key[size-1], hold);
        timer.start("Remove (iterator)      ", size * 2);
        Iterator s2i = s2.entrySet().iterator();
        Set es = s.entrySet();
        while (s2i.hasNext())
            es.remove(s2i.next());
        timer.finish();

        reallyAssert (s.isEmpty());

        timer.start("Clear                  ", size);
        s2.clear();
        timer.finish();
        reallyAssert (s2.isEmpty() && s.isEmpty());
    }



    static void test(NavigableMap s, Integer[] key) {
        int size = key.length;

        t3("Put (absent)           ", size, s, key, size);
        t3("Put (present)          ", size, s, key, 0);
        t7("ContainsKey            ", size, s, key, absent);
        t4("ContainsKey            ", size, s, key, size);
        ktest(s, size, key);
        t4("ContainsKey            ", absentSize, s, absent, 0);
        t6("Get                    ", size, s, key, absent);
        t1("Get (present)          ", size, s, key, size);
        t1("Get (absent)           ", absentSize, s, absent, 0);
        t2("Remove (absent)        ", absentSize, s, absent, 0);
        t5("Remove (present)       ", size, s, key, size / 2);
        t3("Put (half present)     ", size, s, key, size / 2);

        ittest(s, size);
        rittest(s, size);
        higherTest(s);
        ceilingTest(s);
        floorTest(s);
        lowerTest(s);
        t9(s);
        rtest(s, size);

        t4("ContainsKey            ", size, s, key, 0);
        t2("Remove (absent)        ", size, s, key, 0);
        t3("Put (presized)         ", size, s, key, size);
        dtest(s, size, key);
    }

    static class TestTimer {
        private String name;
        private long numOps;
        private long startTime;
        private String cname;

        static final java.util.TreeMap accum = new java.util.TreeMap();

        static void printStats() {
            for (Iterator it = accum.entrySet().iterator(); it.hasNext(); ) {
                Map.Entry e = (Map.Entry) it.next();
                Stats stats = (Stats) e.getValue();
                int n = stats.number;
                double t;
                if (n > 0)
                    t = stats.sum / n;
                else
                    t = stats.least;
                long nano = Math.round(1000000.0 * t);
                System.out.println(e.getKey() + ": " + nano);
            }
        }

        void start(String name, long numOps) {
            this.name = name;
            this.cname = classify();
            this.numOps = numOps;
            startTime = System.currentTimeMillis();
        }


        String classify() {
            if (name.startsWith("Get"))
                return "Get                    ";
            else if (name.startsWith("Put"))
                return "Put                    ";
            else if (name.startsWith("Remove"))
                return "Remove                 ";
            else if (name.startsWith("Iter"))
                return "Iter                   ";
            else
                return null;
        }

        void finish() {
            long endTime = System.currentTimeMillis();
            long time = endTime - startTime;
            double timePerOp = (double) time /numOps;

            Object st = accum.get(name);
            if (st == null)
                accum.put(name, new Stats(timePerOp));
            else {
                Stats stats = (Stats) st;
                stats.sum += timePerOp;
                stats.number++;
                if (timePerOp < stats.least) stats.least = timePerOp;
            }

            if (cname != null) {
                st = accum.get(cname);
                if (st == null)
                    accum.put(cname, new Stats(timePerOp));
                else {
                    Stats stats = (Stats) st;
                    stats.sum += timePerOp;
                    stats.number++;
                    if (timePerOp < stats.least) stats.least = timePerOp;
                }
            }

        }

    }

    static class Stats {
        double sum = 0;
        double least;
        int number = 0;
        Stats(double t) { least = t; }
    }

    static Random rng = new Random(111);

    static void shuffle(Integer[] keys) {
        int size = keys.length;
        for (int i=size; i>1; i--) {
            int r = rng.nextInt(i);
            Integer t = keys[i-1];
            keys[i-1] = keys[r];
            keys[r] = t;
        }
    }

}
