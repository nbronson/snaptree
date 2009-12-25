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

public class IntMapCheck {
    static int absentSize;
    static int absentMask;
    static Integer[] absent;
    static final Integer MISSING = new Integer(Integer.MIN_VALUE);
    static TestTimer timer = new TestTimer();

    static void reallyAssert(boolean b) {
        if (!b) throw new Error("Failed Assertion");
    }

    public static void main(String[] args) throws Exception {
        Class mapClass = java.util.concurrent.ConcurrentHashMap.class;
        int numTests = 50;
        int size = 75000;

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

        boolean doSerializeTest = args.length > 3;

        System.out.println("Testing " + mapClass.getName() + " trials: " + numTests + " size: " + size);

        absentSize = 4;
        while (absentSize < size) absentSize <<= 1;
        absentMask = absentSize-1;
        absent = new Integer[absentSize];
        for (int i = 0; i < absentSize/2; ++i)
            absent[i] = Integer.valueOf(-i - 1);
        for (int i = absentSize/2; i < absentSize; ++i)
            absent[i] = Integer.valueOf(size + i + 1);

        Integer[] key = new Integer[size];
        for (int i = 0; i < size; ++i)
            key[i] = Integer.valueOf(i);

        for (int rep = 0; rep < numTests; ++rep) {
            runTest(newMap(mapClass), key);
            if ((rep & 3) == 3 && rep < numTests - 1) {
                shuffle(key);
                //                Thread.sleep(50);
            }
        }

        TestTimer.printStats();


        if (doSerializeTest)
            stest(newMap(mapClass), size);
    }

    static Map<Integer,Integer> newMap(Class cl) {
        try {
            Map m = (Map<Integer,Integer>)cl.newInstance();
            return m;
        } catch (Exception e) {
            throw new RuntimeException("Can't instantiate " + cl + ": " + e);
        }
    }


    static void runTest(Map<Integer,Integer> s, Integer[] key) {
        int size = key.length;
        long startTime = System.nanoTime();
        test(s, key);
        long time = System.nanoTime() - startTime;
    }

    static void t1(String nm, int n, Map<Integer,Integer> s, Integer[] key, int expect, int iters) {
        int sum = 0;
        timer.start(nm, n * iters);
        for (int j = 0; j < iters; ++j) {
            for (int i = 0; i < n; i++) {
                if (s.get(key[i]) != null) ++sum;
            }
        }
        timer.finish();
        reallyAssert (sum == expect * iters);
    }

    static void t1Boxed(String nm, int n, Map<Integer,Integer> s, Integer[] key, int expect) {
        int sum = 0;
        int iters = 8;
        timer.start(nm, n * iters);
        for (int j = 0; j < iters; ++j) {
            for (int i = 0; i < n; i++) {
                if (s.get(i) != i) ++sum;
            }
        }
        timer.finish();
        reallyAssert (sum == expect * iters);
    }


    static void t2(String nm, int n, Map<Integer,Integer> s, Integer[] key, int expect) {
        int sum = 0;
        timer.start(nm, n);
        for (int i = 0; i < n; i++) {
            if (s.remove(key[i]) != null) ++sum;
        }
        timer.finish();
        reallyAssert (sum == expect);
    }

    static void t3(String nm, int n, Map<Integer,Integer> s, Integer[] key, int expect) {
        int sum = 0;
        timer.start(nm, n);
        for (int i = 0; i < n; i++) {
            Integer k = key[i];
            Integer v = absent[i & absentMask];
            if (s.put(k, v) == null) ++sum;
        }
        timer.finish();
        reallyAssert (sum == expect);
    }

    static void t4(String nm, int n, Map<Integer,Integer> s, Integer[] key, int expect) {
        int sum = 0;
        timer.start(nm, n);
        for (int i = 0; i < n; i++) {
            if (s.containsKey(key[i])) ++sum;
        }
        timer.finish();
        reallyAssert (sum == expect);
    }

    static void t5(String nm, int n, Map<Integer,Integer> s, Integer[] key, int expect) {
        int sum = 0;
        timer.start(nm, n/2);
        for (int i = n-2; i >= 0; i-=2) {
            if (s.remove(key[i]) != null) ++sum;
        }
        timer.finish();
        reallyAssert (sum == expect);
    }

    static void t6(String nm, int n, Map<Integer,Integer> s, Integer[] k1, Integer[] k2) {
        int sum = 0;
        timer.start(nm, n * 2);
        for (int i = 0; i < n; i++) {
            if (s.get(k1[i]) != null) ++sum;
            if (s.get(k2[i & absentMask]) != null) ++sum;
        }
        timer.finish();
        reallyAssert (sum == n);
    }

    static void t7(String nm, int n, Map<Integer,Integer> s, Integer[] k1, Integer[] k2) {
        int sum = 0;
        timer.start(nm, n * 2);
        for (int i = 0; i < n; i++) {
            if (s.containsKey(k1[i])) ++sum;
            if (s.containsKey(k2[i & absentMask])) ++sum;
        }
        timer.finish();
        reallyAssert (sum == n);
    }

    static void t8(String nm, int n, Map<Integer,Integer> s, Integer[] key, int expect) {
        int sum = 0;
        timer.start(nm, n);
        for (int i = 0; i < n; i++) {
            if (s.get(key[i]) != null) ++sum;
        }
        timer.finish();
        reallyAssert (sum == expect);
    }


    static void t9(Map<Integer,Integer> s) {
        int sum = 0;
        int iters = 20;
        timer.start("ContainsValue (/n)     ", iters * s.size());
        int step = absentSize / iters;
        for (int i = 0; i < absentSize; i += step)
            if (s.containsValue(absent[i])) ++sum;
        timer.finish();
        reallyAssert (sum != 0);
    }


    static void ktest(Map<Integer,Integer> s, int size, Integer[] key) {
        timer.start("ContainsKey            ", size);
        Set ks = s.keySet();
        int sum = 0;
        for (int i = 0; i < size; i++) {
            if (ks.contains(key[i])) ++sum;
        }
        timer.finish();
        reallyAssert (sum == size);
    }


    static void ittest1(Map<Integer,Integer> s, int size) {
        int sum = 0;
        timer.start("Iter Key               ", size);
        for (Iterator it = s.keySet().iterator(); it.hasNext(); ) {
            if (it.next() != MISSING)
                ++sum;
        }
        timer.finish();
        //        if (sum != size)
        //            System.out.println("iters " + sum + " size " + size);
        reallyAssert (sum == size);
    }

    static void ittest2(Map<Integer,Integer> s, int size) {
        int sum = 0;
        timer.start("Iter Value             ", size);
        for (Iterator it = s.values().iterator(); it.hasNext(); ) {
            if (it.next() != MISSING)
                ++sum;
        }
        timer.finish();
        //        if (sum != size)
        //            System.out.println("iters " + sum + " size " + size);
        reallyAssert (sum == size);
    }
    static void ittest3(Map<Integer,Integer> s, int size) {
        int sum = 0;
        timer.start("Iter Entry             ", size);
        for (Iterator it = s.entrySet().iterator(); it.hasNext(); ) {
            if (it.next() != MISSING)
                ++sum;
        }
        timer.finish();
        reallyAssert (sum == size);
    }

    static void ittest4(Map<Integer,Integer> s, int size, int pos) {
        IdentityHashMap seen = new IdentityHashMap(size);
        reallyAssert (s.size() == size);
        int sum = 0;
        timer.start("Iter XEntry            ", size);
        Iterator it = s.entrySet().iterator();
        Integer k = null;
        Integer v = null;
        for (int i = 0; i < size-pos; ++i) {
            Map.Entry<Integer,Integer> x = (Map.Entry<Integer,Integer>)(it.next());
            k = x.getKey();
            v = x.getValue();
            seen.put(k, k);
            if (v != MISSING)
                ++sum;
        }
        reallyAssert (s.containsKey(k));
        it.remove();
        reallyAssert (!s.containsKey(k));
        while (it.hasNext()) {
            Map.Entry<Integer,Integer> x = (Map.Entry<Integer,Integer>)(it.next());
            Integer k2 = x.getKey();
            seen.put(k2, k2);
            if (k2 != MISSING)
                ++sum;
        }

        reallyAssert (s.size() == size-1);
        s.put(k, v);
        reallyAssert (seen.size() == size);
        timer.finish();
        reallyAssert (sum == size);
        reallyAssert (s.size() == size);
    }


    static void ittest(Map<Integer,Integer> s, int size) {
        for (int i = 0; i < 4; ++i) {
            ittest1(s, size);
            ittest2(s, size);
            ittest3(s, size);
        }
        //        for (int i = 0; i < size-1; ++i)
        //            ittest4(s, size, i);
    }

    static void entest1(Hashtable ht, int size) {
        int sum = 0;

        timer.start("Iter Enumeration Key   ", size);
        for (Enumeration en = ht.keys(); en.hasMoreElements(); ) {
            if (en.nextElement() != MISSING)
                ++sum;
        }
        timer.finish();
        reallyAssert (sum == size);
    }

    static void entest2(Hashtable ht, int size) {
        int sum = 0;
        timer.start("Iter Enumeration Value ", size);
        for (Enumeration en = ht.elements(); en.hasMoreElements(); ) {
            if (en.nextElement() != MISSING)
                ++sum;
        }
        timer.finish();
        reallyAssert (sum == size);
    }


    static void entest3(Hashtable ht, int size) {
        int sum = 0;

        timer.start("Iterf Enumeration Key  ", size);
        Enumeration en = ht.keys();
        for (int i = 0; i < size; ++i) {
            if (en.nextElement() != MISSING)
                ++sum;
        }
        timer.finish();
        reallyAssert (sum == size);
    }

    static void entest4(Hashtable ht, int size) {
        int sum = 0;
        timer.start("Iterf Enumeration Value", size);
        Enumeration en = ht.elements();
        for (int i = 0; i < size; ++i) {
            if (en.nextElement() != MISSING)
                ++sum;
        }
        timer.finish();
        reallyAssert (sum == size);
    }

    static void entest(Map<Integer,Integer> s, int size) {
        if (s instanceof Hashtable) {
            Hashtable ht = (Hashtable) s;
            //            entest3(ht, size);
            //            entest4(ht, size);
            entest1(ht, size);
            entest2(ht, size);
            entest1(ht, size);
            entest2(ht, size);
            entest1(ht, size);
            entest2(ht, size);
        }
    }

    static void rtest(Map<Integer,Integer> s, int size) {
        timer.start("Remove (iterator)      ", size);
        for (Iterator it = s.keySet().iterator(); it.hasNext(); ) {
            it.next();
            it.remove();
        }
        reallyAssert(s.isEmpty());
        timer.finish();
    }

    static void stest(Map<Integer,Integer> s, int size) throws Exception {
        if (!(s instanceof Serializable))
            return;
        System.out.print("Serialize              : ");

        for (int i = 0; i < size; i++) {
            s.put(Integer.valueOf(i), Integer.valueOf(1));
        }

        long startTime = System.nanoTime();

        FileOutputStream fs = new FileOutputStream("IntMapCheck.dat");
        ObjectOutputStream out = new ObjectOutputStream(new BufferedOutputStream(fs));
        out.writeObject(s);
        out.close();

        FileInputStream is = new FileInputStream("IntMapCheck.dat");
        ObjectInputStream in = new ObjectInputStream(new BufferedInputStream(is));
        Map<Integer,Integer> m = (Map<Integer,Integer>)in.readObject();

        long endTime = System.nanoTime();
        long time = endTime - startTime;

        System.out.print(time + "ms");

        if (s instanceof IdentityHashMap) return;
        reallyAssert (s.equals(m));
    }


    static void test(Map<Integer,Integer> s, Integer[] key) {
        int size = key.length;

        t3("Put (absent)           ", size, s, key, size);
        reallyAssert(s.size() == size);
        t1("Get (present)          ", size, s, key, size, 8);
        t1Boxed("Get boxed (present)    ", size, s, key, size);
        ittest1(s, size);
        t3("Put (present)          ", size, s, key, 0);
        reallyAssert(s.size() == size);
        t7("ContainsKey            ", size, s, key, absent);
        t4("ContainsKey            ", size, s, key, size);
        ktest(s, size, key);
        t4("ContainsKey            ", absentSize, s, absent, 0);
        t6("Get                    ", size, s, key, absent);
        t1("Get (present)          ", size, s, key, size, 8);
        t1("Get (absent)           ", absentSize, s, absent, 0, 1);
        reallyAssert(s.size() == size);
        t2("Remove (absent)        ", absentSize, s, absent, 0);
        reallyAssert(s.size() == size);
        t5("Remove (present)       ", size, s, key, size / 2);
        reallyAssert(s.size() == size / 2);
        t1("Get                    ", size, s, key, size / 2, 8);
        ittest1(s, size / 2);
        t3("Put (half present)     ", size, s, key, size / 2);
        reallyAssert(s.size() == size);
        t1("Get (present)          ", size, s, key, size, 4);

        entest(s, size);
        t9(s);
        reallyAssert(s.size() == size);
        timer.start("Clear                  ", size);
        s.clear();
        timer.finish();
        t1("Get (absent)           ", size, s, key, 0, 1);
        t4("ContainsKey            ", size, s, key, 0);
        t2("Remove (absent)        ", size, s, key, 0);
        t3("Put (presized)         ", size, s, key, size);
        t1("Get (present)          ", size, s, key, size, 4);
        reallyAssert(s.size() == size);
        ittest(s, size);
        rtest(s, size);
        reallyAssert(s.size() == 0);
        timer.start("Clear                  ", size);
        s.clear();
        timer.finish();
        t3("Put (presized)         ", size, s, key, size);

        timer.start("Put (putAll)           ", size * 2);
        Map<Integer,Integer> s2 = null;
        try {
            s2 = (Map<Integer,Integer>) (s.getClass().newInstance());
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

        timer.start("Put (present)          ", size * 2);
        s2.putAll(s);
        timer.finish();

        timer.start("Put (present)          ", size);
        int ipsum = 0;
        for (Iterator i0 = s.entrySet().iterator(); i0.hasNext(); ) {
            Map.Entry<Integer,Integer> me = (Map.Entry<Integer,Integer>)(i0.next());
            if (s2.put(me.getKey(), me.getValue()) != null)
                ++ipsum;
        }
        reallyAssert (ipsum == s.size());
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

        Integer hold = s2.get(key[size-1]);
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
        timer.start("Remove (present)       ", size * 2);
        Iterator s2i = s2.entrySet().iterator();
        Set es = s.entrySet();
        while (s2i.hasNext())
            reallyAssert(es.remove(s2i.next()));
        timer.finish();

        reallyAssert (s.isEmpty());

        timer.start("Clear                  ", size);
        s2.clear();
        timer.finish();
        reallyAssert (s2.isEmpty() && s.isEmpty());
    }

    static class TestTimer {
        private String name;
        private long numOps;
        private long startTime;
        private String cname;

        static final java.util.TreeMap accum = new java.util.TreeMap();

        static void printStats() {
            for (Iterator it = accum.entrySet().iterator(); it.hasNext(); ) {
                Map.Entry e = (Map.Entry)(it.next());
                Stats stats = ((Stats) (e.getValue()));
                long n = stats.number;
                double t;
                if (n > 0)
                    t = stats.sum / n;
                else
                    t = stats.least;
                long nano = Math.round(t);
                System.out.println(e.getKey() + ": " + nano);
            }
        }

        void start(String name, long numOps) {
            this.name = name;
            this.cname = classify();
            this.numOps = numOps;
            startTime = System.nanoTime();
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
            long endTime = System.nanoTime();
            long time = endTime - startTime;
            double timePerOp = ((double) time)/numOps;

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
        long number = 0;
        Stats(double t) { least = t; }
    }

    static Random rng = new Random(3152688);

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
