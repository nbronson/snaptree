// from gee.cs.oswego.edu/home/jsr166/jsr166

package jsr166tests.loops;

/*
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */
import java.util.*;
import java.io.*;

public class MapWordLoops {

    static final String[] WORDS_FILES = {
        "kw.txt",
        "class.txt",
        "dir.txt",
        "ids.txt",
        "testwords.txt",
        //        "/usr/dict/words",
    };

    static final int MAX_WORDS = 500000;
    static final int pinsert   = 60;
    static final int premove   = 2;
    static final int NOPS      = 8000000;

    static final int numTests = 3;

    public static void main(String[] args) {
        Class mapClass = null;
        try {
            mapClass = Class.forName(args[0]);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Class " + args[0] + " not found.");
        }

        System.out.println("Testing " + mapClass.getName());

        for (int s = 0; s < WORDS_FILES.length; ++s)
            tests(mapClass, numTests, s);

        for (int s = WORDS_FILES.length-1; s >= 0; --s)
            tests(mapClass, numTests, s);

    }

    static void tests(Class mapClass, int numTests, int sizeIndex) {
        try {
            String[] key = readWords(sizeIndex);
            int size = key.length;

            System.out.print("n = " +LoopHelpers.rightJustify(size) +" : ");
            long least = Long.MAX_VALUE;

            for (int i = 0; i < numTests; ++i) {
                Map<String,String> m = newMap(mapClass);
                long t = doTest(i, mapClass.getName(), m, key);
                if (t < least) least = t;
                m.clear();
                m = null;
            }

            long nano = Math.round(1000000.0 * (least) / NOPS);
            System.out.println(LoopHelpers.rightJustify(nano) + " ns per op");
        } catch (IOException ignore) {
            return; // skip test if can't read file
        }
    }


    static Map<String,String> newMap(Class cl) {
        try {
            Map m = (Map<String,String>)cl.newInstance();
            return m;
        } catch (Exception e) {
            throw new RuntimeException("Can't instantiate " + cl + ": " + e);
        }
    }

    static void pause() {
        try { Thread.sleep(100); }
        catch (InterruptedException ie) { return; }
    }

    static String[] readWords(int sizeIndex) throws IOException {
        String[] l = new String[MAX_WORDS];
        String[] array = null;
        try {
            FileReader fr = new FileReader(WORDS_FILES[sizeIndex]);
            BufferedReader reader = new BufferedReader(fr);
            int k = 0;
            for (;;) {
                String s = reader.readLine();
                if (s == null) break;
                l[k++] = s;
            }
            array = new String[k];
            for (int i = 0; i < k; ++i) {
                array[i] = l[i];
                l[i] = null;
            }
            l = null;
            reader.close();
        }
        catch (IOException ex) {
            System.out.println("Can't read words file:" + ex);
            throw ex;
        }
        return array;
    }

    static long doTest(int id, String name,
                       final Map<String,String> m,
                       final String[] key) {

        //    System.out.print(name + "\t");
        Runner runner = new Runner(id, m, key);
        long startTime = System.currentTimeMillis();
        runner.run();
        long afterRun = System.currentTimeMillis();
        long runTime =  (afterRun - startTime);
        int np = runner.total;
        if (runner.total == runner.hashCode())
            System.out.println("Useless Number" + runner.total);
        int sz = runner.maxsz;
        if (sz == runner.hashCode())
            System.out.println("Useless Number" + sz);
        //        System.out.print(" m = " + sz);
        return runTime;
    }


    static class Runner implements Runnable {
        final Map<String,String> map;
        final String[] key;
        LoopHelpers.SimpleRandom rng;
        final int pctrem;
        final int pctins;
        int nputs = 0;
        int npgets = 0;
        int nagets = 0;
        int nremoves = 0;
        volatile int total;
        int maxsz;

        Runner(int id, Map<String,String> m, String[] k) {
            map = m; key = k;
            pctrem = (int)(((long)premove * (long)(Integer.MAX_VALUE/2)) / 50);
            pctins = (int)(((long)pinsert * (long)(Integer.MAX_VALUE/2)) / 50);
            rng = new LoopHelpers.SimpleRandom((id + 1) * 8862213513L);
        }


        int oneStep(int j) {
            int n = key.length;
            int r = rng.next() & 0x7FFFFFFF;
            int jinc = (r & 7);
            j += jinc - 3;
            if (j >= n) j -= n;
            if (j < 0) j += n;

            int l = n / 4 + j;
            if (l >= n) l -= n;

            String k = key[j];
            String x = map.get(k);

            if (x == null) {
                ++nagets;
                if (r < pctins) {
                    map.put(k, key[l]);
                    ++nputs;
                    int csz = nputs - nremoves;
                    if (csz > maxsz) maxsz = csz;
                }
            }
            else {
                if (k== x) ++npgets;
                if (r < pctrem) {
                    map.remove(k);
                    ++nremoves;
                    j += ((r >>> 8) & 7) +  n / 2;
                    if (j >= n) j -= n;
                }
            }
            return j;
        }

        public void run() {
            int j = key.length / 2;
            for (int i = 0; i < NOPS; ++i) {
                j = oneStep(j);
            }
            total = nputs + npgets + nagets + nremoves;
        }
    }

}
