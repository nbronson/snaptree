// from gee.cs.oswego.edu/home/jsr166/jsr166

package jsr166tests.loops;

/*
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */

public class Finals {
    static int npairs = 2;
    static int iters = 10000000;
    static final int LEN = 4;
    static final Long[] nums = new Long[LEN];
    static volatile boolean done;
    static volatile long total;

    public static void main(String[] args) {
        for (int i = 0; i < LEN; ++i)
            nums[i] = new Long(i+1);
        Thread[] ps = new Thread[npairs];
        Thread[] as = new Reader[npairs];
        for (int i = 0; i < npairs; ++i) {
            ps[i] = new Writer();
            as[i] = new Reader();
        }
        for (int i = 0; i < as.length; ++i) {
            ps[i].start();
            as[i].start();
        }
    }

    static long nextRandom(long seed) {
        return (seed * 0x5DEECE66DL + 0xBL) & ((1L << 48) - 1);
    }

    static long initialSeed(Object x) {
        return (System.currentTimeMillis() + x.hashCode()) | 1;
    }

    static class Writer extends Thread {
        public void run() {
            long s = initialSeed(this);
            int n = iters;
            while (!done && n-- > 0) {
                int k = (int) (s & (LEN-1));
                int l = (k+1) & (LEN-1);
                nums[k] = new Long(s);
                nums[l] = new Long(s);
                s = nextRandom(s);
                if (s == 0) s = initialSeed(this);
            }
            done = true;
            total += s;
        }
    }

    static class Reader extends Thread {
        public void run() {
            int n = iters;
            long s = initialSeed(this);
            while (s != 0 && n > 0) {
                long nexts = nums[(int) (s & (LEN-1))].longValue();
                if (nexts != s)
                    --n;
                else if (done)
                    break;
                s = nexts;
            }
            done = true;
            total += s;
            if (s == 0)
                throw new Error("Saw uninitialized value");
        }
    }
}
