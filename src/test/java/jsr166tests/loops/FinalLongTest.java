// from gee.cs.oswego.edu/home/jsr166/jsr166

package jsr166tests.loops;

/*
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */

public class FinalLongTest {
    static int npairs = 2;
    static int iters = 10000000;
    static  int LEN = 2;
    static final Long[] nums = new Long[LEN];
    static volatile boolean done;
    static volatile long total;
    static Long n0 = new Long(21);
    static Long n1 = new Long(22);
    static Long n2 = new Long(23);
    static Long n3 = new Long(23);


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
            int n = iters / 2;
            Long[] ns = nums;

            while (n-- > 0) {
                //                int k = (int) (s & (LEN-1));
                //                if (k < 0 || k >= LEN) k = 1;
                //                int l = (k+1) & (LEN-1);
                //                if (l < 0 || l >= LEN) l = 0;
                //                int k = (s & 1) == 0? 0 : 1;
                //                int l = (k == 0)? 1 : 0;
                if ((s & (LEN-1)) == 0) {
                    n3 = n1;
                    n0 = new Long(s);
                    n2 = n1;
                    n1 = new Long(s);
                }
                else {
                    n3 = n0;
                    n1 = new Long(s);
                    n2 = n0;
                    n0 = new Long(s);
                }
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
                long nexts; // = nums[(int) (s & (LEN-1))].longValue();
                if ((s & (LEN-1)) == 0)
                    nexts = n0.longValue();
                else
                    nexts = n1.longValue();
                if (nexts != 0) {
                    if ((s & 4) == 0)
                        nexts = n2.longValue();
                    else
                        nexts = n3.longValue();
                }

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
