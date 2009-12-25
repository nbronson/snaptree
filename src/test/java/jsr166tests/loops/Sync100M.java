// from gee.cs.oswego.edu/home/jsr166/jsr166

package jsr166tests.loops;

/*
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */

class Sync100M {
    public static void main(String[] args) throws Exception {
        int x = loop((int) System.nanoTime(), 100000);
        x = loop(x, 100000);
        x = loop(x, 100000);
        long start = System.nanoTime();
        x = loop(x, 100000000);
        if (x == 0) System.out.print(" ");
        long time = System.nanoTime() - start;
        double secs = (double) time / 1000000000.0;
        System.out.println("time: " + secs);
        start = System.nanoTime();
        x = loop(x, 100000000);
        if (x == 0) System.out.print(" ");
        time = System.nanoTime() - start;
        secs = (double) time / 1000000000.0;
        System.out.println("time: " + secs);

    }

    static final Object obj = new Object();

    static int loop(int x, int iters) {
        for (int i = iters; i > 0; --i) {
            synchronized(obj) {
                x = x * 134775813 + 1;
            }
        }
        return x;
    }

}
