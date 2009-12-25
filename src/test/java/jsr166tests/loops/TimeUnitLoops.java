// from gee.cs.oswego.edu/home/jsr166/jsr166

package jsr166tests.loops;

/*
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */
import java.util.concurrent.*;
import java.util.Random;

public class TimeUnitLoops {

    static final LoopHelpers.SimpleRandom rng = new LoopHelpers.SimpleRandom();

    /**
     * False value allows aggressive inlining of cvt() calls from
     * test(). True value prevents any inlining, requiring virtual
     * method dispatch.
     */
    static final boolean PREVENT_INLINING = true;

    // The following all are used by inlining prevention clause:
    static int index = 0;
    static final int NUNITS = 100;
    static final TimeUnit[] units = new TimeUnit[NUNITS];
    static {
        TimeUnit[] v = TimeUnit.values();
        for (int i = 0; i < NUNITS; ++i)
            units[i] = v[rng.next() % v.length];
    }

    public static void main(String[] args) throws Exception {
        long start = System.currentTimeMillis();
        long s = 0;
        for (int i = 0; i < 100; ++i) {
            s += test();
            if (s == start) System.out.println(" ");
        }
        long end = System.currentTimeMillis();
        System.out.println("Time: " + (end - start) + " ms");
    }

    static long test() {
        long sum = 0;
        int x = rng.next();
        for (int i = 0; i < 1000000; ++i) {
            long l = (long) x + (long) x;
            sum += cvt(l, TimeUnit.SECONDS);
            sum += TimeUnit.MILLISECONDS.toMicros(l+2);
            sum += cvt(l+17, TimeUnit.NANOSECONDS);
            sum += cvt(l+42, TimeUnit.MILLISECONDS);
            x = LoopHelpers.compute4(x);
        }
        return sum + x;
    }

    static long cvt(long d, TimeUnit u) {
        if (PREVENT_INLINING) {
            u = units[index];
            index = (index+1) % NUNITS;
        }

        return u.toNanos(d);
    }
}
