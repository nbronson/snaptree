// from gee.cs.oswego.edu/home/jsr166/jsr166

package jsr166tests.jtreg.util.Random;

/*
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */

/*
 * @test
 * @bug 4949279
 * @summary Independent instantiations of Random() have distinct seeds.
 */

import java.util.Random;

public class DistinctSeeds {
    public static void main(String[] args) throws Exception {
        // Strictly speaking, it is possible for these to randomly fail,
        // but the probability should be *extremely* small (< 2**-63).
        if (new Random().nextLong() == new Random().nextLong() ||
            new Random().nextLong() == new Random().nextLong())
            throw new RuntimeException("Random() seeds not unique.");
    }
}
