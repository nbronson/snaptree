/* CCSTM - (c) 2009 Stanford University - PPL */

// AllSnapTreeMap
package jsr166tests;

public class AllSnapTreeMap {
    public static void main(final String[] args) throws Throwable {
        jsr166tests.jtreg.util.Collection.IteratorAtEnd.main(new String[0]);
        jsr166tests.jtreg.util.Collection.MOAT.main(new String[0]);
        jsr166tests.jtreg.util.NavigableMap.LockStep.main(new String[0]);
        jsr166tests.jtreg.util.concurrent.ConcurrentMap.ConcurrentModification.main(new String[0]);
        jsr166tests.jtreg.util.Collections.RacingCollections.main(new String[0]);
        jsr166tests.jtreg.util.Map.Get.main(new String[0]);
        jsr166tests.jtreg.util.Map.LockStep.main(new String[0]);
        //jsr166tests.tck.JSR166TestCase.main(new String[0]);
        jsr166tests.tck.SnapTreeSubMapTest.main(new String[0]);
        jsr166tests.tck.SnapTreeMapTest.main(new String[0]);
    }
}
