/* SnapTree - (c) 2009 Stanford University - PPL */

// AllSnapHashMap
package jsr166tests;

public class AllSnapHashMap {
    public static void main(final String[] args) throws Throwable {
        jsr166tests.jtreg.util.Collection.IteratorAtEnd.main(new String[0]);
        jsr166tests.jtreg.util.Collections.RacingCollections.main(new String[0]);
        jsr166tests.jtreg.util.concurrent.ConcurrentMap.ConcurrentModification.main(new String[0]);
        jsr166tests.jtreg.util.concurrent.SnapHashMap.MapCheck.main(new String[0]);
        jsr166tests.jtreg.util.concurrent.SnapHashMap.MapLoops.main(new String[0]);
        jsr166tests.jtreg.util.concurrent.SnapHashMap.toArray.main(new String[0]);
        jsr166tests.jtreg.util.Hashtable.SelfRef.main(new String[0]);
        jsr166tests.jtreg.util.Map.Get.main(new String[0]);
        //jsr166tests.tck.JSR166TestCase.main(new String[0]);
        jsr166tests.tck.SnapHashMapTest.main(new String[0]);
    }
}