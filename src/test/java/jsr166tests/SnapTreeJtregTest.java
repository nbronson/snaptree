/* SnapTree - (c) 2009 Stanford University - PPL */

// SnapTreeJtregTest
package jsr166tests;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class SnapTreeJtregTest extends TestCase {
    public static void main(String[] args) {
        junit.textui.TestRunner.run (suite());
    }
    public static Test suite() {
        return new TestSuite(SnapTreeJtregTest.class);
    }


    public void testCollectionIteratorAtEnd() throws Throwable {
        jsr166tests.jtreg.util.Collection.IteratorAtEnd.main(new String[0]);
    }

    public void testCollectionMOAT() throws Throwable {
        jsr166tests.jtreg.util.Collection.MOAT.main(new String[0]);
    }

    public void testNavigableMapLockStep() throws Throwable {
        jsr166tests.jtreg.util.NavigableMap.LockStep.main(new String[0]);
    }

    public void testConcurrentMapConcurrentModification() throws Throwable {
        jsr166tests.jtreg.util.concurrent.ConcurrentMap.ConcurrentModification.main(new String[0]);
    }

    public void testCollectionsRacingCollections() throws Throwable {
        jsr166tests.jtreg.util.Collections.RacingCollections.main(new String[0]);
    }

    public void testMapGet() throws Throwable {
        jsr166tests.jtreg.util.Map.Get.main(new String[0]);
    }

    public void testMapLockStep() throws Throwable {
        jsr166tests.jtreg.util.Map.LockStep.main(new String[0]);
    }
}
