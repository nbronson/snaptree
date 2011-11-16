/* SnapTree - (c) 2009 Stanford University - PPL */

// SnapTreeTest
package edu.stanford.ppl.concurrent;

import java.util.Iterator;
import junit.framework.TestCase;

public class SnapTreeTest extends TestCase {

    public void testInnerObjectBug() {
        SnapTreeMap<Integer, Integer> map = new SnapTreeMap<Integer, Integer>();
        SnapTreeMap<Integer, Integer> map2;
        map2 = map.clone();
        map2.put(1, 1);

        Iterator<Integer> iter = map2.values().iterator();
        while (iter.hasNext())
        {
            iter.next();
            iter.remove();
        }

        assert map2.size() == 0;
    }
}
