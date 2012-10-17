package edu.stanford.ppl.concurrent;

import junit.framework.TestCase;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class ConcurrentEmptyTest extends TestCase {
    static final int outerPasses = 10000;
    static final int innerOps = 5000;
    static final int putPct = 50;
    static final int numThreads = 2;

    public void testConcurrentRemoves() {
        final SnapTreeMap<Integer, Integer> map = new SnapTreeMap<Integer, Integer>();
        final Random outerRand = new Random();

//        final int[] actualKeys = new int[2 * numThreads * innerOps];
//        final String[] actualOps = new String[2 * numThreads * innerOps];

        for (int outer = 0; outer < outerPasses; ++outer) {
//            final AtomicInteger pos = new AtomicInteger(actualKeys.length);
//            final AtomicInteger threadIds = new AtomicInteger(1);

            final int keyRange = 1 + outerRand.nextInt(100);

            ParUtil.parallel(numThreads, new Runnable() {
                final Random rand = new Random();

                public void run() {
//                    final int id = threadIds.getAndIncrement();
//                    final String beginPut = id + " begin put";
//                    final String endPut = id + " end put";
//                    final String beginRemove = id + " begin remove";
//                    final String endRemove = id + " end remove";
                    for (int inner = 0; inner < innerOps; ++inner) {
                        final int pct = rand.nextInt(100);
                        final int key = rand.nextInt(keyRange);
//                        final int before = pos.decrementAndGet();
                        if (pct < putPct) {
                            map.put(key, key);
                        } else {
                            map.remove(key);
                        }
//                        final int after = pos.decrementAndGet();
//                        actualKeys[before] = actualKeys[after] = key;
//                        actualOps[before] = pct < putPct ? beginPut : beginRemove;
//                        actualOps[after] = pct < putPct ? endPut : endRemove;
                    }
                }
            });

//            final SnapTreeMap<Integer, Integer> endMap = map.clone();
//            final int endSize = map.size();
//            final boolean endEmpty = map.isEmpty();
//
//            final boolean[] present = new boolean[keyRange];
            for (int i = 0; i < keyRange; ++i) {
                if (map.remove(i) != null) {
//                    present[i] = true;
                }
            }

            assertEquals(0, map.size());
//            if (!map.isEmpty()) {
//                System.out.println("break here!");
//            }
            assertTrue(map.isEmpty());
        }
    }
}
