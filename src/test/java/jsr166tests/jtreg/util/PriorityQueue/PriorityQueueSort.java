// from gee.cs.oswego.edu/home/jsr166/jsr166

package jsr166tests.jtreg.util.PriorityQueue;

/*
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */

/*
 * @test
 * @bug 4486658
 * @summary Checks that a priority queue returns elements in sorted order across various operations
 */

import java.util.*;

public class PriorityQueueSort {

    static class MyComparator implements Comparator<Integer> {
        public int compare(Integer x, Integer y) {
            int i = x.intValue();
            int j = y.intValue();
            if (i < j) return -1;
            if (i > j) return 1;
            return 0;
        }
    }

    public static void main(String[] args) {
        int n = 10000;
        if (args.length > 0)
            n = Integer.parseInt(args[0]);

        List<Integer> sorted = new ArrayList<Integer>(n);
        for (int i = 0; i < n; i++)
            sorted.add(new Integer(i));
        List<Integer> shuffled = new ArrayList<Integer>(sorted);
        Collections.shuffle(shuffled);

        Queue<Integer> pq = new PriorityQueue<Integer>(n, new MyComparator());
        for (Iterator<Integer> i = shuffled.iterator(); i.hasNext(); )
            pq.add(i.next());

        List<Integer> recons = new ArrayList<Integer>();
        while (!pq.isEmpty())
            recons.add(pq.remove());
        if (!recons.equals(sorted))
            throw new RuntimeException("Sort test failed");

        recons.clear();
        pq = new PriorityQueue<Integer>(shuffled);
        while (!pq.isEmpty())
            recons.add(pq.remove());
        if (!recons.equals(sorted))
            throw new RuntimeException("Sort test failed");

        // Remove all odd elements from queue
        pq = new PriorityQueue<Integer>(shuffled);
        for (Iterator<Integer> i = pq.iterator(); i.hasNext(); )
            if ((i.next().intValue() & 1) == 1)
                i.remove();
        recons.clear();
        while (!pq.isEmpty())
            recons.add(pq.remove());

        for (Iterator<Integer> i = sorted.iterator(); i.hasNext(); )
            if ((i.next().intValue() & 1) == 1)
                i.remove();

        if (!recons.equals(sorted))
            throw new RuntimeException("Iterator remove test failed.");
    }
}
