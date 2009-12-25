// from gee.cs.oswego.edu/home/jsr166/jsr166

package jsr166tests.loops;

/*
 * Written by Josh Bloch of Google Inc. and released to the public domain,
 * as explained at http://creativecommons.org/licenses/publicdomain.
 */

import java.util.*;
import java.io.*;

/**
 * Interface-based Deque tester.  This test currently makes three
 * assumptions about the implementation under test:
 *
 *   1) It has no size limitation.
 *   2) It implements Serializable.
 *   3) It has a conventional (Collection) constructor.
 *
 * All of these assumptions could be relaxed.
 */
public class DequeBash {
    static int seed = 7;
    static int nextTail =  0;
    static int nextHead = -1;
    static int size() { return nextTail - nextHead - 1; }


    static int random(int bound) {
        int x = seed;
        int t = (x % 127773) * 16807 - (x / 127773) * 2836;
        seed = (t > 0) ? t : t + 0x7fffffff;
        return (t & 0x7fffffff) % bound;
    }

    static int random() {
        int x = seed;
        int t = (x % 127773) * 16807 - (x / 127773) * 2836;
        seed = (t > 0) ? t : t + 0x7fffffff;
        return (t & 0x7fffffff);
    }

    public static void main(String args[]) throws Exception {
        Class cls = Class.forName(args[0]);
        int n = 1000000;

        for (int j = 0; j < 3; ++j) {
            Deque<Integer> deque = (Deque<Integer>) cls.newInstance();
            nextTail =  0;
            nextHead = -1;
            long start = System.currentTimeMillis();
            mainTest(deque, n);
            long end = System.currentTimeMillis();
            System.out.println("Time: " + (end - start) + "ms");
            if (deque.isEmpty()) System.out.print(" ");
        }

    }

    static void mainTest(Deque<Integer> deque, int n) throws Exception {
        /*
         * Perform a random sequence of operations, keeping contiguous
         * sequence of integers on the deque.
         */
        for (int i = 0; i < n; i++) {
            sizeTests(deque);
            randomOp(deque);

            // Test iterator occasionally
            if ((i & 1023) == 0) {
                testIter(deque);
                testDescendingIter(deque);
            }

            // Test serialization and copying
            if ((i & 4095) == 0) {
                testEqual(deque, deepCopy(deque));
                testEqual(deque, (Deque<Integer>) deque.getClass().
                          getConstructor(Collection.class).newInstance(deque));
            }

            // Test fancy removal stuff once in a blue moon
            if ((i & 8191) == 0)
                testRemove(deque);

         }

        // Stupid tests for clear, toString
        deque.clear();
        testEmpty(deque);
        Collection<Integer> c = Arrays.asList(1, 2, 3);
        deque.addAll(c);
        if (!deque.toString().equals("[1, 2, 3]"))
            throw new Exception("Deque.toString():  " + deque.toString());
    }

    static void testIter(Deque<Integer> deque) throws Exception {
        int next = nextHead + 1;
        int count = 0;
        for (int j : deque) {
            if (j != next++)
                throw new Exception("Element "+ j + " != " + (next-1));
            count++;
        }
        if (count != size())
            throw new Exception("Count " + count + " != " + size());
    }

    static void testDescendingIter(Deque<Integer> deque) throws Exception {
        int next = deque.size() + nextHead;
        int count = 0;
        for (Iterator<Integer> it = deque.descendingIterator(); it.hasNext();) {
            int j = it.next();
            if (j != next--)
                throw new Exception("Element "+ j + " != " + (next-1));
            count++;
        }
        if (count != size())
            throw new Exception("Count " + count + " != " + size());
    }

    static void sizeTests(Deque<Integer> deque) throws Exception {
        if (deque.size() != size())
            throw new Exception("Size: " + deque.size() +
                                ", expecting " + size());
        if (deque.isEmpty() != (size() == 0))
            throw new Exception(
                                "IsEmpty " + deque.isEmpty() + ", size " + size());
        // Check head and tail
        if (size() == 0)
            testEmpty(deque);
        else
            nonEmptyTest(deque);

    }

    static void nonEmptyTest(Deque<Integer> deque) throws Exception {
        if (deque.getFirst() != nextHead + 1)
            throw new Exception("getFirst got: " +
                                deque.getFirst() + " expecting " + (nextHead + 1));
        if (deque.element() != nextHead + 1)
            throw new Exception("element got: " + deque.element() +
                                " expecting " + (nextHead + 1));
        if (deque.peekFirst() != nextHead + 1)
            throw new Exception("peekFirst got: "+deque.peekFirst() +
                                " expecting " + (nextHead + 1));
        if (deque.peek() != nextHead + 1)
            throw new Exception("peek got: " +  deque.peek() +
                                " expecting " + (nextHead + 1));

        if (deque.peekLast() != nextTail - 1)
            throw new Exception("peekLast got: " + deque.peekLast() +
                                " expecting " + (nextTail - 1));
        if (deque.getLast() != nextTail - 1)
            throw new Exception("getLast got: " +
                                deque.getLast() + " expecting " + (nextTail - 1));
    }


    static void randomOp(Deque<Integer> deque) throws Exception {

        // Perform a random operation
        switch (random() & 3) {
        case 0:
            switch (random() & 3) {
            case 0: deque.addLast(nextTail++);   break;
            case 1: deque.offerLast(nextTail++); break;
            case 2: deque.offer(nextTail++);     break;
            case 3: deque.add(nextTail++);       break;
            default: throw new Exception("How'd we get here");
            }
            break;
        case 1:
            if (size() == 0) {
                int result = 666;
                boolean threw = false;
                try {
                    switch (random(3)) {
                    case 0: result = deque.removeFirst(); break;
                    case 1: result = deque.remove();      break;
                    case 2: result = deque.pop();         break;
                    default: throw new Exception("How'd we get here");
                    }
                } catch (NoSuchElementException e) {
                    threw = true;
                }
                if (!threw)
                    throw new Exception("Remove-no exception: " + result);
            } else { // deque nonempty
                int result = -1;
                switch (random(5)) {
                case 0: result = deque.removeFirst(); break;
                case 1: result = deque.remove();      break;
                case 2: result = deque.pop();         break;
                case 3: result = deque.pollFirst();   break;
                case 4: result = deque.poll();        break;
                default: throw new Exception("How'd we get here");
                }
                if (result != ++nextHead)
                    throw new Exception(
                                        "Removed "+ result + " expecting "+(nextHead - 1));
            }
            break;
        case 2:
            switch (random(3)) {
            case 0: deque.addFirst(nextHead--);   break;
            case 1: deque.offerFirst(nextHead--); break;
            case 2: deque.push(nextHead--);       break;
            default: throw new Exception("How'd we get here");
            }
            break;
        case 3:
            if (size() == 0) {
                int result = -1;
                boolean threw = false;
                try {
                    result = deque.removeLast();
                } catch (NoSuchElementException e) {
                    threw = true;
                }
                if (!threw)
                    throw new Exception("Remove-no exception: " + result);
            } else { // deque nonempty
                int result = ((random() & 1) == 0 ?
                              deque.removeLast() : deque.pollLast());
                if (result != --nextTail)
                    throw new Exception(
                        "Removed "+ result + " expecting "+(nextTail + 1));
            }
            break;
        default:
            throw new Exception("How'd we get here");
        }
    }


    private static void testEqual(Deque<Integer> d1, Deque<Integer> d2)
        throws Exception
    {
        if (d1.size() != d2.size())
            throw new Exception("Size " + d1.size() + " != " + d2.size());
        Iterator<Integer> it = d2.iterator();
        for (int i : d1) {
            int j = it.next();
            if (j != i)
                throw new Exception("Element " + j + " != " + i);
        }

        for (int i : d1)
            if (!d2.contains(i))
                throw new Exception("d2 doesn't contain " + i);
        for (int i : d2)
            if (!d1.contains(i))
                throw new Exception("d2 doesn't contain " + i);

        if (d1.contains(Integer.MIN_VALUE))
            throw new Exception("d2 contains Integer.MIN_VALUE");
        if (d2.contains(Integer.MIN_VALUE))
            throw new Exception("d2 contains Integer.MIN_VALUE");
        if (d1.contains(null))
            throw new Exception("d1 contains null");
        if (d2.contains(null))
            throw new Exception("d2 contains null");

        if (!d1.containsAll(d2))
            throw new Exception("d1 doesn't contain all of d2");
        if (!d2.containsAll(d1))
            throw new Exception("d2 doesn't contain all of d1");
        Collection<Integer> c = Collections.singleton(Integer.MIN_VALUE);
        if (d1.containsAll(c))
            throw new Exception("d1 contains all of {Integer.MIN_VALUE }");
        if (d2.containsAll(c))
            throw new Exception("d2 contains all of {Integer.MIN_VALUE }");
    }

    private static <T> T deepCopy(T o) {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(o);
            oos.flush();
            ByteArrayInputStream bin = new ByteArrayInputStream(
                bos.toByteArray());
            ObjectInputStream ois = new ObjectInputStream(bin);
            return (T) ois.readObject();
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
    }

    private static void testRemove(Deque<Integer> deque) throws Exception {
        Deque<Integer> copy = null;
        switch (random() & 1) {
          case 0:
            copy = (Deque<Integer>) deque.getClass().
                getConstructor(Collection.class).newInstance(deque);
            break;
          case 1:
            copy = deepCopy(deque);
            break;
          default:
            throw new Exception("How'd we get here");
        }

        int numRemoved = 0;
        for (Iterator<Integer> it = copy.iterator(); it.hasNext(); ) {
            if ((it.next() & 1) == 0) {
                it.remove();
                numRemoved++;
            }
        }

        if (copy.size() + numRemoved != size())
            throw new Exception((copy.size() + numRemoved) + " != " + size());
        for (int i : copy)
            if ((i & 1) == 0)
                throw new Exception("Even number still present: " + i);

        List<Integer> elements = Arrays.asList(copy.toArray(new Integer[0]));
        Collections.shuffle(elements);
        for (int e : elements) {
            if (!copy.contains(e))
                throw new Exception(e + " missing.");

            boolean removed = false;
            switch (random(3)) {
                case 0:  removed = copy.remove(e);                break;
                case 1:  removed = copy.removeFirstOccurrence(e); break;
                case 2:  removed = copy.removeLastOccurrence(e);  break;
                default: throw new Exception("How'd we get here");
            }
            if (!removed)
                throw new Exception(e + " not removed.");

            if (copy.contains(e))
                throw new Exception(e + " present after removal.");
        }

        testEmpty(copy);

        copy = (Deque<Integer>) deque.getClass().
            getConstructor(Collection.class).newInstance(deque);
        copy.retainAll(deque);
        testEqual(deque, copy);
        copy.removeAll(deque);
        testEmpty(copy);
    }

    static boolean checkedThrows;

    private static void testEmpty(Deque<Integer> deque) throws Exception {
        if (!deque.isEmpty())
            throw new Exception("Deque isn't empty");
        if (deque.size() != 0)
            throw new Exception("Deque size isn't zero");
        if (!(deque.pollFirst() == null))
            throw new Exception("pollFirst lies");
        if (!(deque.poll() == null))
            throw new Exception("poll lies");
        if (!(deque.peekFirst() == null))
            throw new Exception("peekFirst lies");
        if (!(deque.peek() == null))
            throw new Exception("peek lies");
        if (!(deque.pollLast() == null))
            throw new Exception("pollLast lies");
        if (!(deque.peekLast() == null))
            throw new Exception("peekLast lies");

        if (!checkedThrows) {
            checkedThrows = true;
            boolean threw = false;
            int result = 666;
            try {
                result = ((random() & 1) == 0 ?
                          deque.getFirst() : deque.element());
            } catch (NoSuchElementException e) {
                threw = true;
            }
            if (!threw)
                throw new Exception("getFirst-no exception: "+result);
            threw = false;
            try {
                result = deque.getLast();
            } catch (NoSuchElementException e) {
                threw = true;
            }
            if (!threw)
                throw new Exception("getLast-no exception: "+result);
        }

    }
}
