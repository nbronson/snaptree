// from gee.cs.oswego.edu/home/jsr166/jsr166

package jsr166tests.loops;

/*
 * Written by Doug Lea and Bill Scherer with assistance from members
 * of JCP JSR-166 Expert Group and released to the public domain, as
 * explained at http://creativecommons.org/licenses/publicdomain
 */

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;

/**
 * A parallel Traveling Salesperson Problem (TSP) program based on a
 * genetic algorithm using an Exchanger.  A population of chromosomes is
 * distributed among "subpops".  Each chromosomes represents a tour,
 * and its fitness is the total tour length.
 *
 * A set of worker threads perform updates on subpops. The basic
 * update step is:
 * <ol>
 *   <li> Select a breeder b from the subpop
 *   <li> Create a strand of its tour with a random starting point and length
 *   <li> Offer the strand to the exchanger, receiving a strand from
 *        another subpop
 *   <li> Combine b and the received strand using crossing function to
 *        create new chromosome c.
 *   <li> Replace a chromosome in the subpop with c.
 * </ol>
 *
 * This continues for a given number of generations per subpop.
 * Because there are normally more subpops than threads, each worker
 * thread performs small (randomly sized) run of updates for one
 * subpop and then selects another. A run continues until there is at
 * most one remaining thread performing updates.
 *
 * See below for more details.
 */
public class TSPExchangerTest {
    static final int NCPUS = Runtime.getRuntime().availableProcessors();

    /** Runs start with two threads, increasing by two through max */
    static final int DEFAULT_MAX_THREADS  = Math.max(4, NCPUS + NCPUS/2);

    /** The number of replication runs per thread value */
    static final int DEFAULT_REPLICATIONS = 3;

    /** If true, print statistics in SNAPSHOT_RATE intervals */
    static boolean verbose = true;
    static final long SNAPSHOT_RATE = 10000; // in milliseconds

    /**
     * The problem size. Each city is a random point. The goal is to
     * find a tour among them with smallest total Euclidean distance.
     */
    static final int DEFAULT_CITIES = 144;

    // Tuning parameters.

    /**
     * The number of chromosomes per subpop. Must be a power of two.
     *
     * Smaller values lead to faster iterations but poorer quality
     * results
     */
    static final int DEFAULT_SUBPOP_SIZE = 32;

    /**
     * The number of iterations per subpop. Convergence appears
     * to be roughly proportional to #cities-squared
     */
    static final int DEFAULT_GENERATIONS = DEFAULT_CITIES * DEFAULT_CITIES;

    /**
     * The number of subpops. The total population is #subpops * subpopSize,
     * which should be roughly on the order of #cities-squared
     *
     * Smaller values lead to faster total runs but poorer quality
     * results
     */
    static final int DEFAULT_NSUBPOPS = DEFAULT_GENERATIONS / DEFAULT_SUBPOP_SIZE;

    /**
     * The minimum length for a random chromosome strand.
     * Must be at least 1.
     */
    static final int MIN_STRAND_LENGTH = 3;

    /**
     * The probability mask value for creating random strands,
     * that have lengths at least MIN_STRAND_LENGTH, and grow
     * with exponential decay 2^(-(1/(RANDOM_STRAND_MASK + 1)
     * Must be 1 less than a power of two.
     */
    static final int RANDOM_STRAND_MASK = 7;

    /**
     * Probability control for selecting breeders.
     * Breeders are selected starting at the best-fitness chromosome,
     * with exponentially decaying probability
     * 1 / (subpopSize >>> BREEDER_DECAY).
     *
     * Larger values usually cause faster convergence but poorer
     * quality results
     */
    static final int BREEDER_DECAY = 1;

    /**
     * Probability control for selecting dyers.
     * Dyers are selected starting at the worst-fitness chromosome,
     * with exponentially decaying probability
     * 1 / (subpopSize >>> DYER_DECAY)
     *
     * Larger values usually cause faster convergence but poorer
     * quality results
     */
    static final int DYER_DECAY = 1;

    /**
     * The set of cities. Created once per program run, to
     * make it easier to compare solutions across different runs.
     */
    static CitySet cities;

    public static void main(String[] args) throws Exception {
        int maxThreads = DEFAULT_MAX_THREADS;
        int nCities = DEFAULT_CITIES;
        int subpopSize = DEFAULT_SUBPOP_SIZE;
        int nGen = nCities * nCities;
        int nSubpops = nCities * nCities / subpopSize;
        int nReps = DEFAULT_REPLICATIONS;

        try {
            int argc = 0;
            while (argc < args.length) {
                String option = args[argc++];
                if (option.equals("-c")) {
                    nCities = Integer.parseInt(args[argc]);
                    nGen = nCities * nCities;
                    nSubpops = nCities * nCities / subpopSize;
                }
                else if (option.equals("-p"))
                    subpopSize = Integer.parseInt(args[argc]);
                else if (option.equals("-g"))
                    nGen = Integer.parseInt(args[argc]);
                else if (option.equals("-n"))
                    nSubpops = Integer.parseInt(args[argc]);
                else if (option.equals("-q")) {
                    verbose = false;
                    argc--;
                }
                else if (option.equals("-r"))
                    nReps = Integer.parseInt(args[argc]);
                else
                    maxThreads = Integer.parseInt(option);
                argc++;
            }
        }
        catch (Exception e) {
            reportUsageErrorAndDie();
        }

        System.out.print("TSPExchangerTest");
        System.out.print(" -c " + nCities);
        System.out.print(" -g " + nGen);
        System.out.print(" -p " + subpopSize);
        System.out.print(" -n " + nSubpops);
        System.out.print(" -r " + nReps);
        System.out.print(" max threads " + maxThreads);
        System.out.println();

        cities = new CitySet(nCities);

        if (false && NCPUS > 4) {
            int h = NCPUS/2;
            System.out.printf("Threads: %4d Warmup\n", h);
            oneRun(h, nSubpops, subpopSize, nGen);
            Thread.sleep(500);
        }

        int maxt = (maxThreads < nSubpops) ? maxThreads : nSubpops;
        for (int j = 0; j < nReps; ++j) {
            for (int i = 2; i <= maxt; i += 2) {
                System.out.printf("Threads: %4d Replication: %2d\n", i, j);
                oneRun(i, nSubpops, subpopSize, nGen);
                Thread.sleep(500);
            }
        }
    }

    static void reportUsageErrorAndDie() {
        System.out.print("usage: TSPExchangerTest");
        System.out.print(" [-c #cities]");
        System.out.print(" [-p #subpopSize]");
        System.out.print(" [-g #generations]");
        System.out.print(" [-n #subpops]");
        System.out.print(" [-r #replications]");
        System.out.print(" [-q <quiet>]");
        System.out.print(" #threads]");
        System.out.println();
        System.exit(0);
    }

    /**
     * Perform one run with the given parameters.  Each run complete
     * when there are fewer than 2 active threads.  When there is
     * only one remaining thread, it will have no one to exchange
     * with, so it is terminated (via interrupt).
     */
    static void oneRun(int nThreads, int nSubpops, int subpopSize, int nGen)
        throws InterruptedException {
        Population p = new Population(nThreads, nSubpops, subpopSize, nGen);
        ProgressMonitor mon = null;
        if (verbose) {
            p.printSnapshot(0);
            mon = new ProgressMonitor(p);
            mon.start();
        }
        long startTime = System.nanoTime();
        p.start();
        p.awaitDone();
        long stopTime = System.nanoTime();
        if (mon != null)
            mon.interrupt();
        p.shutdown();
        //        Thread.sleep(100);

        long elapsed = stopTime - startTime;
        double secs = (double) elapsed / 1000000000.0;
        p.printSnapshot(secs);
    }


    /**
     * A Population creates the subpops, subpops, and threads for a run
     * and has control methods to start, stop, and report progress.
     */
    static final class Population {
        final Worker[] threads;
        final Subpop[] subpops;
        final Exchanger<Strand> exchanger;
        final CountDownLatch done;
        final int nGen;
        final int subpopSize;
        final int nThreads;

        Population(int nThreads, int nSubpops, int subpopSize, int nGen) {
            this.nThreads = nThreads;
            this.nGen = nGen;
            this.subpopSize = subpopSize;
            this.exchanger = new Exchanger<Strand>();
            this.done = new CountDownLatch(nThreads - 1);

            this.subpops = new Subpop[nSubpops];
            for (int i = 0; i < nSubpops; i++)
                subpops[i] = new Subpop(this);

            this.threads = new Worker[nThreads];
            int maxExchanges = nGen * nSubpops / nThreads;
            for (int i = 0; i < nThreads; ++i) {
                threads[i] = new Worker(this, maxExchanges);
            }

        }

        void start() {
            for (int i = 0; i < nThreads; ++i) {
                threads[i].start();
            }
        }

        /** Stop the tasks */
        void shutdown() {
            for (int i = 0; i < threads.length; ++ i)
                threads[i].interrupt();
        }

        void threadDone() {
            done.countDown();
        }

        /** Wait for tasks to complete */
        void awaitDone() throws InterruptedException {
            done.await();
        }

        int totalExchanges() {
            int xs = 0;
            for (int i = 0; i < threads.length; ++i)
                xs += threads[i].exchanges;
            return xs;
        }

        /**
         * Prints statistics, including best and worst tour lengths
         * for points scaled in [0,1), scaled by the square root of
         * number of points. This simplifies checking results.  The
         * expected optimal TSP for random points is believed to be
         * around 0.76 * sqrt(N). For papers discussing this, see
         * http://www.densis.fee.unicamp.br/~moscato/TSPBIB_home.html
         */
        void printSnapshot(double secs) {
            int xs = totalExchanges();
            long rate = (xs == 0) ? 0L : (long) ((secs * 1000000000.0) / xs);
            Chromosome bestc = subpops[0].chromosomes[0];
            Chromosome worstc = bestc;
            for (int k = 0; k < subpops.length; ++k) {
                Chromosome[] cs = subpops[k].chromosomes;
                if (cs[0].fitness < bestc.fitness)
                    bestc = cs[0];
                int w = cs[cs.length-1].fitness;
                if (cs[cs.length-1].fitness > worstc.fitness)
                    worstc = cs[cs.length-1];
            }
            double sqrtn = Math.sqrt(cities.length);
            double best = bestc.unitTourLength() / sqrtn;
            double worst = worstc.unitTourLength() / sqrtn;
            System.out.printf("N:%4d T:%8.3f B:%6.3f W:%6.3f X:%9d R:%7d\n",
                              nThreads, secs, best, worst, xs, rate);
            //            exchanger.printStats();
            //            System.out.print(" s: " + exchanger.aveSpins());
            //            System.out.print(" p: " + exchanger.aveParks());
        }
    }

    /**
     * Worker threads perform updates on subpops.
     */
    static final class Worker extends Thread {
        final Population pop;
        final int maxExchanges;
        int exchanges;
        final RNG rng = new RNG();

        Worker(Population pop, int maxExchanges) {
            this.pop = pop;
            this.maxExchanges = maxExchanges;
        }

        /**
         * Repeatedly, find a subpop that is not being updated by
         * another thread, and run a random number of updates on it.
         */
        public void run() {
            try {
                int len = pop.subpops.length;
                int pos = (rng.next() & 0x7FFFFFFF) % len;
                while (exchanges < maxExchanges) {
                    Subpop s = pop.subpops[pos];
                    AtomicBoolean busy = s.busy;
                    if (!busy.get() && busy.compareAndSet(false, true)) {
                        exchanges += s.runUpdates();
                        busy.set(false);
                        pos = (rng.next() & 0x7FFFFFFF) % len;
                    }
                    else if (++pos >= len)
                        pos = 0;
                }
                pop.threadDone();
            } catch (InterruptedException fallthrough) {
            }
        }
    }

    /**
     * A Subpop maintains a set of chromosomes..
     */
    static final class Subpop {
        /** The chromosomes, kept in sorted order */
        final Chromosome[] chromosomes;
        /** The parent population */
        final Population pop;
        /** Reservation bit for worker threads */
        final AtomicBoolean busy;
        /** The common exchanger, same for all subpops */
        final Exchanger<Strand> exchanger;
        /** The current strand being exchanged */
        Strand strand;
        /** Bitset used in cross */
        final int[] inTour;
        final RNG rng;
        final int subpopSize;

        Subpop(Population pop) {
            this.pop = pop;
            this.subpopSize = pop.subpopSize;
            this.exchanger = pop.exchanger;
            this.busy = new AtomicBoolean(false);
            this.rng = new RNG();
            int length = cities.length;
            this.strand = new Strand(length);
            this.inTour = new int[(length >>> 5) + 1];
            this.chromosomes = new Chromosome[subpopSize];
            for (int j = 0; j < subpopSize; ++j)
                chromosomes[j] = new Chromosome(length, rng);
            Arrays.sort(chromosomes);
        }

        /**
         * Run a random number of updates.  The number of updates is
         * at least 1 and no more than subpopSize.  This
         * controls the granularity of multiplexing subpop updates on
         * to threads. It is small enough to balance out updates
         * across tasks, but large enough to avoid having runs
         * dominated by subpop selection. It is randomized to avoid
         * long runs where pairs of subpops exchange only with each
         * other.  It is hardwired because small variations of it
         * don't matter much.
         *
         * @param g the first generation to run.
         */
        int runUpdates() throws InterruptedException {
            int n = 1 + (rng.next() & ((subpopSize << 1) - 1));
            for (int i = 0; i < n; ++i)
                update();
            return n;
        }

        /**
         * Choose a breeder, exchange strand with another subpop, and
         * cross them to create new chromosome to replace a chosen
         * dyer.
         */
        void update() throws InterruptedException {
            int b = chooseBreeder();
            int d = chooseDyer(b);
            Chromosome breeder = chromosomes[b];
            Chromosome child = chromosomes[d];
            chooseStrand(breeder);
            strand = exchanger.exchange(strand);
            cross(breeder, child);
            fixOrder(child, d);
        }

        /**
         * Choose a breeder, with exponentially decreasing probability
         * starting at best.
         * @return index of selected breeder
         */
        int chooseBreeder() {
            int mask = (subpopSize >>> BREEDER_DECAY) - 1;
            int b = 0;
            while ((rng.next() & mask) != mask) {
                if (++b >= subpopSize)
                    b = 0;
            }
            return b;
        }

        /**
         * Choose a chromosome that will be replaced, with
         * exponentially decreasing probability starting at
         * worst, ignoring the excluded index
         * @param exclude index to ignore; use -1 to not exclude any
         * @return index of selected dyer
         */
        int chooseDyer(int exclude) {
            int mask = (subpopSize >>> DYER_DECAY)  - 1;
            int d = subpopSize - 1;
            while (d == exclude || (rng.next() & mask) != mask) {
                if (--d < 0)
                    d = subpopSize - 1;
            }
            return d;
        }

        /**
         * Select a random strand of b's.
         * @param breeder the breeder
         */
        void chooseStrand(Chromosome breeder) {
            int[] bs = breeder.alleles;
            int length = bs.length;
            int strandLength = MIN_STRAND_LENGTH;
            while (strandLength < length &&
                   (rng.next() & RANDOM_STRAND_MASK) != RANDOM_STRAND_MASK)
                strandLength++;
            strand.strandLength = strandLength;
            int[] ss = strand.alleles;
            int k = (rng.next() & 0x7FFFFFFF) % length;
            for (int i = 0; i < strandLength; ++i) {
                ss[i] = bs[k];
                if (++k >= length) k = 0;
            }
        }

        /**
         * Copy current strand to start of c's, and then append all
         * remaining b's that aren't in the strand.
         * @param breeder the breeder
         * @param child the child
         */
        void cross(Chromosome breeder, Chromosome child) {
            for (int k = 0; k < inTour.length; ++k) // clear bitset
                inTour[k] = 0;

            // Copy current strand to c
            int[] cs = child.alleles;
            int ssize = strand.strandLength;
            int[] ss = strand.alleles;
            int i;
            for (i = 0; i < ssize; ++i) {
                int x = ss[i];
                cs[i] = x;
                inTour[x >>> 5] |= 1 << (x & 31); // record in bit set
            }

            // Find index of matching origin in b
            int first = cs[0];
            int j = 0;
            int[] bs = breeder.alleles;
            while (bs[j] != first)
                ++j;

            // Append remaining b's that aren't already in tour
            while (i < cs.length) {
                if (++j >= bs.length) j = 0;
                int x = bs[j];
                if ((inTour[x >>> 5] & (1 << (x & 31))) == 0)
                    cs[i++] = x;
            }

        }

        /**
         * Fix the sort order of a changed Chromosome c at position k
         * @param c the chromosome
         * @param k the index
         */
        void fixOrder(Chromosome c, int k) {
            Chromosome[] cs = chromosomes;
            int oldFitness = c.fitness;
            c.recalcFitness();
            int newFitness = c.fitness;
            if (newFitness < oldFitness) {
                int j = k;
                int p = j - 1;
                while (p >= 0 && cs[p].fitness > newFitness) {
                    cs[j] = cs[p];
                    j = p--;
                }
                cs[j] = c;
            } else if (newFitness > oldFitness) {
                int j = k;
                int n = j + 1;
                while (n < cs.length && cs[n].fitness < newFitness) {
                    cs[j] = cs[n];
                    j = n++;
                }
                cs[j] = c;
            }
        }
    }

    /**
     * A Chromosome is a candidate TSP tour.
     */
    static final class Chromosome implements Comparable {
        /** Index of cities in tour order */
        final int[] alleles;
        /** Total tour length */
        int fitness;

        /**
         * Initialize to random tour
         */
        Chromosome(int length, RNG random) {
            alleles = new int[length];
            for (int i = 0; i < length; i++)
                alleles[i] = i;
            for (int i = length - 1; i > 0; i--) {
                int idx = (random.next() & 0x7FFFFFFF) % alleles.length;
                int tmp = alleles[i];
                alleles[i] = alleles[idx];
                alleles[idx] = tmp;
            }
            recalcFitness();
        }

        public int compareTo(Object x) { // to enable sorting
            int xf = ((Chromosome) x).fitness;
            int f = fitness;
            return ((f == xf) ? 0 :((f < xf) ? -1 : 1));
        }

        void recalcFitness() {
            int[] a = alleles;
            int len = a.length;
            int p = a[0];
            long f = cities.distanceBetween(a[len-1], p);
            for (int i = 1; i < len; i++) {
                int n = a[i];
                f += cities.distanceBetween(p, n);
                p = n;
            }
            fitness = (int) (f / len);
        }

        /**
         * Return tour length for points scaled in [0, 1).
         */
        double unitTourLength() {
            int[] a = alleles;
            int len = a.length;
            int p = a[0];
            double f = cities.unitDistanceBetween(a[len-1], p);
            for (int i = 1; i < len; i++) {
                int n = a[i];
                f += cities.unitDistanceBetween(p, n);
                p = n;
            }
            return f;
        }

        /**
         * Check that this tour visits each city
         */
        void validate() {
            int len = alleles.length;
            boolean[] used = new boolean[len];
            for (int i = 0; i < len; ++i)
                used[alleles[i]] = true;
            for (int i = 0; i < len; ++i)
                if (!used[i])
                    throw new Error("Bad tour");
        }

    }

    /**
     * A Strand is a random sub-sequence of a Chromosome.  Each subpop
     * creates only one strand, and then trades it with others,
     * refilling it on each iteration.
     */
    static final class Strand {
        final int[] alleles;
        int strandLength;
        Strand(int length) { alleles = new int[length]; }
    }

    /**
     * A collection of (x,y) points that represent cities.
     */
    static final class CitySet {

        final int length;
        final int[] xPts;
        final int[] yPts;
        final int[][] distances;

        CitySet(int n) {
            this.length = n;
            this.xPts = new int[n];
            this.yPts = new int[n];
            this.distances = new int[n][n];

            RNG random = new RNG();
            for (int i = 0; i < n; i++) {
                xPts[i] = (random.next() & 0x7FFFFFFF);
                yPts[i] = (random.next() & 0x7FFFFFFF);
            }

            for (int i = 0; i < n; i++) {
                for (int j = 0; j < n; j++) {
                    double dx = (double) xPts[i] - (double) xPts[j];
                    double dy = (double) yPts[i] - (double) yPts[j];
                    double dd = Math.hypot(dx, dy) / 2.0;
                    long ld = Math.round(dd);
                    distances[i][j] = (ld >= Integer.MAX_VALUE) ?
                        Integer.MAX_VALUE : (int) ld;
                }
            }
        }

        /**
         *  Returns the cached distance between a pair of cities
         */
        int distanceBetween(int i, int j) {
            return distances[i][j];
        }

        // Scale ints to doubles in [0,1)
        static final double PSCALE = (double) 0x80000000L;

        /**
         * Return distance for points scaled in [0,1). This simplifies
         * checking results.  The expected optimal TSP for random
         * points is believed to be around 0.76 * sqrt(N). For papers
         * discussing this, see
         * http://www.densis.fee.unicamp.br/~moscato/TSPBIB_home.html
         */
        double unitDistanceBetween(int i, int j) {
            double dx = ((double) xPts[i] - (double) xPts[j]) / PSCALE;
            double dy = ((double) yPts[i] - (double) yPts[j]) / PSCALE;
            return Math.hypot(dx, dy);
        }

    }

    /**
     * Cheap XorShift random number generator
     */
    static final class RNG {
        /** Seed generator for XorShift RNGs */
        static final Random seedGenerator = new Random();

        int seed;
        RNG(int seed) { this.seed = seed; }
        RNG()         { this.seed = seedGenerator.nextInt() | 1;  }

        int next() {
            int x = seed;
            x ^= x << 6;
            x ^= x >>> 21;
            x ^= x << 7;
            seed = x;
            return x;
        }
    }

    static final class ProgressMonitor extends Thread {
        final Population pop;
        ProgressMonitor(Population p) { pop = p; }
        public void run() {
            double time = 0;
            try {
                while (!Thread.interrupted()) {
                    sleep(SNAPSHOT_RATE);
                    time += SNAPSHOT_RATE;
                    pop.printSnapshot(time / 1000.0);
                }
            } catch (InterruptedException ie) {}
        }
    }
}
