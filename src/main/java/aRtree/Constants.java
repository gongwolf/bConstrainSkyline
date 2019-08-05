package aRtree;

/* Modified by Josephine Wong 23 Nov 1997
 Improve User Interface of the program
 */

public class Constants {

//        static final int BLOCKLENGTH = 1024;
//    static final int CACHESIZE = 128;
    public static final int BLOCKLENGTH = 256;
    public static final int CACHESIZE = 30;
    public static final int attrs_length = 3;
    public final static int RTDataNode__dimension = 2;
    public final static float MAXREAL = (float) 9.99e20;
    public final static int MAX_DIMENSION = 256;
    // for comparing mbrs
    public final static int OVERLAP = 0;
    public final static int INSIDE = 1;
    public final static int S_NONE = 2;
    // for the insert algorithm
    public final static int SPLIT = 0;
    public final static int REINSERT = 1;
    public final static int NONE = 2;
    // sorting criteria
    public final static int SORT_LOWER_MBR = 0; //for mbrs
    public final static int SORT_UPPER_MBR = 1; //for mbrs
    public final static int SORT_CENTER_MBR = 2; //for mbrs
    public final static int SORT_MINDIST = 3; //for branchlists
    public final static int BLK_SIZE = 4096;
    public final static int MAXLONGINT = 32768;
    public final static int NUM_TRIES = 10;
    /* These values are now set by the users - see UserInterface module.*/
    // for experimental rects
    static final int MAXCOORD = 100;
    static final int MAXWIDTH = 60;
    static final int NUMRECTS = 200;
    static final int DIMENSION = 2;
    // for queries
    static final int RANGEQUERY = 0;
    static final int POINTQUERY = 1;
    static final int CIRCLEQUERY = 2;
    static final int RINGQUERY = 3;
    static final int CONSTQUERY = 4;
    // for buffering
    static final int SIZEOF_BOOLEAN = 1;
    static final int SIZEOF_SHORT = 2;
    static final int SIZEOF_CHAR = 1;
    static final int SIZEOF_BYTE = 1;
    static final int SIZEOF_FLOAT = 4;
    static final int SIZEOF_INT = 4;
    // for header blocks
    public final static int BFHEAD_LENGTH = SIZEOF_INT * 2;

    // for comparisons
    public final static float min(float a, float b) {
        return (a < b) ? a : b;
    }

    public final static int min(int a, int b) {
        return (a < b) ? a : b;
    }

    public final static float max(float a, float b) {
        return (a > b) ? a : b;
    }

    public final static int max(int a, int b) {
        return (a > b) ? a : b;
    }

    // for errors
    public static void error(String msg, boolean fatal) {
        System.out.println(msg);
        if (fatal) {
            System.exit(1);
        }
    }

    // returns the d-dimension area of the mbr
    public static float area(int dimension, float mbr[]) {
        int i;
        float sum;

        sum = (float) 1.0;
        for (i = 0; i < dimension; i++) {
            sum *= mbr[2 * i + 1] - mbr[2 * i];
        }

        return sum;
    }

    // returns the margin of the mbr. That is the sum of all projections
    // to the axes
    public static float margin(int dimension, float mbr[]) {
        int i;
        int ml, mu, m_last;
        float sum;

        sum = (float) 0.0;
        m_last = 2 * dimension;
        ml = 0;
        mu = ml + 1;
        while (mu < m_last) {
            sum += mbr[mu] - mbr[ml];
            ml += 2;
            mu += 2;
        }

        return sum;
    }

    // ist ein Skalar in einem Intervall ?
    public static boolean inside(float p, float lb, float ub) {
        return (p >= lb && p <= ub);
    }

    // ist ein Vektor in einer Box ?
    public static boolean inside(float v[], float mbr[], int dimension) {
        int i;

        for (i = 0; i < dimension; i++) {
            if (!inside(v[i], mbr[2 * i], mbr[2 * i + 1])) {
                return false;
            }
        }
        return true;
    }

    // calcutales the overlapping area of r1 and r2
    // calculate overlap in every dimension and multiplicate the values
    public static float overlap(int dimension, float r1[], float r2[]) {
        float sum;
        int r1pos, r2pos, r1last;
        float r1_lb, r1_ub, r2_lb, r2_ub;

        sum = (float) 1.0;
        r1pos = 0;
        r2pos = 0;
        r1last = 2 * dimension;

        while (r1pos < r1last) {
            r1_lb = r1[r1pos++];
            r1_ub = r1[r1pos++];
            r2_lb = r2[r2pos++];
            r2_ub = r2[r2pos++];

            // calculate overlap in this dimension
            if (inside(r1_ub, r2_lb, r2_ub)) // upper bound of r1 is inside r2
            {
                if (inside(r1_lb, r2_lb, r2_ub)) // and lower bound of r1 is inside
                {
                    sum *= (r1_ub - r1_lb);
                } else {
                    sum *= (r1_ub - r2_lb);
                }
            } else {
                if (inside(r1_lb, r2_lb, r2_ub)) // and lower bound of r1 is inside
                {
                    sum *= (r2_ub - r1_lb);
                } else {
                    if (inside(r2_lb, r1_lb, r1_ub) && inside(r2_ub, r1_lb, r1_ub)) // r1 contains r2
                    {
                        sum *= (r2_ub - r2_lb);
                    } else // r1 and r2 do not overlap
                    {
                        sum = (float) 0.0;
                    }
                }
            }
        }
        return sum;
    }

    // enlarge r in a way that it contains s
    public static void enlarge(int dimension, float mbr[], float r1[], float r2[]) {
        int i;

        //mbr = new float[2*dimension];
        for (i = 0; i < 2 * dimension; i += 2) {
            mbr[i] = min(r1[i], r2[i]);
            mbr[i + 1] = max(r1[i + 1], r2[i + 1]);
        }

        /*System.out.println("Enlarge was called with parameters:");
         System.out.println("r1 = " + r1[0] + " " + r1[1] + " " + r1[2] + " " + r1[3]);
         System.out.println("r2 = " + r2[0] + " " + r2[1] + " " + r2[2] + " " + r2[3]);
         System.out.println("r1 = " + mbr[0] + " " + mbr[1] + " " + mbr[2] + " " + mbr[3]);
         */
        //#ifdef CHECK_MBR
        //   check_mbr(dimension,*mbr);
        //#endif
    }





    /**
     * returns true if the two mbrs intersect
     */
    public static boolean section(int dimension, float mbr1[], float mbr2[]) {
        int i;

        for (i = 0; i < dimension; i++) {
            if (mbr1[2 * i] > mbr2[2 * i + 1] || mbr1[2 * i + 1] < mbr2[2 * i]) {
//                System.out.println(mbr1[2 * i] +","+ mbr2[2 * i + 1] );
//                System.out.println(mbr1[2 * i + 1] +","+ mbr2[2 * i] );
                return false;
            }
        }
        return true;
    }


    /**
     * This is a generic version of C.A.R Hoare's Quick Sort algorithm. This
     * will handle arrays that are already sorted, and arrays with duplicate
     * keys.
     * <p>
     * If you think of a one dimensional array as going from the lowest index on
     * the left to the highest index on the right then the parameters to this
     * function are lowest index or left and highest index or right. The first
     * time you call this function it will be with the parameters 0, a.length -
     * 1.
     *
     * @param a   a Sortable array
     * @param lo0 left boundary of array partition
     * @param hi0 right boundary of array partition
     */
    public static void quickSort(Sortable a[], int lo0, int hi0, int sortCriterion) {
        int lo = lo0;
        int hi = hi0;
        Sortable mid;

        if (hi0 > lo0) {
            /* Arbitrarily establishing partition element as the midpoint of
             * the array.
             */
            mid = a[(lo0 + hi0) / 2];

            // loop through the array until indices cross
            while (lo <= hi) {
                /* find the first element that is greater than or equal to
                 * the partition element starting from the left Index.
                 */
                while ((lo < hi0) && (a[lo].lessThan(mid, sortCriterion))) {
                    ++lo;
                }

                /* find an element that is smaller than or equal to
                 * the partition element starting from the right Index.
                 */
                while ((hi > lo0) && (a[hi].greaterThan(mid, sortCriterion))) {
                    --hi;
                }

                // if the indexes have not crossed, swap
                if (lo <= hi) {
                    swap(a, lo, hi);
                    ++lo;
                    --hi;
                }
            }

            /* If the right index has not reached the left side of array
             * must now sort the left partition.
             */
            if (lo0 < hi) {
                quickSort(a, lo0, hi, sortCriterion);
            }

            /* If the left index has not reached the right side of array
             * must now sort the right partition.
             */
            if (lo < hi0) {
                quickSort(a, lo, hi0, sortCriterion);
            }
        }
    }

    //Swaps two entries in an array of objects to be sorted.
    //See Constants.quickSort()
    public static void swap(Sortable a[], int i, int j) {
        Sortable T;
        T = a[i];
        a[i] = a[j];
        a[j] = T;

    }

    public static void print(float[] costs) {
        System.out.print("[");
        for (double c : costs) {
            System.out.print(c + " ");
        }
        System.out.println("]");
    }


}
