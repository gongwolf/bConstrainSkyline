package Pindex;

import Pindex.myNode;

import java.util.Comparator;
import java.util.PriorityQueue;

public class myNodePriorityQueue {
    PriorityQueue<myNode> queue;

    public myNodePriorityQueue() {
        mycomparator mc = new mycomparator();
        this.queue = new PriorityQueue<>(mc);
    }

    public boolean add(myNode p) {
        return this.queue.add(p);
    }

    public int size() {
        return this.queue.size();
    }

    public boolean isEmpty() {
        return this.queue.isEmpty();
    }

    public myNode pop() {
        return this.queue.poll();
    }

    public static void main(String args[]) {
        myNodePriorityQueue myqueue = new myNodePriorityQueue();
        System.out.println(myqueue.size());

    }
}

class mycomparator implements Comparator<myNode> {
    public int compare(myNode x, myNode y) {
        if (x.EduDist == y.EduDist) {
            return 0;
        } else if (x.EduDist > y.EduDist) {
            return 1;
        } else {
            return -1;
        }
    }
}


class mycomparatorwithTotal implements Comparator<myNode> {
    public int compare(myNode x, myNode y) {
        double xp, yp;
        xp = yp = 0;


        for (double d : x.lowerBound) {
            xp += d;
        }

        for (double d : y.lowerBound) {
            yp += d;
        }

        if (xp == yp) {
            return 0;
        } else if (xp > yp) {
            return 1;
        } else {
            return -1;
        }
    }
}


class mycomparatorwithMin implements Comparator<myNode> {
    public int compare(myNode x, myNode y) {
        double xp, yp;
        xp = yp = 0;
        int n = x.lowerBound.length;

        double xm[] = new double[n];
        double ym[] = new double[n];


        for (path p : x.subRouteSkyline) {
            int xi = 0;
            for (double d : p.getCosts()) {
                if (d < xm[xi]) {
                    xm[xi] = d;
                }
                xi++;
            }
        }

        for (path p : y.subRouteSkyline) {
            int yi = 0;
            for (double d : p.getCosts()) {
                if (d < xm[yi]) {
                    xm[yi] = d;
                }
                yi++;
            }
        }

        for (double d : ym) {
            yp += d;
        }

        for (double d : xm) {
            xp += d;
        }

        if (xp == yp) {
            return 0;
        } else if (xp > yp) {
            return 1;
        } else {
            return -1;
        }
    }
}
