package twoHop;

import javafx.util.Pair;

import java.util.Comparator;
import java.util.PriorityQueue;

public class myPriorityQueue {
    PriorityQueue<Pair<Integer, Double>> queue;

    public myPriorityQueue() {
        myDcomparator mc = new myDcomparator();
        this.queue = new PriorityQueue<>(10000, mc);

    }

    public boolean add(Pair<Integer, Double> vertex) {
        return this.queue.add(vertex);
    }

    public int size() {
        return this.queue.size();
    }

    public boolean isEmpty() {
        return this.queue.isEmpty();
    }

    public Pair<Integer, Double> pop() {
        return this.queue.poll();
    }
}

class myDcomparator implements Comparator<Pair<Integer, Double>> {
    public int compare(Pair<Integer, Double> x, Pair<Integer, Double> y) {
        double xP = x.getValue();
        double yP = y.getValue();
        if (xP == yP) {
            return 0;
        } else if (xP > yP) {
            return 1;
        } else {
            return -1;
        }
    }
}

