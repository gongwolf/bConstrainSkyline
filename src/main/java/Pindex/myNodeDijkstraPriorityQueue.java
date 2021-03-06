package Pindex;

import Pindex.myNode;

import java.util.Comparator;
import java.util.PriorityQueue;

public class myNodeDijkstraPriorityQueue {
    PriorityQueue<myNode> queue;
    String propertiy_type;

    public myNodeDijkstraPriorityQueue(String propertiy_type) {
        myDcomparator mc = new myDcomparator(propertiy_type);
        this.queue = new PriorityQueue<>(1000000, mc);
        this.propertiy_type = propertiy_type;
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

}

class myDcomparator implements Comparator<myNode> {
    String propertiy_type;

    public myDcomparator(String propertiy_type) {
        super();
        this.propertiy_type = propertiy_type;

    }

    public int compare(myNode x, myNode y) {
        double xP = x.getCostFromSource(propertiy_type);
        double yP = y.getCostFromSource(propertiy_type);
        if (xP == yP) {
            return 0;
        } else if (xP > yP) {
            return 1;
        } else {
            return -1;
        }

    }


}
