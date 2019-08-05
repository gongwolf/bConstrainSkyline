package BaseLine;

import java.util.LinkedList;
import java.util.PriorityQueue;
import java.util.Queue;

public class myNodePriorityQueueNoCp {
    Queue<myNode> queue;

    public myNodePriorityQueueNoCp() {
        this.queue = new LinkedList<>();
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