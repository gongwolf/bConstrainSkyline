package GPSkyline;


import GraphPartition.block;

import java.util.LinkedList;
import java.util.PriorityQueue;
import java.util.Queue;

public class BlockQueue {
    Queue<block> queue;

    public BlockQueue() {
//            mycomparator mc = new mycomparator();
        this.queue = new LinkedList<>();
    }

    public boolean add(block p) {
        return this.queue.add(p);
    }

    public int size() {
        return this.queue.size();
    }

    public boolean isEmpty() {
        return this.queue.isEmpty();
    }

    public block pop() {
        return this.queue.poll();
    }

    public static void main(String args[]) {
        BlockQueue myqueue = new BlockQueue();
        System.out.println(myqueue.size());
    }
}
