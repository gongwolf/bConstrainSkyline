package Pindex.inblockSkyline;

import Pindex.myNode;

import javax.sound.sampled.Port;
import java.util.PriorityQueue;

public class PortalPriorityQueue {
    PriorityQueue<Portal> queue;

    public PortalPriorityQueue() {
        this.queue = new PriorityQueue<>();
    }

    public boolean add(Portal p)
    {
        return this.queue.add(p);
    }

    public int size()
    {
        return this.queue.size();
    }

    public boolean isEmpty()
    {
        return this.queue.isEmpty();
    }

    public Portal pop()
    {
        return this.queue.poll();
    }

    public static void main(String args[])
    {
        PortalPriorityQueue myqueue = new PortalPriorityQueue();
        System.out.println(myqueue.size());

    }

}
