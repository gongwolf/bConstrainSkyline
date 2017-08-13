package Pindex;

import Pindex.myNode;

import java.util.Comparator;
import java.util.PriorityQueue;

public class myNodePriorityQueue {
    PriorityQueue<myNode> queue;

    public myNodePriorityQueue() {
        mycomparator mc = new mycomparator();
        this.queue = new PriorityQueue<>(1000000, mc);
    }

    public boolean add(myNode p)
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

    public myNode pop()
    {
        return this.queue.poll();
    }

    public static void main(String args[])
    {
        myNodePriorityQueue myqueue = new myNodePriorityQueue();
        System.out.println(myqueue.size());

    }
}

class mycomparator implements Comparator<myNode> {
    public int compare(myNode x, myNode y) {
        if(x.EduDist == y.EduDist){
            return 0;
        }else if(x.EduDist > y.EduDist){
            return 1;
        }else{
            return -1;
        }
    }
}
