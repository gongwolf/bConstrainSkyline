package TestCase;

import java.util.Comparator;
import java.util.PriorityQueue;

public class queueTest {

    PriorityQueue<testObj> queue;

    public static void main(String args[]) {
        queueTest qt = new queueTest();
        qt.test();
    }

    public void test() {
        queue = new PriorityQueue<>(new NodeComparator());

        testObj t1 = new testObj(1);

        add(t1);

        System.out.println(queue.size());
        add(t1);
        System.out.println(queue.size());
        queue.add(t1);

        add(t1);
        add(t1);
        add(t1);
        add(t1);
        add(t1);
        add(t1);
        System.out.println(queue.size());
        testObj t2 = new testObj(2);
        add(t2);
        System.out.println(queue.size());

        System.out.println("    " + queue.poll().sb);
        System.out.println(queue.size());

        t1.sb.setLength(0);
        t1.sb.append(3);
        System.out.println("    " + queue.poll().sb);
        System.out.println(queue.size());
        System.out.println("    " + queue.poll().sb);
        System.out.println(queue.size());


    }

    private void add(testObj t1) {
        if (!this.queue.contains(t1)) {
            queue.add(t1);
        }
    }
}


class testObj {
    int id;
    StringBuffer sb = new StringBuffer();

    public testObj(int id) {
        this.id = id;
        sb.append(id);
    }
}

class NodeComparator implements Comparator<testObj> {
    @Override
    public int compare(testObj o1, testObj o2) {
        if (o1.id == o2.id) {
            return 0;
        } else if (o1.id > o2.id) {
            return 1;
        } else {
            return -1;
        }
    }
}