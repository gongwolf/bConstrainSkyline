package BaseLine;

import RstarTree.*;

import java.util.ArrayList;
import java.util.Random;

public class Skyline {
    public ArrayList<Data> skylineStaticNodes = new ArrayList<>();
    String treePath = "data/test.rtr";
    RTree rt;
    Random r;

    public Skyline() {
        rt = new RTree(treePath, Constants.CACHESIZE);
        r = new Random(System.nanoTime());
    }

    public Skyline(String treePath) {
        rt = new RTree(treePath, Constants.CACHESIZE);
        r = new Random(System.nanoTime());
    }

    public static void main(String args[]) {
        Skyline sky = new Skyline();
        Data queryD = sky.generateQueryData();
//        Data queryD = new Data(3);
//        queryD.setPlaceId(9999999);
//        float latitude = sky.randomFloatInRange(0f, 180f);
//        float longitude = sky.randomFloatInRange(0f, 180f);
//        queryD.setLocation(new float[]{latitude, longitude});
//        queryD.setData(new float[]{3.7136295f, 0.14032096f, 1.2783748f});
        System.out.println(queryD);
        long rt = System.currentTimeMillis();
        sky.BBS(queryD);
        System.out.println(System.currentTimeMillis() - rt);
//        for (Data d : sky.skylineStaticNodes) {
//            System.out.println(" " + d);
//        }

    }

    public void BBS(Data queryPoint) {
//        addToSkylineResult(queryPoint, queryPoint);
//        System.out.println("====" + this.skylineStaticNodes.size());

        myQueue queue = new myQueue(queryPoint);
        queue.add(rt.root_ptr);

        while (!queue.isEmpty()) {
            Object o = queue.pop();
            if (o.getClass() == RTDirNode.class) {
//                System.out.println(11111);
                RTDirNode dirN = (RTDirNode) o;
                int n = dirN.get_num();
//                System.out.println(n);
                for (int i = 0; i < n; i++) {
                    Object succ_o = dirN.entries[i].get_son();
//                    System.out.println(isDominatedByResult((Node) succ_o, queryPoint));
                    if (!isDominatedByResult((Node) succ_o, queryPoint)) {
                        queue.add(succ_o);
//                    } else {
//                        System.out.println("+++" + succ_o);
                    }
//                    queue.add(succ_o);
                }
            } else if (o.getClass() == RTDataNode.class) {
//                System.out.println(22222);
                RTDataNode dataN = (RTDataNode) o;
                int n = dataN.get_num();
//                System.out.println(n);
                for (int i = 0; i < n; i++) {
//                    System.out.println(checkDominated(queryPoint.getData(), dataN.data[i].getData()));
//                    constants.print(queryPoint.getData());
//                    constants.print(dataN.data[i].getData());
                    if (!checkDominated(queryPoint.getData(), dataN.data[i].getData())) {
                        dataN.data[i].distance_q = Math.pow(dataN.data[i].location[0] - queryPoint.location[0], 2) + Math.pow(dataN.data[i].location[1] - queryPoint.location[1], 2);
                        dataN.data[i].distance_q = Math.sqrt(dataN.data[i].distance_q);
                        this.skylineStaticNodes.add(dataN.data[i]);
                    }
                }
            }
        }

        queryPoint.distance_q = 0;
        this.skylineStaticNodes.add(queryPoint);

//        System.out.println("====" + this.skylineStaticNodes.size());

    }

    public Data generateQueryData() {
        Data d = new Data(3);
        d.setPlaceId(9999999);
        float latitude = randomFloatInRange(0f, 180f);
        float longitude = randomFloatInRange(0f, 180f);
        d.setLocation(new double[]{latitude, longitude});


        float priceLevel = randomFloatInRange(0f, 5f);
        float Rating = randomFloatInRange(0f, 5f);
        float other = randomFloatInRange(0f, 5f);
        d.setData(new float[]{priceLevel, Rating, other});
        return d;
    }

    public float randomFloatInRange(float min, float max) {
        float random = min + r.nextFloat() * (max - min);
        return random;
    }


    private boolean addToSkylineResult(Data d, Data queryPoint) {
        int i = 0;
        if (skylineStaticNodes.isEmpty()) {
            this.skylineStaticNodes.add(d);
        } else {
            boolean can_insert_np = true;
            for (; i < skylineStaticNodes.size(); ) {
                if (checkDominated(skylineStaticNodes.get(i).getData(), d.getData())) {
                    can_insert_np = false;
                    break;
                } else {
                    if (checkDominated(d.getData(), skylineStaticNodes.get(i).getData())) {
                        this.skylineStaticNodes.remove(i);
                    } else {
                        i++;
                    }
                }
            }

            if (can_insert_np) {
                this.skylineStaticNodes.add(d);
                return true;
            }
        }
        return false;
    }

    private boolean isDominatedByResult(Node node, Data queryD) {
        if (skylineStaticNodes.isEmpty()) {
            return false;
        } else {
            double[] q_points = queryD.getData();
//            System.out.println(queryD);
            float[] n_mbr = node.get_mbr();
            boolean flag = true;
            for (int j = 0; j < n_mbr.length; j += 2) {
                flag = flag & (n_mbr[j] > q_points[j / 2]);
                //if one dimension of the node is less than the point d
                //It means the point d can not fall into the left-bottom partition.
                //So the node can not be dominated by this d.
                if (!flag) {
                    break;
                }
            }
            //if one of the data d dominate the node
            if (flag) {
                return true;
            }
        }
        return false;
    }


    private boolean checkDominated(double[] costs, double[] estimatedCosts) {
        for (int i = 0; i < costs.length; i++) {
            if (costs[i] > estimatedCosts[i]) {
                return false;
            }
        }
        return true;
    }
}
