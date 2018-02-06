package Baseline2;

import aRtree.*;

public class BBS {
    String treePath = "data/ar.art";
    public aRTree ar;

    public BBS(String treePath, int case_size) {
        this.treePath = treePath;
        this.ar = new aRTree(this.treePath, 50);
    }

    public static void main(String args[]) {
        BBS b = new BBS("data/ar.art", 50);

        Data queryD = new Data(2);
        queryD.setPlaceId(9999999);
        queryD.setData(new float[]{20.380422592163086f, 9.294476509094238f});
        queryD.setAttrs(new float[]{4.3136826f, 0.45063168f, 3.711781f});

        b.bbs(queryD, 50f);
    }

    private void bbs(Data queryD, float distance_q) {
        System.out.println("==============");
        myQueue queue = new myQueue(queryD);
        queue.add(this.ar.root_ptr);

        int counter = 0;

        while (!queue.isEmpty()) {
            counter++;
            Object o = queue.pop();
            double dis_to_mbr = distance_to_queryPoint(queryD, o);
            System.out.println(o.getClass().getName() + " " + distance_to_queryPoint(queryD, o));
            if (o instanceof Node) {
                constants.print(((Node) o).getAttr_upper());
            } else {
                constants.print(((Data)o).attrs);
            }
            if (o.getClass() == aRTDirNode.class) {
                aRTDirNode dirN = (aRTDirNode) o;
                int n = dirN.get_num();
                for (int i = 0; i < n; i++) {
                    Object succ_o = dirN.entries[i].get_son();

                    if (distance_q <= distance_to_queryPoint(queryD, succ_o)) {
                        queue.add(succ_o);
                    }
                }

            } else if (o.getClass() == aRTDataNode.class) {
                aRTDataNode dataN = (aRTDataNode) o;
                int n = dataN.get_num();
                for (int i = 0; i < n; i++) {
                    Data succ_d = dataN.data[i];

                    if (distance_q <= getDistance_Point(succ_d.get_mbr(), queryD)) {
                        queue.add(succ_d);
                    }
                }

            } else if (o instanceof Data) {

            }
        }

        System.out.println(counter+" ==========");
    }

    public double distance_to_queryPoint(Data queryD, Object o) {
        double dis = 0.0;
        if (o.getClass() == Data.class) {
            Data dy = (Data) o;
            float[] y_mbr = dy.get_mbr();
            dis = getDistance_Point(y_mbr, queryD);
        } else if (o instanceof Node) {
            float[] y_mbr = ((Node) o).get_mbr();
            dis = getDistance_Node(y_mbr, queryD);
        }
        return dis;

    }

    private double getDistance_Node(float[] mbr, Data qD) {
        float sum = (float) 0.0;
        float r;
        int i;

        float points[] = new float[qD.dimension];
        for (int j = 0; j < qD.dimension; j++) {
            points[j] = qD.data[j * 2];

        }

        for (i = 0; i < qD.dimension; i++) {
            if (points[i] < mbr[2 * i]) {
                r = mbr[2 * i];
            } else {
                if (points[i] > mbr[2 * i + 1]) {
                    r = mbr[2 * i + 1];
                } else {
                    r = points[i];
                }
            }

            sum += Math.pow(points[i] - r, 2);
        }
        return Math.sqrt(sum);
    }

    private double getDistance_Point(float[] mbr, Data qD) {
        double dist = 0;
        for (int i = 0; i < 2 * qD.dimension; i += 2) {
            dist += Math.pow(qD.data[i] - mbr[i], 2);
        }
        return Math.sqrt(dist);
    }
}
