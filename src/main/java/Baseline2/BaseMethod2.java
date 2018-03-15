package Baseline2;

import aRtree.Data;
import aRtree.aRTDataNode;
import aRtree.aRTDirNode;
import aRtree.aRTree;
import neo4jTools.connector;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.Transaction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class BaseMethod2 {

    public aRTree ar;
    Random r;
    String treePath = "data/ar.art";
    int graph_size;
    String degree;
    private GraphDatabaseService graphdb;
    private HashMap<Long, myNode> tmpStoreNodes = new HashMap();
    private ArrayList<Result> skyPaths = new ArrayList<>();
    private long bbs_counter;
    private long access_node;
    private long check_skyline_counter;
    private long result_cast_time;
    private long final_calculatuin;
    private long Dominated_checking_boolean;
    private long dt_result;
    private Result i_r;
    private ArrayList<path> qqqq = new ArrayList<>();


    public BaseMethod2(int graph_size, String degree, String treePath, int case_size) {
        r = new Random();
        this.graph_size = graph_size;
        this.degree = degree;
        this.ar = new aRTree(treePath, case_size);
    }

    public BaseMethod2(int graph_size, String degree) {
        r = new Random();
        this.graph_size = graph_size;
        this.degree = degree;
        this.ar = new aRTree(treePath, 1000);

    }


    public static void main(String args[]) {
        int graph_size = 1000;
        String degree = "5";
        int query_num = 1;

        if (args.length == 3) {
            graph_size = Integer.parseInt(args[0]);
            degree = args[1];
            query_num = Integer.parseInt(args[2]);
        }

        for (int i = 0; i < query_num; i++) {
            BaseMethod2 bm = new BaseMethod2(graph_size, degree);
//            Data queryD = bm.generateQueryData();
////
//            System.out.println(queryD);

            Data queryD = new Data(2);
            queryD.setPlaceId(9999999);
            queryD.setData(new float[]{20.380422592163086f, 9.294476509094238f});
            queryD.setAttrs(new float[]{4.3136826f, 0.45063168f, 3.711781f});
//            constants.print(queryD.location);
//            constants.print(queryD.getData());

            long r1 = System.currentTimeMillis();
            bm.baseline(queryD);
            System.out.println("running time -----" + (System.currentTimeMillis() - r1));
        }
    }

    private void baseline(Data queryD) {

        long expasionTime = 0;
        long resultTime = 0;
        long while_loop_rt = 0;

        String graphPath = "/home/gqxwolf/neo4j323/testdb" + this.graph_size + "_" + this.degree + "/databases/graph.db";
        connector n = new connector(graphPath);
        n.startDB();
        this.graphdb = n.getDBObject();


        long total_rt = System.currentTimeMillis();
        try (Transaction tx = this.graphdb.beginTx()) {

            long r1 = System.currentTimeMillis();
            Node startNode = nearestNetworkNode(queryD);
            long nn_rt = System.currentTimeMillis() - r1;
            System.out.println("Find nearest road network " + nn_rt + " ms");
            System.out.println(startNode.getProperty("lat") + " " + startNode.getProperty("log"));


            r1 = System.currentTimeMillis();
            double[] c = new double[constants.path_dimension + 3];
            for (int i = 4; i < c.length; i++) {
                c[i] = queryD.getAttrs()[i - 4];
            }

            this.i_r = new Result(queryD, queryD, c, null);
//            System.out.println(r);
            addToSkyline(i_r);

            bbs(null, queryD);
            System.out.println("Initialization " + (System.currentTimeMillis() - r1));


            System.out.println("==============================   :   " + this.skyPaths.size());
//            for (Result eqweq : this.skyPaths) {
//                System.out.println(eqweq);
//            }
//            n.shutdownDB();
//            System.exit(0);


            myNode s = new myNode(queryD, startNode, this.graphdb);

            myNodePriorityQueue mqueue = new myNodePriorityQueue();
            mqueue.add(s);

            this.tmpStoreNodes.put(s.id, s);

            int counter = 0;


            while_loop_rt = System.nanoTime();
            while (!mqueue.isEmpty()) {
//                System.out.println("===============");
                myNode v = mqueue.pop();
//                System.out.println(v.id);
                counter++;

                for (int i = 0; i < v.skyPaths.size(); i++) {
                    path p = v.skyPaths.get(i);
//                    System.out.println(p);
                    if (!p.expaned) {
                        p.expaned = true;
                        r1 = System.nanoTime();
                        addToSkylineResult(p, queryD);
                        resultTime += (System.nanoTime() - r1);

                        r1 = System.nanoTime();
                        ArrayList<path> new_paths = p.expand();
                        expasionTime += (System.nanoTime() - r1);

                        for (path np : new_paths) {
                            myNode next_n;
                            if (this.tmpStoreNodes.containsKey(np.endNode.getId())) {
                                next_n = tmpStoreNodes.get(np.endNode.getId());
                            } else {
                                next_n = new myNode(queryD, np.endNode, this.graphdb);
                                this.tmpStoreNodes.put(next_n.id, next_n);
                            }

                            //lemma 2
                            if ((this.tmpStoreNodes.get(np.startNode.getId()).distance_q < next_n.distance_q)) {
                                if (next_n.addToSkyline(np)) {
                                    mqueue.add(next_n);
                                }
                            }
                        }
                    }

                }

//                if (counter == 2) {
//                    break;
//                }

            }


//
//
            tx.success();
        }

        total_rt = System.currentTimeMillis() - total_rt;
        while_loop_rt = System.nanoTime() - while_loop_rt;

        System.out.println(skyPaths.size() + "   " + total_rt + " " + (while_loop_rt / 1000000) + " " + (resultTime / 1000000) + " " + (expasionTime / 1000000));

        long tt_sl = 0;
        for (Map.Entry<Long, myNode> entry : tmpStoreNodes.entrySet()) {
            tt_sl += entry.getValue().skyPaths.size();
            for (path p : entry.getValue().skyPaths) {
                addToSkyline_p(p);
            }
        }

        System.out.println(qqqq.size() + "   " + tt_sl);

        System.out.println(tt_sl + "/" + bbs_counter + "=" + (double) tt_sl / bbs_counter);
        System.out.println(resultTime + "/" + bbs_counter + "=" + (double) resultTime / bbs_counter / 1000000);
        System.out.println(this.bbs_counter + " " + this.access_node + " " + this.check_skyline_counter);
        System.out.println((double) check_skyline_counter / access_node);
        System.out.println(resultTime / 1000000);
        long aa = this.result_cast_time + this.final_calculatuin + this.Dominated_checking_boolean + dt_result;
        System.out.print(aa / 1000000 + "=");
        System.out.print(this.result_cast_time / 1000000);
        System.out.print("+" + this.final_calculatuin / 1000000);
        System.out.print("+" + this.Dominated_checking_boolean / 1000000);
        System.out.print("+" + this.dt_result / 1000000);
        System.out.println();

//        for (Result eqweq : this.skyPaths) {
//            System.out.println(eqweq);
//        }
        n.shutdownDB();

    }

    private boolean addToSkylineResult(path np, Data queryD) {
        if (np.rels.isEmpty()) {
//            System.out.println(np + " is a signle node");
            return false;
        }

        return bbs(np, queryD);
    }

    private boolean bbs(path np, Data queryD) {
//        if (np != null)
//            constants.print(np.costs);
        this.bbs_counter++;

        myQueue queue = new myQueue(queryD);
        queue.add(this.ar.root_ptr);


        while (!queue.isEmpty()) {
            Object o = queue.pop();
            this.access_node++;
//            System.out.println(o.getClass());
            if (o.getClass() == aRTDirNode.class) {
                long rc = System.nanoTime();
                aRTDirNode dirN = (aRTDirNode) o;
//                System.out.println(o);
                this.result_cast_time += System.nanoTime() - rc;

                int n = dirN.get_num();
                for (int i = 0; i < n; i++) {
                    double distance_q, distance_end, f_dist = 0;

                    long rfc = System.nanoTime();
                    Object succ_o = dirN.entries[i].get_son();
                    double[] tempCosts = new double[constants.path_dimension + 3];
                    distance_q = distance_to_queryPoint(queryD, succ_o);//distance of the query point to mbr
                    boolean e_is_inside = false;
                    boolean s_is_inside = false;
                    boolean q_is_inside = is_inside(queryD, succ_o);

//                    if (np == null || q_is_inside) {
                        queue.add(succ_o);
//                        continue;
//                    }

//                    if (np != null) {
//                        e_is_inside = is_inside(np.endNode, succ_o);
//                        s_is_inside = is_inside(np.startNode, succ_o);
////                        if (!e_is_inside & !s_is_inside) {
////                            distance_end = distance_to_end_Neo4j_Node(succ_o, np.endNode);//distance from end node of the path to the mbr
////                            System.arraycopy(np.costs, 1, tempCosts, 1, constants.path_dimension - 1);
////                            f_dist = distance_end + np.costs[0];
////                        } else if (s_is_inside && !e_is_inside) {
////                            distance_end = distance_to_end_Neo4j_Node(succ_o, np.endNode);//distance from end node of the path to the mbr
////                            System.arraycopy(np.costs, 1, tempCosts, 1, constants.path_dimension - 1);
////                            f_dist = distance_end;
////                        } else if (!s_is_inside && e_is_inside) {
////                            System.arraycopy(np.costs, 0, tempCosts, 0, constants.path_dimension);
////                            f_dist = np.costs[0];
////                        } else if (s_is_inside && e_is_inside) {
////                            System.arraycopy(np.costs, 0, tempCosts, 0, constants.path_dimension);
////                            f_dist = distance_q;
////                        }
//
//                        if (e_is_inside) {
//                            f_dist += 0;
//                        } else {
//                            f_dist += distance_to_end_Neo4j_Node(succ_o, np.endNode);
//                        }
//
//                        if (s_is_inside) {
//                            f_dist += 0;
//                        } else {
//                            f_dist += distance_to_end_Neo4j_Node(succ_o, np.startNode);
//                        }
//
//                        f_dist += distance_q;
//                    } else {
//                        f_dist = distance_q;
//                    }
//
//                    for (int j = 4; j < constants.path_dimension + 3; j++) {
//                        tempCosts[j] = ((aRtree.Node) succ_o).getAttr_lower()[j - 4];
//                    }
//
//                    tempCosts[0] = f_dist;
//                    this.final_calculatuin += System.nanoTime() - rfc;


//                    rfc = System.nanoTime();
//                    boolean flag = isDominatedByQueryPoint(tempCosts);
//                    boolean flag1 = isDominatedByResult(tempCosts);
//                    this.Dominated_checking_boolean += System.nanoTime() - rfc;

//                    if (!flag1) {
//                    queue.add(succ_o);
//                    }
                }

            } else if (o.getClass() == aRTDataNode.class) {
                double distance_q, distance_end, f_dist = 0;

                long rc = System.nanoTime();
                aRTDataNode dataN = (aRTDataNode) o;
                this.result_cast_time += System.nanoTime() - rc;

                int n = dataN.get_num();
                for (int i = 0; i < n; i++) {
                    long rfc = System.nanoTime();
                    Data succ_d = dataN.data[i];

                    distance_q = distance_to_queryPoint(queryD, succ_d); //distance of the query point to mbr

                    if (np != null) {
                        distance_end = distance_to_end_Neo4j_Node(succ_d, np.endNode);
                        f_dist = distance_end + np.costs[0];
                    } else {
                        f_dist = 0.0;
                    }

                    this.final_calculatuin += System.nanoTime() - rfc;

                    if (np == null) {
                        rfc = System.nanoTime();
                        double[] tempCosts = new double[constants.path_dimension + 3];
                        for (int j = 4; j < constants.path_dimension + 3; j++) {
                            tempCosts[j] = succ_d.attrs[j - 4];
                        }
                        tempCosts[0] = distance_q;
//                        System.out.println(distance_q);
                        Result tmpR = new Result(queryD, succ_d, tempCosts, null);
                        this.final_calculatuin += System.nanoTime() - rfc;
                        this.check_skyline_counter++;

                        long rdt = System.nanoTime();
                        boolean f = addToSkyline(tmpR);
                        this.dt_result += System.nanoTime() - rdt;


                    } else if (distance_q > f_dist) {
                        rfc = System.nanoTime();
                        double[] tempCosts = new double[constants.path_dimension + 3];
                        System.arraycopy(np.costs, 1, tempCosts, 1, np.costs.length - 1);
                        for (int j = 4; j < constants.path_dimension + 3; j++) {
                            tempCosts[j] = succ_d.attrs[j - 4];
                        }
                        tempCosts[0] = f_dist;
                        Result tmpR = new Result(queryD, succ_d, tempCosts, np);
                        this.final_calculatuin += System.nanoTime() - rfc;
                        this.check_skyline_counter++;
                        long rdt = System.nanoTime();
                        addToSkyline(tmpR);
                        this.dt_result += System.nanoTime() - rdt;
                    }
                }
            }

        }
        return true;
    }

    private boolean isDominatedByQueryPoint(double[] tmpR_costs) {
        return checkDominated(this.i_r.costs, tmpR_costs);
    }


    private boolean isDominatedByResult(double[] tempCosts) {
        for (Result r : this.skyPaths) {
            if (checkDominated(r.costs, tempCosts)) {
                return true;
            }
        }
        return false;
    }

    private double distance_to_end_Neo4j_Node(Object o, Node endNode) {
        double dis = 0.0;
        if (o.getClass() == Data.class) {
            Data dy = (Data) o;
            float[] y_mbr = dy.get_mbr();
            dis = getDistance_Point_neo4j(y_mbr, endNode);
        } else if (o instanceof aRtree.Node) {
            float[] y_mbr = ((aRtree.Node) o).get_mbr();
            dis = getDistance_Node_neo4j(y_mbr, endNode);
        }
        return dis;

    }


    public Node nearestNetworkNode(Data queryD) {
        Node nn_node = null;
        double distz = Float.MAX_VALUE;

        ResourceIterable<Node> iter = this.graphdb.getAllNodes();
        for (Node n : iter) {
            double lat = (double) n.getProperty("lat");
            double log = (double) n.getProperty("log");

            float[] q_l = queryD.getData();

            double temp_distz = (Math.pow(lat - q_l[0], 2) + Math.pow(log - q_l[1], 2));
            if (distz > temp_distz) {
                nn_node = n;
                distz = temp_distz;

            }
        }
        return nn_node;
    }

    public double distance_to_queryPoint(Data queryD, Object o) {
        double dis = 0.0;
        if (o.getClass() == Data.class) {
            Data dy = (Data) o;
            float[] y_mbr = dy.get_mbr();
            dis = getDistance_Point(y_mbr, queryD);
        } else if (o instanceof aRtree.Node) {
            float[] y_mbr = ((aRtree.Node) o).get_mbr();
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

    private double getDistance_Node_neo4j(float[] mbr, Node endnode) {
        float sum = (float) 0.0;
        double r;
        int i;

        double points[] = new double[2];

        points[0] = (double) endnode.getProperty("lat");
        points[1] = (double) endnode.getProperty("log");

        for (i = 0; i < points.length; i++) {
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

    private double getDistance_Point_neo4j(float[] mbr, Node endnode) {
        double dist = 0;

        double points[] = new double[2];
        points[0] = (double) endnode.getProperty("lat");
        points[1] = (double) endnode.getProperty("log");

        for (int i = 0; i < 2 * points.length; i += 2) {
            dist += Math.pow(points[i / 2] - mbr[i], 2);
        }
        return Math.sqrt(dist);
    }


    public boolean addToSkyline(Result r) {
        int i = 0;
        if (skyPaths.isEmpty()) {
            this.skyPaths.add(r);
        } else {
            boolean can_insert_np = true;
            for (; i < skyPaths.size(); ) {
                if (checkDominated(skyPaths.get(i).costs, r.costs)) {
                    can_insert_np = false;
                    break;
                } else {
                    if (checkDominated(r.costs, skyPaths.get(i).costs)) {
                        this.skyPaths.remove(i);
                    } else {
                        i++;
                    }
                }
            }

            if (can_insert_np) {
                this.skyPaths.add(r);
                return true;
            }
        }
        return false;
    }

    private boolean checkDominated(double[] costs, double[] estimatedCosts) {
        for (int i = 0; i < costs.length; i++) {
            if (costs[i] * (1.0) > estimatedCosts[i]) {
                return false;
            }
        }
        return true;
    }

    public boolean addToSkyline_p(path np) {
        int i = 0;
        if (this.qqqq.isEmpty()) {
            this.qqqq.add(np);
        } else {
            boolean can_insert_np = true;
            for (; i < qqqq.size(); ) {
                if (checkDominated_p(qqqq.get(i).costs, np.costs)) {
                    can_insert_np = false;
                    break;
                } else {
                    if (checkDominated_p(np.costs, qqqq.get(i).costs)) {

                        this.qqqq.remove(i);
                    } else {
                        i++;
                    }
                }
            }

            if (can_insert_np) {
                this.qqqq.add(np);
                return true;
            }
        }
        return false;
    }

    private boolean checkDominated_p(double[] costs, double[] estimatedCosts) {
        for (int i = 0; i < costs.length; i++) {
            if (costs[i] * (1.0) > estimatedCosts[i]) {
                return false;
            }
        }
        return true;
    }

    public boolean is_inside(Node endnode, Object o) {
        if (o instanceof aRtree.Node) {
            float[] mbr = ((aRtree.Node) o).get_mbr();

            double[] points = new double[2];
            points[0] = (double) endnode.getProperty("lat");
            points[1] = (double) endnode.getProperty("log");

            for (int i = 0; i < points.length; i++) {
                if (points[i] < mbr[2 * i] || points[i] > mbr[2 * i + 1]) {
                    return false;
                }
            }


        } else {
            System.out.println("not a non-leaf node");
            System.exit(1);
        }
        return true;

    }


    public boolean is_inside(Data quryD, Object o) {
        if (o instanceof aRtree.Node) {
            float[] mbr = ((aRtree.Node) o).get_mbr();

            for (int i = 0; i < 2; i++) {
                if (quryD.data[i * 2] < mbr[2 * i] || quryD.data[i * 2] > mbr[2 * i + 1]) {
                    return false;
                }
            }


        } else {
            System.out.println("not a non-leaf node");
            System.exit(1);
        }
        return true;

    }
}
