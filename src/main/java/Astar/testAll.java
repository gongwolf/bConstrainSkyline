package Astar;

import BaseLine.*;
import BaseLine.approximate.range.BaseMethod_approx;
import BaseLine.approximate.range.BaseMethod_approx_index;
import RstarTree.Data;
import neo4jTools.connector;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.Transaction;

import java.util.*;

public class testAll {
    private final LandMarkIndex lmi;
    private final double distance_threshold;
    public ArrayList<Result> skyPaths = new ArrayList<>();
    public GraphDatabaseService graphdb;
    double range;
    int hotels_num;
    Random r;
    int graph_size;
    String degree;
    String graphPath;
    String treePath;
    String dataPath;
    String home_folder = System.getProperty("user.home");
    private ArrayList<Data> sky_hotel;
    private ArrayList<Data> sNodes = new ArrayList<>();
    private HashMap<Integer, ArrayList<Integer>> nearbyList;
    private HashSet<Data> finalDatas = new HashSet<>();
    private double nn_dist;
    private HashMap<Long, myNode> tmpStoreNodes = new HashMap();
    private Data queryD;


    public testAll(int graph_size, String degree, double range, int hotels_num) {
        this.range = range;
        this.hotels_num = hotels_num;
        r = new Random(System.nanoTime());
        this.graph_size = graph_size;
        this.degree = degree;
        this.graphPath = home_folder + "/neo4j334/testdb" + this.graph_size + "_" + this.degree + "/databases/graph.db";
        this.treePath = home_folder + "/shared_git/bConstrainSkyline/data/test_" + this.graph_size + "_" + this.degree + "_" + range + "_" + hotels_num + ".rtr";
        this.dataPath = home_folder + "/shared_git/bConstrainSkyline/data/staticNode_" + this.graph_size + "_" + this.degree + "_" + range + "_" + hotels_num + ".txt";
        this.distance_threshold = range;
        lmi = new LandMarkIndex(graph_size, degree);
        System.out.println(this.distance_threshold);
    }

    public static void main(String args[]) {
        for (int i = 0; i < 1; i++) {

            BaseMethod5 bm5 = new BaseMethod5(1000, "4", 14, 1000);
            int random_place_id = bm5.getRandomNumberInRange_int(0, bm5.getNumberOfHotels() - 1);
            Data queryD = bm5.getDataById(random_place_id);

            testAll tsa = new testAll(1000, "4", 14, 1000);
            tsa.baseline(queryD);

            BaseMethod_approx bs_approx = new BaseMethod_approx(1000, "4", 14, 14, 1000);
            bs_approx.baseline(queryD);

            BaseMethod_approx_index bs_approx_idx = new BaseMethod_approx_index(1000, "4", 14, 14, 1000);
            bs_approx_idx.baseline(queryD);
            System.out.println("=================================");
        }
    }

    public void baseline(Data queryD) {

        this.tmpStoreNodes.clear();

        StringBuffer sb = new StringBuffer();
        sb.append(queryD.getPlaceId() + " ");
        long s_sum = System.currentTimeMillis();
        ArrayList<path> Results = new ArrayList<>();
        Skyline sky = new Skyline(treePath);
        long r1 = System.currentTimeMillis();
        sky.BBS(queryD);
        sNodes = sky.skylineStaticNodes;

        long sk_counter = 0;

        long bbs_rt = System.currentTimeMillis() - r1;


        for (Data d : sNodes) {
            double[] c = new double[constants.path_dimension + 3];
            c[0] = d.distance_q;

            if (c[0] <= this.distance_threshold) {
                double[] d_attrs = d.getData();
                for (int i = 4; i < c.length; i++) {
                    c[i] = d_attrs[i - 4];
                }
                Result r = new Result(queryD, d, c, null);
                addToSkyline(r);
            }
        }

        long db_time = System.currentTimeMillis();
        connector n = new connector(this.graphPath);
        n.startDB();
        this.graphdb = n.getDBObject();


        //this.skyPaths.add(new double[]{0, 0, 0, 0, 1.4886183, 0.01591295, 2.2001169});

        long counter = 0;
        long addResult_rt = 0;
        long expasion_rt = 0;


        try (Transaction tx = this.graphdb.beginTx()) {
            db_time = System.currentTimeMillis() - db_time;
            r1 = System.currentTimeMillis();
            Node startNode = nearestNetworkNode(queryD);

            long nn_rt = System.currentTimeMillis() - r1;

            long rt = System.currentTimeMillis();

            myNode s = new myNode(queryD, startNode.getId(), this.distance_threshold);

            myNodePriorityQueue mqueue = new myNodePriorityQueue();
            mqueue.add(s);

            this.tmpStoreNodes.put(s.id, s);

            while (!mqueue.isEmpty()) {

                myNode v = mqueue.pop();
                v.inqueue = false;

                counter++;

                for (int i = 0; i < v.skyPaths.size(); i++) {
                    path p = v.skyPaths.get(i);

                    //constants.print(p.costs);
                    if (!p.expaned) {
                        p.expaned = true;

                        long ee = System.nanoTime();
                        ArrayList<path> new_paths = p.expand();
                        expasion_rt += (System.nanoTime() - ee);
                        for (path np : new_paths) {
//                            if (!np.hasCycle()) {
                            myNode next_n;
                            if (this.tmpStoreNodes.containsKey(np.endNode)) {
                                next_n = tmpStoreNodes.get(np.endNode);
                            } else {
                                next_n = new myNode(queryD, np.endNode, this.distance_threshold);
                                this.tmpStoreNodes.put(next_n.id, next_n);
                            }

                            if (next_n.addToSkyline(np) && !next_n.inqueue) {
                                mqueue.add(next_n);
                                next_n.inqueue = true;
                            }
                        }
//                        }
                    }
                }
            }

            long exploration_rt = System.currentTimeMillis() - rt;
//            System.out.println("expansion finished " + exploration_rt);

            for (Map.Entry<Long, myNode> mm : this.tmpStoreNodes.entrySet()) {
                sk_counter += mm.getValue().skyPaths.size();
                for (path np : mm.getValue().skyPaths) {
                    addToSkylineResult(np, queryD);
                }
            }

            sb.append(bbs_rt + "," + nn_rt + "," + exploration_rt + ",");

            tx.success();
        }

        List<Result> sortedList = new ArrayList(this.skyPaths);
        Collections.sort(sortedList);
        for (Result r : sortedList) {
            this.finalDatas.add(r.end);
        }

        sb.append(" " + finalDatas.size() + "  " + skyPaths.size());
        n.shutdownDB();

        System.out.println(sb);
    }


    private boolean addToSkylineResult(path np, Data queryD) {
        long r2a = System.nanoTime();

        myNode my_endNode = this.tmpStoreNodes.get(np.endNode);

        boolean flag = false;
        for (Data d : this.sNodes) {
            long rrr = System.nanoTime();
            //this.read_data += (rrr - dsad);


            if (d.getPlaceId() == queryD.getPlaceId()) {
                continue;
            }

            double[] final_costs = new double[np.costs.length + 3];
            System.arraycopy(np.costs, 0, final_costs, 0, np.costs.length);
            double end_distance = Math.sqrt(Math.pow(my_endNode.locations[0] - d.location[0], 2) + Math.pow(my_endNode.locations[1] - d.location[1], 2));

            final_costs[0] += end_distance;

            if (final_costs[0] < d.distance_q && end_distance < this.distance_threshold) {
                double[] d_attrs = d.getData();
                for (int i = 4; i < final_costs.length; i++) {
                    final_costs[i] = d_attrs[i - 4];
                }

                Result r = new Result(queryD, d, final_costs, np);


                boolean t = addToSkyline(r);
                if (!flag && t) {
                    flag = true;
                }
            }


        }

        return flag;
    }

    public boolean addToSkyline(Result r) {
        int i = 0;
//        if (r.end.getPlaceId() == checkedDataId) {
//            System.out.println(r);
//        }
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

    public Node nearestNetworkNode(Data queryD) {
        Node nn_node = null;
        double distz = Float.MAX_VALUE;
        try (Transaction tx = this.graphdb.beginTx()) {


            ResourceIterable<Node> iter = this.graphdb.getAllNodes();
            for (Node n : iter) {
                double lat = (double) n.getProperty("lat");
                double log = (double) n.getProperty("log");

                double temp_distz = (Math.pow(lat - queryD.location[0], 2) + Math.pow(log - queryD.location[1], 2));
                if (distz > temp_distz) {
                    nn_node = n;
                    distz = temp_distz;
                    this.nn_dist = distz;
                }
            }
            tx.success();
        }

        this.nn_dist = distz;

//        nn_dist = (int) Math.ceil(distz);
        return nn_node;
    }


}
