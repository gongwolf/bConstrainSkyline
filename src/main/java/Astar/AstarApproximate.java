package Astar;

import BaseLine.*;
import BaseLine.approximate.range.BaseMethod_approx;
import RstarTree.Data;
import neo4jTools.connector;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.Transaction;

import java.util.*;

public class AstarApproximate {

    private final LandMarkIndex lmi;
    private final double distance_threshold;
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
    public ArrayList<Result> skyPaths = new ArrayList<>();
    private HashMap<Integer, ArrayList<Integer>> nearbyList;
    private HashSet<Data> finalDatas = new HashSet<>();


    public GraphDatabaseService graphdb;
    private double nn_dist;


    public AstarApproximate(int graph_size, String degree, double range, int hotels_num) {
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
    }

    public void baseline(Data queryD) {
//        this.queryD = queryD;
        StringBuffer sb = new StringBuffer();
        sb.append(queryD.getPlaceId() + " ");
        Skyline sky = new Skyline(treePath);


        //find the skyline hotels of the whole dataset.
        sky.findSkyline();
        this.sky_hotel = new ArrayList<>(sky.sky_hotels);

        //Find the hotels that aren't dominated by the query point
        long r1 = System.currentTimeMillis();
        sky.BBS(queryD);
        long bbs_rt = System.currentTimeMillis() - r1;
        sNodes = sky.skylineStaticNodes;
        sb.append(this.sNodes.size() + " " + this.sky_hotel.size() + " ");
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
        sb.append(this.skyPaths.size() + " ");

        NearbyObjects nb = new NearbyObjects(this.graph_size, this.degree, this.range, this.hotels_num);
        this.nearbyList = nb.getNearbyList();

        connector n = new connector(graphPath);
//        System.out.println(graphPath);
        n.startDB();
        this.graphdb = n.getDBObject();

        long rr = System.currentTimeMillis();
        for (Data d : this.sNodes) {
//            if (nearbyList.containsKey(d.getPlaceId()) && d.getPlaceId() != queryD.getPlaceId()) {
            if (d.getPlaceId() != queryD.getPlaceId()) {
                queryResultSourceDestination(queryD, d);
            }
        }

        List<Result> sortedList = new ArrayList(this.skyPaths);
        Collections.sort(sortedList);
        for (Result r : sortedList) {
            this.finalDatas.add(r.end);
        }

        sb.append((System.currentTimeMillis() - rr) + " " + finalDatas.size() + " " + this.skyPaths.size());

        n.shutdownDB();

        System.out.println(sb);
    }

    private void queryResultSourceDestination(Data queryD, Data d) {

        HashMap<Long, myNode> tmpStoreNodes = new HashMap();


        double d_distance_lower = getwaklingLowerBound(d);


        try (Transaction tx = this.graphdb.beginTx()) {

            Node startNode = nearestNetworkNode(queryD);

            myNode s = new myNode(queryD, startNode.getId(), -1);

            myNodePriorityQueue mqueue = new myNodePriorityQueue();
            mqueue.add(s);
            tmpStoreNodes.put(s.id, s);

            while (!mqueue.isEmpty()) {
                myNode v = mqueue.pop();
                v.inqueue = false;


//                System.out.println(v.id);

                for (int i = 0; i < v.skyPaths.size(); i++) {
                    path p = v.skyPaths.get(i);
                    if (!p.expaned) {
                        p.expaned = true;

                        ArrayList<path> new_paths = p.expand();
                        for (path np : new_paths) {

//                            if (!Expended(np, d, d_distance_lower)) {
//                                continue;
//                            }


                            if (!np.hasCycle()) {

//                                myNode next_n;
//                                if (tmpStoreNodes.containsKey(np.endNode)) {
//                                    next_n = tmpStoreNodes.get(np.endNode);
//                                } else {
//                                    next_n = new myNode(queryD, np.endNode, this.distance_threshold);
//                                    tmpStoreNodes.put(next_n.id, next_n);
//                                }


                                myNode next_n;
                                if (tmpStoreNodes.containsKey(np.endNode)) {
                                    next_n = tmpStoreNodes.get(np.endNode);
                                } else {
                                    if (this.nearbyList.get(queryD.getPlaceId()).contains((int) np.endNode)) {
                                        next_n = new myNode(queryD, np.endNode, -1);
                                        tmpStoreNodes.put(next_n.id, next_n);
                                    } else {
                                        next_n = new myNode(queryD, np.endNode, -2);
                                        tmpStoreNodes.put(next_n.id, next_n);
                                    }
                                }

                                //lemma 2
                                //todo:need to be fixed
//                                if (this.nearbyList.get(d.getPlaceId()).contains((int) np.endNode)) {
////                                    System.out.println(np);
//                                    next_n.addToSkyline(np);
//                                } else
                                if (next_n.addToSkyline(np) && !next_n.inqueue) {
                                    mqueue.add(next_n);
                                    next_n.inqueue = true;
                                }

                            }
                        }
                    }
                }

            }

//            for (int nid : nearbyList.get(d.getPlaceId())) {
//                myNode n = tmpStoreNodes.get((long) nid);
//                if (n != null) {
//                    for (path np : n.skyPaths) {
//                        addToSkylineResult(np, d, queryD, n);
////                        System.out.println(n.id+"  "+np);
//                    }
//                }
//            }


            for (Map.Entry<Long, myNode> entry : tmpStoreNodes.entrySet()) {

                myNode my_n = entry.getValue();

                for (path p : my_n.skyPaths) {
                    boolean f = addToSkylineResult(p, queryD, d, my_n);
                }
            }

            tx.success();
        }


    }


    private boolean addToSkylineResult(path np, Data d, Data queryD, myNode my_endNode) {


        if (d.getPlaceId() == queryD.getPlaceId()) {
            return true;
        }

        double[] final_costs = new double[np.costs.length + 3];
        System.arraycopy(np.costs, 0, final_costs, 0, np.costs.length);
        double end_distance = Math.sqrt(Math.pow(my_endNode.locations[0] - d.location[0], 2) + Math.pow(my_endNode.locations[1] - d.location[1], 2));
        final_costs[0] += end_distance;

        if (end_distance < this.distance_threshold) {

            double[] d_attrs = d.getData();
            for (int i = 4; i < final_costs.length; i++) {
                final_costs[i] = d_attrs[i - 4];
            }

            Result r = new Result(queryD, d, final_costs, np);
            boolean t = addToSkyline(r);


            return t;
        }

        return false;
    }

    private double getwaklingLowerBound(Data d) {
        int placeid = d.getPlaceId();
        ArrayList<Integer> nearByNodeId = this.nearbyList.get(placeid);

        double result = Double.MAX_VALUE;
        try (Transaction tx = this.graphdb.beginTx()) {
            for (int nid : nearByNodeId) {
//                System.out.println("     " + nid + " " + placeid);
                double n_lat = (double) this.graphdb.getNodeById(nid).getProperty("lat");
                double n_long = (double) this.graphdb.getNodeById(nid).getProperty("log");
                double n_dist = Math.sqrt(Math.pow(n_lat - d.location[0], 2) + Math.pow(n_long - d.location[1], 2));
                if (n_dist < result) {
                    result = n_dist;
                }

            }
            tx.success();
        }

        return result;
    }

    private boolean Expended(path p, Data d, double walking_lowerbound) {

        int placeid = d.getPlaceId();
        ArrayList<Integer> nearByNodeId = this.nearbyList.get(placeid);


        double[] lowerbound = new double[7];
        lowerbound[0] = walking_lowerbound;
        lowerbound[4] = d.getData()[0];
        lowerbound[5] = d.getData()[1];
        lowerbound[6] = d.getData()[2];
//        System.out.println(" ~~~~ " + lowerbound[4] + " " + lowerbound[5] + " " + lowerbound[6] + " " + lowerbound[0]);

        for (int i = 1; i < 4; i++) {
            lowerbound[i] = Double.POSITIVE_INFINITY;
        }


        for (int nid : nearByNodeId) {
            double[] tmp_lowerbound = lmi.readLandMark(p.endNode, nid);
            for (int i = 0; i < 3; i++) {
                if (tmp_lowerbound[i] < lowerbound[i + 1]) {
                    lowerbound[i + 1] = tmp_lowerbound[i];
                }
            }
        }

//        System.out.println(" ~~~~ " + " " + lowerbound[0] + " " + lowerbound[1] + " " + lowerbound[2] + " " + lowerbound[3] + lowerbound[4] + " " + lowerbound[5] + " " + lowerbound[6]);


        for (int i = 0; i < 4; i++) {
            lowerbound[i] = p.costs[i] + lowerbound[i];

        }

        return !checkDominatedInResults(lowerbound);
//        return false;

    }

    private boolean checkDominatedInResults(double[] lowerbound) {
        boolean result = false;
        for (Result r : this.skyPaths) {
            if (checkDominated(r.costs, lowerbound)) {
                return true;
            }
        }

        return result;
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


    public static void main(String args[]) {
        AstarApproximate asa = new AstarApproximate(1000, "4", 14, 1000);

        BaseMethod5 bm5 = new BaseMethod5(1000, "4", 14, 1000);
        int random_place_id = bm5.getRandomNumberInRange_int(0, bm5.getNumberOfHotels() - 1);
        Data queryD = bm5.getDataById(126);
        asa.baseline(queryD);


        BaseMethod_approx bs_approx = new BaseMethod_approx(1000, "4", 14, 14, 1000);
        bs_approx.baseline(queryD);
    }


}
