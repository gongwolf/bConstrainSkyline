package BaseLine;

import RstarTree.Data;
import neo4jTools.connector;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.Transaction;

import java.util.*;

public class BaseMethod3 {
    Random r;
    String treePath = "/home/gqxwolf/shared_git/bConstrainSkyline/data/test.rtr";
    int graph_size;
    String degree;
    long add_oper = 0;
    long check_add_oper = 0;
    long map_operation = 0;
    long checkEmpty = 0;
    long read_data = 0;
    HashMap<Integer, Double> dominated_checking = new HashMap<>(); //
    private GraphDatabaseService graphdb;
    private HashMap<Long, myNode> tmpStoreNodes = new HashMap();
    private ArrayList<Data> sNodes = new ArrayList<>();
    private ArrayList<Result> skyPaths = new ArrayList<>();
    private ArrayList<Data> sky_hotel;
    private HashSet<Data> finalDatas = new HashSet<Data>();
    private long add_counter; // how many times call the addtoResult function
    private long pro_add_result_counter; // how many path + hotel combination of the results are generated
    private long sky_add_result_counter; // how many results are taken the addtoskyline operation
    private Data queryD;
    private int checkedDataId = 29;


    public BaseMethod3(int graph_size, String degree) {
        r = new Random();
        this.graph_size = graph_size;
        this.degree = degree;
    }


    public static void main(String args[]) {
        int graph_size = 50;
        String degree = "4";
        int query_num = 1;

        if (args.length == 3) {
            graph_size = Integer.parseInt(args[0]);
            degree = args[1];
            query_num = Integer.parseInt(args[2]);
        }

        for (int i = 0; i < query_num; i++) {
            BaseMethod3 bm3 = new BaseMethod3(graph_size, degree);
//            Data queryD = bm.generateQueryData();
////
//            System.out.println(queryD);

            Data queryD = new Data(3);
//            queryD.setPlaceId(9999999);
            queryD.setPlaceId(14);
            queryD.setLocation(new double[]{123.22092, 139.60222});
            queryD.setData(new float[]{2.8372698f, 0.22167504f, 3.7420158f});
            bm3.baseline(queryD);

        }
    }

    public void baseline(Data queryD) {
        this.queryD = queryD;
        StringBuffer sb = new StringBuffer();

        Skyline sky = new Skyline(treePath);


        //find the skyline hotels of the whole dataset.
        sky.findSkyline();

        this.sky_hotel = new ArrayList<>(sky.sky_hotels);
        for (Data sddd : this.sky_hotel) {
            System.out.println(sddd.getPlaceId());
        }
        System.out.println("there are " + this.sky_hotel.size() + " skyline hotels");
        System.out.println("-------------------------");

        long s_sum = System.currentTimeMillis();
        long index_s = 0;
        ArrayList<path> Results = new ArrayList<>();
        HashMap<Integer, HashSet<Long>> hotels_scope;

        long r1 = System.currentTimeMillis();
        //Find the hotels that aren't dominated by the query point
        sky.BBS(queryD);
        System.out.println("Find candidate static node by BBS " + (System.currentTimeMillis() - r1) + "ms ");
        long bbs_rt = System.currentTimeMillis() - r1;
        sNodes = sky.skylineStaticNodes;
        System.out.println("there are " + this.sNodes.size() + " hotels which are not dominated by the query point");

        for (Data d : sNodes) {
            double[] c = new double[constants.path_dimension + 3];
            c[0] = d.distance_q;
            double[] d_attrs = d.getData();
            for (int i = 4; i < c.length; i++) {
                c[i] = d_attrs[i - 4];
            }
            Result r = new Result(queryD, d, c, null);
            addToSkyline(r);
        }


        //find the minimum distance from query point to the skyline hotel that dominate non-skyline hotel cand_d
        for (Data cand_d : sNodes) {
            double h_to_h_dist = Double.MAX_VALUE;
            if (!this.sky_hotel.contains(cand_d)) {
                for (Data s_h : this.sky_hotel) {
                    if (checkDominated(s_h.getData(), cand_d.getData())) {
//                        double tmep_dist = Math.pow(s_h.location[0] - queryD.location[0], 2) + Math.pow(s_h.location[1] - queryD.location[1], 2);
//                        tmep_dist = Math.sqrt(tmep_dist);
                        double tmep_dist = s_h.distance_q;
                        if (tmep_dist < h_to_h_dist) {
                            h_to_h_dist = tmep_dist;
                        }
                    }
                }
            } else {
                h_to_h_dist = Double.MAX_VALUE;
            }
            dominated_checking.put(cand_d.getPlaceId(), h_to_h_dist);
        }

        System.out.println("==========" + this.skyPaths.size());


        String graphPath = "/home/gqxwolf/neo4j334/testdb" + this.graph_size + "_" + this.degree + "/databases/graph.db";
        long db_time = System.currentTimeMillis();
        connector n = new connector(graphPath);
        n.startDB();
        this.graphdb = n.getDBObject();

        long counter = 0;
        long addResult_rt = 0;
        long expasion_rt = 0;


        try (Transaction tx = this.graphdb.beginTx()) {
            db_time = System.currentTimeMillis() - db_time;
            r1 = System.currentTimeMillis();
            Node startNode = nearestNetworkNode(queryD);
            long nn_rt = System.currentTimeMillis() - r1;
            System.out.println("Find nearest road network " + nn_rt + " ms");
            System.out.println(startNode.getProperty("lat") + " " + startNode.getProperty("log"));


            long rt = System.currentTimeMillis();

            myNode s = new myNode(queryD, startNode.getId(),-1);

            myNodePriorityQueue mqueue = new myNodePriorityQueue();
            mqueue.add(s);

            this.tmpStoreNodes.put(s.id, s);

            while (!mqueue.isEmpty()) {

                myNode v = mqueue.pop();
                counter++;

//                if (v.id == this.checkedDataId) {
//                    System.out.println("-" + v.id + " " + v.distance_q + " " + v.locations[0] + ":" + v.locations[1]);
//                }

                for (int i = 0; i < v.skyPaths.size(); i++) {
                    //Todo: I can not decide whether one path needs to be extended
                    path p = v.skyPaths.get(i);


                    if (!p.expaned) {
//                        if (p.startNode.getId() == checkedDataId) {
//                            System.out.println(v.id + "       " + p);
//                        }
                        p.expaned = true;
                        boolean extend_f = false;


                        long ee = System.nanoTime();
                        ArrayList<path> new_paths = p.expand();
                        expasion_rt += (System.nanoTime() - ee);
                        for (path np : new_paths) {
                            myNode next_n;
                            if (this.tmpStoreNodes.containsKey(np.endNode)) {
                                next_n = tmpStoreNodes.get(np.endNode);
                            } else {
                                next_n = new myNode(queryD, np.endNode, -1);
                                this.tmpStoreNodes.put(next_n.id, next_n);
                            }


                            //lemma 2
                            if (!(this.tmpStoreNodes.get(np.startNode).distance_q > next_n.distance_q)) {
                                if (!extend_f)
                                    extend_f = true;

                                if (next_n.addToSkyline(np)) {
                                    mqueue.add(next_n);
                                }
                            }
                        }

                        boolean add_f = addToSkylineResult(p, sNodes);


//                        if (p.startNode.getId() == checkedDataId) {
//                            System.out.println(extend_f + "  " + add_f);
//                        }
                    }
                }
            }


            index_s = System.nanoTime();

            System.out.println("==========================================");
            System.out.println(this.tmpStoreNodes.size());


            int sk_counter = 0; //the number of total candidate hotels of each bus station


            System.out.println("-----------------");

            System.out.println("sk_counter " + sk_counter + " / " + this.tmpStoreNodes.size() + " = " + sk_counter / this.tmpStoreNodes.size()); //average candidate hotels of each hotel
            index_s = System.nanoTime() - index_s;


            System.out.println("begin to analyze");

            HashSet<Integer> r_id = new HashSet<>();
            for (Result r : this.skyPaths) {
                r_id.add(r.end.getPlaceId());
            }

            System.out.println("=================");
            int in_counter = 0;
            int not_in_counter = 0;
            System.out.println("in_counter:" + in_counter + " not_in_counter:" + not_in_counter);

            System.out.println("==================================================");
            System.out.println("There are " + r_id.size() + " different hotels returned in final results");
            long exploration_rt = System.currentTimeMillis() - rt;
            sb.append(bbs_rt + "," + nn_rt + "," + exploration_rt + "," + (index_s / 1000000));
            tx.success();
        }

        long shut_db_time = System.currentTimeMillis();
        n.shutdownDB();
        shut_db_time = System.currentTimeMillis() - shut_db_time;

        s_sum = System.currentTimeMillis() - s_sum;
        sb.append("|" + (s_sum - db_time - shut_db_time - (index_s / 1000000)) + "|");
        sb.append("," + this.skyPaths.size() + "," + counter + "|");
        sb.append(addResult_rt / 1000000 + "(" + (this.add_oper / 1000000) + "+" + (this.check_add_oper / 1000000)
                + "+" + (this.map_operation / 1000000) + "+" + (this.checkEmpty / 1000000) + "+" + (this.read_data / 1000000) + "),");
        sb.append(expasion_rt / 1000000 + " ");
        sb.append("\nadd_to_Skyline_result " + this.add_counter + "  " + this.pro_add_result_counter + "  " + this.sky_add_result_counter + " ");
        sb.append((double) this.sky_add_result_counter / this.pro_add_result_counter);
        System.out.println(sb.toString());

        Result r2 = null;
        List<Result> sortedList = new ArrayList(this.skyPaths);
//
        Collections.sort(sortedList);
        for (Result r : sortedList) {
            this.finalDatas.add(r.end);
        }

        System.out.println("====================");

        System.out.println(finalDatas.size() + " " + this.skyPaths.size());
        System.out.println(addResult_rt + "/" + add_counter + "=" + (double) addResult_rt / add_counter / 1000000);
        System.out.println(sky_add_result_counter + "/" + add_counter + "=" + (double) sky_add_result_counter / add_counter);

        for (Result r : this.skyPaths) {
////            if (r.p != null && r.p.startNode.getId() == 35) {
            if (r.p != null && r.end.getPlaceId() == 23) {
                System.out.println(r);
            }
//
//            if (r.p != null) {
//                System.out.println(r.end.getPlaceId() + " " + r.p.startNode.getId() + " " + r.p.endNode.getId());
//            } else {
//                System.out.println(r.end.getPlaceId() + " " + null);
//
//            }
        }

    }

    private boolean addToSkylineResult(path np, ArrayList<Data> d_list) {
//    private boolean addToSkylineResult(path np, Data d) {
        this.add_counter++;
        long r2a = System.nanoTime();
        if (np.rels.isEmpty()) {
            return false;
        }
        this.checkEmpty += System.nanoTime() - r2a;

        long rr = System.nanoTime();
        myNode my_endNode = this.tmpStoreNodes.get(np.endNode);
        this.map_operation += System.nanoTime() - rr;

        long dsad = System.nanoTime();
        long d1 = 0, d2 = 0;
        boolean flag = false;
        for (Data d : d_list) {
            this.pro_add_result_counter++;
            long rrr = System.nanoTime();

            if (d.getPlaceId() == queryD.getPlaceId()) {
                return false;
            }

            double[] final_costs = new double[np.costs.length + 3];
            System.arraycopy(np.costs, 0, final_costs, 0, np.costs.length);
            double end_distance = Math.sqrt(Math.pow(my_endNode.locations[0] - d.location[0], 2) + Math.pow(my_endNode.locations[1] - d.location[1], 2));

            final_costs[0] += end_distance;


//            if (np.startNode.getId() == checkedDataId) {
//                if (final_costs[0] < d.distance_q && final_costs[0] < this.dominated_checking.get(d.getPlaceId()))
//                    System.out.println("----" + d.getPlaceId());
//            }

            //lemma3&4
            if (final_costs[0] < d.distance_q && final_costs[0] < this.dominated_checking.get(d.getPlaceId())) {


                double[] d_attrs = d.getData();
                for (int i = 4; i < final_costs.length; i++) {
                    final_costs[i] = d_attrs[i - 4];
                }

                Result r = new Result(this.queryD, d, final_costs, np);


                this.check_add_oper += System.nanoTime() - rrr;
                d1 += System.nanoTime() - rrr;
                long rrrr = System.nanoTime();
                this.sky_add_result_counter++;
                boolean t = addToSkyline(r);

                this.add_oper += System.nanoTime() - rrrr;
                d2 += System.nanoTime() - rrrr;

                if (!flag && t) {
                    flag = true;
                }
            }
        }

        this.read_data += (System.nanoTime() - d1 - d2 - dsad);
        return flag;
    }


    public float randomFloatInRange(float min, float max) {
        float random = min + r.nextFloat() * (max - min);
        return random;
    }

    public Node nearestNetworkNode(Data queryD) {

        Node nn_node = null;
        double distz = Float.MAX_VALUE;

        ResourceIterable<Node> iter = this.graphdb.getAllNodes();
        for (Node n : iter) {
            double lat = (double) n.getProperty("lat");
            double log = (double) n.getProperty("log");

            double temp_distz = (Math.pow(lat - queryD.location[0], 2) + Math.pow(log - queryD.location[1], 2));
            if (distz > temp_distz) {
                nn_node = n;
                distz = temp_distz;

            }
        }
        return nn_node;
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
//                    if (r.p != null && r.p.startNode.getId() == checkedDataId) {
//                        System.out.println(r);
//                        System.out.println("Dominated by ");
//                        System.out.println(skyPaths.get(i));
//                        System.out.println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@2");
//                    }
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

//        if (r.end.getPlaceId() == checkedDataId) {
//            System.out.println("false ========");
//        }
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

}
