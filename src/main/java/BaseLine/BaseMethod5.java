package BaseLine;

import RstarTree.Data;
import neo4jTools.connector;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.Transaction;

import java.util.*;

public class BaseMethod5 {
    public ArrayList<path> qqqq = new ArrayList<>();
    Random r;
    String treePath = "/home/gqxwolf/shared_git/bConstrainSkyline/data/test.rtr";
    int graph_size;
    String degree;
    long add_oper = 0;
    long check_add_oper = 0;
    long map_operation = 0;
    long checkEmpty = 0;
    long read_data = 0;
    private GraphDatabaseService graphdb;
    private HashMap<Long, myNode> tmpStoreNodes = new HashMap();
    private ArrayList<Data> sNodes = new ArrayList<>();
    private ArrayList<Result> skyPaths = new ArrayList<>();
    private ArrayList<Data> sky_hotel;
    private HashSet<Data> finalDatas = new HashSet<Data>();
    private int checkedDataId = 9;
    private long add_counter;
    private long pro_add_result_counter;
    private long sky_add_result_counter;
    private Data queryD;

    public BaseMethod5(int graph_size, String degree) {
        r = new Random();
        this.graph_size = graph_size;
        this.degree = degree;
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
            BaseMethod5 bm5 = new BaseMethod5(graph_size, degree);
            BaseMethod3 bm3 = new BaseMethod3(graph_size, degree);
            BaseMethod bm = new BaseMethod(graph_size, degree);
//            Data queryD = bm.generateQueryData();
////
//            System.out.println(queryD);

            Data queryD = new Data(3);
            queryD.setPlaceId(9999999);
            queryD.setLocation(new double[]{20.380422592163086, 9.294476509094238});
            queryD.setData(new float[]{4.3136826f, 0.45063168f, 3.711781f});
            bm.baseline(queryD);

            System.out.println("\n===============================\n");
//
//            bm3.baseline(queryD);
//
//            System.out.println("\n===============================\n");

            bm5.baseline(queryD);
        }
    }

    public void baseline(Data queryD) {
        this.queryD = queryD;
        StringBuffer sb = new StringBuffer();
        long s_sum = System.currentTimeMillis();
        long index_s = 0;
        ArrayList<path> Results = new ArrayList<>();
        HashMap<Integer, HashSet<Long>> hotels_scope;
        Skyline sky = new Skyline(treePath);
        long r1 = System.currentTimeMillis();
        sky.BBS(queryD);
        System.out.println("Find candidate static node by BBS " + (System.currentTimeMillis() - r1) + "ms " + sky.skylineStaticNodes.size());
        long bbs_rt = System.currentTimeMillis() - r1;
        sNodes = sky.skylineStaticNodes;

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


        //Todo: each hotel know the distance to the hotel than dominate it.
        HashMap<Integer,Integer> dominated_checking = new HashMap<>();

        System.out.println("==========" + this.skyPaths.size());


        String graphPath = "/home/gqxwolf/neo4j323/testdb" + this.graph_size + "_" + this.degree + "/databases/graph.db";
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

            myNode s = new myNode(queryD, startNode, this.graphdb);

            myNodePriorityQueue mqueue = new myNodePriorityQueue();
            mqueue.add(s);

            this.tmpStoreNodes.put(s.id, s);

            while (!mqueue.isEmpty()) {

                myNode v = mqueue.pop();
                counter++;

                for (int i = 0; i < v.skyPaths.size(); i++) {
                    path p = v.skyPaths.get(i);
                    if (!p.expaned) {
                        p.expaned = true;

                        long ee = System.nanoTime();
                        ArrayList<path> new_paths = p.expand();
                        expasion_rt += (System.nanoTime() - ee);
                        for (path np : new_paths) {
                            myNode next_n;
                            if (this.tmpStoreNodes.containsKey(np.endNode.getId())) {
                                next_n = tmpStoreNodes.get(np.endNode.getId());
                            } else {
                                next_n = new myNode(queryD, np.endNode, this.graphdb);
                                this.tmpStoreNodes.put(next_n.id, next_n);
                            }


                            //lemma 2
                            if (!(this.tmpStoreNodes.get(np.startNode.getId()).distance_q > next_n.distance_q)) {
                                if (next_n.addToSkyline(np)) {
                                    mqueue.add(next_n);
                                }
                            }
                        }


                    }
                }
            }


            long tt_sl = 0;

            index_s = System.nanoTime();

            System.out.println(this.tmpStoreNodes.size());
            sky.findSkyline();

            this.sky_hotel = new ArrayList<>(sky.sky_hotels);
            for (Data sddd : this.sky_hotel) {
                System.out.println(sddd);
            }
            System.out.println(this.sky_hotel.size());
            System.out.println(this.sNodes.size());
            System.out.println("-------------------------");


            int sk_counter = 0; //the number of total candidate hotels of each bus station
            hotels_scope = new HashMap<>();
            for (Map.Entry<Long, myNode> entry : tmpStoreNodes.entrySet()) {
                myNode my_n = entry.getValue();
                ArrayList<Data> d_list = new ArrayList<>(this.sky_hotel);
//                System.out.println(d_list.size() + " ");
                for (Data d : this.sNodes) {
                    for (Data s_d : this.sky_hotel) {
                        double d1 = Math.sqrt(Math.pow(my_n.locations[0] - s_d.location[0], 2) + Math.pow(my_n.locations[1] - s_d.location[1], 2));
                        double d2 = Math.sqrt(Math.pow(my_n.locations[0] - d.location[0], 2) + Math.pow(my_n.locations[1] - d.location[1], 2));
                        if (checkDominated(s_d.getData(), d.getData()) && d1 > d2) {
                            d_list.add(d);
                            break;
                        }
                    }
                }

                my_n.d_list = new ArrayList<>(d_list);
                sk_counter += d_list.size();


                for (Data cd : d_list) {
                    int c_id = cd.getPlaceId();
                    if (hotels_scope.containsKey(c_id)) {
                        HashSet<Long> t = hotels_scope.get(c_id);
                        t.add(my_n.id);
                    } else {
                        HashSet<Long> t = new HashSet<>();
                        t.add(my_n.id);
                        hotels_scope.put(c_id, t);
                    }
                }
            }


            System.out.println("-----------------");
            for (Map.Entry<Integer, HashSet<Long>> e : hotels_scope.entrySet()) {
                boolean is_overall_skyline = false;
                for (Data sd : sky_hotel) {
                    if (sd.getPlaceId() == e.getKey()) {
                        is_overall_skyline = true;
                        break;
                    }
                }
                System.out.println(e.getKey() + "  " + e.getValue().size() + " " + is_overall_skyline);
            }
            System.out.println("-----------------");

            System.out.println("  sk_counter " + sk_counter + " / " + this.tmpStoreNodes.size() + " = " + sk_counter / this.tmpStoreNodes.size()); //average candidate hotels of each hotel
            index_s = System.nanoTime() - index_s;

            for (Map.Entry<Long, myNode> entry : tmpStoreNodes.entrySet()) {
                myNode my_n = entry.getValue();
                for (path p : my_n.skyPaths) {
                    long ats = System.nanoTime();
                    addToSkylineResult(p, my_n.d_list);
                    addResult_rt += System.nanoTime() - ats;
                }
            }


            HashSet<Integer> r_id = new HashSet<>();
            for (Result r : this.skyPaths) {
                r_id.add(r.end.getPlaceId());
            }

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
        hotels_scope.clear();

        Collections.sort(sortedList);
        for (Result r : sortedList) {
            this.finalDatas.add(r.end);
            if (r.end.getPlaceId() == 9) {
                System.out.println(r);
            }

            int c_id = r.end.getPlaceId();
            if (hotels_scope.containsKey(c_id)) {
                HashSet<Long> t = hotels_scope.get(c_id);
                if (r.p != null) {
                    t.add(r.p.endNode.getId());
                } else {
                    t.add(9999999L);
                }
            } else {
                HashSet<Long> t = new HashSet<>();
                if (r.p != null) {
                    t.add(r.p.endNode.getId());
                } else {
                    t.add(9999999L);
                }
                hotels_scope.put(c_id, t);
            }
        }
        System.out.println("====================");
        for (Map.Entry<Integer, HashSet<Long>> e : hotels_scope.entrySet()) {
            System.out.println(e.getKey() + "  " + e.getValue().size());
        }
        System.out.println("====================");

        System.out.println(finalDatas.size() + " " + this.skyPaths.size());
        System.out.println(addResult_rt + "/" + add_counter + "=" + (double) addResult_rt / add_counter / 1000000);

    }

    private boolean addToSkylineResult(path np, ArrayList<Data> d_list) {
        this.add_counter++;
        long r2a = System.nanoTime();
        if (np.rels.isEmpty()) {
            return false;
        }
        this.checkEmpty += System.nanoTime() - r2a;

        long rr = System.nanoTime();
        myNode my_endNode = this.tmpStoreNodes.get(np.endNode.getId());
        this.map_operation += System.nanoTime() - rr;

        long dsad = System.nanoTime();
        long d1 = 0, d2 = 0;
        boolean flag = false;
        for (Data d : d_list) {
            this.pro_add_result_counter++;
            long rrr = System.nanoTime();

            if (d.getPlaceId() == 9999999) {
                continue;
            }

            double[] final_costs = new double[np.costs.length + 3];
            System.arraycopy(np.costs, 0, final_costs, 0, np.costs.length);
            double end_distance = Math.sqrt(Math.pow(my_endNode.locations[0] - d.location[0], 2) + Math.pow(my_endNode.locations[1] - d.location[1], 2));
            final_costs[0] += end_distance;

            double[] d_attrs = d.getData();
            for (int i = 4; i < final_costs.length; i++) {
                final_costs[i] = d_attrs[i - 4];
            }

            Result r = new Result(this.queryD, d, final_costs, np);
            this.check_add_oper += System.nanoTime() - rrr;
            d1 += System.nanoTime() - rrr;

            long rrrr = System.nanoTime();

            //lemma3
            //Todo: find somewhere to update the information from hotel to the query point
            //Todo: Add one pruning condition that the distance also need to less than the distance form query point to the hotels which dominate d w.r.t q
            double d3 = Math.sqrt(Math.pow(d.location[0] - queryD.location[0], 2) + Math.pow(d.location[1] - queryD.location[1], 2));
            if (final_costs[0] < d3) {
                this.sky_add_result_counter++;

                boolean t = addToSkyline(r);
                if (!flag && t) {
                    flag = true;
                }
//                if (d.getPlaceId() == 9) {
//                    System.out.println(t+" "+r);
//                }
            }
            this.add_oper += System.nanoTime() - rrrr;
            d2 += System.nanoTime() - rrrr;

        }

        this.read_data += (System.nanoTime() - d1 - d2 - dsad);
        return flag;
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
//        if (r.end.getPlaceId() == checkedDataId) {
//            System.out.println(r);
//        }
        if (skyPaths.isEmpty()) {
            this.skyPaths.add(r);
        } else {
            boolean can_insert_np = true;
            for (; i < skyPaths.size(); ) {
                if (checkDominated(skyPaths.get(i).costs, r.costs)) {
//                    if (r.end.getPlaceId() == checkedDataId && skyPaths.get(i).p != null) {
////                    if (skyPaths.get(i).end.getPlaceId() == checkedDataId) {
//                        System.out.println(r);
//                        System.out.println("    dominated by " + skyPaths.get(i) + "\n ----");
//                    }
                    can_insert_np = false;
                    break;
                } else {
                    if (checkDominated(r.costs, skyPaths.get(i).costs)) {
//                        if (skyPaths.get(i).end.getPlaceId() == checkedDataId) {
//                            System.out.println("===============================");
//                            System.out.println("    removed " + skyPaths.get(i));
//                            System.out.println("    by " + r);
//                            System.out.println("===============================");
//                        }
                        this.skyPaths.remove(i);
                    } else {
                        i++;
                    }
                }
            }

            if (can_insert_np) {
                this.skyPaths.add(r);
//                if (r.end.getPlaceId() == checkedDataId) {
//                    System.out.println("    added " + r);
//                    System.out.println("true ========");
//
//                }
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

}
