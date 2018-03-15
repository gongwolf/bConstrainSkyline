package BaseLine;

import RstarTree.Data;
import neo4jTools.connector;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.Transaction;

import java.util.*;

public class BaseMethod {
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
    private HashSet<Data> finalDatas = new HashSet<Data>();
    private int checkedDataId = 4;
    private long add_counter;
    private long pro_add_result_counter;
    private long sky_add_result_counter;


    private ArrayList<Data> sky_hotel;
//    HashMap<Integer, Double> dominated_checking = new HashMap<>();



    public BaseMethod(int graph_size, String degree) {
        r = new Random();
        this.graph_size = graph_size;
        this.degree = degree;
    }

    public static void main(String args[]) {
        int graph_size = 10;
        String degree = "5";
        int query_num = 1;

        if (args.length == 3) {
            graph_size = Integer.parseInt(args[0]);
            degree = args[1];
            query_num = Integer.parseInt(args[2]);
        }

        for (int i = 0; i < query_num; i++) {
            BaseMethod bm = new BaseMethod(graph_size, degree);
//            Data queryD = bm.generateQueryData();
////
//            System.out.println(queryD);

            Data queryD = new Data(3);
            queryD.setPlaceId(9999999);
            queryD.setLocation(new double[]{20.380422592163086, 9.294476509094238});
            queryD.setData(new float[]{4.3136826f, 0.45063168f, 3.711781f});
//            constants.print(queryD.location);
//            constants.print(queryD.getData());


            bm.baseline(queryD);
        }
    }

    public void baseline(Data queryD) {
        StringBuffer sb = new StringBuffer();
        long s_sum = System.currentTimeMillis();
        ArrayList<path> Results = new ArrayList<>();
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

        System.out.println("==========");


        String graphPath = "/home/gqxwolf/neo4j323/testdb" + this.graph_size + "_" + this.degree + "/databases/graph.db";
        //System.out.println(graphPath);
        long db_time = System.currentTimeMillis();
        connector n = new connector(graphPath);
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
            System.out.println("Find nearest road network " + nn_rt + " ms");
            System.out.println(startNode.getProperty("lat") + " " + startNode.getProperty("log"));


            long rt = System.currentTimeMillis();

            myNode s = new myNode(queryD, startNode, this.graphdb);

            myNodePriorityQueue mqueue = new myNodePriorityQueue();
            mqueue.add(s);

            this.tmpStoreNodes.put(s.id, s);

            while (!mqueue.isEmpty()) {

                myNode v = mqueue.pop();
//                System.out.println("-" + v.id + " " + v.distance_q + " " + v.locations[0] + ":" + v.locations[1]);
                counter++;
                //if (++counter % 1000 == 0) {
                //}
                for (int i = 0; i < v.skyPaths.size(); i++) {
                    path p = v.skyPaths.get(i);
                    //constants.print(p.costs);
                    if (!p.expaned) {
//                        System.out.println(counter + "  ......  ");
                        p.expaned = true;
//                        System.out.println("++: " + p);
//                        constants.print(p.costs);

                        long rr = System.nanoTime();
                        addToSkylineResult(p, queryD);
                        addResult_rt += (System.nanoTime() - rr);

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

//                            System.out.println("        ++++++===== " + np + " " + next_n.distance_q + " " + (this.tmpStoreNodes.get(np.startNode.getId()).distance_q > next_n.distance_q));
//                            constants.print(np.costs);


                            //lemma 2
                            if (!(this.tmpStoreNodes.get(np.startNode.getId()).distance_q > next_n.distance_q)) {
                                if (next_n.addToSkyline(np)) {
                                    mqueue.add(next_n);
//                                    System.out.println("        ++++++===== " + np + " " + next_n.distance_q);
//                                    constants.print(np.costs);
                                }
                            }
                        }


                    }
                }

//                break;
            }

            HashSet<Integer> r_id = new HashSet<>();
            for (Result r : this.skyPaths) {
                r_id.add(r.end.getPlaceId());
            }

            System.out.println("==================================================");
            System.out.println("There are " + r_id.size() + " different hotels returned in final results");
            long exploration_rt = System.currentTimeMillis() - rt;

            sb.append(bbs_rt + "," + nn_rt + "," + exploration_rt + ",");

            tx.success();
        }

        //System.out.println("Found " + this.skyPaths.size() + " constrained skyline path in " + (System.currentTimeMillis() - rt) + " ms");
//        for (double[] d : this.skyPaths) {
//            constants.print(d);
//        }
        long shut_db_time = System.currentTimeMillis();
        n.shutdownDB();
        shut_db_time = System.currentTimeMillis() - shut_db_time;

        s_sum = System.currentTimeMillis() - s_sum;
        sb.append("|" + (s_sum - db_time - shut_db_time) + "|");
        sb.append("," + this.skyPaths.size() + "," + counter + "|");
        sb.append(addResult_rt / 1000000 + "(" + (this.add_oper / 1000000) + "+" + (this.check_add_oper / 1000000)
                + "+" + (this.map_operation / 1000000) + "+" + (this.checkEmpty / 1000000) + "+" + (this.read_data / 1000000) + "),");
        sb.append(expasion_rt / 1000000 + " ");
        sb.append("\nadd_to_Skyline_result " + this.add_counter + "  " + this.pro_add_result_counter + "  " + this.sky_add_result_counter + " ");
        sb.append((double) this.sky_add_result_counter / this.pro_add_result_counter);
        System.out.println(sb.toString());
//        System.out.println(finalDatas.size());

        Result r2 = null;
        List<Result> sortedList = new ArrayList(this.skyPaths);
        Collections.sort(sortedList);
        for (Result r : sortedList) {
            this.finalDatas.add(r.end);
//            if (r.p != null && r2 == null) {
//                r2 = r;
//            }
//            System.out.println(r);
        }

        System.out.println(finalDatas.size() + " " + this.skyPaths.size());
        System.out.println(addResult_rt + "/" + add_counter + "=" + (double) addResult_rt / add_counter / 1000000);


        long tt_sl = 0;
        for (Map.Entry<Long, myNode> entry : tmpStoreNodes.entrySet()) {
            tt_sl += entry.getValue().skyPaths.size();
            for (path p : entry.getValue().skyPaths) {
                addToSkyline_p(p);

            }
        }

        System.out.println(qqqq.size() + "   " + tt_sl);

//        double max_values[] = new double[constants.path_dimension + 3];
//
//        for (int i = 0; i < max_values.length; i++) {
//            max_values[i] = Double.MIN_VALUE;
//        }
//
//        double min_values[] = new double[constants.path_dimension + 3];
//        for (int i = 0; i < max_values.length; i++) {
//            min_values[i] = Double.MAX_VALUE;
//        }

//        for (Result r : sortedList) {
//            for (int i = 0; i < max_values.length; i++) {
//                if (r.costs[i] > max_values[i]) {
//                    max_values[i] = r.costs[i];
//                }
//
//                if (r.costs[i] < min_values[i]) {
//                    min_values[i] = r.costs[i];
//                }
//
//            }
//        }
//
//        constants.print(max_values);
//        constants.print(min_values);


//        for (Result r : sortedList) {
//            double score = 0.0;
//            for (int i = 0; i < r.costs.length; i++) {
//                score += (r.costs[i] - min_values[i]) / (max_values[i] - min_values[i]);
//
//            }
//
//
////            for (int i = constants.path_dimension; i < max_values.length; i++) {
////                score += (max_values[i]-r.costs[i]) / (max_values[i] - min_values[i]);
////
////            }
//
//
//            System.out.println(score+" "+r);
//        }

//        System.out.println(r2);

//        System.out.println("=============================");
//        for (Data dt : sNodes) {
//            if (!finalDatas.contains(dt)) {
//                System.out.println(dt);
//            }
//        }

    }

    private boolean addToSkylineResult(path np, Data queryD) {
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
        for (Data d : this.sNodes) {
            this.pro_add_result_counter++;
            long rrr = System.nanoTime();
            //this.read_data += (rrr - dsad);


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

            Result r = new Result(queryD, d, final_costs, np);
            this.check_add_oper += System.nanoTime() - rrr;
            d1 += System.nanoTime() - rrr;

            long rrrr = System.nanoTime();
            //lemma3
            if (final_costs[0] < d.distance_q) {
                this.sky_add_result_counter++;
                boolean t = addToSkyline(r);
                if (!flag && t) {
                    flag = true;
                }
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
        float latitude = randomFloatInRange(0f, 360f);
        float longitude = randomFloatInRange(0f, 360f);
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
        if (skyPaths.isEmpty()) {
            this.skyPaths.add(r);
        } else {
            boolean can_insert_np = true;
            for (; i < skyPaths.size(); ) {
                if (checkDominated(skyPaths.get(i).costs, r.costs)) {
//                    if (r.end.getPlaceId() == checkedDataId && skyPaths.get(i).p != null) {
//                        System.out.println("    dominated by " + skyPaths.get(i));
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
//                }
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

}
