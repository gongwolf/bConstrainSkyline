package BaseLine.compare5D.approximate.mixed;

import BaseLine.*;
import RstarTree.Data;
import neo4jTools.connector;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.Transaction;
import testTools.Index;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;

public class BaseMethod_mixed_index {
    private final String graphPath;
    public ArrayList<Result> skyPaths = new ArrayList<>();
    public ArrayList<Data> sky_hotel;
    Random r;
    String treePath;
    String dataPath;
    int graph_size;
    String degree;
    long add_oper = 0;
    long check_add_oper = 0;
    long map_operation = 0;
    long checkEmpty = 0;
    long read_data = 0;
    //Todo: each hotel know the distance to the hotel than dominate it.
    HashMap<Integer, Double> dominated_checking = new HashMap<>(); //
    //        int distance_threshold = Integer.MAX_VALUE;
    double distance_threshold = 0.001;
    String home_folder = System.getProperty("user.home");
    private int hotels_num;
    private GraphDatabaseService graphdb;
    private HashMap<Long, myNode> tmpStoreNodes = new HashMap();
    private ArrayList<Data> sNodes = new ArrayList<>();
    private HashSet<Integer> finalDatas = new HashSet<Integer>();
    private int checkedDataId = 9;
    private long add_counter; // how many times call the addtoResult function
    private long pro_add_result_counter; // how many path + hotel combination of the results are generated
    private long sky_add_result_counter; // how many results are taken the addtoskyline operation
    private Data queryD;
    private int idx_num;


    public BaseMethod_mixed_index(int graph_size, String degree, double distance_threshold, double range, int hotels_num, int idx_num) {
        r = new Random();
        this.graph_size = graph_size;
        this.degree = degree;
        this.distance_threshold = distance_threshold;
        this.hotels_num = hotels_num;
        this.idx_num = idx_num;
        this.treePath = home_folder + "/shared_git/bConstrainSkyline/data/test_" + graph_size + "_" + degree + "_" + range + "_" + hotels_num +"_"+idx_num +  ".rtr";
        this.dataPath = home_folder + "/shared_git/bConstrainSkyline/data/staticNode_" + graph_size + "_" + degree + "_" + range + "_" + hotels_num +"_"+idx_num +  ".txt";
        this.graphPath = home_folder + "/neo4j334/testdb" + this.graph_size + "_" + this.degree + "/databases/graph.db";

//        System.out.println(this.treePath);
//        System.out.println(this.dataPath);
//        System.exit(0);

//        this.treePath = home_folder + "/shared_git/bConstrainSkyline/data/real_tree.rtr";
//        this.dataPath = home_folder + "/shared_git/bConstrainSkyline/data/staticNode_real.txt";
    }


    public BaseMethod_mixed_index(String tree, String data, String graphPath, double distance_threshold) {
        this.distance_threshold = distance_threshold;
        this.treePath = tree;
        this.dataPath = data;
        this.graphPath = graphPath;
    }


    public void baseline(Data queryD) {
        this.tmpStoreNodes.clear();
//        System.out.println(queryD);
        long r1 = System.currentTimeMillis();

//        String graphPath = home_folder + "/neo4j334/testdb_real_50/databases/graph.db";
        long s_sum = System.currentTimeMillis();
        long db_time = System.currentTimeMillis();
        connector.graphDB=null;
        constants.accessedEdges.clear();
        constants.accessedNodes.clear();
        connector n = new connector(graphPath);
        n.startDB();
        this.graphdb = n.getDBObject();

        long counter = 0;
        long addResult_rt = 0;
        long expasion_rt = 0;
        int sk_counter = 0; //the number of total candidate hotels of each bus station

        db_time = System.currentTimeMillis() - db_time;
        r1 = System.currentTimeMillis();
        long startNode_id = nearestNetworkNode(queryD);
        long nn_rt = System.currentTimeMillis() - r1;
//        System.out.println("find the nearest bus stop " + nn_rt + "  ms  ");


        this.queryD = queryD;
        StringBuffer sb = new StringBuffer();
        sb.append(queryD.getPlaceId() + " ");

        Skyline sky = new Skyline(treePath);


        //find the skyline hotels of the whole dataset.
        sky.findSkyline();

        this.sky_hotel = new ArrayList<>(sky.sky_hotels);
//        for (Data sddd : this.sky_hotel) {
//            System.out.println(sddd.getPlaceId());
//        }
//        System.out.println("there are " + this.sky_hotel.size() + " skyline hotels");
//        System.out.println("-------------------------");

        long index_s = 0;

        r1 = System.currentTimeMillis();
        //Find the hotels that aren't dominated by the query point
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
//                System.out.println(r);
                addToSkyline(r);
            }
        }

        //find the minimum distance from query point to the skyline hotel that dominate non-skyline hotel cand_d
        for (Data cand_d : sNodes) {
            double h_to_h_dist = Double.MAX_VALUE;
            if (!this.sky_hotel.contains(cand_d)) {
                for (Data s_h : this.sky_hotel) {
                    if (checkDominated(s_h.getData(), cand_d.getData())) {
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


        int visited_bus_stop = 0;
        try (Transaction tx = connector.graphDB.beginTx()) {


            long rt = System.currentTimeMillis();

            myNode s = new myNode(queryD, startNode_id, this.distance_threshold);
//            myNode s = new myNode(queryD, startNode_id, -1);

//            myNodePriorityQueueNoCp mqueue = new myNodePriorityQueueNoCp();
            myNodePriorityQueue mqueue = new myNodePriorityQueue();
            mqueue.add(s);

            this.tmpStoreNodes.put(s.id, s);

            while (!mqueue.isEmpty()) {

                myNode v = mqueue.pop();
                v.inqueue = false;
                counter++;

                path[] needToProcess = new path[constants.path_dimension];
                for (path p : v.skyPaths) {
//                    if (!p.expaned) {
                    if (p.isDummyPath()) {
                        needToProcess[0] = p;
                    } else if (needToProcess[1] == null) {
                        for (int i = 1; i < needToProcess.length; i++) {
                            needToProcess[i] = p;
                        }
                    } else {
                        for (int i = 1; i < needToProcess.length; i++) {
                            if (needToProcess[i].costs[i] > p.costs[i]) {
                                needToProcess[i] = p;
                            }
                        }
                    }
//                    }
                }

                for (int i = 0; i < needToProcess.length; i++) {

                    path p = needToProcess[i];
                    if (p != null && !p.expaned) {
                        p.expaned = true;

                        long ee = System.nanoTime();
                        ArrayList<path> new_paths = p.expand();
                        expasion_rt += (System.nanoTime() - ee);


                        for (path np : new_paths) {
                            myNode next_n;
                            if (this.tmpStoreNodes.containsKey(np.endNode)) {
                                next_n = tmpStoreNodes.get(np.endNode);
                            } else {
                                next_n = new myNode(queryD, np.endNode, this.distance_threshold);
//                                next_n = new myNode(queryD, np.endNode, -1);
                                this.tmpStoreNodes.put(next_n.id, next_n);
                            }

                            //lemma 2
                            if (!(this.tmpStoreNodes.get(np.startNode).distance_q > next_n.distance_q)) {
                                if (next_n.addToSkyline(np) && !next_n.inqueue) {
                                    mqueue.add(next_n);
                                    next_n.inqueue = true;
                                }
                            }
                        }
                    }
                }
            }

            long exploration_rt = System.currentTimeMillis() - rt;

            long tt_sl = 0;


//            hotels_scope = new HashMap<>();

//            System.out.println("there are " + this.tmpStoreNodes.size() + " bus stops are visited");

            int process_visited_stations = 0;
            for (Map.Entry<Long, myNode> entry : tmpStoreNodes.entrySet()) {
                sk_counter += entry.getValue().skyPaths.size();
            }

//            System.out.println(sk_counter+"~~~~~");

            Index idx = new Index(this.graph_size, this.degree, this.distance_threshold, this.hotels_num, this.distance_threshold, this.idx_num);
            for (Map.Entry<Long, myNode> entry : tmpStoreNodes.entrySet()) {
                long t_index_s = System.nanoTime();
                myNode my_n = entry.getValue();
                ArrayList<Data> d_list = idx.read_d_list_from_disk(my_n.id);
                index_s += (System.nanoTime() - t_index_s);


//                index_s += (System.nanoTime() - t_index_s);

                for (path p : my_n.skyPaths) {
//                    if (!p.rels.isEmpty() && p.expaned) {
                        if (p.expaned) {
                            long ats = System.nanoTime();
                            boolean f = addToSkylineResult(p, d_list);
                            addResult_rt += System.nanoTime() - ats;
                        }
//                    }
                }
            }

            visited_bus_stop = this.tmpStoreNodes.size();


//            System.out.println(sk_counter);


            sb.append(bbs_rt + "," + nn_rt + "," + exploration_rt + "," + (index_s / 1000000));
            tx.success();
        }

        long shut_db_time = System.currentTimeMillis();
        n.shutdownDB();
        shut_db_time = System.currentTimeMillis() - shut_db_time;

        s_sum = System.currentTimeMillis() - s_sum;
//        System.out.println(s_sum+" "+db_time+" "+shut_db_time+" "+index_s);
        sb.append("|" + (s_sum - db_time - shut_db_time) + "|");
        sb.append("," + this.skyPaths.size() + "," + counter + "|");
        sb.append(addResult_rt / 1000000 + "(" + (this.add_oper / 1000000) + "+" + (this.check_add_oper / 1000000)
                + "+" + (this.map_operation / 1000000) + "+" + (this.checkEmpty / 1000000) + "+" + (this.read_data / 1000000) + "),");
        sb.append(expasion_rt / 1000000 + " ");
//        sb.append("\nadd_to_Skyline_result " + this.add_counter + "  " + this.pro_add_result_counter + "  " + this.sky_add_result_counter + " ");
//        sb.append((double) this.sky_add_result_counter / this.pro_add_result_counter);

        List<Result> sortedList = new ArrayList(this.skyPaths);
        Collections.sort(sortedList);

        HashSet<Long> final_bus_stops = new HashSet<>();

        for (Result r : sortedList) {
            this.finalDatas.add(r.end.getPlaceId());
//            if (r.p != null) {
//                for (Long nn : r.p.nodes) {
//                    final_bus_stops.add(nn);
//                }
//            }
        }


        sb.append(finalDatas.size() + " " + this.skyPaths.size() + " " + sk_counter + "  " + add_counter + " ");

        int bus_stop_in_result = final_bus_stops.size();

        sb.append("  " + visited_bus_stop + "," + bus_stop_in_result + "," + (double) bus_stop_in_result / visited_bus_stop + "   " + this.sky_add_result_counter);

        System.out.println(sb.toString());

//        System.out.println("====================");
//        for (Map.Entry<Integer, HashSet<Long>> e : hotels_scope.entrySet()) {
//            System.out.println(e.getKey() + "  " + e.getValue().size());
//        }
//        System.out.println("====================");

//        System.out.println(finalDatas.size() + " " + this.skyPaths.size());
//        System.out.println(addResult_rt + "/" + add_counter + "=" + (double) addResult_rt / add_counter / 1000000);
//        System.out.println(sky_add_result_counter + "/" + add_counter + "=" + (double) sky_add_result_counter / add_counter);

    }


    private boolean addToSkylineResult(path np, ArrayList<Data> d_list) {
        this.add_counter++;
        long r2a = System.nanoTime();
        this.checkEmpty += System.nanoTime() - r2a;

        long rr = System.nanoTime();
        myNode my_endNode = this.tmpStoreNodes.get(np.endNode);
        this.map_operation += System.nanoTime() - rr;

        long dsad = System.nanoTime();
        long d1 = 0, d2 = 0;
        boolean flag = false;

        for (Data d : d_list) {
            if (!this.dominated_checking.containsKey(d.getPlaceId()) || d.getPlaceId() == queryD.getPlaceId()) {
                continue;
            }
//            if (np.startNode.getId() == 286 && np.endNode.getId() == 1862) {
//                if (d.getPlaceId() == 62) {
//                    System.out.println("true");
//                }
//            }

            this.pro_add_result_counter++;
            long rrr = System.nanoTime();


            double[] final_costs = new double[np.costs.length + 3];
            System.arraycopy(np.costs, 0, final_costs, 0, np.costs.length);
            double end_distance = Math.sqrt(Math.pow(my_endNode.locations[0] - d.location[0], 2) + Math.pow(my_endNode.locations[1] - d.location[1], 2));
            d.distance_q = Math.sqrt(Math.pow(queryD.location[0] - d.location[0], 2) + Math.pow(queryD.location[1] - d.location[1], 2));
//            double end_distance = GoogleMaps.distanceInMeters(my_endNode.locations[0], my_endNode.locations[1], d.location[0], d.location[1]);
//            d.distance_q = GoogleMaps.distanceInMeters(queryD.location[0], queryD.location[1], d.location[0], d.location[1]);


            final_costs[0] += end_distance;
            //lemma3
            //double d3 = Math.sqrt(Math.pow(d.location[0] - queryD.location[0], 2) + Math.pow(d.location[1] - queryD.location[1], 2));

//            if (np.startNode.getId() == 286 && np.endNode.getId() == 1862) {
//                System.out.println(d);
//                System.out.println(final_costs[0] < d.distance_q);
//                System.out.println(final_costs[0] < this.dominated_checking.get(d.getPlaceId()));
//                System.out.println(np);
//                System.out.println("-------------------------------");
//            }


            if (final_costs[0] < d.distance_q && final_costs[0] < this.dominated_checking.get(d.getPlaceId())) {

                double[] d_attrs = d.getData();
                for (int i = 4; i < final_costs.length; i++) {
                    final_costs[i] = d_attrs[i - 4];
                }

                Result r = new Result(this.queryD, d, final_costs, np);
//            if (np.startNode.getId() == 0 && np.endNode.getId() == 7&& np.nodes.size() == 4 && np.rels.size() == 3) {
//                System.out.println((final_costs[0] < d.distance_q) + " " +
//                        (final_costs[0] < this.dominated_checking.get(d.getPlaceId())) + " " +
//                        r);
//            }


                this.check_add_oper += System.nanoTime() - rrr;
                d1 += System.nanoTime() - rrr;
                long rrrr = System.nanoTime();
                this.sky_add_result_counter++;
                boolean t = addToSkyline(r);

//                if (d.getPlaceId() == 23) {
//                    System.out.println(r + " " + (final_costs[0] < d.distance_q) + " " + (final_costs[0] < this.dominated_checking.get(d.getPlaceId())) + " " + t);
//                }
                this.add_oper += System.nanoTime() - rrrr;
                d2 += System.nanoTime() - rrrr;
//                if (np.startNode.getId() == 0 && np.endNode.getId() == 7 && np.nodes.size() == 4 && np.rels.size() == 3) {
//                    System.out.println(t + "\n");
//                }
                if (!flag && t) {
                    flag = true;
                }
            }
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

    public long nearestNetworkNode(Data queryD) {

        Node nn_node = null;
        double distz = Double.MAX_VALUE;
        int counter_in_range = 0;

        try (Transaction tx = connector.graphDB.beginTx()) {

            ResourceIterable<Node> iter = connector.graphDB.getAllNodes();
            for (Node n : iter) {
                double lat = (double) n.getProperty("lat");
                double log = (double) n.getProperty("log");

                double temp_distz = Math.sqrt(Math.pow(lat - queryD.location[0], 2) + Math.pow(log - queryD.location[1], 2));
//                double temp_distz = GoogleMaps.distanceInMeters(lat, log, queryD.location[0], queryD.location[1]);
                if (distz > temp_distz) {
                    nn_node = n;
                    distz = temp_distz;
                }

//                if (temp_distz <= this.distance_threshold) {
//                    counter_in_range++;
//                }
            }

//            this.distance_threshold = distz;

            tx.success();
        }


//        System.out.println(counter_in_range + " bus stations within hotel " + this.distance_threshold);
        return nn_node.getId();
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


    public Data getDataById(int placeId) {
        BufferedReader br = null;
        int linenumber = 0;

        Data queryD = new Data(3);


        try {
            br = new BufferedReader(new FileReader(this.dataPath));
            String line = null;
            while ((line = br.readLine()) != null) {
                if (linenumber == placeId) {
//                    System.out.println(line);
                    String[] infos = line.split(",");
                    Double lat = Double.parseDouble(infos[1]);
                    Double log = Double.parseDouble(infos[2]);


                    Float c1 = Float.parseFloat(infos[3]);
                    Float c2 = Float.parseFloat(infos[4]);
                    Float c3 = Float.parseFloat(infos[5]);


                    queryD.setPlaceId(placeId);
                    queryD.setLocation(new double[]{lat, log});
                    queryD.setData(new float[]{c1, c2, c3});
                    break;
                } else {
                    linenumber++;
                }
            }
            br.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Can not open the file, please check it. ");
        }

        return queryD;

    }


    private int getRandomNumberInRange_int(int min, int max) {

        if (min >= max) {
            throw new IllegalArgumentException("max must be greater than min");
        }

        Random r = new Random();
        return r.nextInt((max - min) + 1) + min;
    }
}
