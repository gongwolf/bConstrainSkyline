package BaseLine;

import RstarTree.Data;
import neo4jTools.connector;
import org.apache.commons.cli.*;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.Transaction;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;

public class BaseMethod5 {
    private static int nn_dist;
    public ArrayList<path> qqqq = new ArrayList<>();
    Random r = new Random(System.nanoTime());
    String treePath;
    String dataPath;
    int graph_size;
    String degree;
    String graphPath = "/home/gqxwolf/neo4j334/testdb" + this.graph_size + "_" + this.degree + "/databases/graph.db";
    long add_oper = 0;
    long check_add_oper = 0;
    long map_operation = 0;
    long checkEmpty = 0;
    long read_data = 0;
    //Todo: each hotel know the distance to the hotel than dominate it.
    HashMap<Integer, Double> dominated_checking = new HashMap<>(); //
    String home_folder = System.getProperty("user.home");
    private GraphDatabaseService graphdb;
    private HashMap<Long, myNode> tmpStoreNodes = new HashMap();
    private ArrayList<Data> sNodes = new ArrayList<>();
    private ArrayList<Result> skyPaths = new ArrayList<>();
    private ArrayList<Data> sky_hotel;
    private HashSet<Data> finalDatas = new HashSet<>();
    private int checkedDataId = 9;
    private long add_counter; // how many times call the addtoResult function
    private long pro_add_result_counter; // how many path + hotel combination of the results are generated
    private long sky_add_result_counter; // how many results are taken the addtoskyline operation
    private Data queryD;
    private long d_list_num = 0;

    public BaseMethod5(int graph_size, String degree, double range, int hotels_num) {
        r = new Random(System.nanoTime());
        this.graph_size = graph_size;
        this.degree = degree;
        this.graphPath = home_folder + "/neo4j334/testdb" + this.graph_size + "_" + this.degree + "/databases/graph.db";
        this.treePath = home_folder + "/shared_git/bConstrainSkyline/data/test_" + this.graph_size + "_" + this.degree + "_" + range + "_" + hotels_num + ".rtr";
        this.dataPath = home_folder + "/shared_git/bConstrainSkyline/data/staticNode_" + this.graph_size + "_" + this.degree + "_" + range + "_" + hotels_num + ".txt";
//        this.treePath= "/home/gqxwolf/shared_git/bConstrainSkyline/data/test.rtr";
//        System.out.println(treePath);
    }

    public static void main(String args[]) throws ParseException {
        int graph_size, query_num, hotels_num;
        String degree;
        double range, threshold;

        Options options = new Options();
        options.addOption("g", "grahpsize", true, "number of nodes in the graph");
        options.addOption("de", "degree", true, "degree of the graphe");
        options.addOption("qn", "querynum", true, "number of querys");
        options.addOption("hn", "hotelsnum", true, "number of hotels in the graph");
        options.addOption("r", "range", true, "range of the distance to be considered");
        options.addOption("t", "threshold", true, "Just consider the bus stops in threshold");
        options.addOption("h", "help", false, "print the help of this command");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        String g_str = cmd.getOptionValue("g");
        String de_str = cmd.getOptionValue("de");
        String qn_str = cmd.getOptionValue("qn");
        String hn_str = cmd.getOptionValue("hn");
        String r_str = cmd.getOptionValue("r");
        String t_str = cmd.getOptionValue("t");


        if (cmd.hasOption("h")) {
            HelpFormatter formatter = new HelpFormatter();
            String header = "Run the code of base line 5 :";
            formatter.printHelp("java -jar Baseline_5.jar", header, options, "", false);
        } else {

            if (g_str == null) {
                graph_size = 4000;
            } else {
                graph_size = Integer.parseInt(g_str);
            }

            if (de_str == null) {
                degree = "4";
            } else {
                degree = de_str;
            }

            if (qn_str == null) {
                query_num = 3;
            } else {
                query_num = Integer.parseInt(qn_str);
            }

            if (hn_str == null) {
                hotels_num = 2000;
            } else {
                hotels_num = Integer.parseInt(hn_str);
            }

            if (r_str == null) {
                range = 10;
            } else {
                range = Integer.parseInt(r_str);
            }

            if (t_str == null) {
                threshold = range;
            } else {
                threshold = Integer.parseInt(t_str);
            }

//            int[] id_list = new int[]{462,472,791};
            Data[] queryList = new Data[query_num];


            for (int i = 0; i < query_num; i++) {
                BaseMethod5 bm5 = new BaseMethod5(graph_size, degree, range, hotels_num);
                int random_place_id = bm5.getRandomNumberInRange_int(0, hotels_num - 1);
                Data queryD = bm5.getDataById(random_place_id);
                queryList[i] = queryD;
            }


            for (int i = 0; i < query_num; i++) {
//                BaseMethod base = new BaseMethod(graph_size, degree, threshold, range, hotels_num);
//                base.baseline(queryList[i]);
//
//
                BaseMethod1 bMethod = new BaseMethod1(graph_size, degree, threshold, range, hotels_num);
                bMethod.baseline(queryList[i]);
            }

            System.out.println("=================================");

            for (int i = 0; i < query_num; i++) {
                BaseMethod5 all_lemmas = new BaseMethod5(graph_size, degree, range, hotels_num);
                all_lemmas.baseline(queryList[i]);
            }

//                BaseMethod_index bs_index = new BaseMethod_index(graph_size, degree, range, hotels_num, threshold);
//                bs_index.baseline(queryList[i]);
//
//
//                BaseMethod_subPath approx_sub = new BaseMethod_subPath(graph_size, degree, threshold, range, hotels_num);
//                approx_sub.baseline(queryList[i]);

//                testTools.statistic.goodnessAnalyze(all_lemmas.skyPaths, approx_method.skyPaths, "edu");
//                testTools.statistic.goodnessAnalyze(all_lemmas.skyPaths, approx_sub.skyPaths, "edu");


//                for(Result r : approx_sub.skyPaths)
//                {
//                    System.out.println(r);
//                }
        }

    }

    public void baseline(Data queryD) {
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

        long s_sum = System.currentTimeMillis();
        long index_s = 0;
        int sk_counter = 0; //the number of total candidate hotels of each bus station

        long r1 = System.currentTimeMillis();
        //Find the hotels that aren't dominated by the query point
        sky.BBS(queryD);
        long bbs_rt = System.currentTimeMillis() - r1;
        sNodes = sky.skylineStaticNodes;
        sb.append(this.sNodes.size() + " " + this.sky_hotel.size() + " ");

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
//        System.out.println(this.skyPaths.size());
//        for (Result r : this.skyPaths) {
//            System.out.println(r);
//        }
//        System.out.println("=====================================================");


        //find the minimum distance from query point to the skyline hotel that dominate non-skyline hotel cand_d
        for (Data cand_d : sNodes) {
            double h_to_h_dist = Double.MAX_VALUE;

            if (!sky_hotel.contains(cand_d)) {
                for (Data s_h : sky_hotel) {
                    if (checkDominated(s_h.getData(), cand_d.getData())) {
//                        double tmep_dist = Math.pow(s_h.location[0] - queryD.location[0], 2) + Math.pow(s_h.location[1] - queryD.location[1], 2);
//                        tmep_dist = Math.sqrt(tmep_dist);
                        double tmep_dist = s_h.distance_q;
                        if (tmep_dist < h_to_h_dist) {
                            h_to_h_dist = tmep_dist;
                        }
                    }
                }
            }

            dominated_checking.put(cand_d.getPlaceId(), h_to_h_dist);
        }

//        System.out.println("==========" + this.skyPaths.size());


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

            long rt = System.currentTimeMillis();

            myNode s = new myNode(queryD, startNode.getId(), -1);

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
                            if (this.tmpStoreNodes.containsKey(np.endNode)) {
                                next_n = tmpStoreNodes.get(np.endNode);
                            } else {
                                next_n = new myNode(queryD, np.endNode, -1);
                                this.tmpStoreNodes.put(next_n.id, next_n);
                            }

                            //lemma 2
                            if (!(this.tmpStoreNodes.get(np.startNode).distance_q > next_n.distance_q)) {
                                if (next_n.addToSkyline(np)) {
                                    mqueue.add(next_n);
                                }
                            }
                        }


                    }
                }
            }

            long exploration_rt = System.currentTimeMillis() - rt;

            long tt_sl = 0;


//            hotels_scope = new HashMap<>();
            for (Map.Entry<Long, myNode> entry : tmpStoreNodes.entrySet()) {
                sk_counter += entry.getValue().skyPaths.size();
                myNode my_n = entry.getValue();

                long t_index_s = System.nanoTime();

//                ArrayList<Data> d_list = new ArrayList<>(this.sky_hotel);
//                //if we can find the distance from the bus_stop n to the hotel d is shorter than the distance to one of the skyline hotels s_d
//                //It means the hotel could be a candidate hotel of the bus stop n.
//                for (Data d : this.sNodes) {
//                    for (Data s_d : this.sky_hotel) {
//                        double d1 = Math.sqrt(Math.pow(my_n.locations[0] - s_d.location[0], 2) + Math.pow(my_n.locations[1] - s_d.location[1], 2));
//                        double d2 = Math.sqrt(Math.pow(my_n.locations[0] - d.location[0], 2) + Math.pow(my_n.locations[1] - d.location[1], 2));
//                        if (checkDominated(s_d.getData(), d.getData()) && d1 > d2) {
//                            d_list.add(d);
//                            break;
//                        }
//                    }
//                }

//                if (my_n.id == 1) {
//                    System.out.println("============================");
//                    System.out.println(sNodes.size() + "    " + skyPaths.size() + " " + d_list.size());
//                    for (Data d : d_list) {
//                        System.out.println(d);
//                    }
//                    System.out.println("============================");
//                }

                index_s += (System.nanoTime() - t_index_s);

                for (path p : my_n.skyPaths) {
//                    if (!p.rels.isEmpty()) {
                    long ats = System.nanoTime();

                    boolean f = addToSkylineResult(p, sNodes);

                    addResult_rt += System.nanoTime() - ats;
//                    }
                }


            }


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
//        sb.append("\nadd_to_Skyline_result " + this.add_counter + "  " + this.pro_add_result_counter + "  " + this.sky_add_result_counter + " ");
//        sb.append((double) this.sky_add_result_counter / this.pro_add_result_counter);

        List<Result> sortedList = new ArrayList(this.skyPaths);
        Collections.sort(sortedList);

        HashSet<Long> final_bus_stops = new HashSet<>();

        for (Result r : sortedList) {
            this.finalDatas.add(r.end);

//            if (r.p != null) {
//                for (Long nn : r.p.nodes) {
//                    final_bus_stops.add(nn);
//                }
//            }
        }


        sb.append(finalDatas.size() + " " + this.skyPaths.size() + " " + sk_counter + "  " + add_counter + " ");

        int visited_bus_stop = this.tmpStoreNodes.size();
        int bus_stop_in_result = final_bus_stops.size();

        sb.append("  " + visited_bus_stop + "," + bus_stop_in_result + "," + (double) bus_stop_in_result / visited_bus_stop + "   " + this.sky_add_result_counter);

        sb.append(" " + sNodes.size());

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
//    private boolean addToSkylineResult(path np, Data d) {
        this.add_counter++;
        long r2a = System.nanoTime();
//        if (np.rels.isEmpty()) {
//            return false;
//        }
        if (np.isDummyPath()) {
            return false;
        }
        this.checkEmpty += System.nanoTime() - r2a;


//        if (np.startNode.getId() == 286 && np.endNode.getId() == 1862) {
//            System.out.println(np);
//
//            for (Data d : d_list) {
//                if (d.getPlaceId() == 62) {
//                    System.out.println("true");
//                    System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
//                }
//            }
//        }


        long rr = System.nanoTime();
        myNode my_endNode = this.tmpStoreNodes.get(np.endNode);
        this.map_operation += System.nanoTime() - rr;

        long dsad = System.nanoTime();
        long d1 = 0, d2 = 0;
        boolean flag = false;

        for (Data d : d_list) {
//            if (np.startNode.getId() == 286 && np.endNode.getId() == 1862) {
//                if (d.getPlaceId() == 62) {
//                    System.out.println("true");
//                }
//            }

            this.pro_add_result_counter++;
            long rrr = System.nanoTime();

            if (d.getPlaceId() == queryD.getPlaceId()) {
                continue;
            }

            double[] final_costs = new double[np.costs.length + 3];
            System.arraycopy(np.costs, 0, final_costs, 0, np.costs.length);
            double end_distance = Math.sqrt(Math.pow(my_endNode.locations[0] - d.location[0], 2) + Math.pow(my_endNode.locations[1] - d.location[1], 2));

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

        nn_dist = (int) Math.ceil(distz);
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
