package BaseLine;

import RstarTree.Data;
import neo4jTools.connector;
import org.apache.commons.cli.*;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.Transaction;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class BaseMethod {
    private final long num_nodes;
    public ArrayList<Result> skyPaths = new ArrayList<>();
    Random r;
    String treePath;
    int graph_size;
    String degree;
    long add_oper = 0;
    long check_add_oper = 0;
    long map_operation = 0;
    long checkEmpty = 0;
    long read_data = 0;
    double threshold = 0;
    String home_folder = System.getProperty("user.home");
    private GraphDatabaseService graphdb;
    private HashMap<Long, myNode> tmpStoreNodes = new HashMap();
    private ArrayList<Data> sNodes = new ArrayList<>();
    private HashSet<Data> finalDatas = new HashSet<>();
    private long add_counter;
    private long pro_add_result_counter;
    private long sky_add_result_counter;
    private ArrayList<Data> sky_hotel;
    private boolean add;
    private String node_info_path;

//    HashMap<Integer, Double> dominated_checking = new HashMap<>();


    public BaseMethod(int graph_size, String degree, double threshold, double range, int hotels_num) {
        r = new Random();
        this.graph_size = graph_size;
        this.degree = degree;
        this.treePath = this.home_folder + "/shared_git/bConstrainSkyline/data/test_" + graph_size + "_" + degree + "_" + range + "_" + hotels_num + ".rtr";
        this.threshold = threshold;
        this.node_info_path = System.getProperty("user.home") + "/mydata/projectData/testGraph" + graph_size + "_" + degree + "/data/NodeInfo.txt";

        this.num_nodes = getLineNumbers();

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
                graph_size = 2000;
            } else {
                graph_size = Integer.parseInt(g_str);
            }

            if (de_str == null) {
                degree = "4";
            } else {
                degree = de_str;
            }

            if (qn_str == null) {
                query_num = 1;
            } else {
                query_num = Integer.parseInt(qn_str);
            }

            if (hn_str == null) {
                hotels_num = 1000;
            } else {
                hotels_num = Integer.parseInt(hn_str);
            }

            if (r_str == null) {
                range = 12;
            } else {
                range = Integer.parseInt(r_str);
            }

            if (t_str == null) {
                threshold = range;
            } else {
                threshold = Integer.parseInt(t_str);
            }

        }
    }

    public void baseline(Data queryD) {
        StringBuffer sb = new StringBuffer();
        sb.append(queryD.getPlaceId() + " ");
        long s_sum = System.currentTimeMillis();
        ArrayList<path> Results = new ArrayList<>();
        Skyline sky = new Skyline(treePath);
        long r1 = System.currentTimeMillis();
        sky.BBS(queryD);

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

        String graphPath = home_folder + "/neo4j334/testdb" + this.graph_size + "_" + this.degree + "/databases/graph.db";
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
            long rt = System.currentTimeMillis();


            for (int node_id = 0; node_id < num_nodes; node_id++) {
//                System.out.println(node_id);
                myNode s;
                if (!tmpStoreNodes.containsKey(node_id)) {
                    s = new myNode(queryD, node_id, -1);
                } else {
                    s = tmpStoreNodes.get(node_id);
//                    path dp = new path(s);
//                    s.addToSkyline(dp);
                }
                myNodePriorityQueue mqueue = new myNodePriorityQueue();
                mqueue.add(s);

                while (!mqueue.isEmpty()) {

                    myNode v = mqueue.pop();

                    counter++;
                    //if (++counter % 1000 == 0) {
                    //}
                    for (int i = 0; i < v.skyPaths.size(); i++) {
                        path p = v.skyPaths.get(i);

                        //constants.print(p.costs);
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

                                if (next_n.addToSkyline(np)) {
                                    mqueue.add(next_n);
                                }
                            }
                        }
                    }

//                break;
                }

            }


            long exploration_rt = System.currentTimeMillis() - rt;

            for (Map.Entry<Long, myNode> mm : this.tmpStoreNodes.entrySet()) {
                for (path np : mm.getValue().skyPaths) {
                    addToSkylineResult(np, queryD);
                }
            }

            sb.append(bbs_rt + "," + 0 + "," + exploration_rt + ",");

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


        sb.append(finalDatas.size() + " " + this.skyPaths.size());

        int visited_bus_stop = this.tmpStoreNodes.size();
        int bus_stop_in_result = final_bus_stops.size();

        sb.append("  " + visited_bus_stop + "," + bus_stop_in_result + "," + (double) bus_stop_in_result / visited_bus_stop + "   " + this.sky_add_result_counter);

        System.out.println(sb.toString());
    }

    private boolean addToSkylineResult(path np, Data queryD) {
        this.add_counter++;
        long r2a = System.nanoTime();
//        if (np.rels.isEmpty()) {
//            return false;
//        }

        if (np.isDummyPath()) {
            return false;
        }
        this.checkEmpty += System.nanoTime() - r2a;

        long rr = System.nanoTime();
        myNode my_endNode = this.tmpStoreNodes.get(np.endNode);
        this.map_operation += System.nanoTime() - rr;

        long dsad = System.nanoTime();
        long d1 = 0, d2 = 0;
        boolean flag = false;
        for (Data d : this.sNodes) {
            this.pro_add_result_counter++;
            long rrr = System.nanoTime();
            //this.read_data += (rrr - dsad);


            if (d.getPlaceId() == queryD.getPlaceId()) {
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

            this.sky_add_result_counter++;
            boolean t = addToSkyline(r);
            if (!flag && t) {
                flag = true;
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


    public long getLineNumbers() {
//        System.out.println(node_info_path);
        long lines = 0;
        try {
            File file = new File(this.node_info_path);
            FileReader fileReader = new FileReader(file);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            String line = "";
            while ((line = bufferedReader.readLine()) != null) {
                long l = Long.valueOf(line.split(" ")[0]);
                if (l > lines) {
                    lines = l;
                }
            }
            fileReader.close();

        } catch (IOException e) {
            e.printStackTrace();
        }

        return lines;
    }

}
