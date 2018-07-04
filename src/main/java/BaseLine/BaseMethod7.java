package BaseLine;

import RstarTree.Data;
import javafx.util.Pair;

import java.io.*;
import java.util.*;

public class BaseMethod7 {
    private final String graphPath;
    private final String dataPath;
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
    private HashMap<Long, myNode> tmpStoreNodes = new HashMap();
    private ArrayList<Data> sNodes = new ArrayList<>();
    private HashSet<Data> finalDatas = new HashSet<>();
    private long add_counter;
    private long pro_add_result_counter;
    private long sky_add_result_counter;
    private ArrayList<Data> sky_hotel;
    private boolean add;


//    HashMap<Integer, Double> dominated_checking = new HashMap<>();


    public BaseMethod7(int graph_size, String degree, double threshold, double range, int hotels_num) {
        r = new Random();
        this.graph_size = graph_size;
        this.degree = degree;
        this.graphPath = home_folder + "/neo4j334/testdb" + this.graph_size + "_" + this.degree + "/databases/graph.db";
        this.treePath = home_folder + "/shared_git/bConstrainSkyline/data/test_" + this.graph_size + "_" + this.degree + "_" + range + "_" + hotels_num + ".rtr";
        this.dataPath = home_folder + "/shared_git/bConstrainSkyline/data/staticNode_" + this.graph_size + "_" + this.degree + "_" + range + "_" + hotels_num + ".txt";
        this.threshold = threshold;
    }

    public BaseMethod7(String city) {
        r = new Random();
        this.threshold = threshold;
        this.graphPath = home_folder + "/neo4j341/testdb_" + city + "_" + "uniform_1/databases/graph.db";
        this.treePath = home_folder + "/shared_git/bConstrainSkyline/data/real_tree_" + city + ".rtr";
        this.dataPath = home_folder + "/shared_git/bConstrainSkyline/data/staticNode_real_" + city + ".txt";
    }

    public BaseMethod7(String tree, String data, String graph) {
        this.graphPath = graph;
        this.treePath = tree;
        this.dataPath = data;
    }

    public BaseMethod7(String city, String graphPath) {
        r = new Random();
        this.threshold = threshold;
        this.graphPath = graphPath;
        this.treePath = home_folder + "/shared_git/bConstrainSkyline/data/real_tree_" + city + ".rtr";
        this.dataPath = home_folder + "/shared_git/bConstrainSkyline/data/staticNode_real_" + city + ".txt";
    }

    public static void main(String args[]) {
        System.out.println("start ....................");
        String graphPath1 = System.getProperty("user.home") + "/neo4j341/testdb_LA_uniform/databases/graph.db";
        String graphPath2 = System.getProperty("user.home") + "/neo4j341/testdb_LA_uniform_1/databases/graph.db";
        String graphPath3 = System.getProperty("user.home") + "/neo4j341/testdb_LA_normal/databases/graph.db";


        int qn = 1;
        int[] ids = new int[qn];

        for (int i = 0; i < qn; i++) {
            BaseMethod7 bs1 = new BaseMethod7("LA", graphPath1);
            int id = bs1.getRandomNumberInRange_int(0, bs1.getNumberOfHotels() - 1);
            ids[i] = 5130;
            System.out.println(ids[i]);
        }

        for (int i = 0; i < qn; i++) {
            BaseMethod7 bs1 = new BaseMethod7("LA", graphPath1);
            bs1.test1(ids[i]);
            System.out.println("----------------------------------");
        }
        //System.out.println("###################################");

        //for (int i = 0; i < qn; i++) {
        //    BaseMethod1 bs2 = new BaseMethod1("LA", graphPath2);
        //    bs2.test1(ids[i]);
        //    System.out.println("----------------------------------");
        //}
        System.out.println("###################################");
        for (int i = 0; i < qn; i++) {
            BaseMethod7 bs3 = new BaseMethod7("LA", graphPath3);
            bs3.test1(ids[i]);
            System.out.println("----------------------------------");
        }
    }

    public void test1(int id) {
        Data query = getDataById(id);
        constants.readData(this.graphPath);
        baseline(query);
    }

    public void baseline(Data queryD) {
        long max_queue_size = 0;
        long expand_path_num = 0;
        long counter = 1;
        long sk_counter = 0;
        StringBuffer sb = new StringBuffer();

        myNodePriorityQueue mqueue = new myNodePriorityQueue();

        try {
            sb.append(queryD.getPlaceId() + " ");
            long s_sum = System.currentTimeMillis();
            ArrayList<path> Results = new ArrayList<>();
            Skyline sky = new Skyline(treePath);
            long r1 = System.currentTimeMillis();
            sky.BBS(queryD);
            sNodes = sky.skylineStaticNodes;


            long bbs_rt = System.currentTimeMillis() - r1;


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

            long db_time = System.currentTimeMillis();
            long addResult_rt = 0;
            long expasion_rt = 0;
            long nn_rt, rt;


            db_time = System.currentTimeMillis() - db_time;
            r1 = System.currentTimeMillis();
            long startNode_id = nearestNetworkNode(queryD);

            nn_rt = System.currentTimeMillis() - r1;


            rt = System.currentTimeMillis();

            myNode s = new myNode(queryD, startNode_id, -1);
            mqueue.add(s);


            this.tmpStoreNodes.put(s.id, s);
            System.out.println("find nn finished " + s.id);


            while (!mqueue.isEmpty()) {
                if (mqueue.size() >= max_queue_size) {
                    max_queue_size = mqueue.size();
                }

                counter++;

                myNode v = mqueue.pop();
                v.inqueue = false;

//                if (counter >= 202703) {
//                    System.out.println("  " + v.id + " " + v.skyPaths.size());
//                }


                ArrayList<Pair<Pair<Long, Long>, double[]>> outgoingEdges = v.getNextEdges();
//                System.out.println(outgoingEdges.size());
                for (int i = 0; i < v.skyPaths.size(); i++) {
                    path p = v.skyPaths.get(i);
                    if (!p.expaned) {
                        p.expaned = true;

                        long ee = System.nanoTime();
                        expasion_rt += (System.nanoTime() - ee);
                        for (Pair<Pair<Long, Long>, double[]> oe : outgoingEdges) {

                            path np = new path(p, oe.getKey().getValue(), oe.getKey().getKey());
                            np.calculateCosts(oe.getValue());
                            myNode next_n;

                            if (this.tmpStoreNodes.containsKey(np.endNode)) {
                                next_n = tmpStoreNodes.get(np.endNode);
                            } else {
                                next_n = new myNode(queryD, np.endNode, -1);
                                this.tmpStoreNodes.put(next_n.id, next_n);
                            }

                            if (next_n.addToSkyline(np)) {
                                expand_path_num++;
                                if (!next_n.inqueue) {
                                    mqueue.add(next_n);
                                    next_n.inqueue = true;
                                }
                            }
                        }
                    }
                }


//                break;
            }

            long exploration_rt = System.currentTimeMillis() - rt;
            System.out.println("expasion finised in " + exploration_rt);
//            System.out.println("expansion finished " + exploration_rt);

            for (Map.Entry<Long, myNode> mm : this.tmpStoreNodes.entrySet()) {
                sk_counter += mm.getValue().skyPaths.size();
//                    for (path np : mm.getValue().skyPaths) {
//                        addToSkylineResult(np, queryD);
//                    }
            }

            sb.append(bbs_rt + "," + nn_rt + "," + exploration_rt + ",");

            //tx.success();
//            }
            long shut_db_time = System.currentTimeMillis();
//            n.shutdownDB();
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

//            for (Result r : sortedList) {
//                this.finalDatas.add(r.end);
//
//                if (r.p != null) {
//                    for (Long nn : r.p.nodes) {
//                        final_bus_stops.add(nn);
//                    }
//                }
//            }


            sb.append(finalDatas.size() + " " + this.skyPaths.size());

            int visited_bus_stop = this.tmpStoreNodes.size();
            int bus_stop_in_result = final_bus_stops.size();

            sb.append("  " + visited_bus_stop + "," + bus_stop_in_result + "," + (double) bus_stop_in_result / visited_bus_stop + "   " + this.sky_add_result_counter);

            sb.append(" ").append(sk_counter);

            //System.out.println(sb.toString());
            System.out.println(counter + "   " + max_queue_size + "   " + expand_path_num + "  " + ((double) expand_path_num / counter) + " " + sk_counter);
        } catch (Exception e) {
            for (Map.Entry<Long, myNode> mm : this.tmpStoreNodes.entrySet()) {
                sk_counter += mm.getValue().skyPaths.size();
            }
            System.err.println("Exception" + counter + "   " + max_queue_size + "   " + expand_path_num + "  " + ((double) expand_path_num / counter) + " " + sk_counter);
            e.printStackTrace();
        } catch (Error r) {
            for (Map.Entry<Long, myNode> mm : this.tmpStoreNodes.entrySet()) {
                sk_counter += mm.getValue().skyPaths.size();
            }
            System.err.println("Error: " + counter + "   " + max_queue_size + "   " + expand_path_num + "  " + ((double) expand_path_num / counter) + " " + sk_counter);
            r.printStackTrace();
            System.out.println("~~~~~~~~~~~~");
        } finally {

        }

    }

    private boolean addToSkylineResult(path np, Data queryD) {
        this.add_counter++;
        long r2a = System.nanoTime();
//        if (np.rels.isEmpty()) {
//            return false;
//        }

//        if (np.isDummyPath()) {
//            return false;
//        }
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
//            double end_distance = GoogleMaps.distanceInMeters(my_endNode.locations[0], my_endNode.locations[1], d.location[0], d.location[1]);

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

    public long nearestNetworkNode(Data queryD) {

        long nn_node = 0;
        double distz = Float.MAX_VALUE;

        for (Map.Entry<Long, Pair<Double, Double>> n : constants.nodes.entrySet()) {
            double lat = n.getValue().getKey();
            double log = n.getValue().getValue();
            double temp_distz = (Math.pow(lat - queryD.location[0], 2) + Math.pow(log - queryD.location[1], 2));
            if (distz > temp_distz) {
                nn_node = n.getKey();
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


    public int getNumberOfHotels() {
        int result = 0;
        File f = new File(this.dataPath);
        BufferedReader b = null;
        try {
            b = new BufferedReader(new FileReader(f));
            String readLine = "";

            while (((readLine = b.readLine()) != null)) {
                result++;
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return result;
    }

    public int getRandomNumberInRange_int(int min, int max) {

        if (min >= max) {
            throw new IllegalArgumentException("max must be greater than min");
        }

        Random r = new Random();
        return r.nextInt((max - min) + 1) + min;
    }


}