package testTools;

import BaseLine.Skyline;
import BaseLine.myNode;
import RstarTree.Data;
import neo4jTools.connector;
import org.apache.commons.cli.*;
import org.apache.commons.io.FileUtils;
import org.neo4j.graphdb.Transaction;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Random;

public class Index {
    private final String base = System.getProperty("user.home") + "/shared_git/bConstrainSkyline/data/index";
    public String home_folder;
    private String dataPath;
    private String treePath;
    private String graphPath;
    private int bits_place_id;
    private String source_data_tree;
    private String neo4j_db;
    //    private final int pagesize_data;
    private int pagesize_list;
    private String node_info_path;
    private long num_nodes;
    private double distance_threshold;


    public Index() {
        this.distance_threshold = 1000;

        this.home_folder = base + "/" + "SF" + "_index_1000/";
        this.graphPath = System.getProperty("user.home") + "/neo4j334/testdb_SF_Random/databases/graph.db";
        this.treePath = System.getProperty("user.home") + "/shared_git/bConstrainSkyline/data/real_tree_SF.rtr";
        this.dataPath = System.getProperty("user.home") + "/shared_git/bConstrainSkyline/data/staticNode_real_SF.txt";


        this.source_data_tree = this.treePath;
        this.neo4j_db = this.graphPath;
        this.node_info_path = System.getProperty("user.home") + "/mydata/projectData/testGraph_real_50_Random/data/"+"SF_"+"NodeInfo.txt";


        this.num_nodes = getLineNumbers();

        this.pagesize_list = 1024;
//        this.pagesize_data = 2048;
    }


    public Index(String city) {
        this.distance_threshold = 0.0105;

        this.home_folder = base + "/" + city + "_index_1000/";
        this.graphPath = System.getProperty("user.home") + "/neo4j334/testdb_"+city+"_Random/databases/graph.db";
        this.treePath = System.getProperty("user.home") + "/shared_git/bConstrainSkyline/data/real_tree_"+city+".rtr";
        this.dataPath = System.getProperty("user.home") + "/shared_git/bConstrainSkyline/data/staticNode_real_"+city+".txt";


        this.source_data_tree = this.treePath;
        this.neo4j_db = this.graphPath;
        this.node_info_path = System.getProperty("user.home") + "/mydata/projectData/testGraph_real_50_Random/data/"+city+"_NodeInfo.txt";


        this.num_nodes = getLineNumbers();

        this.pagesize_list = 1024;
//        this.pagesize_data = 2048;
    }


    public Index(int graphsize, String degree, double range, int num_hotels, double distance_thresholds) {
        this.distance_threshold = distance_thresholds;

        if (distance_thresholds != -1) {
            this.home_folder = base + "/test_" + graphsize + "_" + degree + "_" + range + "_" + num_hotels;
            this.source_data_tree = System.getProperty("user.home") + "/shared_git/bConstrainSkyline/data/test_" + graphsize + "_" + degree + "_" + range + "_" + num_hotels + ".rtr";
            this.neo4j_db = System.getProperty("user.home") + "/neo4j334/testdb" + graphsize + "_" + degree + "/databases/graph.db";
            this.node_info_path = System.getProperty("user.home") + "/mydata/projectData/testGraph" + graphsize + "_" + degree + "/data/NodeInfo.txt";
        } else {
            this.home_folder = base + "/test_" + graphsize + "_" + degree + "_" + range + "_" + num_hotels + "_all";
            this.source_data_tree = System.getProperty("user.home") + "/shared_git/bConstrainSkyline/data/test_" + graphsize + "_" + degree + "_" + range + "_" + num_hotels + ".rtr";
            this.neo4j_db = System.getProperty("user.home") + "/neo4j334/testdb" + graphsize + "_" + degree + "/databases/graph.db";
            this.node_info_path = System.getProperty("user.home") + "/mydata/projectData/testGraph" + graphsize + "_" + degree + "/data/NodeInfo.txt";
        }

//        System.out.println(home_folder);


        this.num_nodes = getLineNumbers();
//        System.out.println(this.home_folder);
//        System.out.println(this.source_data_tree);
//        System.out.println(this.neo4j_db);
//        System.out.println(node_info_path);

        this.pagesize_list = 1024;

    }

    public Index(double distance_threshold) {
        this.distance_threshold = distance_threshold;

        this.home_folder = base + "/real_data_" + this.distance_threshold + "/";
        this.source_data_tree = System.getProperty("user.home") + "/shared_git/bConstrainSkyline/data/real_tree.rtr";
        this.neo4j_db = System.getProperty("user.home") + "/neo4j334/testdb_real_50/databases/graph.db";
        this.node_info_path = System.getProperty("user.home") + "/mydata/projectData/testGraph_real_50_int/data/NodeInfo.txt";

//        this.source_data_tree = "/home/gqxwolf/shared_git/bConstrainSkyline/data/test_8000_4_8.0_5000.rtr";
//        this.neo4j_db = "/home/gqxwolf/neo4j334/testdb8000_4/databases/graph.db";
//        this.node_info_path = "/home/gqxwolf/mydata/projectData/testGraph8000_4/data/NodeInfo.txt";


        this.num_nodes = getLineNumbers();

        this.bits_place_id = Long.toBinaryString(this.num_nodes).length();

        this.pagesize_list = 1024;

    }

    public static void main(String args[]) throws ParseException {


        int graph_size, hotels_num;
        String degree;
        double range, distance_thresholds;

        Options options = new Options();
        options.addOption("g", "grahpsize", true, "number of nodes in the graph");
        options.addOption("de", "degree", true, "degree of the graphe");
        options.addOption("hn", "hotelsnum", true, "number of hotels in the graph");
        options.addOption("r", "range", true, "range of the distance to be considered");
        options.addOption("t", "distance_thresholds", true, "threshold within");
        options.addOption("h", "help", false, "print the help of this command");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        String g_str = cmd.getOptionValue("g");
        String de_str = cmd.getOptionValue("de");
        String hn_str = cmd.getOptionValue("hn");
        String r_str = cmd.getOptionValue("r");
        String t_str = cmd.getOptionValue("t");


        if (cmd.hasOption("h")) {
            HelpFormatter formatter = new HelpFormatter();
            String header = "Build index for given graph and data set :";
            formatter.printHelp("java -jar BuildIndex.jar", header, options, "", false);
        } else {

            if (g_str == null) {
                graph_size = 10000;
            } else {
                graph_size = Integer.parseInt(g_str);
            }

            if (de_str == null) {
                degree = "4";
            } else {
                degree = de_str;
            }

            if (hn_str == null) {
                hotels_num = 1000;
            } else {
                hotels_num = Integer.parseInt(hn_str);
            }

            if (r_str == null) {
                range = 4;
            } else {
                range = Double.parseDouble(r_str);
            }

            if (t_str == null) {
                distance_thresholds = range;
//                distance_thresholds = -1;
            } else {
                distance_thresholds = Double.parseDouble(t_str);
            }

            Index idx = new Index(graph_size, degree, range, hotels_num, distance_thresholds);
//            Index idx = new Index("LA");
            long st = System.currentTimeMillis();
            idx.buildIndex(true);
            System.out.println("index building finished in "+(System.currentTimeMillis()-st));
//            idx.read_d_list_from_disk(452);
//            idx.test();
        }

    }

    public ArrayList<Data> read_d_list_from_disk(long node_id) {

        String header_name = this.home_folder + "/header.idx";
        String list_name = this.home_folder + "/list.idx";
        String Data_file = this.home_folder + "/data.dat";
        ArrayList<Data> d_list = new ArrayList<>();

        try {

            RandomAccessFile header_f = new RandomAccessFile(header_name, "r");
            header_f.seek((node_id * 8));
            int pagenumber = header_f.readInt();
            int d_size = header_f.readInt();
//            System.out.println(node_id + "  " + d_size + "  " + pagenumber);

            RandomAccessFile list_f = new RandomAccessFile(list_name, "r");
            list_f.seek(pagenumber * pagesize_list);


            RandomAccessFile data_f = new RandomAccessFile(Data_file, "r");


            for (int i = 0; i < d_size; i++) {
                int d_id = list_f.readInt();
                Data d = new Data(3);
                data_f.seek(d_id * d.get_size());
                byte[] b_d = new byte[d.get_size()];
                data_f.read(b_d);
                d.read_from_buffer(b_d);
                d_list.add(d);
//                System.out.println(d_id);
            }

            data_f.close();
            header_f.close();
            list_f.close();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return d_list;
    }

    private void test() {
        String Data_file = this.home_folder + "/data.dat";
        System.out.println(Data_file);
        try {

            long d_id = getRandomNumberInRange_int(0, 1000);
            RandomAccessFile data_f = new RandomAccessFile(Data_file, "rw");
            Data d = new Data(3);
            data_f.seek(d_id * d.get_size());
            System.out.println(d.get_size());
            byte[] b_d = new byte[d.get_size()];
            data_f.read(b_d);
            d.read_from_buffer(b_d);
            System.out.println(d);

            String header_name = this.home_folder + "/header.idx";
            String list_name = this.home_folder + "/list.idx";
            RandomAccessFile header_f = new RandomAccessFile(header_name, "rw");
            long node_id = getRandomNumberInRange_int(0, 1000);
            header_f.seek((678 * 8));
            RandomAccessFile list_f = new RandomAccessFile(list_name, "rw");
            list_f.seek(678 * pagesize_list);

            int pagenumber = header_f.readInt();
            int d_size = header_f.readInt();
            System.out.println(node_id + "  " + d_size + "  " + pagenumber);

            for (int i = 0; i < d_size; i++) {
                System.out.println(list_f.readInt());
            }


            data_f.close();
            header_f.close();
            list_f.close();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void writeDataToDisk(ArrayList<Data> allNodes) {
        ArrayList<Data> an = new ArrayList<>(allNodes);
        String Data_file = this.home_folder + "/data.dat";
        try {
            RandomAccessFile data_f = new RandomAccessFile(Data_file, "rw");
            data_f.seek(0);

            Collections.sort(an, new Comparator<Data>() {
                @Override
                public int compare(Data lhs, Data rhs) {
                    return lhs.getPlaceId() > rhs.getPlaceId() ? 1 : (lhs.getPlaceId() < rhs.getPlaceId()) ? -1 : 0;
                }
            });


            for (int i = 0; i < an.size(); i++) {
                Data d = an.get(i);
                byte[] b_d = new byte[d.get_size()];
                d.write_to_buffer(b_d);
                data_f.write(b_d);
            }

            data_f.close();


        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    public void buildIndex(boolean deleteBeforeBuild) {
        if (deleteBeforeBuild) {
            File dataF = new File(home_folder);
            try {
                FileUtils.deleteDirectory(dataF);
                dataF.mkdirs();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        Skyline sk = new Skyline(this.source_data_tree);
        sk.allDatas(); //get all data objects from R-tree
        sk.findSkyline(); //get skyline objects among all the data objects
        System.out.println("number of data objects "+sk.allNodes.size());
        System.out.println("number of skyline data objects:"+sk.sky_hotels.size());
        writeDataToDisk(sk.allNodes);

        String header_name = this.home_folder + "/header.idx";
        String list_name = this.home_folder + "/list.idx";

        System.out.println(this.num_nodes);


        try {
            connector n = new connector(this.neo4j_db);
            n.startDB();
            RandomAccessFile header_f = new RandomAccessFile(header_name, "rw");
            header_f.seek(0);
            RandomAccessFile list_f = new RandomAccessFile(list_name, "rw");
            list_f.seek(0);


            int page_list_number = 0;
            for (int node_id = 0; node_id <= num_nodes; node_id++) {
                if (node_id % 1000 == 0) {
                    System.out.println("========================" + node_id + "=========================");
                }
//                if (node_id == 3) {
//                    break;
//                }
                try (Transaction tx = connector.graphDB.beginTx()) {
                    myNode node = new myNode(node_id);

                    ArrayList<Data> d_list;
                    if (this.distance_threshold == -1) {
                        d_list = new ArrayList<>(sk.sky_hotels);
                    } else {
                        d_list = new ArrayList<>();
                        for (Data d : sk.sky_hotels) {
                            double d2 = Math.sqrt(Math.pow(node.locations[0] - d.location[0], 2) + Math.pow(node.locations[1] - d.location[1], 2));
//                            double d2 = GoogleMaps.distanceInMeters(node.locations[0], node.locations[1], d.location[0], d.location[1]);
                            if (d2 < this.distance_threshold) {
                                d_list.add(d);
                            }
                        }

                    }

                    //if we can find the distance from the bus_stop n to the hotel d is shorter than the distance to one of the skyline hotels s_d
                    //It means the hotel could be a candidate hotel of the bus stop n.
                    for (Data d : sk.allNodes) {
                        boolean flag = true;
                        //distance from node to d
                        double d2 = Math.sqrt(Math.pow(node.locations[0] - d.location[0], 2) + Math.pow(node.locations[1] - d.location[1], 2));
//                        double d2 = GoogleMaps.distanceInMeters(node.locations[0], node.locations[1], d.location[0], d.location[1]);

                        double min_dist = Double.MAX_VALUE;
                        for (Data s_d : sk.sky_hotels) {
                            //distance from node to the skyline data s_d
                            double d1 = Math.sqrt(Math.pow(node.locations[0] - s_d.location[0], 2) + Math.pow(node.locations[1] - s_d.location[1], 2));
//                            double d1 = GoogleMaps.distanceInMeters(node.locations[0], node.locations[1], s_d.location[0], s_d.location[1]);

                            if (checkDominated(s_d.getData(), d.getData()) && d1 < min_dist) {
                                if (distance_threshold == -1) {
                                    min_dist = d1;
                                } else {
                                    if (d1 < this.distance_threshold) {
                                        min_dist = d1;
                                    }
                                }
                            }
                        }

                        if (this.distance_threshold != -1) {

                            if (min_dist > d2 && this.distance_threshold > d2) {
                                d_list.add(d);
//                                if (d.getPlaceId() == 394 && node_id == 453) {
//                                    System.out.println(distance_threshold + " " + d2 + " " + (this.distance_threshold > d2));
//                                }
                            }
                        } else {
//                            System.out.println("--------------------");
                            if (min_dist > d2) {
                                d_list.add(d);
                            }
                        }

                    }


//                    System.out.println(d_list.size());

//                    ArrayList<Data> d_list = new ArrayList<>(sk.sky_hotels);
//                    //if we can find the distance from the bus_stop n to the hotel d is shorter than the distance to one of the skyline hotels s_d
//                    //It means the hotel could be a candidate hotel of the bus stop n.
//                    for (Data d : sk.allNodes) {
//                        for (Data s_d : sk.sky_hotels) {
////                            double d1 = GoogleMaps.distanceInMeters(node.locations[0], node.locations[1], s_d.location[0], s_d.location[1]);
////                            double d2 = GoogleMaps.distanceInMeters(node.locations[0], node.locations[1], d.location[0], d.location[1]);
//                            double d1 = Math.sqrt(Math.pow(node.locations[0] - s_d.location[0], 2) + Math.pow(node.locations[1] - s_d.location[1], 2));
//                            double d2 = Math.sqrt(Math.pow(node.locations[0] - d.location[0], 2) + Math.pow(node.locations[1] - d.location[1], 2));
//                            if (this.distance_threshold != -1) {
//                                if (d1 > d2 && this.distance_threshold > d2 && checkDominated(s_d.getData(), d.getData())) {
//                                    d_list.add(d);
//                                    break;
//                                }
//                            } else {
//                                if (d1 > d2 && checkDominated(s_d.getData(), d.getData())) {
//                                    d_list.add(d);
//                                    break;
//                                }
//                            }
//
//                        }
//                    }

                    int d_size = d_list.size();


                    header_f.writeInt(page_list_number); //start page of the list file
                    header_f.writeInt(d_size); //the size of the list of current node

//                    if(node.id == 452)
//                    {
//                        System.out.println(d_list.size()+" "+page_list_number);
//                    }
//                    System.out.println(d_list.size());
                    int records = 0;
                    for (Data d : d_list) {
//                        if (node_id == 453) {
//                            System.out.println(d);
//                        }
                        list_f.writeInt(d.PlaceId);
//                        if(node.id == 452)
//                        {
//                            System.out.println(d.PlaceId);
//                        }
                        records++;
                        //if page is full, page number ++
                        if ((this.pagesize_list / 4) < (records + 1)) {
                            page_list_number++;
                        }
                    }

                    //fill the remainning page with -1.
                    long list_end = list_f.getFilePointer();
                    for (long i = list_end; i < (page_list_number + 1) * this.pagesize_list; i++) {
                        list_f.writeByte(-1);
                    }

                    page_list_number++;

                    tx.success();
                }
            }

            header_f.close();
            list_f.close();
            n.shutdownDB();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }


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

    private boolean checkDominated(double[] costs, double[] estimatedCosts) {
        for (int i = 0; i < costs.length; i++) {
            if (costs[i] * (1.0) > estimatedCosts[i]) {
                return false;
            }
        }
        return true;
    }

    private int getRandomNumberInRange_int(int min, int max) {

        if (min >= max) {
            throw new IllegalArgumentException("max must be greater than min");
        }

        Random r = new Random();
        return r.nextInt((max - min) + 1) + min;
    }
}
