package testTools;

import RstarTree.Constants;
import RstarTree.Data;
import RstarTree.Node;
import RstarTree.RTree;
import javafx.util.Pair;
import org.apache.commons.cli.*;
import org.apache.commons.math3.distribution.BetaDistribution;

import java.io.*;
import java.util.HashSet;
import java.util.Random;

public class SyntheticData {
    private final int numberOfNodes;
    private final int dimension;
    private final int grahsize;
    private final int degree;
    private final int upper;
    private final String info_path;
    Random r = new Random(System.nanoTime());
    HashSet<Pair<Double, Double>> busLocation = new HashSet<>();
    BetaDistribution bt = new BetaDistribution(2, 19);
    private double range = 0;

    public SyntheticData(int numberOfNodes, int dimension, int graphsize, int degree, double range, int upper) {
        this.numberOfNodes = numberOfNodes;
        this.dimension = dimension;

        this.grahsize = graphsize;
        this.degree = degree;
        this.info_path = "/home/gqxwolf/mydata/projectData/testGraph" + this.grahsize + "_" + this.degree + "/data";

        this.upper = upper;

        this.range = range;
    }

    public static void arguements() {


    }

    public static void main(String args[]) throws ParseException {
        //deal with the parameter
        int numberOfNodes, dimension, graphsize, degree, upper;
        double range;

        Options options = new Options();
        options.addOption("g", "grahpsize", true, "number of nodes in the graph");
        options.addOption("n", "numberofNode", true, "number of nodes you want to generate");
        options.addOption("di", "dimension", true, "number of dimension");
        options.addOption("r", "range", true, "range of the distance to be considered");
        options.addOption("de", "degree", true, "degree of the graphe");
        options.addOption("u", "upperbound", true, "upper bound of the beta distribution sampling");
        options.addOption("h","help",false,"print the help of this command");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        String g_str = cmd.getOptionValue("g");
        String n_str = cmd.getOptionValue("n");
        String di_str = cmd.getOptionValue("di");
        String r_str = cmd.getOptionValue("r");
        String de_str = cmd.getOptionValue("de");
        String u_str = cmd.getOptionValue("u");
        if (cmd.hasOption("h")) {
            HelpFormatter formatter = new HelpFormatter();
            String header = "Generate Synthetic Data, the number of bus stop within the given range follows the beta distribution:";
            formatter.printHelp("java -jar SyntheticData.jar", header, options, "", false);
        } else {

            if (g_str == null) {
                graphsize = 2000;
            } else {
                graphsize = Integer.parseInt(g_str);
            }

            if (n_str == null) {
                numberOfNodes = 1000;
            } else {
                numberOfNodes = Integer.parseInt(n_str);
            }

            if (di_str == null) {
                dimension = 3;
            } else {
                dimension = Integer.parseInt(di_str);
            }

            if (r_str == null) {
                range = 10;
            } else {
                range = Double.parseDouble(r_str);
            }

            if (de_str == null) {
                degree = 4;
            } else {
                degree = Integer.parseInt(de_str);
            }

            if (u_str == null) {
                upper = 60;
            } else {
                upper = Integer.parseInt(u_str);
            }


            SyntheticData sd = new SyntheticData(numberOfNodes, dimension, graphsize, degree, range, upper);
            sd.test();
//        sd.createStaticNodes(numberOfNodes, dimension);
            sd.testStaticRTree();

        }


    }

    private void test() {
        readNodeInfo();

        String treePath = "/home/gqxwolf/shared_git/bConstrainSkyline/data/test_" + grahsize + "_" + degree + "_" + range + "_" + numberOfNodes + ".rtr";
        File fp = new File(treePath);

        if (fp.exists()) {
            fp.delete();
        }


        String infoPath = "/home/gqxwolf/shared_git/bConstrainSkyline/data/staticNode_" + grahsize + "_" + degree + "_" + range + "_" + numberOfNodes + ".txt";
        File file = new File(infoPath);
        if (file.exists()) {
            file.delete();
        }

        System.out.println(infoPath);

        HashSet<Data> result = new HashSet<>();

        FileWriter fw = null;
        BufferedWriter bw = null;
        try {
            fw = new FileWriter(file.getAbsoluteFile(), true);
            bw = new BufferedWriter(fw);

            RTree rt = new RTree(treePath, Constants.BLOCKLENGTH, Constants.CACHESIZE, dimension);

            int i = 0;
            while (i < this.numberOfNodes) {
                int numberOfBusStopInRange = (int) Math.floor(bt.sample() * upper);
                int counter = 0;

//                System.out.println(i + "  " + numberOfBusStopInRange);

                Data d = new Data(this.dimension);
                d.setPlaceId(i);

                float latitude, longitude;
                do {

                    latitude = randomFloatInRange(0f, 360f);
                    longitude = randomFloatInRange(0f, 360f);


                    for (Pair<Double, Double> p : this.busLocation) {
                        double distance = Math.sqrt(Math.pow(latitude - p.getKey(), 2) + Math.pow(longitude - p.getValue(), 2));
                        if (distance <= range) {
                            counter++;
                        }
                    }

                    if (counter != numberOfBusStopInRange) {
                        counter = 0;
                    }
                } while (counter != numberOfBusStopInRange);


                d.setLocation(new double[]{latitude, longitude});


                float priceLevel = randomFloatInRange(0f, 5f);
                float Rating = randomFloatInRange(0f, 5f);
                float other = randomFloatInRange(0f, 5f);

                d.setData(new float[]{priceLevel, Rating, other});

                bw.write(i + "," + latitude + "," + longitude + "," + priceLevel + "," + Rating + "," + other + "\n");

                System.out.println(d + " " + numberOfBusStopInRange);

                result.add(d);

                rt.insert(d);
                i++;
            }

            rt.delete();

            System.out.println(result.size() + "!!!!!");
        } catch (IOException e) {
            e.printStackTrace();
        } finally {

            try {

                if (bw != null)
                    bw.close();

                if (fw != null)
                    fw.close();
//                System.out.println("Done!! See MCP_Results.csv for MCP of each cow for each day.");

            } catch (IOException ex) {

                ex.printStackTrace();

            }
        }

    }

    private void readNodeInfo() {
        String bus_data = this.info_path + "/NodeInfo.txt";
        try {
            File f = new File(bus_data);
            BufferedReader b = new BufferedReader(new FileReader(f));
            String readLine = "";
//            System.out.println("Reading file using Buffered Reader");
            while (((readLine = b.readLine()) != null)) {

                String[] infos = readLine.trim().split(" ");
                double latitude = Double.valueOf(infos[1]);
                double longitude = Double.valueOf(infos[2]);
//                    System.out.println(LatAndLong+" "+latitude+" "+longitude);
                this.busLocation.add(new Pair<>(latitude, longitude));

            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("there are " + this.busLocation.size() + " bus stops");

    }

    private void createStaticNodes() {
        File fp = new File("/home/gqxwolf/shared_git/bConstrainSkyline/data/test.rtr");

        if (fp.exists()) {
            fp.delete();
        }


        File file = new File("/home/gqxwolf/shared_git/bConstrainSkyline/data/staticNode.txt");
        if (file.exists()) {
            file.delete();
        }

        FileWriter fw = null;
        BufferedWriter bw = null;
        try {
            fw = new FileWriter(file.getAbsoluteFile(), true);
            bw = new BufferedWriter(fw);

            RTree rt = new RTree("/home/gqxwolf/shared_git/bConstrainSkyline/data/test.rtr", Constants.BLOCKLENGTH, Constants.CACHESIZE, dimension);

            for (int i = 0; i < this.numberOfNodes; i++) {
                Data d = new Data(this.dimension);
                d.setPlaceId(i);
                float latitude = randomFloatInRange(0f, 360f);
                float longitude = randomFloatInRange(0f, 360f);
                d.setLocation(new double[]{latitude, longitude});


                float priceLevel = randomFloatInRange(0f, 5f);
                float Rating = randomFloatInRange(0f, 5f);
                float other = randomFloatInRange(0f, 5f);

                d.setData(new float[]{priceLevel, Rating, other});

                bw.write(i + "," + latitude + "," + longitude + "," + priceLevel + "," + Rating + "," + other + "\n");

                System.out.println(d);

                rt.insert(d);

            }
            rt.delete();

        } catch (IOException e) {
            e.printStackTrace();
        } finally {

            try {

                if (bw != null)
                    bw.close();

                if (fw != null)
                    fw.close();
//                System.out.println("Done!! See MCP_Results.csv for MCP of each cow for each day.");

            } catch (IOException ex) {

                ex.printStackTrace();

            }
        }
    }


    public void testStaticRTree() {
        String treePath = "/home/gqxwolf/shared_git/bConstrainSkyline/data/test_" + grahsize + "_" + degree + "_" + range + "_" + this.numberOfNodes + ".rtr";

        RTree rt = new RTree(treePath, Constants.CACHESIZE);

        System.out.println((((Node) rt.root_ptr).get_num_of_data()));
//        System.out.println((((RTDataNode) rt.root_ptr).data[0].getPlaceId()));
    }

    public float randomFloatInRange(float min, float max) {
        float random = min + r.nextFloat() * (max - min);
        return random;
    }
}
