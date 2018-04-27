package testTools;

import RstarTree.Constants;
import RstarTree.Data;
import RstarTree.Node;
import RstarTree.RTree;
import javafx.util.Pair;
import org.apache.commons.math3.distribution.BetaDistribution;

import java.io.*;
import java.util.HashSet;
import java.util.Random;

public class SyntheticData {
    private final int numberOfNodes;
    private final int dimension;
    private final int grahsize;
    private final int degree;
    private final String info_path;
    Random r = new Random(System.nanoTime());
    HashSet<Pair<Double, Double>> busLocation = new HashSet<>();
    BetaDistribution bt = new BetaDistribution(1.89, 19);
    private double range = 0;

    public SyntheticData(int numberOfNodes, int dimension, int graphsize, int degree, double range) {
        this.numberOfNodes = numberOfNodes;
        this.dimension = dimension;

        this.grahsize = graphsize;
        this.degree = degree;
        this.info_path = "/home/gqxwolf/mydata/projectData/testGraph" + this.grahsize + "_" + this.degree + "/data";

        this.range = range;
    }

    public static void main(String args[]) {
        int numberOfNodes = 2000;
        int dimension = 3;

        int graphsize = 8000;
        int degree = 4;
        double range = 6;

        if (args.length == 5) {
            numberOfNodes = Integer.parseInt(args[0]);
            dimension = Integer.parseInt(args[1]);
            graphsize = Integer.parseInt(args[2]);
            degree = Integer.parseInt(args[3]);
            range = Double.parseDouble(args[4]);
        }

        SyntheticData sd = new SyntheticData(numberOfNodes, dimension, graphsize, degree, range);
        sd.test();
//        sd.createStaticNodes(numberOfNodes, dimension);
        sd.testStaticRTree();
    }

    private void test() {
        readNodeInfo();

        String treePath = "/home/gqxwolf/shared_git/bConstrainSkyline/data/test_" + grahsize + "_" + degree + "_" + range + ".rtr";
        File fp = new File(treePath);

        if (fp.exists()) {
            fp.delete();
        }


        String infoPath = "/home/gqxwolf/shared_git/bConstrainSkyline/data/staticNode_" + grahsize + "_" + degree + "_" + range + ".txt";
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
                int numberOfBusStopInRange = (int) Math.floor(bt.sample() * 50);
                int counter = 0;

                System.out.println(i + "  " + numberOfBusStopInRange);

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
        String treePath = "/home/gqxwolf/shared_git/bConstrainSkyline/data/test_" + grahsize + "_" + degree + "_" + range + ".rtr";

        RTree rt = new RTree(treePath, Constants.CACHESIZE);

        System.out.println((((Node) rt.root_ptr).get_num_of_data()));
//        System.out.println((((RTDataNode) rt.root_ptr).data[0].getPlaceId()));
    }

    public float randomFloatInRange(float min, float max) {
        float random = min + r.nextFloat() * (max - min);
        return random;
    }
}
