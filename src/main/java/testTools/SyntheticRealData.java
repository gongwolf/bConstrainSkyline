package testTools;

import RstarTree.Constants;
import RstarTree.Data;
import RstarTree.Node;
import RstarTree.RTree;

import java.io.*;
import java.util.Random;

public class SyntheticRealData {
    int dimension;
    Random r = new Random(System.nanoTime());

    String path_base = "/home/gqxwolf/shared_git/bConstrainSkyline/data/";
    String bus_data = path_base + "IOP_data";

    String[] cities = new String[]{"New York", "San Francisco", "Los Angeles"};
    String[] p_types = new String[]{"food", "lodging", "restaurant"};

    int max_id = 0;

    public SyntheticRealData(int dimension) {
        this.dimension = dimension;

    }

    public static void main(String args[]) {
        int dimension = 3;


        if (args.length == 1) {
            dimension = Integer.parseInt(args[0]);
        }

        SyntheticRealData sd = new SyntheticRealData(dimension);
        sd.readPOIsData();
        sd.testStaticRTree();
    }

    private void readPOIsData() {
        for (String city : this.cities) {
            for (String type : p_types) {
                String path = "outfilename_" + type + "_" + city;
                try {
                    File f = new File(path);
                    BufferedReader b = new BufferedReader(new FileReader(f));
                    String line = "";

                    POIObject poi_obj = new POIObject();

                    poi_obj.placeID = this.max_id;
                    poi_obj.data = new float[dimension];
                    poi_obj.g_p_id = "";
                    poi_obj.locations = new float[2];

                    while (((line = b.readLine()) != null)) {

                        if (line.startsWith("=======================================================================")) {
                            //Todo:Create new data and clean the data
                        }

                        //Todo:find attributes


                    }
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }


    public void testStaticRTree() {
        RTree rt = new RTree("/home/gqxwolf/shared_git/bConstrainSkyline/data/test.rtr", Constants.CACHESIZE);

        System.out.println((((Node) rt.root_ptr).get_num_of_data()));
//        System.out.println((((RTDataNode) rt.root_ptr).data[0].getPlaceId()));
    }

    public float randomFloatInRange(float min, float max) {
        float random = min + r.nextFloat() * (max - min);
        return random;
    }

    private double getGaussian(double mean, double sd) {
        double value = r.nextGaussian() * sd + mean;

        while (value <= 0) {
            value = r.nextGaussian() * sd + mean;
        }

        return value;
    }
}
