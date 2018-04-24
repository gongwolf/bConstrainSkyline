package testTools;

import RstarTree.Constants;
import RstarTree.Data;
import RstarTree.Node;
import RstarTree.RTree;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class SyntheticData {
    Random r = new Random(System.nanoTime());

    public static void main(String args[]) {
        int numberOfNodes = 1000;
        int dimension =3;

        if (args.length == 2) {
            numberOfNodes = Integer.parseInt(args[0]);
            dimension = Integer.parseInt(args[1]);
        }

        SyntheticData sd = new SyntheticData();
        sd.createStaticNodes(numberOfNodes, dimension);
        sd.testStaticRTree();
    }

    private void createStaticNodes(int NumberOfNodes, int dimension) {
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

            for (int i = 0; i < NumberOfNodes; i++) {
                Data d = new Data(dimension);
                d.setPlaceId(i);
                float latitude = randomFloatInRange(0f, 360f);
                float longitude = randomFloatInRange(0f, 360f);
                d.setLocation(new double[]{latitude, longitude});


                float priceLevel = randomFloatInRange(0f, 5f);
                float Rating = randomFloatInRange(0f, 5f);
                float other = randomFloatInRange(0f, 5f);

                d.setData(new float[]{priceLevel, Rating, other});

                bw.write(i+","+latitude+","+longitude+","+priceLevel+","+Rating+","+other+"\n");

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
        RTree rt = new RTree("/home/gqxwolf/shared_git/bConstrainSkyline/data/test.rtr", Constants.CACHESIZE);

        System.out.println((((Node) rt.root_ptr).get_num_of_data()));
//        System.out.println((((RTDataNode) rt.root_ptr).data[0].getPlaceId()));
    }

    public float randomFloatInRange(float min, float max) {
        float random = min + r.nextFloat() * (max - min);
        return random;
    }
}
