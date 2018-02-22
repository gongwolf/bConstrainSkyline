package aRtree;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public class myTest {
    ArrayList<Data> dataf = new ArrayList<>();

    public myTest() {
        Data d = new Data(2);
        d.setPlaceId(1);
        d.setAttrs(new float[]{1f, 2f, 3f});
        d.setData(new float[]{4f, 10f});
        dataf.add(d);


        d = new Data(2);
        d.setPlaceId(2);
        d.setAttrs(new float[]{3f, 4f, 2f});
        d.setData(new float[]{2f, 1f});
        dataf.add(d);


        d = new Data(2);
        d.setPlaceId(3);
        d.setAttrs(new float[]{4f, 5f, 1f});
        d.setData(new float[]{4f, 1f});
        dataf.add(d);

        d = new Data(2);
        d.setPlaceId(4);
        d.setAttrs(new float[]{8f, 9f, 8f});
        d.setData(new float[]{5f, 4f});
        dataf.add(d);

        d = new Data(2);
        d.setPlaceId(5);
        d.setAttrs(new float[]{9f, 5f, 1f});
        d.setData(new float[]{3f, 9f});
        dataf.add(d);


        d = new Data(2);
        d.setPlaceId(5);
        d.setAttrs(new float[]{10f, 2f, 3f});
        d.setData(new float[]{7f, 12f});
        dataf.add(d);

        d = new Data(2);
        d.setPlaceId(5);
        d.setAttrs(new float[]{10f, 2f, 3f});
        d.setData(new float[]{7f, 12f});
        dataf.add(d);

        d = new Data(2);
        d.setPlaceId(5);
        d.setAttrs(new float[]{1, 10f, 5f});
        d.setData(new float[]{17f, 20f});
        dataf.add(d);

        d = new Data(2);
        d.setPlaceId(5);
        d.setAttrs(new float[]{11f, 22f, 32f});
        d.setData(new float[]{8f, 5f});
        dataf.add(d);

        d = new Data(2);
        d.setPlaceId(5);
        d.setAttrs(new float[]{18f, 10f, 19f});
        d.setData(new float[]{18f, 19f});
        dataf.add(d);


    }

    public static void main(String args[]) {
        myTest t = new myTest();
//        t.test1();
//        t.readTreetest();

        t.test3();
//        t.readTreetest();

    }

    private void readTreetest() {
        aRTree ar = new aRTree("data/ar.art", Constants.CACHESIZE);
        ar.print();
    }

    public void test1() {

        String fname = "data/ar.art";
        File f = new File(fname);
        if (f.exists()) {
            f.delete();
            System.out.println("delete the old file");
        }

        aRTree ar = new aRTree("data/ar.art", Constants.BLOCKLENGTH, Constants.CACHESIZE, 2);
        for (Data d : dataf) {
            ar.insert(d);
            System.out.println("==========================");
        }

        ar.delete();
    }

    public void test3() {

        String fname = "data/ar.art";
        File f = new File(fname);
        if (f.exists()) {
            f.delete();
            System.out.println("delete the old file");
        }

        aRTree ar = new aRTree("data/ar.art", Constants.BLOCKLENGTH, 3, 2);


        try {
            File file = new File("data/staticNode.txt");
            FileReader fileReader = new FileReader(file);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                System.out.println(line);

                String infos[] = line.split(",");
                Data d = new Data(2);
                d.setPlaceId(Integer.parseInt(infos[0]));
                d.setData(new float[]{Float.valueOf(infos[1]), Float.valueOf(infos[2])});
                d.setAttrs(new float[]{Float.valueOf(infos[3]), Float.valueOf(infos[4]), Float.valueOf(infos[5])});
                ar.insert(d);
                System.out.println("=========================");
            }
            fileReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        ar.delete();
    }
}
