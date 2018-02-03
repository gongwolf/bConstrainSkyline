package aRtree;

import java.io.File;
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


    }

    public static void main(String args[]) {
        myTest t = new myTest();
        t.test1();
    }

    public void test1() {

        String fname = "data/ar.art";
        File f = new File(fname);
        if (f.exists()) {
            f.delete();
            System.out.println("delete the old file");
        }

        aRTree ar = new aRTree("data/ar.art", 150, 3, 2);
        for (Data d : dataf) {
            ar.insert(d);
            System.out.println("==========================");
        }


    }
}
