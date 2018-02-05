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
        t.readTreetest();
    }

    private void readTreetest() {
        aRTree ar = new aRTree("data/ar.art",3);
        ar.print();
    }

    public void test1() {

        String fname = "data/ar.art";
        File f = new File(fname);
        if (f.exists()) {
            f.delete();
            System.out.println("delete the old file");
        }

        aRTree ar = new aRTree("data/ar.art", Constants.BLOCKLENGTH, 3, 2);
        for (Data d : dataf) {
            ar.insert(d);
            System.out.println("==========================");
        }

        ar.delete();
    }
}
