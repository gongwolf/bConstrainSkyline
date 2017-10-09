package GPSkyline;

import GPSkyline.modifyFinal.runTest;
import GraphPartition.path;
import Pindex.myshortestPathUseNodeFinal;

import neo4jTools.connector;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import java.util.ArrayList;
import java.util.Random;

public class Test {
    connector n;
    GraphDatabaseService graphdb;
    GPSkylineSearch gps;

    public void ConnectDB() {
        this.n = new connector("/home/gqxwolf/neo4j323/testdb20000/databases/graph.db");
        this.n.startDB();
        this.graphdb = this.n.getDBObject();
    }

    public static void main(String[] args) {
        Test t = new Test();
        t.ConnectDB();
        t.createBPObject();
        t.gps.setGraphObject(t.graphdb);
        testASRC rt = new testASRC();
        rt.setgraphDBObject(t.graphdb);

        for (int i = 0; i < 10; i++) {
//            String sid = String.valueOf("1872");
//            String did = String.valueOf("357");
            String sid = String.valueOf(t.getRandomNumberInRange(0, 19999));
            String did = String.valueOf(t.getRandomNumberInRange(0, 19999));
//            t.runTest(sid, did);
            System.out.println("=======================================================");
            rt.runTest(sid, did);
            t.runGPSearch(sid, did);
        }
        t.n.shutdownDB();
    }

    private void createBPObject() {
        this.gps = new GPSkylineSearch(this.graphdb);
        int num_parts = 200;
        long graphsize = 20000;
        String portalSelector = "Blinks";
        String lowerboundSelector = "landmark";
        gps.BuildGPartitions(num_parts, graphsize, portalSelector, lowerboundSelector);
    }


    public void runGPSearch(String sid, String did) {
        long sid_long = Long.parseLong(sid);
        long did_long = Long.parseLong(did);
        Node Source;
        Node Destination;
        try (Transaction tx = graphdb.beginTx()) {
            Source = graphdb.getNodeById(sid_long);
            Destination = graphdb.getNodeById(did_long);
            tx.success();
        }
        long run1 = System.nanoTime();
        ArrayList<path> r1 = gps.findSkylines(Source, Destination);
        run1 = (System.nanoTime() - run1) / 1000000;
        int size = r1 == null ? 0 : r1.size();
        System.out.println("GPSkyline:" + sid + "==>" + did + " skyline path size:" + size + "         running time:" + run1 + " ms");
        if (r1 != null) {
            for (path p : r1) {
                System.out.println(p);
            }
        }


    }


    private int getRandomNumberInRange(int min, int max) {

        if (min >= max) {
            throw new IllegalArgumentException("max must be greater than min");
        }

        Random r = new Random(System.nanoTime());
        return r.nextInt((max - min) + 1) + min;
    }


}
