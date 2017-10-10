package GPSkyline;

import GraphPartition.path;

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
        this.n = new connector("/home/gqxwolf/neo4j323/testdb2000/databases/graph.db");
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

        for (int i = 0; i < 1; i++) {
            String sid = String.valueOf("889");
            String did = String.valueOf("1707");
//            String sid = String.valueOf("1872");
//            String did = String.valueOf("357");
//            String sid = String.valueOf(t.getRandomNumberInRange(0, 1999));
//            String did = String.valueOf(t.getRandomNumberInRange(0, 1999));
//            t.runTest(sid, did);
            System.out.println("=======================================================");
            ArrayList<Pindex.path> s1 = rt.runTest(sid, did);
            ArrayList<path> s2 = t.runGPSearch(sid, did);

            int ns1 = s1 == null?0:s1.size();
            int ns2 = s2 == null?0:s2.size();

            if(ns1!=ns2)
            {
                System.out.println("!!!!!!!!!!!!!");
            }
        }
        t.n.shutdownDB();
    }

    private void createBPObject() {
        this.gps = new GPSkylineSearch(this.graphdb);
        int num_parts = 20;
        long graphsize = 2000;
        String portalSelector = "Blinks";
        String lowerboundSelector = "oneToAll";
        gps.BuildGPartitions(num_parts, graphsize, portalSelector, lowerboundSelector);
    }


    public ArrayList<path> runGPSearch(String sid, String did) {
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
        int size = (r1 == null) ? 0 : r1.size();
        System.out.println("GPSkyline:" + sid + "==>" + did + " skyline path size:" + size + "         running time:" + run1 + " ms");
        if (r1 != null) {
            for (path p : r1) {
//                if (p.startNode != null) {
                    System.out.println(p);
//                }
            }
        }

        return r1;


    }


    private int getRandomNumberInRange(int min, int max) {

        if (min >= max) {
            throw new IllegalArgumentException("max must be greater than min");
        }

        Random r = new Random(System.nanoTime());
        return r.nextInt((max - min) + 1) + min;
    }


}
