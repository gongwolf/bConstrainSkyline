package GPSkyline;

import Pindex.myshortestPathUseNodeFinal;
import Pindex.path;
import neo4jTools.BNode;
import neo4jTools.connector;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import java.lang.reflect.Array;
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
        t.createBPObject();
        t.ConnectDB();
        t.gps.setGraphObject(t.graphdb);
        for (int i = 0; i < 1; i++) {
            String sid = String.valueOf("1872");
            String did = String.valueOf(t.getRandomNumberInRange(0, 1999));
//            t.runTest(sid, did);
            t.runGPSearch(sid,did);
        }
        t.n.shutdownDB();
    }

    private void createBPObject() {
        this.gps = new GPSkylineSearch(this.graphdb);
        int num_parts = 20;
        long graphsize = 2000;
        String portalSelector = "Blinks";
        String lowerboundSelector = "landmark";
        gps.BuildGPartitions(num_parts,graphsize,portalSelector,lowerboundSelector);
    }

    private void runTest(String sid, String did) {
        long run1 = System.nanoTime();
        ArrayList<path> r1 = runUseNodeFinal(sid,did, this.graphdb);
        run1 = (System.nanoTime() - run1) / 1000000;
        int size = r1==null?0:r1.size();
        System.out.println(sid + "==>" + did + " skyline path size:" + size + "         running time:" + run1 + " ms");

    }

    public void runGPSearch(String sid, String did){
        long sid_long = Long.parseLong(sid);
        long did_long = Long.parseLong(did);
        Node Source;
        Node Destination;
        try (Transaction tx = graphdb.beginTx()) {
            Source = graphdb.getNodeById(sid_long);
            Destination = graphdb.getNodeById(did_long);
            tx.success();
        }
        gps.findSkylines(Source,Destination);

    }

    public ArrayList<path> runUseNodeFinal(String sid, String did, GraphDatabaseService graphdb) {
//        connector n = new connector("/home/gqxwolf/neo4j/csldb/databases/graph.db");
        long sid_long = Long.parseLong(sid);
        long did_long = Long.parseLong(did);
        Node Source;
        Node Destination;
        try (Transaction tx = graphdb.beginTx()) {
            Source = graphdb.getNodeById(sid_long);
            Destination = graphdb.getNodeById(did_long);
            tx.success();
        }
        myshortestPathUseNodeFinal mspNode = new myshortestPathUseNodeFinal(graphdb);
        ArrayList<path> r = mspNode.getSkylinePath(Source, Destination);
        return r;
    }


    private int getRandomNumberInRange(int min, int max) {

        if (min >= max) {
            throw new IllegalArgumentException("max must be greater than min");
        }

        Random r = new Random(System.nanoTime());
        return r.nextInt((max - min) + 1) + min;
    }


}
