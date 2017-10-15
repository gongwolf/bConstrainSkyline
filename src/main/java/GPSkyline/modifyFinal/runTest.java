package GPSkyline.modifyFinal;

import GraphPartition.myNode;
import GraphPartition.path;
import neo4jTools.connector;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

public class runTest {
    connector n;
    GraphDatabaseService graphdb;

    public void ConnectDB() {
        this.n = new connector("/home/gqxwolf/neo4j323/testdb40000/databases/graph.db");
        this.n.startDB();
        this.graphdb = this.n.getDBObject();
    }

    public static void main(String[] args) {
        runTest t = new runTest();
        t.ConnectDB();
        for (int i = 0; i < 10; i++) {
//            String sid = String.valueOf("1872");
//            String did = String.valueOf("357");
            String sid = String.valueOf(t.getRandomNumberInRange(0, 39999));
            String did = String.valueOf(t.getRandomNumberInRange(0, 39999));
            t.run(sid, did);
//            t.runGPSearch(sid,did);
        }
        t.n.shutdownDB();
    }


    public void run(String sid, String did) {
        long run1 = System.nanoTime();
        HashMap<String, myNode> r1 = runUseNodeFinal(sid, did, this.graphdb);
        run1 = (System.nanoTime() - run1) / 1000000;
        int size = r1 == null ? 0 : r1.size();
        System.out.println(sid + "==>" + did + " skyline path size:" + size + "         running time:" + run1 + " ms");
//        if (size != 0) {
//            for (path p : r1.get(did).subRouteSkyline) {
//                System.out.println(p);
//            }
//        }

    }

    public HashMap<String, myNode> runUseNodeFinal(String sid, String did, GraphDatabaseService graphdb) {
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
        myFinalModification mspNode = new myFinalModification(graphdb);
        HashMap<String, myNode> r = mspNode.getSkylinePath(Source, Destination);
        return r;
    }

    public void setgraphDBObject(GraphDatabaseService graphdb) {
        this.graphdb = graphdb;
    }

    private int getRandomNumberInRange(int min, int max) {
        if (min >= max) {
            throw new IllegalArgumentException("max must be greater than min");
        }
        Random r = new Random(System.nanoTime());
        return r.nextInt((max - min) + 1) + min;
    }
}
