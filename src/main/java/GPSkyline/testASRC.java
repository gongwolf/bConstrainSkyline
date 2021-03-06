package GPSkyline;

import Pindex.myshortestPathUseNodeFinal;
import Pindex.path;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import java.util.ArrayList;

public class testASRC {
    GraphDatabaseService graphdb;

    public ArrayList<path> runTest(String sid, String did) {
        long run1 = System.nanoTime();
        ArrayList<path> r1 = runUseNodeFinal(sid, did, this.graphdb);
        run1 = (System.nanoTime() - run1) / 1000000;
        int size = r1 == null ? 0 : r1.size();
        System.out.println("ASRC:" + sid + "==>" + did + " skyline path size:" + size + "         running time:" + run1 + " ms");
//        if (r1 != null) {
//            for (path p : r1) {
//                System.out.println(p);
//            }
//        }

//        System.out.println("-----------------------------------------");
        return r1;

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


    public void setgraphDBObject(GraphDatabaseService graphdb) {
        this.graphdb = graphdb;
    }
}
