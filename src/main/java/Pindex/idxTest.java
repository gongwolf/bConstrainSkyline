package Pindex;

import neo4jTools.BNode;
import neo4jTools.connector;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import java.util.ArrayList;

public class idxTest {
    public static void main(String args[])
    {
        idxTest t = new idxTest();
        connector n = new connector();
        n.startDB();
        GraphDatabaseService graphdb = n.getDBObject();
        String sid = "1";
        String eid = "99";
        t.runIdxSkyline(sid,eid,graphdb);
        n.shutdownDB();

    }

    public ArrayList<path> runIdxSkyline(String sid, String did, GraphDatabaseService graphdb) {
//        connector n = new connector("/home/gqxwolf/neo4j/csldb/databases/graph.db");
        Node Source;
        Node Destination;
        try (Transaction tx = graphdb.beginTx()) {
            Source = graphdb.findNode(BNode.BusNode, "name", sid);
            Destination = graphdb.findNode(BNode.BusNode, "name", did);
            tx.success();
        }
        indexSkyline idxNode = new indexSkyline(graphdb);
        ArrayList<path> r = idxNode.getSkylinePath(Source, Destination);
        return r;
    }
}
