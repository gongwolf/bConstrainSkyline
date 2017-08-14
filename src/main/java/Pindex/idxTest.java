package Pindex;

import neo4jTools.BNode;
import neo4jTools.Line;
import neo4jTools.connector;
import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphalgo.WeightedPath;
import org.neo4j.graphdb.*;

import java.util.ArrayList;

public class idxTest {
    public static void main(String args[])
    {
        idxTest t = new idxTest();
        long ct = System.nanoTime();
        t.test();
        System.out.println((System.nanoTime()-ct)/1000000);

    }

    private void test() {
//        connector n = new connector("/home/gqxwolf/neo4j/csldb/databases/graph.db");
        connector n = new connector();
        n.startDB();
        GraphDatabaseService graphdb = n.getDBObject();
        String sid = "2";
        String eid = "99";
        runIdxSkyline(sid,eid,graphdb);
//        long ct = System.nanoTime();
//        getShortestCost(sid,eid,graphdb);
//        System.out.println((System.nanoTime()-ct)/1000000);
//        ct = System.nanoTime();
//        runUseNodeFinal(sid,eid,graphdb);
//        System.out.println((System.nanoTime()-ct)/1000000);
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

    public ArrayList<path> runUseNodeFinal(String sid, String did, GraphDatabaseService graphdb) {
//        connector n = new connector("/home/gqxwolf/neo4j/csldb/databases/graph.db");
        Node Source;
        Node Destination;
        try (Transaction tx = graphdb.beginTx()) {
            Source = graphdb.findNode(BNode.BusNode, "name", sid);
            Destination = graphdb.findNode(BNode.BusNode, "name", did);
            tx.success();
        }
        myshortestPathUseNodeFinal mspNode = new myshortestPathUseNodeFinal(graphdb);
        ArrayList<path> r = mspNode.getSkylinePath(Source, Destination);
        return r;
    }

    public double[] getShortestCost(String sid, String did, GraphDatabaseService graphdb) {
//        connector n = new connector("/home/gqxwolf/neo4j/csldb/databases/graph.db");

        Node Source;
        Node Destination;
        double[] iniLowerBound;
        try (Transaction tx = graphdb.beginTx()) {
            Source = graphdb.findNode(BNode.BusNode, "name", sid);
            Destination = graphdb.findNode(BNode.BusNode, "name", did);
            path dummyP = new path(Source);
            int i = 0;
            iniLowerBound = new double[dummyP.NumberOfProperties];

            for (int j = 0; j < iniLowerBound.length; j++) {
                iniLowerBound[j] = -1;
            }

            for (String property_name : dummyP.propertiesName) {
                PathFinder<WeightedPath> finder = GraphAlgoFactory
                        .dijkstra(PathExpanders.forTypeAndDirection(Line.Linked, Direction.OUTGOING), property_name);
                WeightedPath paths = finder.findSinglePath(dummyP.endNode, Destination);
                if (paths != null) {
                    iniLowerBound[i++] = paths.weight();
                } else {
                    break;
                }
            }
            tx.success();
            // System.out.println(printCosts(iniLowerBound));
        }

        return iniLowerBound;
    }
}
