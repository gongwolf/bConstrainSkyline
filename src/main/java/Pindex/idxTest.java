package Pindex;

import javafx.util.Pair;
import neo4jTools.BNode;
import neo4jTools.Line;
import neo4jTools.connector;
import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphalgo.WeightedPath;
import org.neo4j.graphdb.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;

public class idxTest {
    public HashMap<String, Pair<String, String>> partitionInfos = new HashMap<>();
    public static String PathBase = "/home/gqxwolf/mydata/projectData/ConstrainSkyline/data/";
    public static String paritionFile = PathBase + "partitions_info.txt";

    public static void main(String args[]) {
        idxTest t = new idxTest();
        long ct = System.nanoTime();
        t.test();
        System.out.println((System.nanoTime() - ct) / 1000000);


    }

    private void test() {
        connector n = new connector("/home/gqxwolf/neo4j/csldb/databases/graph.db");
        n.startDB();
        GraphDatabaseService graphdb = n.getDBObject();
        long ct = System.nanoTime();
        getShortestCost("30825","35003",graphdb);
        System.out.println("running time "+ (System.nanoTime() - ct) / 1000000);

//        readPartionsInfo(paritionFile);
////        connector n = new connector();
//        n.startDB();
//        GraphDatabaseService graphdb = n.getDBObject();
//        String sid = "30825";
//        String eid = "35003";
//        String pid = "112";
//
//        System.out.println("============================");
//        long run1 = System.nanoTime();
//        ArrayList<path> r1 = runUseNodeFinal(sid, eid, graphdb);
//        System.out.println(r1.size());
//        removePathNotWithinBlock(pid, r1);
//        run1 = (System.nanoTime()-run1)/1000000;
//        System.out.println(r1.size());
////        System.out.println(r1.get(0));
//        System.out.println("============================");
//        long run2 = System.nanoTime();
//        ArrayList<path> r2 = runSkylineInBlock(sid, eid, pid, graphdb);
//        removePathNotWithinBlock(pid,r2);
//        run2 = (System.nanoTime()-run2)/1000000;
//        System.out.println(r2.size());
////        System.out.println(r2.get(0));
//
//
//        System.out.println("============================");
//        System.out.println(run1 + "   " + run2);
////        long ct = System.nanoTime();
////        getShortestCost(sid,eid,graphdb);
////        System.out.println((System.nanoTime()-ct)/1000000);
////        ct = System.nanoTime();
////        runUseNodeFinal(sid,eid,graphdb);
////        System.out.println((System.nanoTime()-ct)/1000000);
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


    public ArrayList<path> runSkylineInBlock(String sid, String did, String pid, GraphDatabaseService graphdb) {
//        connector n = new connector("/home/gqxwolf/neo4j/csldb/databases/graph.db");
        Node Source;
        Node Destination;
        try (Transaction tx = graphdb.beginTx()) {
            Source = graphdb.findNode(BNode.BusNode, "name", sid);
            Destination = graphdb.findNode(BNode.BusNode, "name", did);
            tx.success();
        }
        mySkylineInBlock ibNode = new mySkylineInBlock(graphdb);
        ArrayList<path> r = ibNode.getSkylinePath(Source, Destination, pid, this.partitionInfos);
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

    public void readPartionsInfo(String paritionFile) {
        try (BufferedReader br = new BufferedReader(new FileReader(paritionFile))) {
            String line = null;
            while ((line = br.readLine()) != null) {
                String NodeId = line.split(" ")[0];
                String Cid = line.split(" ")[1];
                String Pid = line.split(" ")[2];
                Pair<String, String> p = new Pair<>(Cid, Pid);
                this.partitionInfos.put(NodeId, p);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public void removePathNotWithinBlock(String pid, ArrayList<path> paths) {

        int i = 0;
        for (; i < paths.size(); ) {
            path p = paths.get(i);

            // System.out.println(p);
            // System.out.println(printCosts(p.getCosts()));

            long sid = p.startNode.getId();
            long eid = p.endNode.getId();

            boolean flag = true;
            for (Node n : p.Nodes) {
                if (n.getId() != sid && n.getId() != eid) {
                    String nid = String.valueOf(n.getId() + 1);
                    String n_pid = this.partitionInfos.get(nid).getValue();
                    if (!n_pid.equals(pid)) {
                        flag = false;
                        break;
                    }

                }
            }
            // System.out.println(flag);
            if (!flag) {
                paths.remove(i);
            } else {
                i++;
            }
        }

    }

}
