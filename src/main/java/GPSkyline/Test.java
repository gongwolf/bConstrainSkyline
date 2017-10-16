package GPSkyline;

import GraphPartition.path;

import neo4jTools.connector;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

import java.util.ArrayList;
import java.util.Random;

public class Test {
    connector n;
    GraphDatabaseService graphdb;
    GPSkylineSearch gps;

    public void ConnectDB(long graphsize, String degree) {
        String db_path = "/home/gqxwolf/neo4j323/testdb" + graphsize + "_" + degree + "/databases/graph.db";
//        this.n = new connector("/home/gqxwolf/neo4j323/testdb2000_2_5/databases/graph.db");
        this.n = new connector(db_path);
        this.n.startDB();
        this.graphdb = this.n.getDBObject();
    }

    public static void main(String[] args) {

        int num_parts = 20;
        long graphsize = 2000;
        String degree = "2_5";
        String portalSelector = "Blinks";
        String lowerboundSelector = "oneToAll";

        if (args.length == 5) {
            num_parts = Integer.parseInt(args[0]);
            graphsize = Long.parseLong(args[1]);
            degree = args[2];
            portalSelector = args[3];
            lowerboundSelector = args[4];
        }


        String basePath = "/home/gqxwolf/mydata/projectData/testGraph" + graphsize + "_" + degree + "/data/";
        Test t = new Test();
        t.ConnectDB(graphsize, degree);
        t.createBPObject(num_parts, graphsize, portalSelector, lowerboundSelector, basePath);
        t.gps.setGraphObject(t.graphdb);
        testASRC rt = new testASRC();
        rt.setgraphDBObject(t.graphdb);

        int num = 20;
        int i = 0;
        while (i != 20) {
//            String sid = String.valueOf("697");
//            String did = String.valueOf("1392");
////            String sid = String.valueOf("1872");
////            String did = String.valueOf("357");
            String sid = String.valueOf(t.getRandomNumberInRange(0, (int) graphsize - 1));
            String did = String.valueOf(t.getRandomNumberInRange(0, (int) graphsize - 1));
//            ArrayList<Pindex.path> s1 = rt.runTest(sid, did);
            ArrayList<path> s2 = t.runGPSearch(sid, did);
            if (s2 != null && s2.size() != 0) {
                i++;
            }
//            System.out.println("result set is same? " + t.compareResult(s1, s2));

//            int ns1 = s1 == null ? 0 : s1.size();
//            int ns2 = s2 == null ? 0 : s2.size();
//
//            if (ns1 != ns2) {
//                System.out.println("!!!!!!!!!!!!!");
//            }
        }
        t.n.shutdownDB();
    }

    private void createBPObject(int num_parts, long graphsize, String portalSelector, String lowerboundSelector, String basePath) {
        this.gps = new GPSkylineSearch(this.graphdb);
        gps.BuildGPartitions(basePath, num_parts, graphsize, portalSelector, lowerboundSelector);
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
        if (size != 0) {
            System.out.print("GPSkyline:" + sid + "==>" + did + " skyline path size:" + size + "         running time:" + run1 + " ms " + gps.count1 + " " + gps.count2 + " ");
            System.out.println(gps.concatenetPath / 1000000 + "  " + gps.expandInBlock / 1000000 + " " + gps.skylineTime / 1000000
                    + " " + gps.usedInExpand / 1000000 + " " + gps.findAdjBlock / 1000000 + " " + gps.getNodeTime / 1000000 + " " + gps.checkCycle / 1000000
                    + " " + gps.AddToqueueTime / 100000);
        }
//        if (r1 != null) {
//            for (path p : r1) {
////                if (p.startNode != null) {
//                System.out.println(p);
////                }
//            }
//        }

        return r1;


    }


    private int getRandomNumberInRange(int min, int max) {

        if (min >= max) {
            throw new IllegalArgumentException("max must be greater than min");
        }

        Random r = new Random(System.nanoTime());
        return r.nextInt((max - min) + 1) + min;
    }

    public boolean compareResult(ArrayList<Pindex.path> s1, ArrayList<path> s2) {
        int size1 = s1 == null ? 0 : s1.size();
        int size2 = s2 == null ? 0 : s2.size();

        if (size1 != size2) {
            return false;
        } else {

            for (Pindex.path p1 : s1) {
                if (!comparePath(p1, s2)) {
                    return false;
                }
            }

            return true;

        }

    }

    private boolean comparePath(Pindex.path p1, ArrayList<path> s2) {

        boolean flag = false;

        for (path p2 : s2) {
            if (p2.Nodes.size() == p1.Nodes.size() && p2.relationships.size() == p1.relationships.size()) {
                if (compareNodes(p1.Nodes, p2.Nodes) && compareRels(p1.relationships, p2.relationships)) {
                    return true;
                }
            }
        }
        return flag;
    }

    private boolean compareRels(ArrayList<Relationship> relationships, ArrayList<Relationship> relationships1) {
        boolean flag = true;
        for (int i = 0; i < relationships.size(); i++) {
            if (relationships.get(i).getId() != relationships1.get(i).getId()) {
                flag = false;
                break;
            }
        }
        return flag;
    }

    private boolean compareNodes(ArrayList<Node> nodes, ArrayList<Node> nodes1) {
        boolean flag = true;
        for (int i = 0; i < nodes.size(); i++) {
            if (nodes.get(i).getId() != nodes1.get(i).getId()) {
                flag = false;
                break;
            }
        }
        return flag;

    }


}
