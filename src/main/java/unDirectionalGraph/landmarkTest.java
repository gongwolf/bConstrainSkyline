package unDirectionalGraph;

import com.sun.org.apache.xpath.internal.operations.Bool;
import neo4jTools.connector;
import org.neo4j.cypher.internal.frontend.v2_3.ast.In;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Random;

public class landmarkTest {
    static long graph_size = 4000;
    static String degree = "5";
    static long landmarkNumber = 5;
    static boolean rebuild_index = false;
    static boolean initalized = false;
    static int lowerBound_method = 0;
    static String Graph_path = "/home/gqxwolf/mydata/projectData/un_testGraph" + graph_size + "_" + degree + "/data/";

    public static void main(String args[]) {


        if (args.length == 6) {
            landmarkNumber = Long.parseLong(args[0]);
            graph_size = Long.parseLong(args[1]);
            degree = args[2];
            initalized = Boolean.parseBoolean(args[3]);
            lowerBound_method = Integer.parseInt(args[4]);
            rebuild_index = Boolean.parseBoolean(args[5]);
        }

        LandMark landmark = new LandMark(graph_size, degree, landmarkNumber);
        long run1 = System.currentTimeMillis();
        if (rebuild_index) {
            landmark.buildIndex();
        }

        landmarkTest lt = new landmarkTest();



        int num = 20;
        int i = 0;
        while (i != num) {
            connector n = new connector("/home/gqxwolf/neo4j323/test_un_db" + graph_size + "_" + degree + "/databases/graph.db");
            n.startDB();
            GraphDatabaseService graphdb = n.getDBObject();
            Node Source;
            Node Destination;

            long sid = getRandomNumberInRange(0, (int) graph_size - 1);
            long did = getRandomNumberInRange(0, (int) graph_size - 1);
            try (Transaction tx = graphdb.beginTx()) {
                Source = graphdb.getNodeById(sid);
                Destination = graphdb.getNodeById(did);
                tx.success();
            }

//            SkylineBFS s2 = new SkylineBFS(graphdb, 5, 4000, "5", true, 2);
//            SkylineBFS s1 = new SkylineBFS(graphdb, 5, 4000, "5", true, 1);
//            SkylineBFS s0 = new SkylineBFS(graphdb, 5, 4000, "5", true, 0);
            SkylineBFS s = new SkylineBFS(graphdb, landmarkNumber, graph_size, degree, initalized, lowerBound_method);


//            run1 = System.currentTimeMillis();
//            ArrayList<path> r = s.getSkylinePath(Source, Destination);
//            long noRun = System.currentTimeMillis() - run1;

            String r = s.getSkylinePath(Source, Destination);
            if (r != null) {
                System.out.println(r);
                i++;

//                long run3 = System.currentTimeMillis();
//                ArrayList<path> r1 = s1.getSkylinePath(Source, Destination);
//                long DiRun = System.currentTimeMillis() - run3;
//
//                long run2 = System.currentTimeMillis();
//                ArrayList<path> r0 = s0.getSkylinePath(Source, Destination);
//                long lmRun = System.currentTimeMillis() - run2;
//
//                System.out.println(sid + " -> " + did + ":" + lmRun + "ms " + DiRun + "ms " + lt.compareResult(r2, r1) + " ");
//                System.out.println(sid + " -> " + did + ":" + noRun + "ms " + DiRun + "ms " + lt.compareResult(r0, r1) + " ");
//////                for(path ppp:r2)
//////                {
//////                    System.out.println(lt.printCosts(ppp.getCosts()));
//////                }
//                System.out.println("--------------------------------------");

            }
            n.shutdownDB();
        }
    }


    public boolean compareResult(ArrayList<path> s1, ArrayList<path> s2) {
        int size1 = s1 == null ? 0 : s1.size();
        int size2 = s2 == null ? 0 : s2.size();

        if (size1 != size2) {
            return false;
        } else {

            for (path p1 : s1) {
                if (!comparePath(p1, s2)) {
                    return false;
                }
            }

            return true;

        }

    }

    private boolean comparePath(path p1, ArrayList<path> s2) {

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

    private static int getRandomNumberInRange(int min, int max) {
        if (min >= max) {
            throw new IllegalArgumentException("max must be greater than min");
        }
        Random r = new Random(System.nanoTime());
        return r.nextInt((max - min) + 1) + min;
    }

    public String printCosts(double costs[]) {
        StringBuffer sb = new StringBuffer();
        sb.append("[");
        int i = 0;
        for (; i < costs.length - 1; i++) {
            sb.append(costs[i] + ",");
        }
        sb.append(costs[i] + "]");
        return sb.toString();
    }
}
