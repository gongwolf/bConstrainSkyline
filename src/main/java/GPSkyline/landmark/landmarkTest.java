package GPSkyline.landmark;

import GPSkyline.BFS.path;
import neo4jTools.connector;
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
    static long graph_size = 2000;
    static String degree = "5";
    static long landmarkNumber = 5;
    static boolean rebuild_index = false;
    static String Graph_path = "/home/gqxwolf/mydata/projectData/testGraph" + graph_size + "_" + degree + "/data/";

    public static void main(String args[]) {


//        if (args.length == 3) {
//            graph_size = Long.parseLong(args[0]);
//            degree = args[1];
//            landmarkNumber = Long.parseLong(args[2]);
//            rebuild_index = Boolean.parseBoolean(args[3]);
//        } else {
//            System.out.println("please check your input");
//            System.exit(0);
//        }

        LandMark landmark = new LandMark(graph_size, degree, landmarkNumber);
        long run1 = System.currentTimeMillis();
        if (rebuild_index) {
            landmark.buildIndex();
        }
        System.out.println("Build index :" + (System.currentTimeMillis() - run1) + " ms");
//        run1 = System.currentTimeMillis();
        landmarkTest lt = new landmarkTest();

        int num = 20;
        int i = 0;
        while (i != num) {

            long sid = getRandomNumberInRange(0, (int) graph_size - 1);
            long did = getRandomNumberInRange(0, (int) graph_size - 1);
//            long sid = 2541;
//            long did = 1022;
            run1 = System.currentTimeMillis();
            ArrayList<path> r1 = lt.runTest(sid, did);
//            ArrayList<path> r1 = lt.runBFSDijkstar(sid, did);

            long lmRun = System.currentTimeMillis() - run1;

            if (r1 != null) {
//                long run2 = System.currentTimeMillis();
//                ArrayList<path> r2 = lt.runBFSDijkstar(sid, did);
//                long DiRun = System.currentTimeMillis() - run2;
//
//                System.out.println(sid + " -> " + did + ":" + lmRun + "ms " + DiRun + "ms " + lt.compareResult(r1, r2) + " ");
////                for(path ppp:r2)
////                {
////                    System.out.println(lt.printCosts(ppp.getCosts()));
////                }
//                System.out.println("--------------------------------------");
                i++;

            }

        }
//        System.out.println("find the skyline result:" + (System.currentTimeMillis() - run1) + " ms");

//        lt.readFile();
    }

    private ArrayList<path> runTest(long sid, long did) {
        connector n = new connector("/home/gqxwolf/neo4j323/testdb" + graph_size + "_" + degree + "/databases/graph.db");
//        System.out.println("/home/gqxwolf/neo4j323/testdb" + graph_size + "_" + degree + "/databases/graph.db");
        n.startDB();
        GraphDatabaseService graphdb = n.getDBObject();
        Node Source;
        Node Destination;
        try (Transaction tx = graphdb.beginTx()) {
            Source = graphdb.getNodeById(sid);
            Destination = graphdb.getNodeById(did);
            tx.success();
        }
        myshortestPathUseNodeFinal mspNode = new myshortestPathUseNodeFinal(graphdb, landmarkNumber, graph_size, degree);
        ArrayList<path> r = mspNode.getSkylinePath(Source, Destination);
//        System.out.println(r);
        n.shutdownDB();
        return r;
    }

    private ArrayList<path> runBFSDijkstar(long sid, long did) {
        connector n = new connector("/home/gqxwolf/neo4j323/testdb" + graph_size + "_" + degree + "/databases/graph.db");
        n.startDB();
        GraphDatabaseService graphdb = n.getDBObject();
        Node Source;
        Node Destination;
        try (Transaction tx = graphdb.beginTx()) {
            Source = graphdb.getNodeById(sid);
            Destination = graphdb.getNodeById(did);
            tx.success();
        }
        GPSkyline.BFS.myshortestPathUseNodeFinal mspNode = new GPSkyline.BFS.myshortestPathUseNodeFinal(graphdb);
        ArrayList<path> r = mspNode.getSkylinePath(Source, Destination);
//        System.out.println(r);
        n.shutdownDB();
        return r;
    }

    private void readFile() {
        String lm_index = this.Graph_path + "landmark/from/";
        File lFile = new File(lm_index);
        for (File lmkFile : lFile.listFiles()) {
            System.out.println(lmkFile.getName());
            try {
                long pos = 0;
                RandomAccessFile file = new RandomAccessFile(lmkFile, "r");
                file.seek(pos);
                System.out.print(file.readLong() + " ");
                System.out.print(file.readDouble() + " ");
                System.out.print(file.readDouble() + " ");
                System.out.println(file.readDouble());
                System.out.print(file.readLong() + " ");
                System.out.print(file.readDouble() + " ");
                System.out.print(file.readDouble() + " ");
                System.out.println(file.readDouble());
                System.out.print(file.readLong() + " ");
                System.out.print(file.readDouble() + " ");
                System.out.print(file.readDouble() + " ");
                System.out.println(file.readDouble());
                file.close();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }

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
