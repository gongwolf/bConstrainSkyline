package Pindex.vctest;

import Pindex.myshortestPathUseNodeFinal;
import Pindex.path;
import javafx.util.Pair;
import neo4jTools.BNode;
import neo4jTools.connector;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;

public class ResultComparesion {
    public static void main(String[] args) {
        ResultComparesion rsc = new ResultComparesion();
        rsc.batchComparison();

    }
    public void batchComparison()
    {
        String basepath = "/home/gqxwolf/mydata/projectData/testGraph2/data/";
        connector n = new connector("/home/gqxwolf/neo4j323/testdb2/databases/graph.db");
        n.startDB();
        GraphDatabaseService graphdb = n.getDBObject();
        vctest vt = new vctest(basepath);
        long building_time = System.nanoTime();
        VCNode root = vt.buildVCTree(50, graphdb);
        System.out.println("index built in " + (System.nanoTime()-building_time)/1000000);


        HashSet<Pair<String,String>> batch = new HashSet<>();
//        System.out.println("Begin to Generate test cases");

        while(batch.size()!=2000)
        {
            int sid = getRandomNumberInRange(0,9999);
            int did = getRandomNumberInRange(0,9999);
            while(sid==did)
            {
                did = getRandomNumberInRange(0,9999);
            }
            batch.add(new Pair<>(String.valueOf(sid),String.valueOf(did)));
        }

//        System.out.println("Finish the process to Generate test cases");


        for(Pair<String,String> p:batch) {
            compareResult(p.getKey(),p.getValue(), graphdb, root, vt);
        }


        n.shutdownDB();



    }

    private void compareResult(String src, String dest, GraphDatabaseService graphdb, VCNode root, vctest vt) {
        long run1 = System.nanoTime();
        ArrayList<path> r1 = runUseNodeFinal(src, dest, graphdb);
        run1 = (System.nanoTime() - run1)/1000000;
//                    System.out.println("Index building success in " + (System.nanoTime() - building) / 1000000 + "ms ");

//        for (path p : r1) {
//            System.out.println(p + " " + p.printCosts());
//        }


        if(r1!=null) {
            long run2 = System.nanoTime();
            ArrayList<myPath> r2 = vt.getSkyline(src, dest, graphdb, root);
            run2 = (System.nanoTime() - run2) / 1000000;

//        for (myPath p : r2) {
//            System.out.println(p + " " + p.printCosts());
//        }

            System.out.println(src + " " + dest + " " + run1 + "  " + run2 + "  " + checkPathResultEquation(r1, r2) + "  " + r2.size());
        }

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


    public static boolean checkPathResultEquation(ArrayList<path> r1, ArrayList<myPath> r2) {
        if(r1==null&&r2.size()==0)
        {
            return true;
        }
        boolean[] results = new boolean[r1.size()];
        if (r1.size() != r2.size())
            return false;
        int i = 0;
        for (path p1 : r1) {
            for (myPath p2 : r2) {
                if (isEqualPath(p1, p2)) {
                    results[i] = true;
                    continue;
                }
            }
            i++;
        }

        for (boolean f : results) {
            if (!f) {
                return false;
            }
        }


        return true;
    }

    private static boolean isEqualPath(path p1, myPath p2) {
        return isEqualNodes(p1.Nodes, p2.Nodes) && isEqualEdges(p1.relationships, p2.relationships);
    }

    private static boolean isEqualEdges(ArrayList<Relationship> relationships1, ArrayList<Relationship> relationships2) {
        if (relationships1.size() != relationships2.size()) {
            return false;
        }


        for (int i = 0; i < relationships1.size(); i++) {
            if (relationships1.get(i).getId() != relationships2.get(i).getId()) {
                return false;
            }

        }
        return true;
    }


    private static boolean isEqualNodes(ArrayList<Node> nodes1, ArrayList<Node> nodes2) {
        if (nodes1.size() != nodes2.size()) {
            return false;
        }


        for (int i = 0; i < nodes1.size(); i++) {
            if (nodes1.get(i).getId() != nodes2.get(i).getId()) {
                return false;
            }

        }
        return true;
    }

    private int getRandomNumberInRange(int min, int max) {
        Random r = new Random();
        if (min > max) {
            throw new IllegalArgumentException("max must be greater than min");
        }

        return r.nextInt((max - min) + 1) + min;
    }
}
