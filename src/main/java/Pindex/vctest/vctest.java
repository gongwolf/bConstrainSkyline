package Pindex.vctest;

import neo4jTools.BNode;
import neo4jTools.Line;
import neo4jTools.connector;
import org.neo4j.graphdb.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;

public class vctest {
    VCNode root;
    String basePath = "/home/gqxwolf/mydata/projectData/testGraph/data/";
    String nodesPath = basePath + "NodeInfo.txt";
    String edgesPath = basePath + "SegInfo.txt";


    public static void main(String args[]) {
        vctest vt = new vctest();
        vt.buildVCTree(50);
    }

    private void buildVCTree(int threshold) {
        connector n = new connector("/home/gqxwolf/neo4j323/testdb/databases/graph.db");
        n.startDB();
        GraphDatabaseService graphdb = n.getDBObject();
        try (Transaction tx = graphdb.beginTx()) {

            ArrayList<Node> nodes = getNodes(graphdb);
            ArrayList<Relationship> edges = getEgdes(graphdb);
            System.out.println("There are " + nodes.size() + " nodes,\nThere are " + edges.size() + " Edges.");

            long building = System.nanoTime();
            root = new VCNode(graphdb);
            root.buildDistanceGraph(new Graph(nodes, edges), threshold);
            System.out.println("Index building success in " + (System.nanoTime() - building) / 1000000 + "ms ");
//        retrivalTree(root);
            VCNode src_lowestNode = getLowestNode(root, "0");
            VCNode Dest_lowestNode = getLowestNode(root, "220");
            if (src_lowestNode != null) {
                System.out.println(src_lowestNode.level);
            } else {
                System.out.println("not a vc not");
            }

            System.out.println("-----------------------------");

            if (Dest_lowestNode != null) {
                System.out.println(Dest_lowestNode.level);
            } else {
                System.out.println("not a vc not");
                HashSet<Node> ns = getNeighbor(graphdb, "220");
                System.out.println(ns.size());
                for (Node nn : ns) {
                    VCNode dAdj = getLowestNode(root, nn);
                    if (dAdj != null) {
                        System.out.println(dAdj.level);
                    } else {
                        System.out.println("not a vc not");
                    }
                }
            }
            tx.success();
        }
        n.shutdownDB();
    }

    private HashSet<Node> getNeighbor(GraphDatabaseService graphdb, String id) {
        HashSet<Node> result = new HashSet<>();
        Node node = graphdb.findNode(BNode.BusNode, "name", id);
        Iterable<Relationship> rels = node.getRelationships(Line.Linked, Direction.INCOMING);
        Iterator<Relationship> rels_iter = rels.iterator();
        while (rels_iter.hasNext()) {
            result.add(rels_iter.next().getStartNode());
        }
        return result;
    }

    private VCNode getLowestNode(VCNode root, String sid) {
        Node Source = null;
        int maxlevel = Integer.MIN_VALUE;
        VCNode tmpNode = null;

        try (Transaction tx = root.graphdb.beginTx()) {
            Source = root.graphdb.findNode(BNode.BusNode, "name", sid);
            tx.success();
        }

        Stack<VCNode> q = new Stack<>();
        q.add(root);
        while (!q.isEmpty()) {
            VCNode node = q.pop();
            String tabStr = "    ";
            if (node.level > maxlevel && node.dg.nodes.contains(Source)) {
                maxlevel = node.level;
                tmpNode = node;
            }

            if (node.children != null) {
                for (VCNode vc : node.children) {
                    if (vc.dg.nodes.contains(Source)) {
                        maxlevel = vc.level;
                        tmpNode = vc;
                        q.add(vc);
                    }
                }
            }

        }
        return tmpNode;
    }

    private VCNode getLowestNode(VCNode root, Node Source) {
        int maxlevel = Integer.MIN_VALUE;
        VCNode tmpNode = null;


        Stack<VCNode> q = new Stack<>();
        q.add(root);
        while (!q.isEmpty()) {
            VCNode node = q.pop();
            String tabStr = "    ";
            if (node.level > maxlevel && node.dg.nodes.contains(Source)) {
                maxlevel = node.level;
                tmpNode = node;
            }

            if (node.children != null) {
                for (VCNode vc : node.children) {
                    if (vc.dg.nodes.contains(Source)) {
                        maxlevel = vc.level;
                        tmpNode = vc;
                        q.add(vc);
                    }
                }
            }

        }
        return tmpNode;
    }

    private void retrivalTree(VCNode root) {
        Stack<VCNode> q = new Stack<>();
        q.add(root);
        while (!q.isEmpty()) {
            VCNode node = q.pop();
            String tabStr = "    ";
            System.out.println(new String(new char[node.level]).replace("\0", tabStr) + node.level);
            for (Node n : node.dg.nodes) {
                System.out.println(new String(new char[node.level]).replace("\0", tabStr) + n);
            }
//            for(DistanceEdge n:node.dg.edges)
//            {
//                System.out.println(new String(new char[node.level]).replace("\0", tabStr)+n);
//            }

            if (node.children != null) {
                for (VCNode vc : node.children) {
                    q.add(vc);
                }
            }

        }
    }

    private ArrayList<Node> getNodes(GraphDatabaseService graphdb) {
        ArrayList<Node> nodes = new ArrayList<>();
        try (Transaction tx = graphdb.beginTx();
             BufferedReader br = new BufferedReader(new FileReader(nodesPath))) {
            String line = null;
            while ((line = br.readLine()) != null) {
//                System.out.println(line.substring(0,line.indexOf(" ")));
                Node node = graphdb.findNode(BNode.BusNode, "name", line.substring(0, line.indexOf(" ")));
                nodes.add(node);
            }
            tx.success();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return nodes;
    }

    private ArrayList<Relationship> getEgdes(GraphDatabaseService graphdb) {
        ArrayList<Relationship> Rels = new ArrayList<>();
        try (Transaction tx = graphdb.beginTx()) {
            ResourceIterable<Relationship> iter_Rels = graphdb.getAllRelationships();
            ResourceIterator<Relationship> iterator = iter_Rels.iterator();
            System.out.println(iter_Rels.stream().count());
            int i = 0;
            try {
                while (iterator.hasNext()) {
                    i++;
                    Relationship item = iterator.next();
                    Rels.add(item);
                }
//                System.out.println("-----------"+i);
            } finally {
                iterator.close();
            }
            tx.success();
        }
        return Rels;

    }

}
