package Pindex.vctest;

import neo4jTools.BNode;
import neo4jTools.connector;
import org.neo4j.graphdb.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;

public class vctest {
    VCNode root;
    String basePath = "/home/gqxwolf/mydata/projectData/testGraph/data/";
    String nodesPath = basePath+"NodeInfo.txt";
    String edgesPath = basePath+"SegInfo.txt";


    public static void main(String args[])
    {
        vctest vt = new vctest();
        vt.buildVCTree(10);
    }

    private void buildVCTree(int threshold) {
        connector n = new connector("/home/gqxwolf/neo4j323/testdb/databases/graph.db");
        n.startDB();
        GraphDatabaseService graphdb = n.getDBObject();

        ArrayList<Node> nodes = getNodes(graphdb);
        ArrayList<Relationship> edges = getEgdes(graphdb);
        System.out.println("There are "+nodes.size()+ " nodes,\nThere are "+ edges.size()+" Edges.");

        root = new VCNode(graphdb);
        root.buildDistanceGraph(new Graph(nodes,edges),threshold);
        n.shutdownDB();
    }

    private ArrayList<Node> getNodes(GraphDatabaseService graphdb) {
        ArrayList<Node> nodes = new ArrayList<>();
        try (Transaction tx = graphdb.beginTx();
             BufferedReader br = new BufferedReader(new FileReader(nodesPath))) {
            String line = null;
            while ((line = br.readLine()) != null) {
//                System.out.println(line.substring(0,line.indexOf(" ")));
                Node node = graphdb.findNode(BNode.BusNode, "name", line.substring(0,line.indexOf(" ")));
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
        try(Transaction tx = graphdb.beginTx()) {
            ResourceIterable<Relationship> iter_Rels = graphdb.getAllRelationships();
            ResourceIterator<Relationship> iterator = iter_Rels.iterator();
            System.out.println(iter_Rels.stream().count());
            int i = 0 ;
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
