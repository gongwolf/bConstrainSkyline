package neo4jTools;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexManager;

import java.util.Iterator;
import java.util.Random;

public class indexTesting {
    public static void main(String args[]) {
        indexTesting it = new indexTesting();
        it.test();
        it.runningTimeTesting();
    }

    private void runningTimeTesting() {
        int[] Gsize = new int[]{2000, 10000, 20000};
        for (int s : Gsize) {
            String dbpath = "";
            switch (s) {
                case 2000:
                    dbpath = "/home/gqxwolf/neo4j323/testdb/databases/graph.db";
                    break;
                case 10000:
                    dbpath = "/home/gqxwolf/neo4j323/testdb10000/databases/graph.db";
                    break;
                case 20000:
                    dbpath = "/home/gqxwolf/neo4j323/testdb20000/databases/graph.db";
                    break;
            }
            long runningTime_s = System.currentTimeMillis();
            connector n = new connector(dbpath);
            n.startDB();
            GraphDatabaseService graphDB = n.getDBObject();
            long query_num = 100000;
            RandomGetInformation(dbpath, s, graphDB, query_num);
            long runningtime = System.currentTimeMillis() - runningTime_s;
//            System.out.println("Ruuning time in "+s+" size graph get random 1000 node and it's edges information, used "+runningtime+" ms");
            System.out.println(runningtime);
            n.shutdownDB();
        }
    }

    private void RandomGetInformation(String dbpath, int size, GraphDatabaseService graphDB, long query_num) {
        try (Transaction tx = graphDB.beginTx()) {
            for (int i = 0; i < query_num; i++) {
                String nodeID = String.valueOf(getRandomNumberInRange(0, size - 1));
                Node node = graphDB.findNode(BNode.BusNode, "name", nodeID);
                Iterable<Relationship> rels = node.getRelationships(Line.Linked, Direction.BOTH);
                Iterator<Relationship> rel_Iter = rels.iterator();
                while (rel_Iter.hasNext()) {
                    Relationship rel = rel_Iter.next();
                    Node nextNode = rel.getStartNode();
                    Double cost = Double.parseDouble(rel.getProperty("MetersDistance").toString());
                }

                if (i % 5000 == 0)
                    tx.success();
            }
            tx.success();
        }
    }

    private void test() {
        connector n = new connector("/home/gqxwolf/neo4j323/testdb10000/databases/graph.db");
        n.startDB();
        GraphDatabaseService graphDB = n.getDBObject();
        try (Transaction tx = graphDB.beginTx()) {

            IndexManager index = graphDB.index();
            boolean f1 = index.existsForNodes("Name");
            System.out.println(f1);
            Index<Node> names = index.forNodes("Name");
            boolean f2 = index.existsForNodes("Name");
            System.out.println(f2);
            names.delete();

            tx.success();
        }
        n.shutdownDB();
    }

    private int getRandomNumberInRange(int min, int max) {

        if (min >= max) {
            throw new IllegalArgumentException("max must be greater than min");
        }

        Random r = new Random();
        return r.nextInt((max - min) + 1) + min;
    }
}
