package neo4jTools;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexManager;

import java.util.Iterator;
import java.util.Random;

public class indexTesting {
    long nodetime, reltime,getCost;

    public static void main(String args[]) {
        indexTesting it = new indexTesting();
//        it.test();
        it.runningTimeTesting();
        it.getAllNode();
    }

    private void runningTimeTesting() {
        int[] Gsize = new int[]{2000, 10000,10001, 20000};
        for (int s : Gsize) {
            String dbpath = "";
            switch (s) {
                case 2000:
                    dbpath = "/home/gqxwolf/neo4j323/testdb/databases/graph.db";
                    break;
                case 10000:
                    dbpath = "/home/gqxwolf/neo4j323/testdb10000/databases/graph.db";
                    break;
                case 10001:
                    dbpath = "/home/gqxwolf/neo4j323/testdb10000_idx/databases/graph.db";
                    break;
                case 20000:
                    dbpath = "/home/gqxwolf/neo4j323/testdb20000/databases/graph.db";
                    break;
            }
            long runningTime_s = System.currentTimeMillis();
            connector n = new connector(dbpath);
            n.startDB();
            GraphDatabaseService graphDB = n.getDBObject();
            long query_num = 1000;
            RandomGetInformation(dbpath, s, graphDB, query_num);
            long runningtime = System.currentTimeMillis() - runningTime_s;
//            System.out.println("Ruuning time in "+s+" size graph get random 1000 node and it's edges information, used "+runningtime+" ms");
            System.out.println(runningtime+","+this.nodetime/1000000+","+this.reltime/1000000+","+this.getCost/1000000);
            n.shutdownDB();
        }
    }

    private void RandomGetInformation(String dbpath, int size, GraphDatabaseService graphDB, long query_num) {
        try (Transaction tx = graphDB.beginTx()) {
            for (int i = 0; i < query_num; i++) {
                String nodeID = String.valueOf(getRandomNumberInRange(0, size - 1));
                long r1 = System.nanoTime();
//                Node node = graphDB.findNode(BNode.BusNode, "Id", nodeID);
                Node node = graphDB.getNodeById(Long.parseLong(nodeID));
                this.nodetime += System.nanoTime()-r1;
                r1 = System.nanoTime();
                Iterable<Relationship> rels = node.getRelationships(Line.Linked, Direction.BOTH);
                Iterator<Relationship> rel_Iter = rels.iterator();
                this.reltime += System.nanoTime()-r1;

                while (rel_Iter.hasNext()) {
                    r1 = System.nanoTime();
                    Relationship rel = rel_Iter.next();
                    Node nextNode = rel.getStartNode();
                    this.reltime += System.nanoTime()-r1;

                    r1 = System.nanoTime();
                    Double cost = Double.parseDouble(rel.getProperty("MetersDistance").toString());
                    this.getCost+=System.nanoTime()-r1;
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

    public void getAllNode()
    {
        connector n = new connector("/home/gqxwolf/neo4j323/testdb10000/databases/graph.db");
        n.startDB();
        GraphDatabaseService graphDB = n.getDBObject();
        try (Transaction tx = graphDB.beginTx()) {
            ResourceIterator<Node> itor = graphDB.findNodes(BNode.BusNode);
            while(itor.hasNext())
            {
//                System.out.println("1111");
                Node node = itor.next();
                String str_id = (String)node.getProperty("name");
                String embed_id = String.valueOf(node.getId());
                if(!str_id.equals(embed_id))
                {
                    System.out.println(str_id);
                }
            }
        }
        n.shutdownDB();

    }
}
