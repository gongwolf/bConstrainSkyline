package neo4jTools;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexManager;

public class indexTesting {
    public static void main(String args[]) {
        indexTesting it = new indexTesting();
        it.test();
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
}
