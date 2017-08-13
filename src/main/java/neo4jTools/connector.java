package neo4jTools;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileUtils;

import java.io.File;
import java.io.IOException;

public class connector {
    String DB_PATH = "/home/gqxwolf/neo4j/neo4j-community-3.2.3/testdb/databases/graph.db";
    GraphDatabaseService graphDB;

    public connector(String DB_PATH) {
        this.DB_PATH = DB_PATH;
    }

    public connector() {
    }

    public void startDB() {
        //this.graphDB = new GraphDatabaseFactory().newEmbeddedDatabase(new File(DB_PATH));
        this.graphDB = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(new File(this.DB_PATH))
                .setConfig(GraphDatabaseSettings.mapped_memory_page_size, "8k")
                .setConfig(GraphDatabaseSettings.pagecache_memory, "2G")
                .newGraphDatabase();
        //this.graphDB = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(new File(DB_PATH)).loadPropertiesFromFile("/home/gqxwolf/neo4j/conf/neo4j.properties").newGraphDatabase();


        registerShutdownHook(this.graphDB);
        if (graphDB == null) {
            //System.out.println("Initialize fault");
        } else {
            //System.out.println("Initialize success");
        }
    }


    private static void registerShutdownHook(final GraphDatabaseService graphDb) {
        // Registers a shutdown hook for the Neo4j instance so that it
        // // shuts down nicely when the VM exits (even if you "Ctrl-C" the
        // // running application).
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                graphDb.shutdown();
            }
        });
    }

    public void shutdownDB() {
        //System.out.println("Shut downing....");
        this.graphDB.shutdown();
    }

    public void clean() {
        startDB();
        if (this.graphDB != null)
        {
            System.out.println("Connect the db successfully");
        }
        try (Transaction tx = this.graphDB.beginTx()) {
            this.graphDB.execute("match (n) detach delete n;");
            tx.success();
        }
        shutdownDB();

        //System.out.println("Clean data base success");
    }

    public void test() {
        startDB();
        if (this.graphDB != null)
        {
            System.out.println("Connect the db successfully");
        }
        try (Transaction tx = this.graphDB.beginTx()) {

            Node javaNode = this.graphDB.createNode(BNode.BusNode);
            javaNode.setProperty("TutorialID", "JAVA001");
            javaNode.setProperty("Title", "Learn Java");
            javaNode.setProperty("NoOfChapters", "25");
            javaNode.setProperty("Status", "Completed");

            Node scalaNode = graphDB.createNode(BNode.BusNode);
            scalaNode.setProperty("TutorialID", "SCALA001");
            scalaNode.setProperty("Title", "Learn Scala");
            scalaNode.setProperty("NoOfChapters", "20");
            scalaNode.setProperty("Status", "Completed");

            Relationship relationship = javaNode.createRelationshipTo(scalaNode, Line.Linked);
            relationship.setProperty("Id", "1234");
            relationship.setProperty("OOPS", "YES");
            relationship.setProperty("FP", "YES");
            tx.success();
        }
        System.out.println("Done successfully");
        shutdownDB();

    }

    public static void main(String args[]) {
        connector n = new connector();
//        n.test();
        n.clean();

    }

    public void deleteDB() {
        try {
            FileUtils.deleteRecursively(new File(DB_PATH));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public GraphDatabaseService getDBObject() {
        return this.graphDB;
    }
}
