package neo4jTools;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class CreateDB {
    String DBBase = "/home/gqxwolf/mydata/projectData/testGraph1/data/";
    String NodesPath = DBBase + "NodeInfo.txt";
    String SegsPath = DBBase + "SegInfo.txt";
    private GraphDatabaseService graphdb = null;

    public static void main(String args[]) {
        CreateDB db = new CreateDB();
        db.createDatabase();
    }

    public void createDatabase() {
        connector nconn = new connector("/home/gqxwolf/neo4j323/testdb1/databases/graph.db");
        nconn.deleteDB();
        nconn.startDB();
        this.graphdb = nconn.getDBObject();


        try (Transaction tx = this.graphdb.beginTx()) {
            BufferedReader br = new BufferedReader(new FileReader(NodesPath));
            String line = null;
            while ((line = br.readLine()) != null) {
                //System.out.println(line);
                String[] attrs = line.split(" ");

                String id = attrs[0];
                double lat = Double.parseDouble(attrs[1]);
                double log = Double.parseDouble(attrs[2]);
                Node n = createNode(id, lat, log);
            }

            br = new BufferedReader(new FileReader(SegsPath));
            line = null;
            while ((line = br.readLine()) != null) {
                System.out.println(line);
                String attrs[] = line.split(" ");
                String src = attrs[0];
                String des = attrs[1];
                double EDistence = Double.parseDouble(attrs[2]);
                double MetersDistance = Double.parseDouble(attrs[3]);
                double RunningTime = Double.parseDouble(attrs[4]);
                createRelation(src, des, EDistence, MetersDistance, RunningTime);
            }

            tx.success();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        nconn.shutdownDB();
    }

    private void createRelation(String src, String des, double eDistence, double metersDistance, double runningTime) {
        Node srcNode = this.graphdb.findNode(BNode.BusNode, "name", src);
        Node desNode = this.graphdb.findNode(BNode.BusNode, "name", des);
        Relationship rel = srcNode.createRelationshipTo(desNode, Line.Linked);
        rel.setProperty("EDistence", eDistence);
        rel.setProperty("MetersDistance", metersDistance);
        rel.setProperty("RunningTime", runningTime);
    }

    private Node createNode(String id, double lat, double log) {
        Node n = this.graphdb.createNode(BNode.BusNode);
        n.setProperty("name", id);
        n.setProperty("lat", lat);
        n.setProperty("log", log);
        return n;
    }
}