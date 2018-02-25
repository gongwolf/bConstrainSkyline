package neo4jTools;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexManager;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class CreateDB {
    String DBBase = "/home/gqxwolf/mydata/projectData/un_testGraph2000_5/data/";
    String DB_PATH = "/home/gqxwolf/neo4j323/test_un_db2000_5/databases/graph.db";
    String NodesPath = DBBase + "NodeInfo.txt";
    String SegsPath = DBBase + "SegInfo.txt";
    private GraphDatabaseService graphdb = null;

    public CreateDB(int graphsize, int degree)
    {
        this.DBBase = "/home/gqxwolf/mydata/projectData/testGraph"+graphsize+"_"+degree+"/data/";
        this.DB_PATH = "/home/gqxwolf/neo4j323/testdb"+graphsize+"_"+degree+"/databases/graph.db";
        NodesPath = DBBase + "NodeInfo.txt";
        SegsPath = DBBase + "SegInfo.txt";    }

    public static void main(String args[]) {
        CreateDB db = new CreateDB(20,5);
//        db.createDatabasewithIndex("Id");
        db.createDatabase();
    }

    public void createDatabase() {
        //System.out.println(DB_PATH);
        //System.out.println(DBBase);
        //System.out.println("=============");

        connector nconn = new connector(DB_PATH);
        //delete the data base at first
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
                //System.out.println(line);
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
        System.out.println("Database is created, the location of the db file is "+this.DB_PATH);
    }


    public void createDatabasewithIndex(String property) {
        connector nconn = new connector(DB_PATH);
        //delete the data base at first
        nconn.deleteDB();
        nconn.startDB();
        this.graphdb = nconn.getDBObject();


        try (Transaction tx = this.graphdb.beginTx()) {
            IndexManager indexm = this.graphdb.index();
            Index<Node> indexs = indexm.forNodes(property);
            BufferedReader br = new BufferedReader(new FileReader(NodesPath));
            String line = null;
            while ((line = br.readLine()) != null) {
                System.out.println(line);
                String[] attrs = line.split(" ");

                String id = attrs[0];
                double lat = Double.parseDouble(attrs[1]);
                double log = Double.parseDouble(attrs[2]);
                Node n = createNode(id, lat, log);
                indexs.add(n,"name",id);
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