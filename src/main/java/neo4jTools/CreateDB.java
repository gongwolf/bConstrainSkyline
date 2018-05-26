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

    public CreateDB(int graphsize, int degree) {
//        this.DBBase = "/home/gqxwolf/mydata/projectData/testGraph" + graphsize + "_" + degree + "/data/";
//        this.DB_PATH = "/home/gqxwolf/neo4j334/testdb" + graphsize + "_" + degree + "/databases/graph.db";
        this.DB_PATH = "/home/gqxwolf/neo4j334/testdb_" + "SF" + "/databases/graph.db";
        this.DBBase = "/home/gqxwolf/mydata/projectData/testGraph_real_50/data/";
        //this.DBBase = "/home/gqxwolf/mydata/projectData/testGraph_real/data/";
        //this.DB_PATH = "/home/gqxwolf/neo4j334/testdb_real/databases/graph.db";

        NodesPath = DBBase + "SF_NodeInfo.txt";
        SegsPath = DBBase + "SF_SegInfo.txt";
    }


    public CreateDB() {

        this.DB_PATH = "/home/gqxwolf/neo4j334/testdb_" + "NY" + "/databases/graph.db";
        this.DBBase = "/home/gqxwolf/mydata/projectData/testGraph_real_50/data/";
        NodesPath = DBBase + "NY_NodeInfo.txt";
        SegsPath = DBBase + "NY_SegInfo.txt";
    }

    public static void main(String args[]) {

//        int graphsize = 10000;
//        int degree =1;
//        int dimension=3;
//
//        if (args.length == 3) {
//            graphsize = Integer.parseInt(args[0]);
//            degree = Integer.parseInt(args[1]);
//            dimension = Integer.parseInt(args[2]);
//        }
//
//        generateGraph g = new generateGraph(graphsize, degree, dimension);
//        g.generateG(true);

        CreateDB db = new CreateDB();
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

        int num_node = 0, num_edge = 0;


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
                num_node++;
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
                num_edge++;
            }

            tx.success();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        nconn.shutdownDB();
        System.out.println("Database is created, the location of the db file is " + this.DB_PATH);
        System.out.println("there are total "+num_node+" nodes and "+num_edge+" edges");
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
                indexs.add(n, "name", id);
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
        try {
            Node srcNode = this.graphdb.findNode(BNode.BusNode, "name", src);
            Node desNode = this.graphdb.findNode(BNode.BusNode, "name", des);
            Relationship rel = srcNode.createRelationshipTo(desNode, Line.Linked);
            rel.setProperty("EDistence", eDistence);
            rel.setProperty("MetersDistance", metersDistance);
            rel.setProperty("RunningTime", runningTime);
        } catch (Exception e) {
            System.out.println(src + "-->" + des);
        }
    }

    private Node createNode(String id, double lat, double log) {
        Node n = this.graphdb.createNode(BNode.BusNode);
        n.setProperty("name", id);
        n.setProperty("lat", lat);
        n.setProperty("log", log);
        return n;
    }
}