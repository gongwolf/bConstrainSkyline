import javafx.util.Pair;
import neo4jTools.BNode;
import neo4jTools.connector;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import java.io.BufferedReader;
import java.io.FileReader;

public class LandMarkIndex {

    private GraphDatabaseService graphdb;
    private String nodesFilePath = "/home/gqxwolf/mydata/projectData/ConstrainSkyline/data/NodeInfo.txt";
    private connector conn;

    public static void main(String args[])
    {
        LandMarkIndex lmd = new LandMarkIndex();
        lmd.buildIndex();
    }

    public void buildIndex()
    {
        buildConnection();
        loadNodesDegrees();
        DestroyConnection();
    }

    private void loadNodesDegrees() {
        try (BufferedReader br = new BufferedReader(new FileReader(nodesFilePath));){
            String line = null;
            while ((line = br.readLine()) != null) {
                String node_id = line.split(",")[0];
                try(Transaction tx = this.graphdb.beginTx())
                {
                    Node n = graphdb.findNode(BNode.BusNode, "name", "2");
                    int outgoing_degree = n.getDegree(Direction.OUTGOING);
                    int in_comming_degree = n.getDegree(Direction.INCOMING);
                    int total_degree = n.getDegree();
                    System.out.println(outgoing_degree+"  "+in_comming_degree+" "+total_degree);
                    tx.success();
                }

                break;

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void buildConnection() {
        connector n = new connector("/home/gqxwolf/neo4j/csldb/databases/graph.db");
        this.conn = n ;
        this.conn.startDB();
        this.graphdb = n.getDBObject();
    }

    private void DestroyConnection(){
        this.conn.shutdownDB();
    }


}
