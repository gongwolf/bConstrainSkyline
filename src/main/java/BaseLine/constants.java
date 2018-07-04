package BaseLine;

import javafx.util.Pair;
import neo4jTools.connector;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class constants {
    public static final int path_dimension = 4; //1(edu_dis)+3(road net work attrs)+3(static node attrs);

    public static HashMap<Long, Long> accessedNodes = new HashMap<>();
    public static HashMap<Long, Long> accessedEdges = new HashMap<>();

    public static HashMap<Long, Pair<Double, Double>> nodes = new HashMap<>();
    public static HashMap<Long, ArrayList<Pair<Pair<Long,Long>, double[]>>> edges = new HashMap<>(); //node_id --> ArrayList<<end_id,rel_id>, <double costs[]>>


    public static void print(double[] costs) {
        System.out.print("[");
        for (double c : costs) {
            System.out.print(c + " ");
        }
        System.out.println("]");
    }

    public static void readData(String graphPath) {
        nodes = new HashMap<>();
        edges = new HashMap<>();
        System.out.println(graphPath);
        connector.graphDB = null;
        connector n = new connector(graphPath);
        n.startDB(true);
        try (Transaction tx = connector.graphDB.beginTx()) {
            readNodes();
            readRelationShips();
            tx.success();
        }
        n.shutdownDB();
    }

    private static void readNodes() {
        try (ResourceIterator<Node> allnodes = connector.graphDB.getAllNodes().iterator()) {
            while (allnodes.hasNext()) {
                Node n = allnodes.next();
                double latitude = (double) n.getProperty("lat");
                double longitude = (double) n.getProperty("log");
//                System.out.println(n.getId() + " " + latitude + " " + longitude);

                constants.nodes.put(n.getId(), new Pair<>(latitude, longitude));

            }
        }
        System.out.println("Finish reading the nodes " + constants.nodes.size());
    }

    private static void readRelationShips() {
        try (ResourceIterator<Relationship> allrelations = connector.graphDB.getAllRelationships().iterator()) {
            while (allrelations.hasNext()) {
                Relationship r = allrelations.next();
                double c1 = (double) r.getProperty("EDistence");
                double c2 = (double) r.getProperty("MetersDistance");
                double c3 = (double) r.getProperty("RunningTime");
//                System.out.println(r.getId() + " " + c1 + " " + c2 + " " + c3);

                double[] costs = new double[]{c1, c2, c3};

                long sid = r.getStartNodeId();
                long did = r.getEndNodeId();

                Pair<Long,Long> keys = new Pair(did,r.getId());

                Pair p = new Pair(keys, costs);

                ArrayList outgoingList = null;
                if ((outgoingList = constants.edges.get(sid)) != null) {
                    outgoingList.add(p);
                } else {
                    outgoingList = new ArrayList();
                    outgoingList.add(p);
                    constants.edges.put(sid, outgoingList);
                }
            }

            int counter = 0;
            for (Map.Entry<Long, ArrayList<Pair<Pair<Long,Long>, double[]>>> entity : constants.edges.entrySet()) {
                counter += entity.getValue().size();
            }
            System.out.println("Finish reading the edges " + counter);
        }
    }
}
