package GraphPartition;

import javafx.util.Pair;
import neo4jTools.BNode;
import neo4jTools.Line;
import org.neo4j.cypher.internal.frontend.v3_2.phases.Do;
import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphalgo.WeightedPath;
import org.neo4j.graphalgo.impl.path.Dijkstra;
import org.neo4j.graphdb.*;

import java.util.*;

public class block {
    private final String pid;
    public ArrayList<String> nodes;
    public ArrayList<String> iportals;
    public ArrayList<String> oportals;
    public ArrayList<String> landMarks;
    public HashMap<String, HashMap<String, double[]>> toLandMarkIndex; //nodeid --> <land_mark_node_id --> costs >
    public HashMap<String, HashMap<String, double[]>> fromLandMarkIndex; //nodeid --> <land_mark_node_id --> costs >
    public HashMap<Pair<String, String>, ArrayList<path>> innerIndex;

    public block(String pid) {
        this.pid = pid;
        nodes = new ArrayList<>();
        iportals = new ArrayList<>();
        oportals = new ArrayList<>();
        landMarks = new ArrayList<>();
        toLandMarkIndex = new HashMap<>();
        fromLandMarkIndex = new HashMap<>();
        innerIndex = new HashMap<>();
    }

    public void buildLandmarkIndex(GraphDatabaseService graphdb) {
//        System.out.println("   landmark_indexing building .......");
        path fakePath = null;
        for (String dnd : this.landMarks) {
            String did = String.valueOf(Integer.parseInt(dnd) - 1);
            for (String snd : this.nodes) {
                boolean needTobeJump = false;
                String sid = String.valueOf(Integer.parseInt(snd) - 1);
                Node source = graphdb.getNodeById(Long.parseLong(sid));
                Node destination = graphdb.getNodeById(Long.parseLong(did));
//                Node source = graphdb.findNode(BNode.BusNode, "name", sid);
//                Node destination = graphdb.findNode(BNode.BusNode, "name", did);

                if (fakePath == null) {
                    fakePath = new path(source);
                }

                //find the cost from node to landmark

                double[] costs = new double[fakePath.NumberOfProperties];
                int i = 0;
                for (String costType : fakePath.propertiesName) {
                    double cost = getShortestPathWeight(source, destination, costType);
                    costs[i] = cost;
                    //if there is no path from source to destination, set the first dimension of the cost to be -1
                    if (cost == -1) {
                        break;
                    }
                }

                //if there is no path from the node to the landmark, set the need to jump variable to be true.
                if (costs[0] == -1) {
                    needTobeJump = true;
                } else {
                    HashMap<String, double[]> nodeToLandMark = this.toLandMarkIndex.get(snd);
                    if (nodeToLandMark == null) {
                        nodeToLandMark = new HashMap<>();
                    }
                    nodeToLandMark.put(dnd, costs);
                    this.toLandMarkIndex.put(snd, nodeToLandMark);
                }


                //find the cost from landmark to node.
                i = 0;
                for (String costType : fakePath.propertiesName) {
                    double cost = getShortestPathWeight(destination, source, costType);
                    costs[i] = cost;
                    //if there is no path from source to destination, set the first dimension of the cost to be -1
                    if (cost == -1) {
                        break;
                    }
                }

                //if there is no path from the node to the landmark,
                //also, there is no path from landmark to node,
                //then jump to next next node.
                if ((costs[0] == -1)) {
                    continue;
                } else {
                    HashMap<String, double[]> LandMarktoNode = this.fromLandMarkIndex.get(snd);
                    if (LandMarktoNode == null) {
                        LandMarktoNode = new HashMap<>();
                    }
                    LandMarktoNode.put(dnd, costs);
                    this.fromLandMarkIndex.put(snd, LandMarktoNode);
                }
            }
        }
    }


    public void buildInnerSkylineIndex(GraphDatabaseService graphdb, String lowerboundSelector) {
        for (String snd : this.iportals) {
            String sid = String.valueOf(Integer.parseInt(snd) - 1);
            for (String dnd : this.oportals) {
                if (snd.equals(dnd)) {
                    continue;
                }

                String did = String.valueOf(Integer.parseInt(dnd) - 1);
//                Node source = graphdb.findNode(BNode.BusNode, "name", sid);
                Node source = graphdb.getNodeById(Long.parseLong(sid));
//                Node destination = graphdb.findNode(BNode.BusNode, "name", did);
                Node destination = graphdb.getNodeById(Long.parseLong(did));
                skylineInBlock sbib = new skylineInBlock(graphdb, this);
                ArrayList<path> skypaths = null;
                if (lowerboundSelector.equals("landmark")) {
//                    System.out.println("run landmark");
                    skypaths = sbib.getSkylineInBlock_blinks(source, destination);
                } else if (lowerboundSelector.equals("dijsktra")) {
                    skypaths = sbib.getSkylineInBlock_Dijkstra(source, destination);
                }

//                System.out.println(skypaths.size()+"-----");

                if (skypaths != null) {
                    Pair<String, String> keyP = new Pair<>(snd, dnd);
                    this.innerIndex.put(keyP, skypaths);
                }
//                break;
            }
//            break;
        }
    }


    private double getShortestPathWeight(Node source, Node destination, String costType) {
        PathFinder<WeightedPath> finder = GraphAlgoFactory
                .dijkstra(PathExpanders.forTypeAndDirection(Line.Linked, Direction.OUTGOING), costType);
        WeightedPath paths = finder.findSinglePath(source, destination);
        if (paths == null) {
            return -1;
        } else {
            return paths.weight();
        }
    }


}




