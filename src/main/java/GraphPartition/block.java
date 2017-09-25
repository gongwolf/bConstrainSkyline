package GraphPartition;

import neo4jTools.BNode;
import neo4jTools.Line;
import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphalgo.WeightedPath;
import org.neo4j.graphdb.*;

import java.util.*;

public class block {
    public ArrayList<String> nodes;
    public ArrayList<String> iportals;
    public ArrayList<String> oportals;
    public ArrayList<String> landMarks;
    public HashMap<String, HashMap<String, double[]>> toLandMartIndex; //nodeid --> <land_mark_node_id --> costs >
    public HashMap<String, HashMap<String, double[]>> fromLandMarkIndex; //nodeid --> <land_mark_node_id --> costs >

    public block() {
        nodes = new ArrayList<>();
        iportals = new ArrayList<>();
        oportals = new ArrayList<>();
        landMarks = new ArrayList<>();
        toLandMartIndex = new HashMap<>();
        fromLandMarkIndex = new HashMap<>();
    }

    public void buildLandmarkIndex(GraphDatabaseService graphdb) {
//        System.out.println("   landmark_indexing building .......");
        path fakePath = null;
        for (String dnd : this.landMarks) {
            String did = String.valueOf(Integer.parseInt(dnd) - 1);
            for (String snd : this.nodes) {
                boolean needTobeJump = false;
                String sid = String.valueOf(Integer.parseInt(snd) - 1);
                Node source = graphdb.findNode(BNode.BusNode, "name", sid);
                Node destination = graphdb.findNode(BNode.BusNode, "name", did);

                if (fakePath == null) {
                    fakePath = new path(source);
                }

                //find the cost from node to landmark
                HashMap<String, double[]> nodeToLandMark = new HashMap<>();
                double[] costs = new double[fakePath.NumberOfProperties];
                int i = 0;
                for (String costType : fakePath.propertiesName) {
                    double cost = initilizeSkylinePath(source, destination, costType);
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
                    nodeToLandMark.put(dnd, costs);
                    this.toLandMartIndex.put(snd, nodeToLandMark);
                }


                //find the cost from landmark to node.
                HashMap<String, double[]> LandMarktoNode = new HashMap<>();
                i = 0;
                for (String costType : fakePath.propertiesName) {
                    double cost = initilizeSkylinePath(destination, source, costType);
                    costs[i] = cost;
                    //if there is no path from source to destination, set the first dimension of the cost to be -1
                    if (cost == -1) {
                        break;
                    }
                }

                //if there is no path from the node to the landmark,
                //also, there is no path from landmark to node,
                //then jump to next next node.
                if (costs[0] == -1 & needTobeJump) {
                    break;
                } else {
                    LandMarktoNode.put(dnd, costs);
                    this.fromLandMarkIndex.put(snd, LandMarktoNode);
                }
            }
        }
    }


    public void buildInnerSkylineIndex(GraphDatabaseService graphdb) {
        for (String snd : this.iportals) {
            String sid = String.valueOf(Integer.parseInt(snd) - 1);
            for (String dnd : this.oportals) {
                if (snd.equals(dnd)) {
                    break;
                }


                String did = String.valueOf(Integer.parseInt(dnd) - 1);
                Node source = graphdb.findNode(BNode.BusNode, "name", sid);
                Node destination = graphdb.findNode(BNode.BusNode, "name", did);

            }
        }
    }

    private double initilizeSkylinePath(Node source, Node destination, String costType) {
        PathFinder<WeightedPath> finder = GraphAlgoFactory
                .dijkstra(PathExpanders.forTypeAndDirection(Line.Linked, Direction.OUTGOING), costType);
        WeightedPath paths = finder.findSinglePath(source, destination);
        if (paths == null)
            return -1;
        return paths.weight();
    }


}




