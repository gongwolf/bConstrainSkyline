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
                    HashMap<String, double[]> nodeToLandMark = this.toLandMartIndex.get(snd);
                    if (nodeToLandMark == null) {
                        nodeToLandMark = new HashMap<>();
                    }
                    nodeToLandMark.put(dnd, costs);
                    this.toLandMartIndex.put(snd, nodeToLandMark);
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


    public void buildInnerSkylineIndex(GraphDatabaseService graphdb) {
        for (String snd : this.iportals) {
            String sid = String.valueOf(Integer.parseInt(snd) - 1);
            for (String dnd : this.oportals) {
                if (snd.equals(dnd)) {
                    continue;
                }

                String did = String.valueOf(Integer.parseInt(dnd) - 1);
                Node source = graphdb.findNode(BNode.BusNode, "name", sid);
                Node destination = graphdb.findNode(BNode.BusNode, "name", did);
                getSkylineInBlock(source, destination, graphdb);
            }
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


    private int NumberOfProperties;

    private void getSkylineInBlock(Node source, Node destination, GraphDatabaseService graphdb) {
        ArrayList<path> result = new ArrayList<>();
        myNodePriorityQueue mqueue = new myNodePriorityQueue();
        path iniPath = new path(source, source);

        this.NumberOfProperties = iniPath.NumberOfProperties;
        int landmark_idx = -1;

        for (String ptype : iniPath.propertiesName) {
            landmark_idx++;
            path sht_path_in_type = DijkstraInType(source, destination, ptype, landmark_idx);
        }
    }

    private path DijkstraInType(Node source, Node destination, String property_type, int landmark_idx) {


        myNodeDijkstraPriorityQueue myDQ = new myNodeDijkstraPriorityQueue();
        myNode sNode = new myNode(source, source, true);
        path spath = new path(source);
        sNode.subRouteSkyline.add(spath);

        HashMap<String, myNode> VistedNodes = new HashMap<>();
        HashMap<String, Double> cost_so_far = new HashMap<>();

        cost_so_far.put(sNode.id, 0.0);

        while (!myDQ.isEmpty()) {
            myNode n = myDQ.pop();
            if (n.id.equals(String.valueOf(destination.getId()))) {
//                return
            }
            VistedNodes.put(n.id, n);

            //get the neighborhood's nodes and its cost
            ArrayList<Pair<myNode, Double>> adjNodes = n.getAdjNodes(property_type);

            for (Pair<myNode, Double> next_pair : adjNodes) {
                myNode nextNode = next_pair.getKey();
                double newCost = next_pair.getValue() + cost_so_far.get(nextNode.id);
                if (!cost_so_far.containsKey(next_pair.getKey()) || newCost < cost_so_far.get(nextNode.id)) {
                    cost_so_far.put(nextNode.id, newCost);
                    double Dij_priority = newCost + Dij_heuristic(destination, nextNode, landmark_idx);
                    nextNode.priority = Dij_priority;
                }
            }
        }

        return null;
    }

    private double Dij_heuristic(Node destination, myNode nextNode, int landmark_idx) {
        double maxValue = Double.NEGATIVE_INFINITY;
        String dnd = String.valueOf(destination.getId() + 1);
        String cnd = String.valueOf(nextNode.current.getId() + 1);

        HashMap<String, double[]> toLowerBound = toLandMartIndex.get(cnd);
        HashMap<String, double[]> fromLowerBound = toLandMartIndex.get(dnd);
        if (toLowerBound != null || fromLowerBound != null) {
            for (String lnd : this.landMarks) {
                double[] t_l_cost = toLowerBound.get(lnd);
                double[] f_l_cost = fromLowerBound.get(lnd);

                if (t_l_cost != null && f_l_cost != null) {
                    double D_value = t_l_cost[landmark_idx]-f_l_cost[landmark_idx];
                    if(maxValue<Math.abs(D_value))
                    {
                        maxValue= D_value;
                    }
                }
            }
        }

        return  maxValue;
    }


}




