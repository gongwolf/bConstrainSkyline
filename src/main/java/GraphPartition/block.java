package GraphPartition;

import javafx.util.Pair;
import neo4jTools.Line;
import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphalgo.WeightedPath;
import org.neo4j.graphdb.*;

import java.util.*;

public class block implements Comparable<block> {
    public final String pid;
    public HashSet<String> nodes;
    public HashSet<String> iportals;
    public HashSet<String> oportals;
    public HashSet<String> landMarks;
    public HashMap<String, HashMap<String, double[]>> toLandMarkIndex; //nodeid --> <land_mark_node_id --> costs >
    public HashMap<String, HashMap<String, double[]>> fromLandMarkIndex; //nodeid --> <land_mark_node_id --> costs >
    public HashMap<Pair<String, String>, ArrayList<path>> innerIndex;
    public HashMap<String, myNode> portalSkyline;
    HashMap<Long, myNode> ProcessedNodes = new HashMap<>();


    public block(String pid) {
        this.pid = pid;
        nodes = new HashSet<>();
        iportals = new HashSet<>();
        oportals = new HashSet<>();
        landMarks = new HashSet<>();
        toLandMarkIndex = new HashMap<>();
        fromLandMarkIndex = new HashMap<>();
        innerIndex = new HashMap<>();
        this.portalSkyline = new HashMap<>();
    }

    public void cleanMemory() {
        this.ProcessedNodes.clear();
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
                    skypaths = sbib.getSkylineInBlock_blinks(source, destination);
                } else if (lowerboundSelector.equals("dijkstra")) {
                    skypaths = sbib.getSkylineInBlock_Dijkstra(source, destination);
                }


                if (skypaths != null) {
//                    if (skypaths.size() != 1) {
//                        System.out.println(snd + "==>" + dnd + "-----");
//                    }
                    Pair<String, String> keyP = new Pair<>(snd, dnd);
                    this.innerIndex.put(keyP, skypaths);
//                    System.out.println(skypaths.get(0));
//                    break;
                }
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


    public boolean hasPathToLandMark(String nid) {
        return toLandMarkIndex.containsKey(nid) && fromLandMarkIndex.containsKey(nid);
    }

    public ArrayList<path> concatenatePath(path p) {
        ArrayList<path> result = new ArrayList<>();
        String p_end_node_str_id = String.valueOf(p.endNode.getId() + 1);
        for (Map.Entry<Pair<String, String>, ArrayList<path>> innerSky_obj : innerIndex.entrySet()) {
            String startNode = innerSky_obj.getKey().getKey();
            if (startNode.equals(p_end_node_str_id)) {
                for (path sp : innerSky_obj.getValue()) {
                    path new_path = new path(p, sp);
                    if (!new_path.isCycle())
                        result.add(new_path);
                }
            }
        }

        return result;
    }

    @Override
    public int compareTo(block o) {
        if (this.pid.equals(o.pid)) {
            return 0;
        } else if (Integer.parseInt(this.pid) < Integer.parseInt(o.pid)) {
            return 1;
        } else {
            return -1;
        }
    }

    @Override
    public boolean equals(Object o) {

        // If the object is compared with itself then return true
        if (o == this) {
            return true;
        }

        /* Check if o is an instance of Complex or not
          "null instanceof [type]" also returns false */
        if (!(o instanceof block)) {
            return false;
        }

        // typecast o to Complex so that we can compare data members
        block c = (block) o;

        // Compare the data members and return accordingly
        return c.pid.equals(((block) o).pid);
    }


    private void DijkstraInType(Node source, String property_type) {
        myNodeDijkstraPriorityQueue myDQ = new myNodeDijkstraPriorityQueue();
        myNode sNode = this.ProcessedNodes.get(source.getId());
        if (sNode == null) {
            sNode = new myNode(source, source, true);
        }
        path spath = new path(source);
        sNode.shortestPaths.put(property_type, spath);
        myDQ.add(sNode);


        HashMap<Long, Double> cost_so_far = new HashMap<>();

        cost_so_far.put(source.getId(), 0.0);

        while (!myDQ.isEmpty()) {
            myNode n = myDQ.pop();
            ProcessedNodes.put(n.current.getId(), n);
//            System.out.println(n.id);

            //get the neighborhood's nodes and its cost
            ArrayList<Relationship> adjNodes = n.getAdjNodes();

            for (Relationship rel : adjNodes) {
                Node nNode = rel.getEndNode();
                myNode nextNode = ProcessedNodes.get(nNode.getId());
                if (nextNode == null) {
                    nextNode = new myNode(source, nNode, false);
                }

                Double cost = Double.parseDouble(rel.getProperty(property_type).toString());
                Double oldCost = cost_so_far.get(n.current.getId());
                double current_cost = Double.POSITIVE_INFINITY;
                if (cost_so_far.get(nNode.getId()) != null) {
                    current_cost = cost_so_far.get(nNode.getId());
                }
                double newCost = cost + oldCost;

                String nextID = String.valueOf(nNode.getId() + 1);
                if (this.nodes.contains(nextID)) { // if the node in this block
                    if (!cost_so_far.containsKey(nNode.getId()) || newCost < current_cost) { // this node did not access or have shorter distance
                        cost_so_far.put(nNode.getId(), newCost);
                        double Dij_priority = newCost;
                        nextNode.priority = Dij_priority;
                        path npath = new path(n.shortestPaths.get(property_type), rel);
//                        System.out.println("--"+npath);
                        nextNode.shortestPaths.put(property_type, npath);
                        myDQ.add(nextNode);
                    }
                }
            }
        }
    }

    public void buildLandmarkIndex_inBlock(GraphDatabaseService graphdb) {
        path fakePath = null;
        for (String snd : this.nodes) {
            boolean needTobeJump = false;
            String sid = String.valueOf(Integer.parseInt(snd) - 1);
            Node source = graphdb.getNodeById(Long.parseLong(sid));
//            Node destination = graphdb.getNodeById(Long.parseLong(did));
//            Node source = graphdb.findNode(BNode.BusNode, "name", sid);
//            Node destination = graphdb.findNode(BNode.BusNode, "name", did);

            if (fakePath == null) {
                fakePath = new path(source);
            }

            //find landmark costs from node to landmark nodes
            for (String ptype : fakePath.propertiesName) {
                DijkstraInType(source, ptype);
            }

            for (String lnd : this.landMarks) {
                boolean flag = false;
                double[] landmark_costs = new double[fakePath.NumberOfProperties];
                int indexLandmk = 0;
                for (String ptype : fakePath.propertiesName) {
                    long lnd_id = Long.parseLong(lnd) - 1;
                    myNode landmarknode = this.ProcessedNodes.get(lnd_id);

                    if (landmarknode != null) {
                        double[] costs = landmarknode.shortestPaths.get(ptype).getCosts();
                        double landmark_value = costs[indexLandmk];
                        landmark_costs[indexLandmk] = landmark_value;
                        indexLandmk++;
                    } else {
                        flag = true;
                        break;
                    }
                }

                if (!flag) {
                    HashMap<String, double[]> nodeToLandMark = this.toLandMarkIndex.get(snd);
                    if (nodeToLandMark == null) {
                        nodeToLandMark = new HashMap<>();
                    }
                    nodeToLandMark.put(lnd, landmark_costs);
                    this.toLandMarkIndex.put(snd, nodeToLandMark);
                }
            }
            cleanMemory();
        }


        for (String lnd : this.landMarks) {
            Node landmarkNode = graphdb.getNodeById(Long.parseLong(lnd) - 1);
            for (String ptype : fakePath.propertiesName) {
                DijkstraInType(landmarkNode, ptype);
            }

            for (String snd : this.nodes) {
                boolean flag = false;
                double[] landmark_costs = new double[fakePath.NumberOfProperties];
                int indexLandmk = 0;
                for (String ptype : fakePath.propertiesName) {
                    long snd_id = Long.parseLong(snd) - 1;
                    myNode node = this.ProcessedNodes.get(snd_id);

                    if (node != null) {
                        double[] costs = node.shortestPaths.get(ptype).getCosts();
                        double landmark_value = costs[indexLandmk];
                        landmark_costs[indexLandmk] = landmark_value;
                        indexLandmk++;
                    } else {
                        flag = true;
                        break;
                    }
                }

                if (!flag) {
                    HashMap<String, double[]> LandMarktoNode = this.fromLandMarkIndex.get(snd);
                    if (LandMarktoNode == null) {
                        LandMarktoNode = new HashMap<>();
                    }
                    LandMarktoNode.put(lnd, landmark_costs);
                    this.fromLandMarkIndex.put(snd, LandMarktoNode);
                }
            }
            cleanMemory();
        }
    }

    public String getNode(int randomNumberInRange) {
        int i = 0;
        Iterator<String> itor = this.nodes.iterator();
        while (itor.hasNext()) {
            String nodeId = itor.next();
            if (i == randomNumberInRange) {
                return nodeId;
            }
            i++;
        }

        return null;
    }

//    public void addToPortalSubSkyline(String o_portal_id, path p) {
//        myNode n = this.portalSkyline.get(o_portal_id);
//
//    }
}




