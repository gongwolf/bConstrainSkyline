package GraphPartition;

import javafx.util.Pair;
import neo4jTools.Line;
import org.neo4j.graphalgo.impl.shortestpath.Dijkstra;
import org.neo4j.graphdb.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class skylineInBlock {

    private block b = null;
    private int NumberOfProperties;
    private myNodePriorityQueue mqueue;
    HashMap<String, myNode> ProcessedNodes = new HashMap<>();
    ArrayList<path> skylinPaths = new ArrayList<>();
    ArrayList<String> propertiesName = new ArrayList<>();
    private GraphDatabaseService graphdb;


    public skylineInBlock(GraphDatabaseService graphdb, block block) {
        this.graphdb = graphdb;
        this.mqueue = new myNodePriorityQueue();
        this.b = block;
    }

    public ArrayList<path> getSkylineInBlock_blinks(Node source, Node destination) {
//        this.ProcessedNodes.clear();

        try (Transaction tx = this.graphdb.beginTx()) {

            path iniPath = new path(source, source);

            this.NumberOfProperties = iniPath.NumberOfProperties;

            try {
                this.propertiesName.addAll(iniPath.propertiesName);
            } catch (NullPointerException e) {
                System.out.println(iniPath.propertiesName.size());
                System.out.println(source.getRelationships(Line.Linked, Direction.BOTH));
                System.exit(0);
            }

            for (String ptype : iniPath.propertiesName) {
//                System.out.println(ptype);
//                System.out.println("111111111111");
                path sht_path_in_type = DijkstraInType(source, destination, ptype);
                if (sht_path_in_type != null) {
                    addToSkylineResult(sht_path_in_type);
                } else {
//                    System.out.println("22222222222");
                    return null;
                }
            }

            //initialize all the shortest paths to skyline paths set of each node
            for (Map.Entry<String, myNode> my_node_info : this.ProcessedNodes.entrySet()) {
                myNode n = my_node_info.getValue();
                for (Map.Entry<String, path> sh_paths : n.shortestPaths.entrySet()) {
                    n.addToSkylineResult(sh_paths.getValue());
                }
            }

            myNode start = ProcessedNodes.get(String.valueOf(source.getId()));

//            myNode start = new myNode(source, source, true);

            if (start != null) {
                mqueue.add(start);
            }
            tx.success();
        }

        try (Transaction tx = this.graphdb.beginTx()) {
            while (!mqueue.isEmpty()) {
                myNode vs = mqueue.pop();
                vs.inqueue = false;

                int index = 0;
                for (; index < vs.subRouteSkyline.size(); ) {
                    path p = vs.subRouteSkyline.get(index);
                    if (!p.processed_flag) {
                        p.processed_flag = true;
                        boolean is_expand = needToBeExpanded_landmark(p, destination);
                        if (!is_expand) {
                            vs.subRouteSkyline.remove(index);
                        } else {
                            ArrayList<path> paths = p.expand();
                            for (path np : paths) {
                                if (b.nodes.contains(String.valueOf(np.endNode.getId() + 1))) {
                                    boolean isCycle = p.containRelationShip(np.getlastRelationship());
                                    if (!isCycle) {
                                        if (np.endNode.equals(destination)) {
                                            addToSkylineResult(np);
                                        } else {
                                            String nextid = String.valueOf(np.endNode.getId());
                                            myNode nextNode = this.ProcessedNodes.get(nextid);
                                            if (nextNode == null) {
                                                nextNode = new myNode(source, np.endNode, false);
                                            }
                                            nextNode.addToSkylineResult(np);
                                            if (!nextNode.inqueue)
                                                mqueue.add(nextNode);
                                        }
                                    }
                                }
                            }
                            index++;
                        }
                    } else {
                        index++;
                    }
                }

            }
            tx.success();
        }
        return skylinPaths;
    }


    private double[] landmark_heuristic(Node destination, Node nextNode) {

        String dnd = String.valueOf(destination.getId() + 1);
        String cnd = String.valueOf(nextNode.getId() + 1);

        double result[] = new double[this.NumberOfProperties];

        HashMap<String, double[]> toLowerBound = b.toLandMarkIndex.get(cnd);
        HashMap<String, double[]> fromLowerBound = b.fromLandMarkIndex.get(dnd);

        int landmark_index = 0;
        for (String ptype : this.propertiesName) {
            double maxValue = Double.NEGATIVE_INFINITY;
            if (toLowerBound != null && fromLowerBound != null) {
                for (String lnd : b.landMarks) {
                    double[] t_l_cost = toLowerBound.get(lnd);
                    double[] f_l_cost = fromLowerBound.get(lnd);
                    if (t_l_cost != null && f_l_cost != null) {
                        double D_value = t_l_cost[landmark_index] - f_l_cost[landmark_index];
                        if (maxValue < Math.abs(D_value)) {
                            maxValue = D_value;
                        }
                    }
                }
            }
            result[landmark_index] = maxValue;
            landmark_index++;
        }

        return result;
    }

    private void addToSkylineResult(path np) {
        int i = 0;
        if (skylinPaths.isEmpty()) {
            this.skylinPaths.add(np);
        } else {
            boolean alreadyinsert = false;
            for (; i < skylinPaths.size(); ) {
                if (checkDominated(skylinPaths.get(i).getCosts(), np.getCosts())) {
                    if (alreadyinsert && i != this.skylinPaths.size() - 1) {
                        this.skylinPaths.remove(this.skylinPaths.size() - 1);
                    }
                    break;
                } else {
                    if (checkDominated(np.getCosts(), skylinPaths.get(i).getCosts())) {
                        this.skylinPaths.remove(i);
                    } else {
                        i++;
                    }
                    if (!alreadyinsert) {
                        this.skylinPaths.add(np);
                        alreadyinsert = true;
                    }

                }
            }
        }
    }

    private boolean checkDominated(double[] costs, double[] estimatedCosts) {
        for (int i = 0; i < costs.length; i++) {
            double c = costs[i];
            double e = estimatedCosts[i];
            if (c > e) {
                return false;
            }
        }
        return true;
    }

    private boolean needToBeExpanded_landmark(path p, Node destination) {
        boolean flag = false;
        double estimatedCosts[] = landmark_heuristic(p.endNode, destination);
        int i = 0;
        for (; i < this.NumberOfProperties; i++) {
            estimatedCosts[i] = p.getCosts()[i] + estimatedCosts[i];
        }
        flag = isdominatedbySkylineResults(estimatedCosts);
        return flag;
    }

    private boolean needToBeExpanded_Dijkstra(path p) {
        boolean flag = false;
        double estimatedCosts[] = new double[p.NumberOfProperties];
        int i = 0;
        String endNode_id = String.valueOf(p.endNode.getId());
        myNode myEndNode = this.ProcessedNodes.get(endNode_id);

        //There is no path from the endNode of the path p to the destination
        if (myEndNode == null) {
            return false;
        }
        for (; i < this.NumberOfProperties; i++) {
            if (myEndNode.lowerBound[i] == Double.POSITIVE_INFINITY) {
                return false;
            }
            estimatedCosts[i] = p.getCosts()[i] + myEndNode.lowerBound[i];
        }

        flag = isdominatedbySkylineResults(estimatedCosts);
        return flag;
    }


    private boolean isdominatedbySkylineResults(double[] estimatedCosts) {
        if (skylinPaths.isEmpty()) {
            return true;
        } else {
            for (path p : skylinPaths) {
                //if any of the path dominated the lowerbond, return false.
                if (checkDominated(p.getCosts(), estimatedCosts))
                    return false;
            }
        }
        // If all the path in skyline results dosen't dominate estimatedCosts;
        return true;
    }


    public void myDijkstra(Node source, Node destination, String property_type) {
        myNodeDijkstraPriorityQueue dijkstraqueue = new myNodeDijkstraPriorityQueue();

        myNode sNode = new myNode(source, destination, true);
        HashMap<String, Double> cost_so_far = new HashMap<>();
        cost_so_far.put(sNode.id, 0.0);
        path spath = new path(destination);
        sNode.shortestPaths.put(property_type, spath);

        dijkstraqueue.add(sNode);

        // int i =0;
        while (!dijkstraqueue.isEmpty()) {
            myNode n = dijkstraqueue.pop();

            ProcessedNodes.put(n.id, n);

            //get the in-comming edges to n.current node.
            //At begin, it is the destination Node
            ArrayList<Relationship> expensions = n.getNeighbor(property_type);

            for (Relationship rel : expensions) {

                Node nNode = rel.getStartNode(); //get the incoming node
                String next_id = String.valueOf(nNode.getId() + 1);

                myNode nextNode = ProcessedNodes.get(String.valueOf(nNode.getId()));
                if (nextNode == null) {
                    nextNode = new myNode(source, nNode, false);
                }

                if (this.b.nodes.contains(next_id)) {
                    Double cost = Double.parseDouble(rel.getProperty(property_type).toString());
                    Double oldCost = cost_so_far.get(nextNode.id);

                    if (oldCost == null) {
                        oldCost = 0.0;
                    }

                    double newCost = cost + oldCost;

                    if (!cost_so_far.containsKey(nextNode.id) || newCost < cost_so_far.get(nextNode.id)) {
                        cost_so_far.put(nextNode.id, newCost);
                        double Dij_priority = newCost;
                        nextNode.priority = Dij_priority;
                        nextNode.setCostFromSource(property_type, newCost);
                        //Todo:deal with the lowerbound of this next node.

                        path npath = new path(rel, n.shortestPaths.get(property_type));
//                        System.out.println("--"+npath);
                        nextNode.shortestPaths.put(property_type, npath);

                        dijkstraqueue.add(nextNode);

                    }
                }
            }
        }
    }

    private path DijkstraInType(Node source, Node destination, String property_type) {
        myNodeDijkstraPriorityQueue myDQ = new myNodeDijkstraPriorityQueue();
        myNode sNode = this.ProcessedNodes.get(String.valueOf(source.getId()));
        if (sNode == null) {
            sNode = new myNode(source, source, true);
        }
        path spath = new path(source);
        sNode.shortestPaths.put(property_type, spath);
        myDQ.add(sNode);


        HashMap<String, Double> cost_so_far = new HashMap<>();

        cost_so_far.put(sNode.id, 0.0);

        while (!myDQ.isEmpty()) {
            myNode n = myDQ.pop();
            ProcessedNodes.put(n.id, n);
//            System.out.println(n.id);

            //get the neighborhood's nodes and its cost
            ArrayList<Relationship> adjNodes = n.getAdjNodes(property_type);

            for (Relationship rel : adjNodes) {
                Node nNode = rel.getEndNode();
                String next_id = String.valueOf(nNode.getId() + 1);
                myNode nextNode = ProcessedNodes.get(String.valueOf(nNode.getId()));
                if (nextNode == null) {
                    nextNode = new myNode(source, nNode, false);
                }

                Double cost = Double.parseDouble(rel.getProperty(property_type).toString());
                Double oldCost = cost_so_far.get(nextNode.id);
                if (oldCost == null) {
                    oldCost = 0.0;
                }
                double newCost = cost + oldCost;

                if (b.nodes.contains(next_id)) { // if the node in this block
                    if (!cost_so_far.containsKey(nextNode.id) || newCost < cost_so_far.get(nextNode.id)) { // this node did not access or have shorter distance
                        cost_so_far.put(nextNode.id, newCost);
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

        myNode tarNode = ProcessedNodes.get(String.valueOf(destination.getId()));
        if (tarNode == null) {
            return null;
        } else {
            return tarNode.shortestPaths.get(property_type);
        }
    }


    public ArrayList<path> getSkylineInBlock_Dijkstra(Node source, Node destination) {
        try (Transaction tx = this.graphdb.beginTx()) {
            path iniPath = new path(source, source);
            this.NumberOfProperties = iniPath.NumberOfProperties;
            //initialize the lower bound
            for (String p_type : iniPath.getPropertiesName()) {
                myDijkstra(source, destination, p_type);
            }

            myNode start = this.ProcessedNodes.get(String.valueOf(source.getId()));
            //can not reach by back-wards dijkstra from destination to source
            if (start == null) {
                return null;
            } else {
                for (Map.Entry<String, path> sp_obj : start.shortestPaths.entrySet()) {
                    path p = sp_obj.getValue();
                    addToSkylineResult(p);
                }
                //initialize the sub-skyline path of start node
                start.addToSkylineResult(iniPath);
                mqueue.add(start);
                start.inqueue = true;
            }
            tx.success();
        }

        try (Transaction tx = this.graphdb.beginTx()) {
            while (!mqueue.isEmpty()) {
                myNode vs = mqueue.pop();
                vs.inqueue = false;

                int index = 0;
                for (; index < vs.subRouteSkyline.size(); ) {
                    path p = vs.subRouteSkyline.get(index);


                    if (!p.processed_flag) {
                        p.processed_flag = true;
                        boolean is_expand = needToBeExpanded_Dijkstra(p);
                        if (!is_expand) {
                            vs.subRouteSkyline.remove(index);
                        } else {
                            ArrayList<path> paths = p.expand();
                            for (path np : paths) {
                                boolean isCycle = p.containRelationShip(np.getlastRelationship());
                                if (!isCycle) {
                                    if (np.endNode.getId() == destination.getId()) {
                                        addToSkylineResult(np);
                                    } else {
                                        String nextid = String.valueOf(np.endNode.getId());
                                        String mapped_nexid = String.valueOf(np.endNode.getId() + 1);
                                        myNode nextNode = this.ProcessedNodes.get(nextid);
                                        if (nextNode == null) {
                                            continue;
                                        }
                                        nextNode.addToSkylineResult(np);
                                        //If nextNode is not in the queue, and the next node is in the same block with source node.
                                        //Also, the next node is not a portal node, because destination node is a portal node.
                                        //The condition to check whether it is a portal node need to be confirmed.
                                        if (!nextNode.inqueue && this.b.nodes.contains(mapped_nexid))
                                            mqueue.add(nextNode);
                                    }
                                }
                            }
                            index++;
                        }
                    } else {
                        index++;
                    }

                }
            }
            tx.success();
        }

        return skylinPaths;
    }
}
