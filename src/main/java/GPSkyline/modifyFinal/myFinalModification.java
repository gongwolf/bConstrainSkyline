package GPSkyline.modifyFinal;


import GraphPartition.myNode;
import GraphPartition.myNodeDijkstraPriorityQueue;
import GraphPartition.myNodePriorityQueue;
import GraphPartition.path;
import neo4jTools.Line;
import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphalgo.WeightedPath;
import org.neo4j.graphdb.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class myFinalModification {
    GraphDatabaseService graphdb;
    myNodePriorityQueue mqueue;
    ArrayList<path> skylinPaths = new ArrayList<>();
    HashMap<String, myNode> ProcessedNodes = new HashMap<>();
    ArrayList<String> propertiesName = new ArrayList<>();
    int NumberOfProperties = 0;
    double[] iniLowerBound = null;


    public myFinalModification(GraphDatabaseService graphdb) {
        this.graphdb = graphdb;
        mqueue = new myNodePriorityQueue();
    }

    // public void getSkylinePath(Node source, Node destination) {
    public ArrayList<path> getSkylinePath(Node source, Node destination) {
        try (Transaction tx = this.graphdb.beginTx()) {
            path iniPath = new path(source, source);
            this.NumberOfProperties = iniPath.NumberOfProperties;
            this.propertiesName.addAll(iniPath.propertiesName);
            //initialize the lower bound
            for (String p_type : iniPath.getPropertiesName()) {
                System.out.println(p_type);
                myDijkstra(source, destination, p_type);
            }

//            System.out.println()

            myNode start = this.ProcessedNodes.get(String.valueOf(source.getId()));
            System.out.println(start.id);
            //can not reach by back-wards dijkstra from destination to source
            if (start == null) {
                return null;
            } else {
                for (Map.Entry<String, path> sp_obj : start.shortestPaths.entrySet()) {
                    path p = sp_obj.getValue();
                    System.out.println("~"+p);
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
//                    System.out.println("11" + p);


                    if (!p.processed_flag) {
                        p.processed_flag = true;
                        boolean is_expand = needToBeExpanded(p);
                        System.out.println(is_expand);
                        if (!is_expand) {
                            vs.subRouteSkyline.remove(index);
                        } else {
                            ArrayList<path> paths = p.expand();
                            for (path np : paths) {
                                boolean isCycle = np.isCycle();
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
                                        if (!nextNode.inqueue) {
                                            mqueue.add(nextNode);
                                            nextNode.inqueue = true;
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

    private boolean needToBeExpanded(path p) {
        boolean flag = false;
        double estimatedCosts[] = new double[p.NumberOfProperties];
        int i = 0;
        String endNode_id = String.valueOf(p.endNode.getId());
        myNode myEndNode = this.ProcessedNodes.get(endNode_id);

        if (myEndNode == null) {
//            System.out.println(endNode_id + " is null");
            return false;
        }

        for (String ptype : this.propertiesName) {
            double s_cost = myEndNode.shortestPaths.get(ptype).getCosts()[i];
//            System.out.println(s_cost);
            estimatedCosts[i] = p.getCosts()[i] + s_cost;
            i++;
        }


//        System.out.println(printCosts(estimatedCosts));
        flag = isdominatedbySkylineResults(estimatedCosts);
        return flag;
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

    private boolean isdominatedbySkylineResults(double[] estimatedCosts) {
        if (skylinPaths.isEmpty()) {
            return true;
        } else {
            for (path p : skylinPaths) {
                if (checkDominated(p.getCosts(), estimatedCosts))
                    return false;
            }
        }
        return true;
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

    public void myDijkstra(Node source, Node destination, String property_type) {
        myNodeDijkstraPriorityQueue dijkstraqueue = new myNodeDijkstraPriorityQueue();
        myNode sNode = this.ProcessedNodes.get(String.valueOf(destination.getId()));
        if (sNode == null) {
//            System.out.println("can not find the node   "+destination.getId());
            sNode = new myNode(source, destination, true);
        }
        HashMap<String, Double> cost_so_far = new HashMap<>();
        cost_so_far.put(sNode.id, 0.0);
        path spath = new path(destination);
        sNode.shortestPaths.put(property_type, spath);
        System.out.println(property_type);

        dijkstraqueue.add(sNode);

        int i = 1;
        while (!dijkstraqueue.isEmpty()) {
            myNode n = dijkstraqueue.pop();
//            System.out.println(n.id);
//            if (i % 10 == 0) {
//                break;
//            }
//            i++;

            ProcessedNodes.put(n.id, n);

            //get the in-comming edges to n.current node.
            //At begin, it is the destination Node
            ArrayList<Relationship> expensions = n.getNeighbor();

            for (Relationship rel : expensions) {

                Node nNode = rel.getStartNode(); //get the incoming node
//                System.out.println("    "+rel);
                String next_id = String.valueOf(nNode.getId());

                myNode nextNode = ProcessedNodes.get(next_id);

                if (nextNode == null) {
                    nextNode = new myNode(source, nNode, false);
                }

                String nextID = String.valueOf(nNode.getId() + 1);
                Double cost = Double.parseDouble(rel.getProperty(property_type).toString());
                Double oldCost = cost_so_far.get(n.id);


                double newCost = cost + oldCost;
//                System.out.println("    "+newCost);
                double current_cost = Double.POSITIVE_INFINITY;
                if (cost_so_far.get(nextNode.id) != null) {
                    current_cost = cost_so_far.get(nextNode.id);
                }

                if (!cost_so_far.containsKey(nextNode.id) || newCost < current_cost) {
                    cost_so_far.put(nextNode.id, newCost);
                    double Dij_priority = newCost;
                    nextNode.priority = Dij_priority;

                    path npath = new path(rel, n.shortestPaths.get(property_type));
//                    System.out.println("    "+npath);
                    nextNode.shortestPaths.put(property_type, npath);
                    dijkstraqueue.add(nextNode);
                }
            }
        }
    }

    public String printCosts(double costs[]) {
        StringBuffer sb = new StringBuffer();
        sb.append("[");
        int i = 0;
        for (; i < costs.length - 1; i++) {
            sb.append(costs[i] + ",");
        }
        sb.append(costs[i] + "]");
        return sb.toString();
    }
}
