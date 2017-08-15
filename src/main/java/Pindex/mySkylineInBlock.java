package Pindex;

import javafx.util.Pair;
import neo4jTools.BNode;
import neo4jTools.Line;
import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphalgo.WeightedPath;
import org.neo4j.graphdb.*;

import java.util.ArrayList;
import java.util.HashMap;

public class mySkylineInBlock {
    GraphDatabaseService graphdb;
    myNodePriorityQueue mqueue;
    ArrayList<path> skylinPaths = new ArrayList<>();
    HashMap<String, myNode> processedNodeList = new HashMap<>();
    ArrayList<String> p_list = new ArrayList<>();
    ArrayList<path> ppp = new ArrayList<>();
    int NumberOfProperties = 0;
    double[] iniLowerBound = null;


    public mySkylineInBlock(GraphDatabaseService graphdb) {
        this.graphdb = graphdb;
        mqueue = new myNodePriorityQueue();
    }


    public ArrayList<path> getSkylinePath(Node source, Node destination, String pid, HashMap<String, Pair<String, String>> partitionInfos) {
        Long numInIPath;
        long usedInNode = 0;
        long usedInMain = 0;
        int maxQueueSize = Integer.MIN_VALUE;


        try (Transaction tx = this.graphdb.beginTx()) {
            path iniPath = new path(source, source);
            this.NumberOfProperties = iniPath.NumberOfProperties;
            this.iniLowerBound = new double[this.NumberOfProperties];
            initilizeSkylinePath(iniPath, destination);
//            System.out.println(this.skylinPaths.size());
//            numInIPath = (long) this.skylinPaths.size();
//            if (numInIPath == 0) {
//                return null;
//            }

//            for (String p_type : iniPath.getPropertiesName()) {
//                myDijkstra(source, destination, p_type);
//            }

//            myNode start = processedNodeList.get(String.valueOf(source.getId()));
            myNode start = new myNode(source, source);
            this.processedNodeList.put(start.id, start);
//            System.out.println("---"+start.id);
            if (start != null) {
                mqueue.add(start);
            }
            tx.success();
        }

        int i = 0;
        try (Transaction tx = this.graphdb.beginTx()) {
            while (!mqueue.isEmpty()) {

                if (mqueue.size() > maxQueueSize) {
                    maxQueueSize = mqueue.size();
                }

                myNode vs = mqueue.pop();
//                System.out.println("---"+ vs.id);
                vs.inqueue = false;

                if (!p_list.contains(vs.id)) {
                    p_list.add(vs.id);
                }

                i++;

                int index = 0;
                // System.out.println(vs.id+" "+vs.subRouteSkyline.size());
                for (; index < vs.subRouteSkyline.size(); ) {
                    path p = vs.subRouteSkyline.get(index);
                    // System.out.println("is processed " + p.processed_flag + "
                    // : " + p);
                    if (!p.processed_flag) {
                        p.processed_flag = true;
                        // if is_expand is false, it means this sub skyline
                        // path's upper bound is dominated by all paths in
                        // skyline results.
                        // So, it should not be expanded, and need to be removed
                        // from subRouteSkyline.
                        // Because all expansions from this p should not be a
                        // finally result.
                        boolean is_expand = needToBeExpanded(p, destination);
//                        System.out.println("----"+is_expand);

                        if (!is_expand) {
                            vs.subRouteSkyline.remove(index);
                            // System.out.println("----- discard "+p);
                        } else {
                            ArrayList<path> paths = p.expand();

                            for (path np : paths) {
//                                System.out.println(np);
//                                System.out.println(partitionInfos.get(String.valueOf(np.endNode.getId())).getValue());
//                                System.out.println("--------");
                                boolean isCycle = p.containRelationShip(np.getlastRelationship());
                                // checkCycle;
                                if (!isCycle) {
                                    if (np.endNode.equals(destination)) {
                                        long mrt = System.nanoTime();
                                        addToSkylineResult(np);
                                        long emrt = System.nanoTime() - mrt;
                                        usedInMain = usedInMain + emrt;
                                    } else {
                                        String nextid = String.valueOf(np.endNode.getId());
                                        myNode nextNode = this.processedNodeList.get(nextid);
                                        if (nextNode == null) {
                                            nextNode = new myNode(source, np.endNode);
                                        }
                                        long nrt = System.nanoTime();
                                        int[] info = nextNode.addToSkylineResult(np);
                                        usedInNode = usedInNode + (System.nanoTime() - nrt);
                                        if (!nextNode.inqueue && pid.equals(partitionInfos.get(nextid).getValue()))
                                            if (!this.processedNodeList.containsKey(nextid)) {
                                                this.processedNodeList.put(nextid, nextNode);
                                            }
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
        return this.skylinPaths;
    }

    private void initilizeSkylinePath(path p, Node destination) {
        int i = 0;
        for (String property_name : p.propertiesName) {
            PathFinder<WeightedPath> finder = GraphAlgoFactory
                    .dijkstra(PathExpanders.forTypeAndDirection(Line.Linked, Direction.OUTGOING), property_name);
            WeightedPath paths = finder.findSinglePath(p.endNode, destination);
            if (paths != null) {
                path np = new path(paths);
                addToSkylineResult(np);
                this.iniLowerBound[i++] = paths.weight();
            } else {
                break;
            }
        }
    }


    private boolean checkDominated(double[] costs, double[] estimatedCosts) {
        int numberOfLessThan = 0;
        for (int i = 0; i < costs.length; i++) {
            //double c = (double) Math.round(costs[i] * 1000000d) / 1000000d;
            //double e = (double) Math.round(estimatedCosts[i] * 1000000d) / 1000000d;
            double c = costs[i];
            double e = estimatedCosts[i];
            if (c > e) {
                return false;
            }

            // if (numberOfLessThan == 0 && c < e) {
            // numberOfLessThan = 1;
            // }
        }
        // if (numberOfLessThan == 0) {
        // return false;
        // } else {
        return true;
        // }
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

    private void addToSkylineResult(path np) {
        int i = 0;
        if (skylinPaths.isEmpty()) {
            this.skylinPaths.add(np);
        } else {
            boolean alreadyinsert = false;
            for (; i < skylinPaths.size(); ) {
                // if p dominate new path np,
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


    private boolean needToBeExpanded(path p, Node destination) {
        boolean flag = false;
        double estimatedCosts[] = new double[p.NumberOfProperties];
        // double tmplCosts[] = new double[p.NumberOfProperties];
        int i = 0;
        String endNode_id = String.valueOf(p.endNode.getId());
        String destination_id = String.valueOf(destination.getId());
        myNode myEndNode;
        // System.out.println(" "+printCosts(myEndNode.lowerBound));
        // System.out.println(" "+printCosts(p.getCosts()));
//        myEndNode = processedNodeList.get(endNode_id);
        double lowerBound[] = getShortestCost(endNode_id, destination_id);
        for (; i < this.NumberOfProperties; i++) {
            if (lowerBound[i] == -1) {
                return false;
            }
            estimatedCosts[i] = p.getCosts()[i] + lowerBound[i];
        }

        // if the return value is true, it means all the path in result set
        // dominate the lower bound, we don't need to expand it.
        // if the return value is false, it means that at least one path in the
        // result set doesn't dominate the estimatedCosts. So, they may not
        // dominated each other or estimatedCosts should be discard in future,
        // so we need to expands it.
        // flag = (!isdominatedbySkylineResults(estimatedCosts));
        //
        //
        //
        // True, all the path in result set doesn't dominate the lower bound.
        // False, at least path in the result set dominate the lower bound, it means the min value from vi to vt already is dominated by one of the path in result set.
        flag = isdominatedbySkylineResults(estimatedCosts);
        return flag;
    }


    public double[] getShortestCost(String sid, String did) {
//        connector n = new connector("/home/gqxwolf/neo4j/csldb/databases/graph.db");

        Node Source;
        Node Destination;
        double[] iniLowerBound;
        try (Transaction tx = graphdb.beginTx()) {
            Source = graphdb.findNode(BNode.BusNode, "name", sid);
            Destination = graphdb.findNode(BNode.BusNode, "name", did);
            path dummyP = new path(Source);
            int i = 0;
            iniLowerBound = new double[dummyP.NumberOfProperties];

            for (int j = 0; j < iniLowerBound.length; j++) {
                iniLowerBound[j] = -1;
            }

            for (String property_name : dummyP.propertiesName) {
                PathFinder<WeightedPath> finder = GraphAlgoFactory
                        .dijkstra(PathExpanders.forTypeAndDirection(Line.Linked, Direction.OUTGOING), property_name);
                WeightedPath paths = finder.findSinglePath(dummyP.endNode, Destination);
                if (paths != null) {
                    iniLowerBound[i++] = paths.weight();
                } else {
                    break;
                }
            }
            tx.success();
        }
        return iniLowerBound;
    }
}
