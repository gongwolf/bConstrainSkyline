package GPSkyline.BFS;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;

import javafx.util.Pair;

import javax.management.ObjectName;

import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphdb.PathExpanders;
import org.neo4j.graphalgo.WeightedPath;
import org.neo4j.graphdb.Transaction;
import org.neo4j.jmx.*;

import neo4jTools.*;

public class myshortestPathUseNodeFinal {
    GraphDatabaseService graphdb;
    myNodePriorityQueue mqueue;
    ArrayList<path> skylinPaths = new ArrayList<>(2000);
    HashMap<String, myNode> processedNodeList = new HashMap<>();
    ArrayList<String> p_list = new ArrayList<>();
    ArrayList<path> ppp = new ArrayList<>();
    int NumberOfProperties = 0;
    long usedInDijkstra = 0;
    long qTime = 0;
    long used_In_skyline_operation = 0;
    double[] iniLowerBound = null;
    private int totalDegree;
    private int removedPath;
    private int insertedPath;

    public myshortestPathUseNodeFinal(GraphDatabaseService graphdb) {
        this.graphdb = graphdb;
        mqueue = new myNodePriorityQueue();
    }

    // public void getSkylinePath(Node source, Node destination) {
    public ArrayList<path> getSkylinePath(Node source, Node destination) {
        String sourceId = String.valueOf(source.getId());
        String destinationId = String.valueOf(destination.getId());
        Long pagecachedInFindNodes = 0L;
        Long pagecachedInInital = 0L;
        Long pagecachedInFindSkyPath = 0L;
        Long pinsInFindNodes = 0L;
        Long pinsInInital = 0L;
        Long PinsInFindSkyPath = 0L;
        Long numInIPath = 0L;
        Long numFinalPath = 0L;
        Long numIters = 0L;
        Long numAccessedNodes = 0L;
        long Pages_2 = 0L;
        long Pins_2 = 0L;
        long time_2 = 0L;
        long Pages_3 = 0L;
        long Pins_3 = 0L;
        long time_3 = 0L;

        long usedInNode = 0;
        long usedInMain = 0;
        long usedInCheckExpand = 0;
        long usedInExpansion = 0;
        int maxQueueSize = Integer.MIN_VALUE;
        int maxLengthOfPath = Integer.MIN_VALUE;
        long pruningNum = 0;
        long extendedPathNum = 0;

        long time_1 = System.nanoTime();
        long Pages_1 = getFromManagementBean("Page cache", "Faults", graphdb);
        long Pins_1 = getFromManagementBean("Page cache", "Pins", graphdb);

        try (Transaction tx = this.graphdb.beginTx()) {
            path iniPath = new path(source, source);
            this.NumberOfProperties = iniPath.NumberOfProperties;
            this.iniLowerBound = new double[this.NumberOfProperties];
            initilizeSkylinePath(iniPath, destination);
            numInIPath = (long) this.skylinPaths.size();
            if (numInIPath == 0) {
                return null;
            }

//            for (path p : skylinPaths) {
//                System.out.println(printCosts(p.getCosts()));
//                // System.out.println(p);
//                System.out.println("---------------------------------");
//            }
//            System.out.println("initilzed the sky line path");
            for (String p_type : iniPath.getPropertiesName()) {
                myDijkstra(source, destination, p_type);
            }

//            System.out.println(printCosts(processedNodeList.get(String.valueOf(destinationId)).lowerBound));
//            System.out.println(printCosts(processedNodeList.get("76").lowerBound));
//            System.out.println(processedNodeList.get(String.valueOf(destinationId)).id);

            myNode start = processedNodeList.get(String.valueOf(source.getId()));
            if (start != null) {
                mqueue.add(start);
            }

            tx.success();
        }
        Pages_2 = getFromManagementBean("Page cache", "Faults", graphdb);
        Pins_2 = getFromManagementBean("Page cache", "Pins", graphdb);
        time_2 = System.nanoTime();


        int i = 0;
        try (Transaction tx = this.graphdb.beginTx()) {
            while (!mqueue.isEmpty()) {

                if (mqueue.size() > maxQueueSize) {
                    maxQueueSize = mqueue.size();
                }

                myNode vs = mqueue.pop();
                vs.inqueue = false;
                // System.out.println("-------------------------------------");
                // System.out.println("pop: "+vs.id);

//                if (!p_list.contains(vs.id)) {
//                    this.totalDegree += vs.degree;
//                    p_list.add(vs.id);
//                }

                i++;
                // if(i==20)
                // {
                // break;
                // }
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
                        long cheexprt = System.nanoTime();
                        boolean is_expand = needToBeExpanded(p, destination);
                        // System.out.println(is_expand);
                        usedInCheckExpand += (System.nanoTime() - cheexprt);

                        if (!is_expand) {
                            vs.subRouteSkyline.remove(index);
                            // System.out.println("----- discard "+p);
                            pruningNum++;
                        } else {
                            extendedPathNum++;
                            long expRt = System.nanoTime();
                            ArrayList<path> paths = p.expand();
                            usedInExpansion += System.nanoTime() - expRt;

                            for (path np : paths) {
                                // long checkCycle = System.nanoTime();
                                boolean isCycle = p.containRelationShip(np.getlastRelationship());
                                // this.UsedInCheckCycle += System.nanoTime() -
                                // checkCycle;
                                if (!isCycle) {
                                    if (np.endNode.equals(destination)) {
                                        long mrt = System.nanoTime();
                                        addToSkylineResult(np);
                                        long emrt = System.nanoTime() - mrt;
                                        usedInMain = usedInMain + emrt;
                                        // System.out.println(" $$$$
                                        // insertToSkyline "+ np);
                                    } else {
                                        String nextid = String.valueOf(np.endNode.getId());
                                        myNode nextNode = this.processedNodeList.get(nextid);
                                        if (nextNode == null) {
                                            continue;
                                        }
                                        long nrt = System.nanoTime();
                                        int[] info = nextNode.addToSkylineResult(np);
                                        this.insertedPath += info[0];
                                        this.removedPath += info[1];
                                        usedInNode = usedInNode + (System.nanoTime() - nrt);
                                        if (!nextNode.inqueue)
                                            mqueue.add(nextNode);
                                        // System.out.println("expand :" + np);
                                        // System.out.println("push into queue
                                        // :"+ nextNode.id);
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
        Pages_3 = getFromManagementBean("Page cache", "Faults", graphdb);
        Pins_3 = getFromManagementBean("Page cache", "Pins", graphdb);
        time_3 = System.nanoTime();


        pagecachedInFindNodes = Pages_1;
        pagecachedInInital = Pages_2 - Pages_1;
        pagecachedInFindSkyPath = Pages_3 - Pages_2;

        pinsInFindNodes = Pins_1;
        pinsInInital = Pins_2 - Pins_1;
        PinsInFindSkyPath = Pins_3 - Pins_2;

        long timeInInitial = time_2 - time_1;
        long timeInFindSkyPath = time_3 - time_2;

        numIters = (long) i;
        numAccessedNodes = (long) p_list.size();

        numFinalPath = (long) this.skylinPaths.size();
        DecimalFormat formatter = new DecimalFormat("#0.00");
        StringBuffer sb = new StringBuffer();
        // long totalRt = (timeInInitial + timeInFindSkyPath) * 1000000;
        used_In_skyline_operation = used_In_skyline_operation + usedInNode + usedInMain;

        sb.append(sourceId + ",").append(destinationId + ",");
        sb.append(pagecachedInFindNodes).append(",");
        sb.append(pagecachedInInital).append(",");
        sb.append(pagecachedInFindSkyPath).append(":");
        sb.append(pinsInFindNodes).append(",");
        sb.append(pinsInInital).append(",");
        sb.append(PinsInFindSkyPath).append(":");
        sb.append(formatter.format((double) timeInInitial / 1000000)).append(",");
        sb.append(formatter.format((double) timeInFindSkyPath / 1000000)).append(":");
        sb.append(numIters).append(",");
        sb.append(numAccessedNodes).append(",");
        sb.append(numInIPath).append(",");
        sb.append(numFinalPath).append("|");
        sb.append(pruningNum).append(",");
        sb.append(extendedPathNum).append("|");

//        sb.append(formatter.format(usedInDijkstra / 1000000)).append(",");
        sb.append(formatter.format(usedInCheckExpand / 1000000)).append(",");
        sb.append(formatter.format(usedInExpansion / 1000000)).append(",");
        sb.append(formatter.format((double) used_In_skyline_operation / 1000000)).append(":");

        // long totalRt = timeInInitial+timeInFindSkyPath*1000000;
        // System.out.println(totalRt);
        // System.out.println(
        // "usedInNode:" + usedInNode / 1000000 + " " + formatter.format(100 *
        // (double) usedInNode / totalRt));
        // System.out.println(
        // "usedInMain:" + usedInMain / 1000000 + " " + formatter.format(100 *
        // (double) usedInMain / totalRt));
        // System.out.println("usedInCheckExpand:" + usedInCheckExpand / 1000000
        // + " " + formatter.format(100 * (double) usedInCheckExpand /
        // totalRt));
        // System.out.println(" usedInDijkstra:" + usedInDijkstra / 1000000 + "
        // "+ formatter.format(100 * (double) usedInDijkstra / totalRt));
        // System.out.println("usedInExpansion:" + usedInExpansion / 1000000 + "
        // "+ formatter.format(100 * (double) usedInExpansion / totalRt));

        // System.out.println("used_In_skyline_operation:"
        // +used_In_skyline_operation / 1000000 + " "+ formatter.format(100 *
        // (double) used_In_skyline_operation /totalRt));
        // System.out.println(100*(double)(usedInNode + usedInMain +
        // usedInCheckExpand + usedInExpansion) * 1.00 / totalRt);
        double avg_slr_inNode = 0.00;
        for (myNode n : this.processedNodeList.values()) {
            avg_slr_inNode += n.subRouteSkyline.size();
        }

        double avg_lenght_in_final = 0.00;
        for (path n : this.skylinPaths) {
            avg_lenght_in_final += n.getLenght();
            if (n.getLenght() > maxLengthOfPath) {
                maxLengthOfPath = n.getLenght();
            }
        }

        // long totalDegree = 0;
        // try (Transaction tx = this.graphdb.beginTx()) {
        // for (path n : this.skylinPaths) {
        // totalDegree += n.totalDegree();

        // }
        // tx.success();
        // }
        //

        sb.append(avg_slr_inNode / numAccessedNodes).append(",");
        sb.append(this.totalDegree).append(",");
        sb.append(avg_lenght_in_final / this.skylinPaths.size()).append(",");
        sb.append(maxLengthOfPath).append(",");
        sb.append(maxQueueSize).append(",");
        sb.append(numAccessedNodes).append(",");
        sb.append(pruningNum).append(",");
        sb.append(extendedPathNum);
        // sb.append(insertedPath).append(",");
        // sb.append(removedPath);
        // System.out.println(sb);
        // for (path p : skylinPaths) {
        // if (p.getCosts()[0] == 69049.41455738951) {
        // System.out.println(printCosts(p.getCosts()));
        // System.out.println(p);
        // // System.out.println("---------------");
        // }
        // }

//        for (path p :
//                this.skylinPaths) {
//            System.out.println(p);
////            System.out.println(printCosts(p.cost));
//        }
//        return sb.toString();
        System.out.println(sb);
        return this.skylinPaths;
    }

    private boolean needToBeExpanded(path p, Node destination) {
        boolean flag = false;
        double estimatedCosts[] = new double[p.NumberOfProperties];
        // double tmplCosts[] = new double[p.NumberOfProperties];
        int i = 0;
        String endNode_id = String.valueOf(p.endNode.getId());
        myNode myEndNode = this.processedNodeList.get(endNode_id);
        // System.out.println(" "+printCosts(myEndNode.lowerBound));
        // System.out.println(" "+printCosts(p.getCosts()));
        myEndNode = processedNodeList.get(endNode_id);
        for (; i < this.NumberOfProperties; i++) {
//            if (myEndNode.lowerBound[i] == Double.POSITIVE_INFINITY) {
//                return false;
//            }
            estimatedCosts[i] = p.getCosts()[i] + myEndNode.lowerBound[i];
        }

//        System.out.println(printCosts(estimatedCosts));

        long usedInskyline = System.nanoTime();
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
        this.used_In_skyline_operation += System.nanoTime() - usedInskyline;
        return flag;
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

    // true, means, all of the value in each dimension in costs is smaller or
    // equal than the value in estimatedCosts, so costs dominate estimatedCosts;
    // false, means, costs' value is greater than estimatedCosts, so costs do no
    // dominate estimatedCosts
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
        // System.out.println(np);
        // System.out.println(printCosts(np.getCosts()));
        // System.out.println("=====");
        int i = 0;
        if (skylinPaths.isEmpty()) {
            this.insertedPath++;
            this.skylinPaths.add(np);
            // System.out.println("###################");
            // System.out.println("Insert to skyline Paths ArrayList:");
            // System.out.println(np);
            // System.out.println(printCosts(np.getCosts()));
            // System.out.println("end the insert to skyline");
            // System.out.println("###################");
        } else {
            boolean alreadyinsert = false;
            // System.out.println("============================");
            // System.out.println(this.skylinPaths.size());
            for (; i < skylinPaths.size(); ) {
                // System.out.println(printCosts(skylinPaths.get(i).getCosts())
                // + " " + printCosts(np.getCosts()) + " "
                // + checkDominated(skylinPaths.get(i).getCosts(),
                // np.getCosts()));
                //
                //
                // if p dominate new path np,
                if (checkDominated(skylinPaths.get(i).getCosts(), np.getCosts())) {
                    if (alreadyinsert && i != this.skylinPaths.size() - 1) {
                        this.skylinPaths.remove(this.skylinPaths.size() - 1);
                        this.removedPath++;
                    }
                    // System.out.println("Jump it and break, because it already
                    // is dominated by this record "
                    // + this.skylinPaths.size());
                    break;
                } else {
                    if (checkDominated(np.getCosts(), skylinPaths.get(i).getCosts())) {
                        // System.out.println("remove the old one and insert
                        // it");
                        this.removedPath++;
                        this.skylinPaths.remove(i);
                    } else {
                        i++;
                    }
                    if (!alreadyinsert) {
                        // System.out.println("insert it because it does not
                        // dominated each other");
                        this.skylinPaths.add(np);
                        this.insertedPath++;
                        alreadyinsert = true;
                    }

                }
            }
            // System.out.println(this.skylinPaths.size());
            // System.out.println("===========================");
        }
    }

    public double myDijkstra(Node source, Node destination, String property_type) {
        String sid = String.valueOf(source.getId());
        String did = String.valueOf(destination.getId());

        HashMap<String, myNode> NodeList = new HashMap<>();
        myNodeDijkstraPriorityQueue dijkstraqueue = new myNodeDijkstraPriorityQueue(property_type);
        myNode sNode = new myNode(source, destination, true);
        dijkstraqueue.add(sNode);

        // int i =0;
        while (!dijkstraqueue.isEmpty()) {
            myNode n = dijkstraqueue.pop();
            ArrayList<Pair<myNode, Double>> expensions = n.getNeighbor(property_type);


            //if n is already processed
            if (NodeList.containsKey(n.id)) {
                continue;
            }
            NodeList.put(n.id, n);

            myNode tmpNode = this.processedNodeList.get(n.id);
            if (tmpNode == null) {
                this.processedNodeList.put(n.id, n);
            } else {
                double tcost = n.getCostFromSource(property_type);
                tmpNode.setCostFromSource(property_type, tcost);
                this.processedNodeList.put(tmpNode.id, tmpNode);
            }

            for (Pair<myNode, Double> p : expensions) {
                myNode nextNode = p.getKey();

                Double cost = p.getValue();
                Double newCost = n.getCostFromSource(property_type) + cost;
                if (!NodeList.containsKey(nextNode.id) || (nextNode.getCostFromSource(property_type) > newCost)) {
                    nextNode.setCostFromSource(property_type, newCost);
                    nextNode.setComeFromNode(n.id);
                    dijkstraqueue.add(nextNode);
                }
            }
        }
        return NodeList.get(String.valueOf(destination.getId())).getCostFromSource(property_type);
    }

    private void printPath(HashMap<String, myNode> nodeList, String sid, String did) {
        String currentId = sid;
        while (!currentId.equals(did)) {
            System.out.print("[" + currentId + "]");
            System.out.print("->");
            myNode n = nodeList.get(currentId);
            currentId = n.ComeFromNode;
        }
        System.out.println("[" + currentId + "]");

    }

    private Long getFromManagementBean(String Object, String Attribuite, GraphDatabaseService graphDb) {
        ObjectName objectName = JmxUtils.getObjectName(graphDb, Object);
        Long value = JmxUtils.getAttribute(objectName, Attribuite);
        return value;
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