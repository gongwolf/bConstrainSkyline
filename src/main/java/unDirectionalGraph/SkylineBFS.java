package unDirectionalGraph;


import javafx.util.Pair;
import neo4jTools.Line;
import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphalgo.WeightedPath;
import org.neo4j.graphdb.*;
import org.neo4j.jmx.JmxUtils;

import javax.management.ObjectName;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;


public class SkylineBFS {
    private final File Graph_path;
    private final int lowerBound_Method;
    private final boolean inialized;
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
    private long landmarkNumber;
    private HashMap<String, RandomAccessFile> LandMarkFile = new HashMap<>();
    private boolean setfakePath = false;

    public SkylineBFS(GraphDatabaseService graphdb, long landmarkNumber, long graph_size, String degree, boolean initalized, int lowerBound_Method) {
        this.graphdb = graphdb;
        mqueue = new myNodePriorityQueue();
        this.landmarkNumber = landmarkNumber;
        this.inialized = initalized;
        this.lowerBound_Method = lowerBound_Method;
        this.Graph_path = new File("/home/gqxwolf/mydata/projectData/un_testGraph" + graph_size + "_" + degree + "/data/");
    }

    // public void getSkylinePath(Node source, Node destination) {
    public String getSkylinePath(Node source, Node destination) {
        if (this.lowerBound_Method == 2) {
            this.used_In_skyline_operation = 0;
            openLandmarkFile();
        }
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


            if (this.inialized) {
                initilizeSkylinePath(iniPath, destination);
                numInIPath = (long) this.skylinPaths.size();
                if (numInIPath == 0) {
                    return null;
                }
            }

            if (this.lowerBound_Method == 1) {
                for (String p_type : iniPath.getPropertiesName()) {
                    myDijkstra(source, destination, p_type);
                }
            }

            myNode start = processedNodeList.get(String.valueOf(source.getId()));
            if (start == null) {
                start = new myNode(source, source, false);
                processedNodeList.put(String.valueOf(source.getId()), start);
            }

            mqueue.add(start);


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

//                if (!p_list.contains(vs.id)) {
//                    this.totalDegree += vs.degree;
//                    p_list.add(vs.id);
//                }

                i++;
                int index = 0;
                for (; index < vs.subRouteSkyline.size(); ) {
                    path p = vs.subRouteSkyline.get(index);
                    if (!p.processed_flag) {
                        p.processed_flag = true;
                        long cheexprt = System.nanoTime();
                        boolean is_expand = needToBeExpanded(p, destination);
                        usedInCheckExpand += (System.nanoTime() - cheexprt);

                        if (!is_expand) {
                            vs.subRouteSkyline.remove(index);
                            pruningNum++;
                        } else {
                            extendedPathNum++;
                            long expRt = System.nanoTime();
                            ArrayList<path> paths = p.expand();
                            usedInExpansion += System.nanoTime() - expRt;

                            for (path np : paths) {
                                boolean isCycle = p.containRelationShip(np.getlastRelationship());
                                if (!isCycle) {
                                    if (np.endNode.equals(destination)) {
                                        long mrt = System.nanoTime();
                                        addToSkylineResult(np);
                                        usedInMain += (System.nanoTime() - mrt);
                                        // System.out.println(" $$$$
                                        // insertToSkyline "+ np);
                                    } else {
                                        String nextid = String.valueOf(np.endNode.getId());
                                        myNode nextNode = this.processedNodeList.get(nextid);
                                        if (nextNode == null) {
                                            nextNode = new myNode(source, np.endNode, false);
                                            processedNodeList.put(nextid, nextNode);
                                        }
                                        long nrt = System.nanoTime();
                                        nextNode.addToSkylineResult(np);
                                        usedInNode += (System.nanoTime() - nrt);
                                        if (!nextNode.inqueue)
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
        used_In_skyline_operation = usedInNode + usedInMain;

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

        sb.append(avg_slr_inNode / numAccessedNodes).append(",");
        sb.append(this.totalDegree).append(",");
        sb.append(avg_lenght_in_final / this.skylinPaths.size()).append(",");
        sb.append(maxLengthOfPath).append(",");
        sb.append(maxQueueSize);

//        System.out.println(sb);
        if (this.lowerBound_Method == 2) {
            closeOpenedFile();
        }

        if (this.skylinPaths.size() == 0) {
            return null;
        } else {
            return sb.toString();
        }
    }

    private void initilizeSkylineLandMark(path p, Node destination) {
        double upperBound[] = getUpperBound(p.startNode.getId(), destination.getId());
        if (upperBound[0] != Double.POSITIVE_INFINITY) {
            p.setCosts(upperBound);
            addToSkylineResult(p);
        }

    }

    private void closeOpenedFile() {
        try {
            for (String f_name : this.LandMarkFile.keySet()) {
                this.LandMarkFile.get(f_name).close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.LandMarkFile.clear();
    }

    private void openLandmarkFile() {
        File landmark_file = new File(this.Graph_path.getAbsolutePath() + "/landmark/");


        for (File l_file : landmark_file.listFiles()) {
            try {
                RandomAccessFile l_f = new RandomAccessFile(l_file, "r");
                this.LandMarkFile.put(l_file.getName(), l_f);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }
    }

    private boolean needToBeExpanded(path p, Node destination) {
        boolean flag = false;
        double estimatedCosts[] = new double[p.NumberOfProperties];

        double[] estimatedCosts2;
        if (this.lowerBound_Method == 0) {
            for (int i = 0; i < estimatedCosts.length; i++) {
//                System.out.println("aaaaaa");
                estimatedCosts[i] = p.getCosts()[i];
            }
        } else if (this.lowerBound_Method == 1) {
            String endNode_id = String.valueOf(p.endNode.getId());
            myNode myEndNode = this.processedNodeList.get(endNode_id);
            myEndNode = processedNodeList.get(endNode_id);
            for (int i = 0; i < this.NumberOfProperties; i++) {
                if (myEndNode.lowerBound[i] == Double.POSITIVE_INFINITY) {
                    return false;
                }
                estimatedCosts[i] = p.getCosts()[i] + myEndNode.lowerBound[i];
            }
        } else if (this.lowerBound_Method == 2) {
            System.out.println("bbbbb");
            estimatedCosts2 = getEstimaedCost(p.endNode.getId(), destination.getId());
            for (int i = 0; i < estimatedCosts.length; i++) {
                if (estimatedCosts2[i] == Double.NEGATIVE_INFINITY) {
                    return true;
                }
                estimatedCosts[i] = p.getCosts()[i] + estimatedCosts2[i];
            }
        }


//        long usedInskyline = System.nanoTime();
        flag = isdominatedbySkylineResults(estimatedCosts);
//        this.used_In_skyline_operation += System.nanoTime() - usedInskyline;
        return flag;
    }

    private double[] getEstimaedCost(long sid, long did) {
        double estimatedCosts[] = new double[NumberOfProperties];
        for (int i = 0; i < estimatedCosts.length; i++) {
            estimatedCosts[i] = Double.NEGATIVE_INFINITY;
        }


        long spos = 32 * sid + 8;
        long dpos = 32 * did + 8;
        try {
            for (String f_name : this.LandMarkFile.keySet()) {
                RandomAccessFile l_f = this.LandMarkFile.get(f_name);
                l_f.seek(spos);

                double td1 = l_f.readDouble();
                if (td1 != -1) {
                    double td2 = l_f.readDouble();
                    double td3 = l_f.readDouble();
                    //distance from landmark to destination
                    l_f.seek(dpos);
                    double fd1 = l_f.readDouble();
                    double fd2 = l_f.readDouble();
                    double fd3 = l_f.readDouble();


                    if (fd1 != -1) {
                        //lower bound of the distance from s->l->t
                        double a0 = Math.abs(td1 - fd1);
                        double a1 = Math.abs(td2 - fd2);
                        double a2 = Math.abs(td3 - fd3);


                        if (a0 > estimatedCosts[0]) {
                            estimatedCosts[0] = a0;
                        }
                        if (a1 > estimatedCosts[1]) {
                            estimatedCosts[1] = a1;
                        }
                        if (a2 > estimatedCosts[2]) {
                            estimatedCosts[2] = a2;
                        }
                    }
                }

            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return estimatedCosts;
    }

    private void initilizeSkylinePath(path p, Node destination) {
        int i = 0;
        for (String property_name : p.propertiesName) {
            PathFinder<WeightedPath> finder = GraphAlgoFactory
                    .dijkstra(PathExpanders.forTypeAndDirection(Line.Linked, Direction.BOTH), property_name);
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

    private double[] getUpperBound(long sid, long did) {
        double upperbound[] = new double[NumberOfProperties];
        for (int i = 0; i < upperbound.length; i++) {
            upperbound[i] = Double.POSITIVE_INFINITY;
        }


        long spos = 32 * sid + 8;
        long dpos = 32 * did + 8;
        try {
            for (String f_name : this.LandMarkFile.keySet()) {
                RandomAccessFile to_f = this.LandMarkFile.get(f_name);
                to_f.seek(spos);
                double td1 = to_f.readDouble();
                if (td1 != -1) {
                    double td2 = to_f.readDouble();
                    double td3 = to_f.readDouble();

                    to_f.seek(dpos);
                    double fd1 = to_f.readDouble();
                    if (fd1 != -1) {
                        double fd2 = to_f.readDouble();
                        double fd3 = to_f.readDouble();
                        double a0 = Math.abs(td1 + fd1);
                        double a1 = Math.abs(td2 + fd2);
                        double a2 = Math.abs(td3 + fd3);

                        if (a0 < upperbound[0]) {
                            upperbound[0] = a0;
                        }
                        if (a1 < upperbound[1]) {
                            upperbound[1] = a1;
                        }
                        if (a2 < upperbound[2]) {
                            upperbound[2] = a2;
                        }


                    }
                }

            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return upperbound;
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
        int i = 0;
        if (skylinPaths.isEmpty()) {
            this.insertedPath++;
            this.skylinPaths.add(np);
        } else {
            boolean alreadyinsert = false;
            for (; i < skylinPaths.size(); ) {
                if (checkDominated(skylinPaths.get(i).getCosts(), np.getCosts())) {
                    if (alreadyinsert && i != this.skylinPaths.size() - 1) {
                        this.skylinPaths.remove(this.skylinPaths.size() - 1);
                        this.removedPath++;
                    }
                    break;
                } else {
                    if (checkDominated(np.getCosts(), skylinPaths.get(i).getCosts())) {
                        this.removedPath++;
                        this.skylinPaths.remove(i);
                    } else {
                        i++;
                    }
                    if (!alreadyinsert) {
                        this.skylinPaths.add(np);
                        this.insertedPath++;
                        alreadyinsert = true;
                    }

                }
            }
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
