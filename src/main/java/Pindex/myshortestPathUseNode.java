package Pindex;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;

import javax.management.ObjectName;

import Pindex.myNode;
import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphdb.PathExpanders;
import org.neo4j.graphalgo.WeightedPath;
import org.neo4j.graphdb.Transaction;
import org.neo4j.jmx.*;

import neo4jTools.Line;
import Pindex.path;

public class myshortestPathUseNode {

    GraphDatabaseService graphdb;
    myNodePriorityQueue mqueue;
    ArrayList<path> skylinPaths = new ArrayList<>();
    HashMap<String, myNode> processedNodeList = new HashMap<>();
    ArrayList<path> ppp = new ArrayList<>();
    int NumberOfProperties = 0;
    long usedInDijkstra = 0;
    long used_In_skyline_operation = 0;
    private long usedInWrite = 0;
    private long usedInRead = 0;

    public myshortestPathUseNode(GraphDatabaseService graphdb) {
        this.graphdb = graphdb;
        mqueue = new myNodePriorityQueue();
    }

    public void getSkylinePath(Node source, Node destination) {

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
        Long Pages_2 = 0L;
        Long Pins_2 = 0L;
        Long time_2 = 0L;

        long usedInNode = 0;
        long usedInMain = 0;
        long usedInCheckExpand = 0;
        long usedInExpansion = 0;

        Long time_1 = System.currentTimeMillis();
        Long Pages_1 = getFromManagementBean("Page cache", "Faults", graphdb);
        Long Pins_1 = getFromManagementBean("Page cache", "Pins", graphdb);
        try (Transaction tx = this.graphdb.beginTx()) {
            // System.out.println(getFromManagementBean("Page cache",
            // "Faults"));
            path iniPath = new path(source, source);
            this.NumberOfProperties = iniPath.NumberOfProperties;
            initilizeSkylinePath(iniPath, destination);

            numInIPath = (long) this.skylinPaths.size();
            Pages_2 = getFromManagementBean("Page cache", "Faults", graphdb);
            Pins_2 = getFromManagementBean("Page cache", "Pins", graphdb);
            time_2 = System.currentTimeMillis();
            // cleanCache();

            // System.out.println(source + "----> "+destination);
            // System.out.println(source.getId());
            // System.out.println(destination.getId());

            for (path p : this.skylinPaths) {
                //System.out.println(p);
                System.out.println(p.propertiesName.get(0));
                System.out.println(p.propertiesName.get(1));
                System.out.println(p.propertiesName.get(2));
                System.out.println(printCosts(p.getCosts()));
            }
            System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");

            myNode start = new myNode(source, source);
            mqueue.add(start);
            tx.success();
        }

        int i = 0;
        try (Transaction tx = this.graphdb.beginTx()) {
            while (!mqueue.isEmpty()) {
                // System.out.println("--------------------------------------");
                myNode vs = mqueue.pop();
                // System.out.println(i++);

                i++;
                // System.out.println("times : " + i);
                processedNodeList.put(vs.id, vs);
                // System.out.println("From node: " + vs.current);

                int index = 0;
                for (; index < vs.subRouteSkyline.size(); ) {
                    path p = vs.subRouteSkyline.get(index);

                    if (!p.processed_flag) {
                        p.processed_flag = true;
                        // System.out.println(p);

                        // if is_expand is false, it means this sub skyline
                        // path's upper bound is dominated by all paths in
                        // skyline results.
                        // So, it should not be expanded, and need to be removed
                        // from subRouteSkyline.
                        // Because all expansions from this p should not be a
                        // finally result.
                        long cheexprt = System.nanoTime();
                        boolean is_expand = needToBeExpanded(p, destination);
                        usedInCheckExpand += (System.nanoTime() - cheexprt);

                        if (!is_expand) {
                            vs.subRouteSkyline.remove(index);
                        } else {

                            long expRt = System.nanoTime();
                            ArrayList<path> paths = p.expand();
                            usedInExpansion += System.nanoTime() - expRt;

                            for (path np : paths) {
                                if (!p.containRelationShip(np.getlastRelationship())) {
                                    if (np.endNode.equals(destination)) {
                                        long mrt = System.nanoTime();
                                        addToSkylineResult(np);
                                        long emrt = System.nanoTime() - mrt;
                                        usedInMain = usedInMain + emrt;
                                    } else {
                                        String nextid = String.valueOf(np.endNode.getId());
                                        myNode nextNode = null;
                                        if (this.processedNodeList.containsKey(nextid)) {
                                            nextNode = processedNodeList.get(nextid);
                                        } else {
                                            nextNode = new myNode(source, np.endNode, this.NumberOfProperties);
                                        }
                                        long nrt = System.nanoTime();
                                        nextNode.addToSkylineResult(np);
                                        usedInNode = usedInNode + (System.nanoTime() - nrt);

                                        mqueue.add(nextNode);
                                        this.processedNodeList.put(nextNode.id, nextNode);
                                    }
                                }
                            }
                            index++;
                        }
                    } else {
                        index++;
                    }

                    if (i % 50000 == 0) {
                        tx.success();
                    }
                }

            }
            tx.success();
        }

        Long Pages_3 = getFromManagementBean("Page cache", "Faults", graphdb);
        Long Pins_3 = getFromManagementBean("Page cache", "Pins", graphdb);
        Long time_3 = System.currentTimeMillis();

        pagecachedInFindNodes = Pages_1;
        pagecachedInInital = Pages_2 - Pages_1;
        pagecachedInFindSkyPath = Pages_3 - Pages_2;

        pinsInFindNodes = Pins_1;
        pinsInInital = Pins_2 - Pins_1;
        PinsInFindSkyPath = Pins_3 - Pins_2;

        long timeInInitial = time_2 - time_1;
        long timeInFindSkyPath = time_3 - time_2;

        numIters = (long) i;
        numAccessedNodes = (long) processedNodeList.size();

        numFinalPath = (long) this.skylinPaths.size();

        StringBuffer sb = new StringBuffer();
        sb.append(sourceId + ",").append(destinationId + ",");
        sb.append(pagecachedInFindNodes).append(",");
        sb.append(pagecachedInInital).append(",");
        sb.append(pagecachedInFindSkyPath).append(",");
        sb.append(pinsInFindNodes).append(",");
        sb.append(pinsInInital).append(",");
        sb.append(PinsInFindSkyPath).append(",");
        sb.append(timeInInitial).append(",");
        sb.append(timeInFindSkyPath).append(",");
        sb.append(numIters).append(",");
        sb.append(numAccessedNodes).append(",");
        sb.append(numInIPath).append(",");
        sb.append(numFinalPath);
        System.out.println(sb);

        long totalRt = (timeInInitial + timeInFindSkyPath) * 1000000;
        System.out.println(totalRt);
        DecimalFormat formatter = new DecimalFormat("#0.00");
        System.out.println(
                "usedInNode:" + usedInNode / 1000000 + "  " + formatter.format(100 * (double) usedInNode / totalRt));
        System.out.println(
                "usedInMain:" + usedInMain / 1000000 + "  " + formatter.format(100 * (double) usedInMain / totalRt));
        System.out.println("usedInCheckExpand:" + usedInCheckExpand / 1000000 + "     "
                + formatter.format(100 * (double) usedInCheckExpand / totalRt));
        System.out.println("    usedInRead:" + usedInRead / 1000000 + "     "
                + formatter.format(100 * (double) usedInRead / totalRt));
        System.out.println("    usedInWrite:" + usedInWrite / 1000000 + "     "
                + formatter.format(100 * (double) usedInWrite / totalRt));
        System.out.println("    usedInDijkstra:" + usedInDijkstra / 1000000 + "     "
                + formatter.format(100 * (double) usedInDijkstra / totalRt));
        System.out.println("usedInExpansion:" + usedInExpansion / 1000000 + "     "
                + formatter.format(100 * (double) usedInExpansion / totalRt));

        used_In_skyline_operation = used_In_skyline_operation + usedInNode + usedInMain;
        System.out.println("used_In_skyline_operation:" + used_In_skyline_operation / 1000000 + "     "
                + formatter.format(100 * (double) used_In_skyline_operation / totalRt));
        System.out.println((usedInNode + usedInMain + usedInCheckExpand + usedInExpansion) * 1.00 / totalRt);
        double avg_slr_inNode = 0.00;
        for (myNode n : this.processedNodeList.values()) {
            avg_slr_inNode += n.subRouteSkyline.size();
        }

        System.out.println(avg_slr_inNode / this.processedNodeList.size());
    }

    private void initilizeSkylinePath(path p, Node destination) {
        for (String property_name : p.propertiesName) {
            PathFinder<WeightedPath> finder = GraphAlgoFactory
                    .dijkstra(PathExpanders.forTypeAndDirection(Line.Linked, Direction.OUTGOING), property_name);
            WeightedPath paths = finder.findSinglePath(p.endNode, destination);
            if (paths != null) {
                path np = new path(paths);
                // System.out.println("----------------------");
                // System.out.println(printCosts(np.getCosts()));
                addToSkylineResult(np);
            }
        }
        // System.out.println("initialized the skyline result with " +
        // this.skylinPaths.size() + " paths");
        // for(path sp:this.skylinPaths)
        // {
        // System.out.println(sp);
        // System.out.println(printCosts(sp.getCosts()));
        // }
    }

    private boolean needToBeExpanded(path p, Node destination) {
        boolean flag = false;
        double estimatedCosts[] = new double[p.NumberOfProperties];
        double tmplCosts[] = new double[p.NumberOfProperties];
        int i = 0;

        long reTime = System.nanoTime();
        if ((tmplCosts = checkinFile(p.endNode.getId(), destination.getId())) != null) {
            this.usedInRead += System.nanoTime() - reTime;
            // System.out.println(p + ": "+destination);
            // System.out.println(printCosts(tmplCosts));
            // System.out.println("----------------------");
            if (tmplCosts[0] != -1) {
                for (String property_name : p.propertiesName) {
                    estimatedCosts[i] = p.getCosts()[i] + tmplCosts[i];
                    i++;
                }
            } else {
                return false;
            }
        } else {
            this.usedInRead += System.nanoTime() - reTime;
            tmplCosts = new double[NumberOfProperties];

            // calculate the upper bound of this path to destination
            // if path's costs add the shortest cost from the path to
            // destination is dominated by skyline result
            // do not expand it
            for (String property_name : p.propertiesName) {
                long dijS = System.nanoTime();
                PathFinder<WeightedPath> finder = GraphAlgoFactory
                        .dijkstra(PathExpanders.forTypeAndDirection(Line.Linked, Direction.OUTGOING), property_name);

                WeightedPath paths = finder.findSinglePath(p.endNode, destination);
                this.usedInDijkstra += (System.nanoTime() - dijS);
                // if we can not find a shortest path from p.endNode to
                // destination
                if (paths == null) {
                    // System.out.println("can not get a path from " +
                    // p.endNode.getId() + " to " + destination.getId()
                    // + " based on " + property_name);
                    for (int j = 0; j < tmplCosts.length; j++) {
                        tmplCosts[j] = -1;
                    }
                    long wrEnd = System.nanoTime();
                    writeToFile(p.endNode.getId(), destination.getId(), tmplCosts);
                    this.usedInWrite += (System.nanoTime() - wrEnd);
                    return false;
                }
                double weight = paths.weight();
                estimatedCosts[i] = p.getCosts()[i] + weight;
                tmplCosts[i] = weight;
                i++;
            }
            long wrEnd = System.nanoTime();
            writeToFile(p.endNode.getId(), destination.getId(), tmplCosts);
            this.usedInWrite += (System.nanoTime() - wrEnd);

        }
        // System.out.println("estimated costs: " + printCosts(estimatedCosts));
        // if flag = true, it means estimated path is dominated by all paths in
        // skyline results, we do not need to expand.
        // if flag = false, it means estimated path dominate at least one path
        // in skyline result, we need to expand.
        long usedInskyline = System.nanoTime();
        flag = (!isdominatedbySkylineResults(estimatedCosts));
        this.used_In_skyline_operation += System.nanoTime() - usedInskyline;

        return flag;
    }

    private void writeToFile(long sid, long did, double[] tmplCosts) {

        long numOfFolder = sid / 1000;
        String filepath = "data/tmpFile/" + numOfFolder;
        File f = new File(filepath);
        if (!f.exists()) {
            // System.out.println("mkdir "+f);
            f.mkdirs();
        }
        filepath += "/" + sid + ".txt";
        try {
            BufferedWriter bw = new BufferedWriter(new FileWriter(filepath, true));
            StringBuffer sb = new StringBuffer();
            sb.append(did).append(",");
            for (double d : tmplCosts) {
                sb.append(d).append(",");
            }
            sb.deleteCharAt(sb.lastIndexOf(","));
            sb.append("\n");
            bw.write(sb.toString());
            bw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private double[] checkinFile(long sid, long did) {
        long numOfFolder = sid / 1000;
        String filepath = "data/tmpFile/" + numOfFolder + "/" + sid + ".txt";

        double[] result = new double[NumberOfProperties];

        File f = new File(filepath);
        if (!f.exists()) {
            return null;
        }

        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(filepath));
            String line = null;

            while ((line = br.readLine()) != null) {
                String attrs[] = line.split(",");

                if (attrs[0].equals(String.valueOf(did))) {
                    for (int i = 1; i <= NumberOfProperties; i++) {
                        result[i - 1] = Double.valueOf(attrs[i]);
                    }
                    br.close();
                    return result;
                }
            }
            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    // check whether estimated path is dominated by all paths in skyline
    // results;
    // if at least one path is dominated by estimated path, it means we need to
    // expand it
    // Because it is the potential path dominated one path in the skyline path.
    // if all paths in skyline results dominate estimated path, we do not need
    // to expand estimated path,
    // because it's upper bound already dominated by all skyline paths right
    // now.
    private boolean isdominatedbySkylineResults(double[] estimatedCosts) {
        if (skylinPaths.isEmpty()) {
            return false;
        } else {
            for (path p : skylinPaths) {
                // checkDominated() return false it means p do not dominate
                // estimatedCosts.
                // checkDominated() return true it means p dominate
                // estimatedCosts.
                // if one path p in skyline results do not dominate estimated
                // path, means we can continue our expansion on this estimated
                // path.
                // System.out.println(printCosts(p.getCosts())+"
                // "+printCosts(estimatedCosts)+" "+
                // checkDominated(p.getCosts(), estimatedCosts));
                if (!checkDominated(p.getCosts(), estimatedCosts))
                    return false;
            }
        }
        return true;
    }

    // whether costs is dominated by estimated costs.
    // if costs dominated estimatedCosts return ture, else costs do not
    // dominated estimatedCosts, return false.
    // if costs has one attribute is greater than estimatedCosts, it means
    // estimatedCosts is better than costs in this dimenson;
    // else all costs attribute is less or equal than estimatedCosts, so cost
    // dominated estimatedCosts
    private boolean checkDominated(double[] costs, double[] estimatedCosts) {
        for (int i = 0; i < costs.length; i++) {
            if (costs[i] > estimatedCosts[i]) {
                return false;
            }
        }
        return true;
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

    private void addToSkylineResult(path np) {
        int i = 0;
        if (skylinPaths.isEmpty()) {
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
                if (checkDominated(skylinPaths.get(i).getCosts(), np.getCosts())) {
                    if (alreadyinsert && i != this.skylinPaths.size() - 1) {
                        this.skylinPaths.remove(this.skylinPaths.size() - 1);
                    }
                    // System.out.println("Jump it and break, because it already
                    // is dominated by this record "
                    // + this.skylinPaths.size());
                    break;
                } else {
                    if (checkDominated(np.getCosts(), skylinPaths.get(i).getCosts())) {
                        // System.out.println("remove the old one and insert
                        // it");
                        this.skylinPaths.remove(i);
                    } else {
                        i++;
                    }
                    if (!alreadyinsert) {
                        // System.out.println("insert it because it does not
                        // dominated each other");
                        this.skylinPaths.add(np);
                        alreadyinsert = true;
                    }

                }
            }
            // System.out.println(this.skylinPaths.size());
            // System.out.println("===========================");
        }
    }

    public void cleanCache() {
        ObjectName objectName = JmxUtils.getObjectName(this.graphdb, "Configuration");

        // JmxUtils.invoke(objectName,"org.neo4j.management.Cache.clear",null,null);
        // System.out.println("Test JMX
        // output:"+(String)JmxUtils.getAttribute(objectName,
        // "unsupported.dbms.block_size.strings"));
    }

    private Long getFromManagementBean(String Object, String Attribuite, GraphDatabaseService graphDb) {
        ObjectName objectName = JmxUtils.getObjectName(graphDb, Object);
        Long value = JmxUtils.getAttribute(objectName, Attribuite);
        return value;
    }

}
