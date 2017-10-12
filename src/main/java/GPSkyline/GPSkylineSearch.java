package GPSkyline;

import GraphPartition.*;
import javafx.util.Pair;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import java.util.*;

public class GPSkylineSearch {
    private GraphDatabaseService graphdb;
    BlinksPartition bp = null;
    ArrayList<path> skylines = new ArrayList<>();
    private HashMap<Long, myNode> processedNodeList = new HashMap<>();
    private int NumberOfProperties;
    private ArrayList<String> propertiesName;

    public GPSkylineSearch(GraphDatabaseService graphdb) {
        this.graphdb = graphdb;
    }

    public static void main(String args[]) {
        GPSkylineSearch gps = new GPSkylineSearch(null);
        int num_parts = 20;
        long graphsize = 2000;
        String portalSelector = "VC";
        String lowerboundSelector = "landmark";
        gps.BuildGPartitions(num_parts, graphsize, portalSelector, lowerboundSelector);
//        gps.findSkylines();

    }

    public void BuildGPartitions(int num_parts, long graphsize, String portalSelector, String lowerboundSelector) {
        this.bp = new BlinksPartition(num_parts, graphsize, portalSelector, lowerboundSelector);
        if (portalSelector.equals("Blinks")) {
            System.out.println("run Blinks");
            bp.getPortalsBlinks();
        } else if (portalSelector.equals("VC")) {
            System.out.println("run VC");
            bp.getPortalsVertexCover();
        }
        System.out.println("===========================");
        System.out.println(bp.portals.size());
//        bp.cleanFadePortal();
        System.out.println(bp.portals.size());
        bp.createBlocks();
//        bp.createBlocks_blinks();


        System.out.println(bp.prts.blocks.size());
        System.out.println(bp.prts.portals.size());


        bp.prts.randomSelectLandMark(3);
        long buildlandmark = System.currentTimeMillis();
        if (lowerboundSelector.equals("landmark")) {
            System.out.println("run landmark");
        } else if (lowerboundSelector.equals("dijkstra")) {
            System.out.println("run dijkstra");
        }
        bp.prts.buildIndexes(graphsize, lowerboundSelector, this.graphdb);
        System.out.println("The time usage to build the landmark index " + (System.currentTimeMillis() - buildlandmark) + " ms");
        System.out.println("===="+bp.prts.portalList.size());
        System.out.println("===="+bp.prts.nodeToBlockId.size());


        for (String pid : bp.prts.blocks.keySet()) {
            block b = bp.prts.blocks.get(pid);
            System.out.print(pid + "  " + b.nodes.size() + " " + b.iportals.size() + " " + b.oportals.size()
                    + " " + b.landMarks.size() + " " + b.fromLandMarkIndex.size() + " " + b.toLandMarkIndex.size()
                    + "  " + b.innerIndex.size());
            int count = 0;

            for (Map.Entry<Pair<String, String>, ArrayList<path>> p : b.innerIndex.entrySet()) {
                count += p.getValue().size();
            }

            System.out.print("  " + count + "\n");
        }
    }

    long concatenetPath = 0;

    public ArrayList<path> findSkylines(Node source, Node destination) {
        cleanMemory();
        String str_sid = String.valueOf(source.getId() + 1);
        String str_did = String.valueOf(destination.getId() + 1);
        myNodePriorityQueue mqueue = new myNodePriorityQueue();

        try (Transaction tx = this.graphdb.beginTx()) {

            ArrayList<block> targetBlock = new ArrayList<>();
            boolean target_is_portal = false;
            //portal node
            if (this.bp.prts.portalList.containsKey(str_did)) {
                HashSet<String> outList = this.bp.prts.portalList.get(str_did).get("out");
//                System.out.println("target node " + destination + " is a portal node");
                if (outList != null) {
                    for (String o_blockID : outList) {
                        targetBlock.add(this.bp.prts.blocks.get(o_blockID));
                    }
                } else {
                    System.out.println("there is no in-comming edges to it.");
                    System.exit(0);
                }
                target_is_portal = true;
            } else {//normal node
                String blockID = this.bp.prts.nodeToBlockId.get(str_did);
                targetBlock.add(this.bp.prts.blocks.get(blockID));
            }


//            boolean start_is_portal = false;
//            ArrayList<block> startBlock = new ArrayList<>();
//            if (this.bp.prts.portalList.containsKey(str_sid)) {
//                HashSet<String> inList = this.bp.prts.portalList.get(str_sid).get("in");
//                if (inList != null) {
//                    for (String o_blockID : inList) {
//                        startBlock.add(this.bp.prts.blocks.get(o_blockID));
//                    }
//                    start_is_portal = true;
//                }
//            } else {//normal node
//                String blockID = this.bp.prts.nodeToBlockId.get(str_sid);
//                startBlock.add(this.bp.prts.blocks.get(blockID));
//            }
//
//
//            System.out.println("start node is a portal " + start_is_portal + " " + startBlock.size());
//            if (target_is_portal) {
//                for (block sbb : startBlock) {
//                    System.out.println(sbb.pid);
//                }
//            }
//            System.out.println("target node is a portal " + target_is_portal + " " + targetBlock.size());
//            if (target_is_portal) {
//                for (block tbb : targetBlock) {
//                    System.out.println(tbb.pid);
//                }
//            }
//
//            System.out.println("    " + bp.prts.nodeToBlockId.get("1849"));
//            System.out.println("    " + bp.prts.nodeToBlockId.get("890"));
////            System.out.println("===================================================");
////
////            if (this.bp.prts.portalList.containsKey("1708")) {
////                HashSet<String> inList = this.bp.prts.portalList.get("1708").get("in");
////                if (inList != null) {
////                    for (String o_blockID : inList) {
////                        System.out.println(o_blockID);
////                    }
////                    start_is_portal = true;
////                }
////                System.out.println("---------------------");
////                HashSet<String> outList = this.bp.prts.portalList.get("1708").get("out");
////                if (outList != null) {
////                    for (String o_blockID : outList) {
////                        System.out.println(o_blockID);
////                    }
////                }
////            }
////
////            System.out.println("====");
////
////            if (this.bp.prts.portalList.containsKey("232")) {
////                HashSet<String> inList = this.bp.prts.portalList.get("232").get("in");
////                if (inList != null) {
////                    for (String o_blockID : inList) {
////                        System.out.println(o_blockID);
////                    }
////                    start_is_portal = true;
////                }
////                System.out.println("---------------------");
////                HashSet<String> outList = this.bp.prts.portalList.get("232").get("out");
////                if (outList != null) {
////                    for (String o_blockID : outList) {
////                        System.out.println(o_blockID);
////                    }
////                }
////            }
//
//
//            for (Map.Entry<Pair<String, String>, ArrayList<path>> aa : bp.prts.blocks.get("13").innerIndex.entrySet()) {
//                if (aa.getKey().getKey().equals("232")) {
//                    System.out.println(aa.getKey());
//                    for (path ppp : aa.getValue()) {
//                        System.out.println("    " +ppp);
//                    }
//                }
//
//            }
//            System.out.println("===================================================");

//
//            for (block tb : targetBlock) {
//                System.out.println("target is the out portal of block " + tb.pid);
//            }


            myNode start = processedNodeList.get(source.getId());
            if (start == null) {
                start = new myNode(source, source, true);
                this.processedNodeList.put(source.getId(), start);
            }
            this.NumberOfProperties = start.propertiesName.size();
//            System.out.println(start.propertiesName.size());
            this.propertiesName = start.propertiesName;
            mqueue.add(start);

//            double[] cost_upper_bound = upperbound(source, destination, startBlock, targetBlock);
//            path upper_path = new path(cost_upper_bound);
//            this.skylines.add(upper_path);


            int i = 1;
            while (!mqueue.isEmpty()) {
                myNode vs = mqueue.pop();
                vs.inqueue = false;

                String str_vs_id = String.valueOf(vs.current.getId() + 1);
                ArrayList<block> vsBlock = new ArrayList<>();
                boolean vs_is_portal = false;
                //portal node
                if (this.bp.prts.portalList.containsKey(str_vs_id)) {
                    //find the block in which the vs node is a in-coming portal
                    HashSet<String> inList = this.bp.prts.portalList.get(str_vs_id).get("in");
                    if (inList != null) {
                        for (String o_blockID : inList) {
                            vsBlock.add(this.bp.prts.blocks.get(o_blockID));
                        }
                        vs_is_portal = true;
                    }
                } else {//normal node
                    String blockID = this.bp.prts.nodeToBlockId.get(str_vs_id);
                    vsBlock.add(this.bp.prts.blocks.get(blockID));
                }

                if(vsBlock.size()==0)
                {
                    System.out.println(str_vs_id+"  !! "+vs_is_portal);
                }


//                if (i % 10 == 0) {
//                    break;
//                }
//
//                i++;
//                if (vs.id.equals("231")) {
//                    System.out.println(vs.id + " " + vs_is_portal + " " + vsBlock.size()+"  "+vsBlock.get(0).pid);
////                    if (vs_is_portal) {
////                        for (block bb : vsBlock) {
////                            System.out.println(bb.pid);
////                        }
////                    }
//                }

                int index = 0;
                // System.out.println(vs.id+" "+vs.subRouteSkyline.size());
                for (; index < vs.subRouteSkyline.size(); ) {
                    path p = vs.subRouteSkyline.get(index);
//                    System.out.println("    "+p);
                    if (!p.processed_flag) {
                        p.processed_flag = true;
                        //two ways to find the destination of the block -portal node or non-portal node
                        boolean is_expand = checkingExpansion(p, destination, vsBlock, targetBlock);

//                        if (vs.id.equals("231")) {
//                            System.out.println("  is_expand " + is_expand);
//                        }

                        if (!is_expand) {
                            vs.subRouteSkyline.remove(index);
                        } else {
                            if (vs_is_portal) {
//                                System.out.println("   "+vsBlock.size());
                                for (block adjb : vsBlock) {
//                                    System.out.println("        "+adjb.pid);

                                    long concatenetPath_start = System.nanoTime();
                                    ArrayList<path> paths = adjb.concatenatePath(p);
                                    this.concatenetPath += (System.nanoTime()-concatenetPath_start);

                                    if (!target_is_portal && adjb.pid.equals(targetBlock.get(0).pid)) {
//                                        System.out.println("    reach target block");
                                        for (path inT_path : p.expand(adjb)) {
                                            if (!inT_path.isCycle()) {
                                                paths.add(inT_path);
//                                                if (vs.id.equals("231")) {
//                                                    System.out.println("   reach " + inT_path);
//                                                }
                                            }
                                        }
                                    }
//                                    System.out.println("        paths size "+paths.size());
                                    for (path np : paths) {
//                                        if (vs.id.equals("231")) {
//                                            System.out.println("   " + np);
//                                        }
                                        if (!np.isCycle()) {
                                            if (np.endNode.getId() == destination.getId()) {
//                                                System.out.println(np);
                                                addToSkylineResult(np);
                                            } else {
                                                long nextid = np.endNode.getId();
                                                myNode nextNode = this.processedNodeList.get(nextid);
                                                if (nextNode == null) {
                                                    nextNode = new myNode(source, np.endNode);
                                                    processedNodeList.put(nextNode.current.getId(), nextNode);
                                                }
                                                nextNode.addToSkylineResult(np);
                                                if (!nextNode.inqueue) {
//                                                    processedNodeList.put(nextNode.current.getId(), nextNode);
                                                    mqueue.add(nextNode);
                                                    nextNode.inqueue = true;
                                                }
                                            }
                                        }
                                    }
                                }
                            } else {
                                ArrayList<path> paths = p.expand(vsBlock.get(0));
                                for (path np : paths) {
                                    if (!np.isCycle()) {
                                        if (np.endNode.getId() == destination.getId()) {
                                            addToSkylineResult(np);
                                        } else {
                                            long nextid = np.endNode.getId();
                                            myNode nextNode = this.processedNodeList.get(nextid);
                                            if (nextNode == null) {
                                                nextNode = new myNode(source, np.endNode);
                                                processedNodeList.put(nextNode.current.getId(), nextNode);
                                            }
                                            nextNode.addToSkylineResult(np);
                                            if (!nextNode.inqueue) {
                                                mqueue.add(nextNode);
                                                nextNode.inqueue = true;
                                            }
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
        }
        System.out.println("----" + count1 + " " + count2 + " " + count3);

        int index_delete = 0;
        for (path p : this.skylines) {
            if (p.endNode == null) {
                break;

            }
            index_delete++;
        }

        if (index_delete < this.skylines.size()) {
            this.skylines.remove(index_delete);
        }

        return this.skylines;


    }

    private void cleanMemory() {
        this.processedNodeList.clear();
        this.count1 = this.count2 = this.count3 = 0;
        this.skylines.clear();
        concatenetPath=0;
    }

    private double[] upperbound(Node source, Node destination, ArrayList<block> vsblock, ArrayList<block> targetBlock) {
        String cnd = String.valueOf(source.getId() + 1);
        String dnd = String.valueOf(destination.getId() + 1);
        double[] estimatedCost = new double[this.NumberOfProperties];
        for (int i = 0; i < this.NumberOfProperties; i++) {
            estimatedCost[i] = Double.MAX_VALUE;
        }

        for (block cb : vsblock) {
            for (block targetb : targetBlock) {
                HashMap<String, double[]> toLowerBound = cb.toLandMarkIndex.get(cnd);
                HashMap<String, double[]> fromLowerBound = targetb.fromLandMarkIndex.get(dnd);
                if (toLowerBound != null && fromLowerBound != null) {
                    for (String cb_lmk : toLowerBound.keySet()) {
                        for (String targetB_lmk : fromLowerBound.keySet()) {
                            Pair<String, String> key = new Pair<>(cb_lmk, targetB_lmk);
                            double[] betweenLand = this.bp.prts.outerLandMark.get(key);
                            double[] t_l_cost = toLowerBound.get(cb_lmk);
                            double[] f_l_cost = fromLowerBound.get(targetB_lmk);
                            if (t_l_cost != null && f_l_cost != null && betweenLand != null) {
                                int landmark_index = 0;
                                for (String ptype : this.propertiesName) {
                                    double D_value = Math.abs(t_l_cost[landmark_index] + betweenLand[landmark_index] + f_l_cost[landmark_index]);
                                    if (estimatedCost[landmark_index] > D_value) {
                                        estimatedCost[landmark_index] = D_value;
                                    }
                                    landmark_index++;
                                }
                            }
                        }
                    }
                }
            }
        }
        return estimatedCost;
    }

    //Todo: finished it in for loop

    int count1, count2, count3;

    private boolean checkingExpansion(path p, Node destination, ArrayList<block> vsblock, ArrayList<block> targetBlock) {
        String cnd = String.valueOf(p.endNode.getId() + 1);
        String dnd = String.valueOf(destination.getId() + 1);
        double[] estimatedCost = new double[p.NumberOfProperties];
        for (int i = 0; i < estimatedCost.length; i++) {
            estimatedCost[i] = Double.NEGATIVE_INFINITY;
        }

        for (block cb : vsblock) {
            for (block targetb : targetBlock) {
                HashMap<String, double[]> toLowerBound = cb.toLandMarkIndex.get(cnd);
                HashMap<String, double[]> fromLowerBound = targetb.fromLandMarkIndex.get(dnd);

                if (toLowerBound != null && fromLowerBound != null) {

                    for (String cb_lmk : toLowerBound.keySet()) {
                        for (String targetB_lmk : fromLowerBound.keySet()) {
                            Pair<String, String> key = new Pair<>(cb_lmk, targetB_lmk);
                            double[] betweenLand = this.bp.prts.outerLandMark.get(key);
                            double[] t_l_cost = toLowerBound.get(cb_lmk);
                            double[] f_l_cost = fromLowerBound.get(targetB_lmk);
                            if (t_l_cost != null && f_l_cost != null && betweenLand != null) {
                                int landmark_index = 0;
                                for (String ptype : p.propertiesName) {
                                    double a1 = Math.abs(t_l_cost[landmark_index] + betweenLand[landmark_index]);
                                    double a2 = Math.abs(t_l_cost[landmark_index] - betweenLand[landmark_index]);
                                    double b1 = f_l_cost[landmark_index]-a1;
                                    double b2 = a2 - f_l_cost[landmark_index];
                                    double D_value = b1 > b2 ? b2 : b1;

                                    if (estimatedCost[landmark_index] < D_value && D_value>0) {
                                        estimatedCost[landmark_index] = D_value;
                                    }
                                    landmark_index++;
                                }
                            }
                        }
                    }

                }
            }
        }

        boolean flag;

        for (int i = 0; i < estimatedCost.length; i++) {
            if (estimatedCost[i] == Double.NEGATIVE_INFINITY) {
                estimatedCost = new double[this.NumberOfProperties];
                break;
            }
        }

        for (int i = 0; i < p.NumberOfProperties; i++) {
            estimatedCost[i] = p.getCosts()[i] + estimatedCost[i];
        }

        flag = isdominatedbySkylineResults(estimatedCost);
        if (flag) {
            count1++;
        } else {
            count2++;
        }
        return flag;
    }

    private boolean isdominatedbySkylineResults(double[] estimatedCosts) {
        if (this.skylines.isEmpty()) {
            return true;
        } else {
            for (path p : skylines) {
                //if any of the path dominated the lowerbond, return false.
                if (checkDominated(p.getCosts(), estimatedCosts))
                    return false;
            }
        }
        // If all the path in skyline results dosen't dominate estimatedCosts;
        return true;
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

    public void setGraphObject(GraphDatabaseService graphObject) {
        this.graphdb = graphObject;
    }

    private void addToSkylineResult(path np) {
        int i = 0;
        if (this.skylines.isEmpty()) {
            this.skylines.add(np);
        } else {
            boolean alreadyinsert = false;
            for (; i < this.skylines.size(); ) {
                if (checkDominated(this.skylines.get(i).getCosts(), np.getCosts())) {
                    if (alreadyinsert && i != this.skylines.size() - 1) {
                        this.skylines.remove(this.skylines.size() - 1);
                    }
                    break;
                } else {
                    if (checkDominated(np.getCosts(), this.skylines.get(i).getCosts())) {
                        this.skylines.remove(i);
                    } else {
                        i++;
                    }
                    if (!alreadyinsert) {
                        this.skylines.add(np);
                        alreadyinsert = true;
                    }

                }
            }
        }
    }
}
