package GPSkyline;

import GraphPartition.*;
import javafx.util.Pair;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class GPSkylineSearch {
    private GraphDatabaseService graphdb;
    BlinksPartition bp = null;
    ArrayList<path> skylines = new ArrayList<>();

    public GPSkylineSearch(GraphDatabaseService graphdb) {
        this.graphdb = graphdb;
    }

    public static void main(String args[]) {
        GPSkylineSearch gps = new GPSkylineSearch(null);
        int num_parts = 20;
        long graphsize = 2000;
        String portalSelector = "Blinks";
        String lowerboundSelector = "landmark";
        gps.BuildGPartitions(num_parts, graphsize, portalSelector, lowerboundSelector);
//        gps.findSkylines();

    }

    public void BuildGPartitions(int num_parts, long graphsize, String portalSelector, String lowerboundSelector) {
        this.bp = new BlinksPartition(num_parts, graphsize, portalSelector, lowerboundSelector);
        if (portalSelector.equals("Blinks")) {
            bp.getPortalsBlinks();
        } else if (portalSelector.equals("VC")) {
            bp.getPortalsVertexCover();
        }
        System.out.println("===========================");
        System.out.println(bp.portals.size());
        bp.cleanFadePortal();
        bp.createBlocks();

        System.out.println(bp.prts.blocks.size());


        bp.prts.randomSelectLandMark(3);
        long buildlandmark = System.currentTimeMillis();
        if (lowerboundSelector.equals("landmark")) {
            System.out.println("run landmark");
        } else if (lowerboundSelector.equals("dijkstra")) {
            System.out.println("run dijkstra");
        }
        bp.prts.buildIndexes(graphsize, lowerboundSelector);
        System.out.println("The time usage to build the landmark index " + (System.currentTimeMillis() - buildlandmark) + " ms");
    }

    public void findSkylines(Node source, Node destination) {
        String str_sid = String.valueOf(source.getId() + 1);
        String str_did = String.valueOf(destination.getId() + 1);
        BlockQueue bqueue = new BlockQueue();
        try (Transaction tx = this.graphdb.beginTx()) {
            if (this.bp.prts.isPortals(str_sid)) {
                System.out.println(str_sid + " is a portal node");
                ArrayList<block> adjBlocks = bp.prts.getOutBlockOfPortal(str_did);
                for (block b : adjBlocks) {
                    System.out.println("    " + b.pid);
                    for (Map.Entry<Pair<String, String>, ArrayList<path>> skypaths_obj : b.innerIndex.entrySet()) {
                        //find the skyline results that start with this startnode id
                        if (skypaths_obj.getKey().getKey().equals(str_did)) {
                            for (path p : skypaths_obj.getValue()) {
                                String o_portal_id = skypaths_obj.getKey().getValue();
                                if (!b.portalSkyline.containsKey(o_portal_id)) {
                                    myNode n = new myNode(source, p.endNode, false);
                                    n.addToSkylineResult(p);
                                    b.portalSkyline.put(o_portal_id, n);
                                } else {
                                    myNode n = b.portalSkyline.get(o_portal_id);
                                    n.addToSkylineResult(p);
                                }

                            }
                        }

                    }
                    bqueue.add(b);
                }
            } else {
                block b = this.bp.prts.getPid(str_sid);
                System.out.println(str_sid + " is not a portal node, it is in the block " + b.pid);
                skylineInBlock sbib = new skylineInBlock(this.graphdb, b);
                for (String o_portal_id : b.oportals) {
                    Node o_p_node = this.graphdb.getNodeById(Long.parseLong(o_portal_id) - 1);
                    ArrayList<path> skyToOutPortals = sbib.getSkylineInBlock_blinks(source, o_p_node);
                    int size = skyToOutPortals == null ? 0 : skyToOutPortals.size();
                    if (size != 0) {
                        System.out.println(o_portal_id + " !! " + size);
                        for (path p : skyToOutPortals) {
                            if (!b.portalSkyline.containsKey(o_portal_id)) {
                                myNode n = new myNode(source, o_p_node, false);
                                n.addToSkylineResult(p);
                                b.portalSkyline.put(o_portal_id, n);
                            } else {
                                myNode n = b.portalSkyline.get(o_portal_id);
                                n.addToSkylineResult(p);
                            }
                        }
                    }
                    sbib.clearMemeory();
                }
                bqueue.add(b);
            }
//            tx.success();
        }


        block targetBlock = null;
        try (Transaction tx = this.graphdb.beginTx()) {
            if (this.bp.prts.isPortals(str_did)) {
                System.out.println(str_did + " is a portal node");
                ArrayList<block> adjBlocks = bp.prts.getBlocskOfPortal(str_did); //the block in which the target is a portal
            } else {
                targetBlock = this.bp.prts.getPid(str_did);
                System.out.println(str_did + " is not a portal node, it is in the block " + targetBlock.pid);
            }
//            tx.success();
        }


        try (Transaction tx = this.graphdb.beginTx()) {
            while (!bqueue.isEmpty()) {
                block b = bqueue.pop();
                for (Map.Entry<String, myNode> nodeEntery : b.portalSkyline.entrySet()) {
                    myNode vs = nodeEntery.getValue();
                    int index = 0;
                    for (; index < vs.subRouteSkyline.size(); ) {
                        path p = vs.subRouteSkyline.get(index);
                        if (!p.processed_flag) {
                            p.processed_flag=true;
                            ArrayList<block> adjblocks = bp.prts.getOutBlockOfPortal(String.valueOf(p.endNode.getId() + 1));
                            boolean couldBeRemove = true;
                            for (block adj_block : adjblocks) {
                                //can not expand to the adj_block;
                                //Todo: It only means I can not expand the path to this adj_block. But if it sitll could expand to other adj_block.
                                //Todo: If I already removed it from vs object, it will affect later calcultion.
                                boolean could_expend = checkingExpansion(p, destination, adj_block, targetBlock);
                                if(could_expend)
                                {
                                    //it means the path could be expand to this adj_block
                                    couldBeRemove=false;
                                    ArrayList<path> expandPaths = adj_block.concatenatePath(p);
                                }
                            }

                            if(couldBeRemove)
                            {
                                vs.subRouteSkyline.remove(index);
                            }else
                            {
                                index++;
                            }
                        } else {
                            index++;
                        }
                    }
                }

            }
        }
    }

    private boolean checkingExpansion(path p, Node destination, block cb, block targetb) {
        String cnd = String.valueOf(p.endNode.getId() + 1);
        String dnd = String.valueOf(destination.getId() + 1);
        double[] estimatedCost = new double[p.NumberOfProperties];
        //get the landmark distance from p.endNodes to the landmark in the block cb
        HashMap<String, double[]> toLowerBound = cb.toLandMarkIndex.get(cnd);
        HashMap<String, double[]> fromLowerBound = targetb.fromLandMarkIndex.get(dnd);

        if (toLowerBound != null && fromLowerBound != null) {
            int landmark_index = 0;
            for (String ptype : p.propertiesName) {
                double maxValue = Double.NEGATIVE_INFINITY;
                for (String cb_lmk : toLowerBound.keySet()) {
                    for (String targetB_lmk : fromLowerBound.keySet()) {
                        Pair<String, String> key = new Pair<>(cb_lmk, targetB_lmk);
                        double[] betweenLand = this.bp.prts.outerLandMark.get(key);
                        double[] t_l_cost = toLowerBound.get(cb_lmk);
                        double[] f_l_cost = fromLowerBound.get(targetB_lmk);
                        if (t_l_cost != null && f_l_cost != null) {
                            double D_value = t_l_cost[landmark_index] - f_l_cost[landmark_index] - betweenLand[landmark_index];
                            if (maxValue < Math.abs(D_value)) {
                                maxValue = D_value;
                            }
                        }
                    }
                }
                estimatedCost[landmark_index] = maxValue;
                landmark_index++;
            }
        }

        boolean flag = false;
        for (int i = 0; i < p.NumberOfProperties; i++) {
            estimatedCost[i] = p.getCosts()[i] + estimatedCost[i];
        }
        flag = isdominatedbySkylineResults(estimatedCost);
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
}
