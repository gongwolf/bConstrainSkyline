package GPSkyline;

import GraphPartition.BlinksPartition;
import GraphPartition.block;
import GraphPartition.path;
import GraphPartition.skylineInBlock;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import java.lang.reflect.Array;
import java.util.ArrayList;

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
        try (Transaction tx = this.graphdb.beginTx()) {

            if (this.bp.prts.isPortals(str_sid)) {
                System.out.println(str_sid + " is a portal node");
                ArrayList<block> adjBlocks = bp.prts.getOutBlockOfPortal(str_did);
                for (block b : adjBlocks) {
                    System.out.println("    " + b.pid);
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
                            b.addToPortalSubSkyline(o_portal_id, p);
                        }
                    }
                    sbib.clearMemeory();
                }
            }
//            tx.success();
        }
    }

    public void setGraphObject(GraphDatabaseService graphObject) {
        this.graphdb = graphObject;
    }
}
