package GraphPartition;

import javafx.util.Pair;
import neo4jTools.Line;
import neo4jTools.StringComparator;
import neo4jTools.connector;
import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphalgo.WeightedPath;
import org.neo4j.graphdb.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;

public class blocks {
    public TreeMap<String, block> blocks; // block_id -> block
    public HashMap<Pair<String, String>, double[]> outerLandMark;
    public HashMap<String, String> nodeToBlockId = new HashMap<>();
    public HashMap<String, HashMap<String, HashSet<String>>> portalList = new HashMap<>();

    public blocks() {
        blocks = new TreeMap<>(new StringComparator());
        this.outerLandMark = new HashMap<>();
    }

    /**
     * @param numberofLandmard the number of landmarks in each blocks
     */
    public void randomSelectLandMark(int numberofLandmard) {
        for (Map.Entry<String, block> b_obj : blocks.entrySet()) {
            block b = b_obj.getValue();

            for (int i = 0; i < numberofLandmard; i++) {
                String node_id = b.nodes.get(getRandomNumberInRange(0, b.nodes.size() - 1));
                while (b.landMarks.contains(node_id)) {
                    node_id = b.nodes.get(getRandomNumberInRange(0, b.nodes.size() - 1));
                }
                b.landMarks.add(node_id);
            }

        }
    }

    public void buildIndexes(long graphsize, String lowerboundSelector) {
        String DB_PATH = "/home/gqxwolf/neo4j323/testdb" + graphsize + "/databases/graph.db";
        connector n = new connector(DB_PATH);
        System.out.println("Connect to the database : " + DB_PATH);
        n.startDB();
        GraphDatabaseService graphDB = n.getDBObject();
        try (Transaction tx = graphDB.beginTx()) {
            for (Map.Entry<String, block> b_obj : blocks.entrySet()) {
                block b = b_obj.getValue();
                Long run_ms = System.currentTimeMillis();
                b.buildLandmarkIndex(graphDB);
                long rms1 = System.currentTimeMillis();
//                System.out.println(b.nodes.size()+" "+b.iportals.size()+" "+b.oportals.size()+" "+b.landMarks.size()+" "+b.fromLandMarkIndex.size()+" "+b.toLandMarkIndex.size());
                long run_ms1 = rms1 - run_ms;
                b.buildInnerSkylineIndex(graphDB, lowerboundSelector);
                Long run_ms2 = System.currentTimeMillis() - rms1;
                System.out.print("Build index for partition " + b_obj.getKey() + "  " + run_ms1 + " " + run_ms2 + " " + (System.currentTimeMillis() - run_ms) + " ms \n");
//                break;
            }

            long run_ms3 = System.currentTimeMillis();
            path fakePath = null;
            for (Map.Entry<String, block> b_obj : blocks.entrySet()) {
                for (Map.Entry<String, block> others_b_obj : blocks.entrySet()) {
                    //not the same block
                    if (!b_obj.getKey().equals(others_b_obj.getKey())) {
                        for (String landMark_b : b_obj.getValue().landMarks) {
                            for (String landMark_other : others_b_obj.getValue().landMarks) {

                                Node source = graphDB.getNodeById(Long.parseLong(landMark_b) - 1);
                                Node destination = graphDB.getNodeById(Long.parseLong(landMark_other) - 1);

                                if (fakePath == null) {
                                    fakePath = new path(source);
                                }

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

                                Pair<String, String> landmark_pair = new Pair(landMark_b, landMark_other);
                                this.outerLandMark.put(landmark_pair, costs);
                            }
                        }
                    }
                }

            }
            buildNodeInforIndex();
            System.out.println(portalList.size() + "   " + nodeToBlockId.size());
            System.out.print("Build outer landmark " + (System.currentTimeMillis() - run_ms3) + " ms \n");

//            tx.success();
        }
        n.shutdownDB();

    }

    private int getRandomNumberInRange(int min, int max) {

        if (min >= max) {
            throw new IllegalArgumentException("max must be greater than min");
        }

        Random r = new Random();
        return r.nextInt((max - min) + 1) + min;
    }


    public block getPid(String nid) {
        for (Map.Entry<String, block> b_Obj : this.blocks.entrySet()) {
            String pid = b_Obj.getKey();
            block b = b_Obj.getValue();

            for (String vid : b.nodes) {
                if (nid.equals(vid)) {
                    if (b.oportals.contains(nid) || b.iportals.contains(nid)) {
                        System.out.println(nid + "is a portal node");
                    } else {
                        return b;
                    }
                }
            }
        }
        return null;
    }

    public boolean isPortals(String nid) {
        for (Map.Entry<String, block> b_Obj : this.blocks.entrySet()) {
            String pid = b_Obj.getKey();
            block b = b_Obj.getValue();

            for (String vid : b.nodes) {
                if (nid.equals(vid)) {
                    if (b.oportals.contains(nid) || b.iportals.contains(nid)) {
                        return true;
                    }
                }
            }
        }
        return false;

    }

    /**
     * Put the node_id -> block_id information to nodeToBlockId Structure.
     */
    public void buildNodeInforIndex() {
        for (Map.Entry<String, block> b_obj : this.blocks.entrySet()) {
            String block_id = b_obj.getKey();
            block b = b_obj.getValue();
            for (String node_id : b.nodes) {
                //non-portal node
                if (!b.iportals.contains(node_id) && !b.oportals.contains(node_id)) {
                    this.nodeToBlockId.put(node_id, block_id);
                } else {
                    HashMap<String, HashSet<String>> portalList_obj = this.portalList.get(node_id);
                    if (portalList_obj == null) {
                        portalList_obj = new HashMap<>();
                    }

                    if (b.iportals.contains(node_id)) {
                        HashSet<String> inList = portalList_obj.get("in");
                        if (inList == null) {
                            inList = new HashSet<>();
                        }
                        inList.add(block_id);
                        portalList_obj.put("in", inList);
                    }


                    if (b.oportals.contains(node_id)) {
                        HashSet<String> outList = portalList_obj.get("out");
                        if (outList == null) {
                            outList = new HashSet<>();
                        }
                        outList.add(block_id);
                        portalList_obj.put("out", outList);
                    }

                    this.portalList.put(node_id, portalList_obj);
                }
            }
        }

//        checkingNodeList();
        System.out.println("built the node and portal list mapping list");
    }

    private void checkingNodeList() {
        for (int i = 1; i <= 2000; i++) {
            String nid = String.valueOf(i);
            String normal_node = this.nodeToBlockId.get(nid);
            HashMap<String, HashSet<String>> adjList = this.portalList.get(nid);

            if (normal_node != null) {
                System.out.println(i + ",normal," + normal_node);
            }

            if (adjList != null) {
                System.out.println(i + ",portal");
                HashSet<String> inList = adjList.get("in");
                if (inList != null) {
                    System.out.print("  in,");
                    for (String n : inList) {
                        System.out.print("," + n);
                    }
                    System.out.print("\n");
                }
                HashSet<String> outList = adjList.get("out");
                if (outList != null) {
                    System.out.print("  out,");
                    for (String n : outList) {
                        System.out.print("," + n);
                    }
                    System.out.println();
                }
            }

            if(normal_node==null&&adjList==null)
            {
                System.out.println(i+",none");
            }

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
}