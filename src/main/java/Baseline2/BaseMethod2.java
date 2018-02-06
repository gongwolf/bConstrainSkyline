package Baseline2;

import aRtree.Data;
import neo4jTools.connector;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.Transaction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

public class BaseMethod2 {

    Random r;
    String treePath = "/home/gqxwolf/shared_git/bConstrainSkyline/data/test.rtr";
    int graph_size;
    String degree;
    private GraphDatabaseService graphdb;
    private HashMap<Long, myNode> tmpStoreNodes = new HashMap();


    public BaseMethod2(int graph_size, String degree) {
        r = new Random();
        this.graph_size = graph_size;
        this.degree = degree;
    }


    public static void main(String args[]) {
        int graph_size = 1000;
        String degree = "5";
        int query_num = 1;

        if (args.length == 3) {
            graph_size = Integer.parseInt(args[0]);
            degree = args[1];
            query_num = Integer.parseInt(args[2]);
        }

        for (int i = 0; i < query_num; i++) {
            BaseMethod2 bm = new BaseMethod2(graph_size, degree);
//            Data queryD = bm.generateQueryData();
////
//            System.out.println(queryD);

            Data queryD = new Data(2);
            queryD.setPlaceId(9999999);
            queryD.setData(new float[]{20.380422592163086f, 9.294476509094238f});
            queryD.setAttrs(new float[]{4.3136826f, 0.45063168f, 3.711781f});
//            constants.print(queryD.location);
//            constants.print(queryD.getData());

            bm.baseline(queryD);
            System.out.println("-----");
        }
    }

    private void baseline(Data queryD) {

        String graphPath = "/home/gqxwolf/neo4j323/testdb" + this.graph_size + "_" + this.degree + "/databases/graph.db";
        long db_time = System.currentTimeMillis();
        connector n = new connector(graphPath);
        n.startDB();
        this.graphdb = n.getDBObject();
        try (Transaction tx = this.graphdb.beginTx()) {

            long r1 = System.currentTimeMillis();
            Node startNode = nearestNetworkNode(queryD);
            long nn_rt = System.currentTimeMillis() - r1;
            System.out.println("Find nearest road network " + nn_rt + " ms");
            System.out.println(startNode.getProperty("lat") + " " + startNode.getProperty("log"));

            myNode s = new myNode(queryD, startNode, this.graphdb);

            myNodePriorityQueue mqueue = new myNodePriorityQueue();
            mqueue.add(s);

            this.tmpStoreNodes.put(s.id, s);


            while (!mqueue.isEmpty()) {
                myNode v = mqueue.pop();
                for (int i = 0; i < v.skyPaths.size(); i++) {
                    path p = v.skyPaths.get(i);
                    if (!p.expaned) {
//                        System.out.println(counter + "  ......  ");
                        p.expaned = true;


//                        addToSkylineResult(p, queryD);


                        ArrayList<path> new_paths = p.expand();

                        for (path np : new_paths) {
                            myNode next_n;
                            if (this.tmpStoreNodes.containsKey(np.endNode.getId())) {
                                next_n = tmpStoreNodes.get(np.endNode.getId());
                            } else {
                                next_n = new myNode(queryD, np.endNode, this.graphdb);
                                this.tmpStoreNodes.put(next_n.id, next_n);
                            }

                            //lemma 2
                            if (!(this.tmpStoreNodes.get(np.startNode.getId()).distance_q > next_n.distance_q)) {
                                if (next_n.addToSkyline(np)) {
                                    mqueue.add(next_n);
                                }
                            }
                        }
                    }

                }

            }


            tx.success();
        }
        n.shutdownDB();
    }


    public Node nearestNetworkNode(Data queryD) {

        Node nn_node = null;
        double distz = Float.MAX_VALUE;

        ResourceIterable<Node> iter = this.graphdb.getAllNodes();
        for (Node n : iter) {
            double lat = (double) n.getProperty("lat");
            double log = (double) n.getProperty("log");

            float[] q_l = queryD.getData();

            double temp_distz = (Math.pow(lat - q_l[0], 2) + Math.pow(log - q_l[1], 2));
            if (distz > temp_distz) {
                nn_node = n;
                distz = temp_distz;

            }
        }
        return nn_node;
    }
}
