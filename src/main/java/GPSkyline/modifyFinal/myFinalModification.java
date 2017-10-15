package GPSkyline.modifyFinal;


import GraphPartition.myNode;
import GraphPartition.myNodePriorityQueue;
import GraphPartition.path;
import org.neo4j.graphdb.*;

import java.util.ArrayList;
import java.util.HashMap;

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
    public HashMap<String, myNode> getSkylinePath(Node source, Node destination) {
        try (Transaction tx = this.graphdb.beginTx()) {
            path iniPath = new path(source, source);
            this.NumberOfProperties = iniPath.NumberOfProperties;
            this.propertiesName.addAll(iniPath.propertiesName);
            //initialize the lower bound
//            for (String p_type : iniPath.getPropertiesName()) {
//                System.out.println(p_type);
//                myDijkstra(source, destination, p_type);
//            }

//            System.out.println()

            myNode start = this.ProcessedNodes.get(String.valueOf(source.getId()));
            //can not reach by back-wards dijkstra from destination to source
            if (start == null) {
                start = new myNode(source, source, true);
                this.ProcessedNodes.put(start.id,start);
            }
            start.addToSkylineResult(iniPath);
            mqueue.add(start);
            start.inqueue = true;
            tx.success();
        }

        try (Transaction tx = this.graphdb.beginTx()) {
            while (!mqueue.isEmpty()) {
                myNode vs = mqueue.pop();
                vs.inqueue = false;

                int index = 0;
                for (; index < vs.subRouteSkyline.size(); ) {
                    path p = vs.subRouteSkyline.get(index);
                    if (!p.processed_flag) {
                        p.processed_flag = true;
                        ArrayList<path> paths = p.expand();
                        for (path np : paths) {
                            boolean isCycle = np.isCycle();
                            if (!isCycle) {
                                String nextid = String.valueOf(np.endNode.getId());
                                myNode nextNode = this.ProcessedNodes.get(nextid);
                                if (nextNode == null) {
                                    nextNode = new myNode(source,np.endNode,false);
                                    this.ProcessedNodes.put(nextid,nextNode);
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
                        index++;
                    } else {
                        index++;
                    }

                }
            }
            tx.success();
        }

        return this.ProcessedNodes;
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
