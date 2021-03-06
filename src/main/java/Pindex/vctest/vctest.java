package Pindex.vctest;

import javafx.util.Pair;
import neo4jTools.BNode;
import neo4jTools.Line;
import neo4jTools.connector;
import org.apache.shiro.crypto.hash.Hash;
import org.neo4j.graphdb.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;

public class vctest {

    String basePath = "/home/gqxwolf/mydata/projectData/testGraph/data/";
    String nodesPath = basePath + "NodeInfo.txt";
    String edgesPath = basePath + "SegInfo.txt";

    public vctest(String basepath) {
        this.basePath = basepath;
    }

    public vctest() {

    }


    public static void main(String args[]) {
        connector n = new connector("/home/gqxwolf/neo4j323/testdb/databases/graph.db");
        n.startDB();
        GraphDatabaseService graphdb = n.getDBObject();
        vctest vt = new vctest();
        VCNode root = vt.buildVCTree(40, graphdb);
        String sid = "7";
        String did = "64";
        vt.CreateLable(root, graphdb, sid, did);

        ArrayList<myPath> s = vt.getSkyline("7", "64", graphdb, root);
        for (myPath p : s) {
            System.out.println(p + " " + p.printCosts());
        }
        n.shutdownDB();
    }

    private void CreateLable(VCNode root, GraphDatabaseService graphdb, String sid, String did) {
        HashMap<String, HashMap<String, LinkedList<myPath>>> labelings = new HashMap<>();
        getOutLebel(root, graphdb, sid);
        getInLbel(root,graphdb,did);
    }

    private void getInLbel(VCNode root, GraphDatabaseService graphdb, String did) {
        HashMap<String, LinkedList<myPath>> labelInfos = new HashMap<>();
        try (Transaction tx = graphdb.beginTx()) {

            Node dest = root.graphdb.findNode(BNode.BusNode, "name", did);


//            initial the node self.
//            LinkedList<myPath> initlist = new LinkedList<>();
//            initlist.add(new myPath(source));
//            labelInfos.put(sid, initlist);
            myNode dnode = new myNode(dest, true);

            HashMap<String, myNode> ProcessedNode = new HashMap<>();

            Queue<myNode> unmarked = new LinkedList<>();
            unmarked.add(dnode);


            int i = 0;
            while (!unmarked.isEmpty()) {
                myNode node = unmarked.remove();
                ProcessedNode.put(node.id, node);

//                if (i == 50) {
//                    break;
//                }

                VCNode tmpNode = getLowestNode(root, node.node);
                ArrayList<DistanceEdge> expand_nodes;
                if (tmpNode == null) {
                    expand_nodes = root.getNodesFromGraph(node.node,"In");
                } else {
//                    System.out.println(tmpNode.level);
                    expand_nodes = tmpNode.getNodesFromDisGraph(node.node,"In");
                }

                if (expand_nodes.size() != 0) {
                    for (DistanceEdge nextDe : expand_nodes) {
                        String nextId = String.valueOf(nextDe.endNode.getId());
                        myNode nextNode;
                        if (ProcessedNode.containsKey(nextId)) {
                            nextNode = ProcessedNode.get(nextId);
                        } else {
                            nextNode = new myNode(nextDe.endNode, false);
                        }


                        if (tmpNode != null && tmpNode.level >= getLowestNode(root, nextNode.node).level) {
//                            System.out.println(nextDe+" "+getLowestNode(root, nextNode.node).level + " next one");
                            continue;
//                        }else if(tmpNode!=null)
//                        {
//                            System.out.println(nextDe+" "+getLowestNode(root, nextNode.node).level +" add to queue");

                        }


                        for (myPath sp : node.subRouteSkyline) {
                            for (myPath ep : nextDe.paths) {
                                myPath new_path = new myPath(sp, ep);
                                if (!new_path.hasCycle()) {
                                    nextNode.addToSkylineResult(new_path);
                                }
                            }
                        }
                        unmarked.add(nextNode);
                    }
                }
                i++;
            }

            for(Map.Entry<String,myNode> e:ProcessedNode.entrySet())
            {
                System.out.println(e.getKey());
                for(myPath p:e.getValue().subRouteSkyline)
                {
                    System.out.println("        "+p);
                }

            }

            tx.success();
        }
    }

    private void getOutLebel(VCNode root, GraphDatabaseService graphdb, String sid) {
        HashMap<String, LinkedList<myPath>> labelInfos = new HashMap<>();
        try (Transaction tx = graphdb.beginTx()) {

            Node source = root.graphdb.findNode(BNode.BusNode, "name", sid);


//            initial the node self.
//            LinkedList<myPath> initlist = new LinkedList<>();
//            initlist.add(new myPath(source));
//            labelInfos.put(sid, initlist);
            myNode src = new myNode(source, true);

            HashMap<String, myNode> ProcessedNode = new HashMap<>();

            Queue<myNode> unmarked = new LinkedList<>();
            unmarked.add(src);


            int i = 0;
            while (!unmarked.isEmpty()) {
                myNode node = unmarked.remove();
                ProcessedNode.put(node.id, node);

//                if (i == 50) {
//                    break;
//                }

                VCNode tmpNode = getLowestNode(root, node.node);
                ArrayList<DistanceEdge> expand_nodes;
                if (tmpNode == null) {
                    expand_nodes = root.getNodesFromGraph(node.node,"Out");
                } else {
//                    System.out.println(tmpNode.level);
                    expand_nodes = tmpNode.getNodesFromDisGraph(node.node,"Out");
                }


                //Todo, create path with incoming edges
                if (expand_nodes.size() != 0) {
                    for (DistanceEdge nextDe : expand_nodes) {
                        String nextId = String.valueOf(nextDe.endNode.getId());
                        myNode nextNode;
                        if (ProcessedNode.containsKey(nextId)) {
                            nextNode = ProcessedNode.get(nextId);
                        } else {
                            nextNode = new myNode(nextDe.endNode, false);
                        }


                        if (tmpNode != null && tmpNode.level >= getLowestNode(root, nextNode.node).level) {
//                            System.out.println(nextDe+" "+getLowestNode(root, nextNode.node).level + " next one");
                            continue;
//                        }else if(tmpNode!=null)
//                        {
//                            System.out.println(nextDe+" "+getLowestNode(root, nextNode.node).level +" add to queue");

                        }


                        for (myPath sp : node.subRouteSkyline) {
                            for (myPath ep : nextDe.paths) {
                                myPath new_path = new myPath(sp, ep);
                                if (!new_path.hasCycle()) {
                                    nextNode.addToSkylineResult(new_path);
                                }
                            }
                        }
                        unmarked.add(nextNode);
                    }
                }
                i++;
            }

            for(Map.Entry<String,myNode> e:ProcessedNode.entrySet())
            {
                System.out.println(e.getKey());
                for(myPath p:e.getValue().subRouteSkyline)
                {
                    System.out.println("        "+p);
                }

            }

            tx.success();
        }

    }

    private void getTheExpansionToNextLevel(Pair<myNode, Boolean> nodePair, VCNode tmpNode) {
        myNode node = nodePair.getKey();
        VCNode nextVCNode;
        if (nodePair.getValue()) {
            nextVCNode = tmpNode;
            for (Relationship rel : tmpNode.G.getOutGoingRels(node.node)) {
                System.out.println(rel + "  " + tmpNode.dg.nodes.contains(rel.getEndNode()));
            }
        } else {
            for (DistanceEdge de : tmpNode.dg.getOutGoingRels(node.node)) {
                for (myPath p : de.paths) {

                }
            }
        }

    }

    private VCNode findTheHighestLevelContains(Node source, VCNode root) {
        boolean flag = false;
        VCNode tmpNode = root;
        if (root.nodesLeftInRoot.contains(source)) {
            return tmpNode;
        } else {
            while (!tmpNode.nodeInThisLevel.contains(source)) {
                if (tmpNode.children != null) {
                    tmpNode = tmpNode.children.get(0);
                } else {
                    tmpNode = null;
                    break;
                }

            }
        }


        return tmpNode;


    }

    public ArrayList<myPath> getSkyline(String src, String dest, GraphDatabaseService graphdb, VCNode root) {
        ArrayList<myPath> skyline = new ArrayList<>();
        try (Transaction tx = graphdb.beginTx()) {
            String sid = src;
            String did = dest;
            VCNode src_lowestNode = getLowestNode(root, sid);
            VCNode dest_lowestNode = getLowestNode(root, did);

            HashSet<Node> src_ns_dest = null;

            if (src_lowestNode != null) {
//                System.out.println("src is vc node, src is in the " + src_lowestNode.level + " level");
            } else {
                src_ns_dest = getOutGoingNeighbor(graphdb, sid);
//                System.out.println("src not a vc not");
//                System.out.println("find " + src_ns_dest.size() + " neighbor nodes");

            }

//            System.out.println("-----------------------------");

            HashSet<Node> dest_ns_dest = null;
            if (dest_lowestNode != null) {
//                System.out.println("dest is vc node, dest is in the " + dest_lowestNode.level + " level");
//                System.out.println(dest_lowestNode.level);
            } else {
//                System.out.println("dest is not a vc not");
                dest_ns_dest = getInComingNeighbor(graphdb, did);
//                System.out.println("find " + dest_ns_dest.size() + " neighbor nodes");
            }
////
////            for(int j=0;j<20;j++)
////            {
            if (src_lowestNode == null && dest_lowestNode == null) {
                ArrayList<myPath> tSkyline = new ArrayList<>();
                for (Node snode : src_ns_dest) {
                    ArrayList<myPath> tmpSkyline = Query(root, dest_ns_dest, String.valueOf(snode.getId()));
                    tSkyline.addAll(tmpSkyline);
                }
//                skyline.addAll(tSkyline);

//                for(myPath mp:tSkyline)
//                {
//                    System.out.println(mp+" "+mp.printCosts());
//                }
//                System.out.println("-------------------------------");

                Node sNode = graphdb.findNode(BNode.BusNode, "name", sid);
                myNode smyNode = new myNode(sNode, false);
                Iterable<Relationship> srels = sNode.getRelationships(Line.Linked, Direction.OUTGOING);
                Iterator<Relationship> srels_iter = srels.iterator();

                while (srels_iter.hasNext()) {
                    myPath sp = new myPath(srels_iter.next());
//                    System.out.println(eP + " " + eP.printCosts());
                    for (myPath ep : tSkyline) {
                        if (sp.endNode.getId() == ep.startNode.getId()) {
//                            System.out.println(sp + " " + sp.printCosts());
                            myPath final_p = new myPath(sp, ep);
//                            System.out.println(final_p+" "+final_p.printCosts());

                            skyline.add(final_p);
                        }
                    }
                }

//                System.out.println("-------------------------------");


                Node dNode = graphdb.findNode(BNode.BusNode, "name", did);
                myNode dmyNode = new myNode(dNode, false);
                Iterable<Relationship> drels = dNode.getRelationships(Line.Linked, Direction.INCOMING);
                Iterator<Relationship> drels_iter = drels.iterator();
                while (drels_iter.hasNext()) {
                    myPath eP = new myPath(drels_iter.next());
//                    System.out.println(eP + " " + eP.printCosts());
                    for (myPath sp : skyline) {
                        if (sp.endNode.getId() == eP.startNode.getId()) {
//                            System.out.println(sp + " " + sp.printCosts());
                            myPath final_p = new myPath(sp, eP);
//                            System.out.println(final_p+" "+final_p.printCosts());
                            dmyNode.addToSkylineResult(final_p);
                        }
                    }
                }

//                System.out.println("-------------------------------");


//                for (myPath p : dmyNode.subRouteSkyline) {
//                    System.out.println(p + " " + p.printCosts());
//                }

                skyline.clear();
                skyline.addAll(dmyNode.subRouteSkyline);

            } else if (src_lowestNode != null && dest_lowestNode == null) {
                ArrayList<myPath> tmpSkyline = Query(root, dest_ns_dest, sid);

                skyline.addAll(tmpSkyline);
                Node dNode = graphdb.findNode(BNode.BusNode, "name", did);
                myNode dmyNode = new myNode(dNode, false);
                Iterable<Relationship> rels = dNode.getRelationships(Line.Linked, Direction.INCOMING);
                Iterator<Relationship> rels_iter = rels.iterator();
                while (rels_iter.hasNext()) {
                    myPath eP = new myPath(rels_iter.next());
//                    System.out.println(eP + " " + eP.printCosts());
                    for (myPath sp : skyline) {
                        if (sp.endNode.getId() == eP.startNode.getId()) {
//                            System.out.println(sp + " " + sp.printCosts());
                            myPath final_p = new myPath(sp, eP);
                            dmyNode.addToSkylineResult(final_p);
                        }
                    }
                }

//                for (myPath p : dmyNode.subRouteSkyline) {
//                    System.out.println(p + " " + p.printCosts());
//                }
                skyline.clear();
                skyline.addAll(dmyNode.subRouteSkyline);
            } else if (src_lowestNode != null && dest_lowestNode != null) {
                HashMap<String, myNode> ProcessedNode = new HashMap<>();
//                long run3 = System.nanoTime();
                ArrayList<myPath> tmpSkyline = Skyline_Buttom_to_Top(sid, did, graphdb, ProcessedNode, root);
//                System.out.println("Query in " + (System.nanoTime() - run3) / 1000000 + "ms ");

                skyline.addAll(tmpSkyline);

//                for (myPath p : skyline) {
//                    System.out.println(p + " " + p.printCosts());
//                }
//                skyline.clear();
//                skyline.addAll(skyline);
            } else {
                ArrayList<myPath> tSkyline = new ArrayList<>();
                for (Node snode : src_ns_dest) {
                    HashMap<String, myNode> ProcessedNode = new HashMap<>();
//                    System.out.println(getLowestNode(root,String.valueOf(snode.getId()))+"  "+snode.getId()+" "+root.dg.nodes.contains(snode));
                    ArrayList<myPath> tmpSkyline = Skyline_Buttom_to_Top(String.valueOf(snode.getId()), did, graphdb, ProcessedNode, root);
//                    System.out.println("   "+tmpSkyline.size());
                    tSkyline.addAll(tmpSkyline);
                }
                skyline.addAll(tSkyline);

                Node sNode = graphdb.findNode(BNode.BusNode, "name", sid);
                myNode smyNode = new myNode(sNode, false);
                Iterable<Relationship> srels = sNode.getRelationships(Line.Linked, Direction.OUTGOING);
                Iterator<Relationship> srels_iter = srels.iterator();
                while (srels_iter.hasNext()) {
                    myPath sp = new myPath(srels_iter.next());
//                    System.out.println(eP + " " + eP.printCosts());
                    for (myPath ep : skyline) {
                        if (sp.endNode.getId() == ep.startNode.getId()) {
//                            System.out.println(sp + " " + sp.printCosts());
                            myPath final_p = new myPath(sp, ep);
                            smyNode.addToSkylineResult(final_p);
                        }
                    }
                }

//                for (myPath p : smyNode.subRouteSkyline) {
//                    System.out.println(p + " " + p.printCosts());
//                }

                skyline.clear();
                skyline.addAll(smyNode.subRouteSkyline);

            }
            tx.success();
        }

        return skyline;

    }

    public VCNode buildVCTree(int threshold, GraphDatabaseService graphdb) {
        VCNode t_root;

        try (Transaction tx = graphdb.beginTx()) {

            ArrayList<Node> nodes = getNodes(graphdb);
            ArrayList<Relationship> edges = getEgdes(graphdb);
//            System.out.println("There are " + nodes.size() + " nodes,\nThere are " + edges.size() + " Edges.");

//            long building = System.nanoTime();
            t_root = new VCNode(graphdb);
            Graph oGraph = new Graph(nodes, edges);
//            System.out.println("1111111");
            t_root.buildDistanceGraph(oGraph, threshold);
//            Node ssss = graphdb.findNode(BNode.BusNode, "name", "235");
//            Node sssss = graphdb.findNode(BNode.BusNode, "name", "97");
//
//            for (DistanceEdge de : root.dg.edges) {
//                if (de.startNode.getId() == 235) {
//                    System.out.println(de);
//                }
//            }
//
//            System.out.println(root.dg.nodes.contains(ssss));
//            System.out.println(root.dg.nodes.contains(sssss));
//
//            for (Relationship rel : oGraph.getOutGoingRels(ssss)) {
//                System.out.println(rel);
//
//            }
//
//            for (Relationship rel : oGraph.getOutGoingRels(sssss)) {
//                System.out.println(rel);
//
//            }
//
//            for (DistanceEdge rel : root.dg.getOutGoingRels(ssss)) {
//                System.out.println(rel);
//            }


//            for(DistanceEdge de:root.dg.edges)
//            {
//                System.out.println(printCosts(de.paths.get(0).getCosts()));
//
//            }


//            System.out.println("Index building success in " + (System.nanoTime() - building) / 1000000 + "ms ");
//            System.out.println("Total depth is " + getHigh(root));
//            long traveralTime = System.nanoTime();
//            retrivalTree(t_root);
//            System.out.println("Traveral Used " + (System.nanoTime() - traveralTime) / 1000000 + "ms ");


//            }
            tx.success();
        }

        return t_root;

    }

    private int getHigh(VCNode root) {
        ArrayList<VCNode> visited = new ArrayList<>();
        int maxlevel = Integer.MIN_VALUE;

        Stack<VCNode> q = new Stack<>();
        q.add(root);
        while (!q.isEmpty()) {
            VCNode node = q.pop();
            visited.add(node);
            String tabStr = "    ";
            if (node.level > maxlevel) {
                maxlevel = node.level;
            }

            if (node.children != null) {
                for (VCNode vc : node.children) {
                    if (!visited.contains(vc)) {
                        q.add(vc);
                    }
                }
            }

        }
        return maxlevel;
    }

    private ArrayList<myPath> Query(VCNode root, HashSet<Node> ns_dest, String sid) {
        GraphDatabaseService graphdb = root.graphdb;
//        VCNode tmpNode = src_lowestNode;
        HashMap<String, myNode> ProcessedNode = new HashMap<>();
//        System.out.println("find the lowest node " + sid + " at the VC node at the level " + src_lowestNode.level + " contains " + tmpNode.dg.numberOfNodes() + " nodes and " + tmpNode.dg.numberOfEdges() + " edges");

//        long run2 = System.nanoTime();
//        Skyline_Query_leaf(src_lowestNode, sid, root.graphdb, ProcessedNode);
//        System.out.println("run2: " + (System.nanoTime() - run2) / 1000000 + " ms");

//        Node s = root.graphdb.findNode(BNode.BusNode, "name", sid);
//        long run1 = System.nanoTime();
//        VCNode c_node = findCommonVCNodes(s, ns_dest);
//        System.out.println("Find all: " + (System.nanoTime() - run1) / 1000000 + " ms");
//
//        if (c_node != null) {
//            ProcessedNode.clear();
//            Skyline_Query_leaf(c_node, sid, root.graphdb, ProcessedNode);
//        }

        ArrayList<myPath> result = new ArrayList<>();
        for (Node nn : ns_dest) {
            String did = String.valueOf(nn.getId());
//            ProcessedNode.clear();
//            long run1 = System.nanoTime();
//            VCNode nn_ndoe = findCommonVCNodes(s, nn);
//            Skyline_Query_leaf(nn_ndoe, sid, did, graphdb, ProcessedNode);
//            System.out.println("Find one: " + (System.nanoTime() - run1) / 1000000 + " ms");
////            System.out.println(ProcessedNode.get(did));
//            if (ProcessedNode.containsKey(did)) {
//                for (myPath p : ProcessedNode.get(did).subRouteSkyline) {
//                    System.out.println(p);
//                }
//                System.out.println("        ======      ");
//            }


//            ProcessedNode.clear();
//            long run1 = System.nanoTime();
            result.addAll(Skyline_Buttom_to_Top(sid, did, graphdb, ProcessedNode, root));
//            System.out.println("Skyline_Buttom_to_Top: " + (System.nanoTime() - run1) / 1000000 + "ms");
//            System.out.println("--------------------------------------");
        }

        return result;
    }

    private ArrayList<myPath> Skyline_Buttom_to_Top(String sid, String did, GraphDatabaseService graphdb, HashMap<String, myNode> processedNode, VCNode root) {
//        System.out.println("start:----Skyline Buttom to Top:" + processedNode.size());
        VCNode src_lowestNode = getLowestNode(root, sid);
        VCNode temp_node = src_lowestNode;
        Skyline_Query_leaf(src_lowestNode, sid, graphdb, processedNode);


        while (!temp_node.isRoot) {
            temp_node = expand_in_parent(temp_node, processedNode);
        }


//        System.out.println("end:---- Skyline Buttom to Top:");
//        if (processedNode.containsKey(did)) {
//            System.out.println("    ---- Find skyline from " + sid + " to " + did + " that contains " + processedNode.get(did).subRouteSkyline.size());
//            for (myPath p : processedNode.get(did).subRouteSkyline) {
//                System.out.println("    ----:" + p + "  " + p.printCosts());
//            }
//        }

        if (processedNode.containsKey(did)) {
            return processedNode.get(did).subRouteSkyline;
        } else {
            return new ArrayList<myPath>();
        }
    }

//    private VCNode findCommonVCNodes(Node s, Node nn) {
//        ArrayList<VCNode> visited = new ArrayList<>();
//        int maxlevel = Integer.MIN_VALUE;
//        VCNode tmpNode = null;
//
//        Stack<VCNode> q = new Stack<>();
//        q.add(root);
//        while (!q.isEmpty()) {
//            VCNode node = q.pop();
//            String tabStr = "    ";
//
//            visited.add(node);
//            if (maxlevel < node.level && node.dg.nodes.contains(s)) {
//                if (node.dg.nodes.contains(nn)) {
//                    maxlevel = node.level;
//                    tmpNode = node;
//                }
//
//            }
//
//            if (node.children != null) {
//                for (VCNode vc : node.children) {
//                    if (!visited.contains(vc)) {
//                        q.add(vc);
//                    }
//                }
//            }
//        }
//        System.out.println("Found the node " + s.getId() + " and the node " + nn.getId() +
//                " in the level " + maxlevel + " contains " + tmpNode.dg.numberOfNodes() + " nodes and " + tmpNode.dg.numberOfEdges() + " edges");
//        return tmpNode;
//    }

//    private VCNode findCommonVCNodes(Node s, HashSet<Node> ns_dest) {
//        ArrayList<VCNode> visited = new ArrayList<>();
//        int maxlevel = Integer.MIN_VALUE;
//        VCNode tmpNode = null;
//
//        Stack<VCNode> q = new Stack<>();
//        q.add(root);
//        while (!q.isEmpty()) {
//            VCNode node = q.pop();
//            String tabStr = "    ";
//
//            visited.add(node);
//            if (maxlevel < node.level && node.dg.nodes.contains(s)) {
//                boolean f = true;
//                for (Node n : ns_dest) {
//                    if (!node.dg.nodes.contains(n)) {
//                        f = false;
//                        break;
//                    }
//                }
//                if (f) {
//                    maxlevel = node.level;
//                    tmpNode = node;
//                }
//
//            }
//
//            if (node.children != null) {
//                for (VCNode vc : node.children) {
//                    if (!visited.contains(vc)) {
//                        q.add(vc);
//                    }
//                }
//            }
//        }
//        System.out.println("all in the level " + maxlevel + " contains " + tmpNode.dg.numberOfNodes() + " nodes and " + tmpNode.dg.numberOfEdges() + " edges");
//
//        return tmpNode;
//    }

    private VCNode expand_in_parent(VCNode tmpNode, HashMap<String, myNode> processedNode) {
        VCNode parentNode = tmpNode.parent;

        for (Node n : parentNode.dg.nodes) {
            //if n is not processed.
            if (!tmpNode.dg.nodes.contains(n)) {
                //create a new myNode
                myNode new_node = new myNode(n, false);

//            System.out.println(new_node+" is null");
                ArrayList<myPath> paths = parentNode.dg.getIncomingEdges(n);
                for (myPath ep : paths) {
//                    System.out.println(ep);
                    try {
                        myNode startNode = processedNode.get(String.valueOf(ep.startNode.getId()));
                        if (startNode != null) {
                            for (myPath sp : startNode.subRouteSkyline) {
                                myPath new_p = new myPath(sp, ep);
                                new_node.addToSkylineResult(new_p);
                            }
                        }
                    } catch (Exception e) {
                        System.out.println("==========================");

                        for (Map.Entry<String, myNode> mn : processedNode.entrySet()) {
                            System.out.println(mn.getKey() + "   " + mn.getValue().subRouteSkyline.size());

                        }


                        System.out.println(n + "  " + ep);

                        for (Node nindg : tmpNode.dg.nodes) {
                            System.out.println(nindg);
                        }

                        for (DistanceEdge nindg : tmpNode.dg.edges) {
                            System.out.println(nindg);
                        }
                        System.exit(0);
                        System.out.println("==========================");
                    }
                }
                processedNode.put(new_node.id, new_node);
            }
        }


        return parentNode;
    }

    private void Skyline_Query_leaf(VCNode src_lowestNode, String sid, GraphDatabaseService graphdb, HashMap<String, myNode> ProcessedNode) {
//        System.out.println("Begin to query!!");
        myNodePriorityQueue mqueue = new myNodePriorityQueue();
        Node s = graphdb.findNode(BNode.BusNode, "name", sid);
        myNode source = new myNode(s, true);
        mqueue.add(source);
        source.inqueue = true;
        ProcessedNode.put(source.id, source);


        int i = 0;

        long runtime = System.nanoTime();
        while (!mqueue.isEmpty()) {
            myNode n = mqueue.pop();
//            System.out.println("pop out:"+n.id);
            ProcessedNode.put(n.id, n);
            n.inqueue = false;
            int index = 0;
//            if(i==10)
//            {
//                break;
//            }
//            i++;
            for (; index < n.subRouteSkyline.size(); ) {
                myPath p = n.subRouteSkyline.get(index);
//                System.out.println("    find skyline path in "+n.id+" "+p+" "+printCosts(p.getCosts()));

                if (!p.processed_flag) {
                    p.processed_flag = true;
                    ArrayList<myPath> paths = new ArrayList<>();
                    try {
                        paths = src_lowestNode.expand_in_dg(p);
                    } catch (Exception e) {
//                        System.out.println(src_lowestNode.expand_in_dg(p)==null);
                        System.out.println(src_lowestNode == null);
                    }
//                    System.out.println(src_lowestNode.dg.nodes.contains(p.endNode)+"  "+p.endNode+"  "+paths.size());
//                    for (DistanceEdge nnnn:src_lowestNode.dg.edges)
//                    {
//                        if(nnnn.startNode.getId()==p.endNode.getId())
//                        {
//                            System.out.println(nnnn);
//                        }
//                    }

                    for (myPath np : paths) {
//                        if(np.startNode.getId()==0)
//                        {
//                            System.out.println(np);
//                        }

                        myNode n_node = null;
                        String nextID = String.valueOf(np.endNode.getId());
                        if (ProcessedNode.containsKey(nextID)) {
                            n_node = ProcessedNode.get(nextID);
                        } else {
                            n_node = new myNode(np.endNode, false);
                            ProcessedNode.put(nextID, n_node);
                        }

                        boolean insertflag = n_node.addToSkylineResult(np);
//                        System.out.println("     "+np+" "+np.printCosts()+ "  "+insertflag+" "+n_node.inqueue);

                        if (!n_node.inqueue) {
                            mqueue.add(n_node);
//                            System.out.println("    pop in "+n_node.id);
                            n_node.inqueue = true;
                        }
                    }
                    index++;
                } else {
                    index++;
                }
            }
        }
//        for(Map.Entry<String,myNode> e:ProcessedNode.entrySet())
//        {
//            System.out.println(e.getKey()+"  "+e.getValue());
//
//        }


//        System.out.println("Finished Query in " + (System.nanoTime() - runtime) / 1000000 + " ms,  processNode size is " + ProcessedNode.keySet().size());


    }

    private void Skyline_Query_leaf(VCNode src_lowestNode, String sid, String did, GraphDatabaseService graphdb, HashMap<String, myNode> ProcessedNode) {
        System.out.println("Begin to query");
        myNodePriorityQueue mqueue = new myNodePriorityQueue();
        Node src = graphdb.findNode(BNode.BusNode, "name", sid);
        Node dest = graphdb.findNode(BNode.BusNode, "name", did);
        myNode source = new myNode(src, true);
        mqueue.add(source);
        source.inqueue = true;
        ProcessedNode.put(source.id, source);


        int i = 0;

        long runtime = System.nanoTime();
        while (!mqueue.isEmpty()) {
            myNode n = mqueue.pop();
//            System.out.println("pop out:"+n.id);
            ProcessedNode.put(n.id, n);
            n.inqueue = false;
            int index = 0;
//            if(i==10)
//            {
//                break;
//            }
//            i++;
            for (; index < n.subRouteSkyline.size(); ) {
                myPath p = n.subRouteSkyline.get(index);
//                System.out.println("    find skyline path in "+n.id+" "+p+" "+printCosts(p.getCosts()));

                if (!p.processed_flag) {
                    p.processed_flag = true;
                    ArrayList<myPath> paths = src_lowestNode.expand_in_dg(p);
                    for (myPath np : paths) {
//                        System.out.println("     "+np+" "+np.printCosts());
                        myNode n_node;
                        String nextID = String.valueOf(np.endNode.getId());

                        if (ProcessedNode.containsKey(nextID)) {
                            n_node = ProcessedNode.get(nextID);
                        } else {
                            n_node = new myNode(np.endNode, false);
                            ProcessedNode.put(nextID, n_node);
                        }

                        if (nextID.equals(did)) {
                            n_node.addToSkylineResult(np);
                            continue;
                        }


                        n_node.addToSkylineResult(np);
                        if (!n_node.inqueue) {
                            mqueue.add(n_node);
//                            System.out.println("    pop in "+n_node.id);
                            n_node.inqueue = true;
                        }
                    }
                    index++;
                } else {
                    index++;
                }
            }
        }
        System.out.println("Finished Query in " + (System.nanoTime() - runtime) / 1000000 + " ms");

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

    private HashSet<Node> getInComingNeighbor(GraphDatabaseService graphdb, String id) {
        HashSet<Node> result = new HashSet<>();
        Node node = graphdb.findNode(BNode.BusNode, "name", id);
        Iterable<Relationship> rels = node.getRelationships(Line.Linked, Direction.INCOMING);
        Iterator<Relationship> rels_iter = rels.iterator();
        while (rels_iter.hasNext()) {
            result.add(rels_iter.next().getStartNode());
        }
        return result;
    }

    private HashSet<Node> getOutGoingNeighbor(GraphDatabaseService graphdb, String id) {
        HashSet<Node> result = new HashSet<>();
        Node node = graphdb.findNode(BNode.BusNode, "name", id);
        Iterable<Relationship> rels = node.getRelationships(Line.Linked, Direction.OUTGOING);
        Iterator<Relationship> rels_iter = rels.iterator();
        while (rels_iter.hasNext()) {
            Relationship rel = rels_iter.next();
            result.add(rel.getEndNode());
//            System.out.println(rel);
        }
        return result;
    }

    private VCNode getLowestNode(VCNode root, String sid) {
        Node Source = null;
        int maxlevel = Integer.MIN_VALUE;
        VCNode tmpNode = null;
        ArrayList<VCNode> visited = new ArrayList<>();


        try (Transaction tx = root.graphdb.beginTx()) {
            Source = root.graphdb.findNode(BNode.BusNode, "name", sid);
            tx.success();
        }

        Stack<VCNode> q = new Stack<>();
        q.add(root);
        while (!q.isEmpty()) {
            VCNode node = q.pop();
            visited.add(node);


            if (node.level > maxlevel && node.dg.nodes.contains(Source)) {
                maxlevel = node.level;
                tmpNode = node;
            }

            if (node.children != null && node.children.size() != 0) {
                for (VCNode vc : node.children) {
                    if (vc.dg.nodes.contains(Source) && !visited.contains(vc)) {
                        q.add(vc);
                    }
                }
            }
        }

        return tmpNode;
    }

    private VCNode getLowestNode(VCNode root, Node Source) {
        int maxlevel = Integer.MIN_VALUE;
        VCNode tmpNode = null;
        ArrayList<VCNode> visited = new ArrayList<>();


        Stack<VCNode> q = new Stack<>();
        q.add(root);
        while (!q.isEmpty()) {
            VCNode node = q.pop();
            String tabStr = "    ";
            visited.add(node);
            if (node.level > maxlevel && node.dg.nodes.contains(Source)) {
                maxlevel = node.level;
                tmpNode = node;
            }

            if (node.children != null) {
                for (VCNode vc : node.children) {
                    if (vc.dg.nodes.contains(Source)) {
                        maxlevel = vc.level;
                        tmpNode = vc;
                        if (!visited.contains(vc)) {
                            q.add(vc);
                        }
                    }
                }
            }

        }
        return tmpNode;
    }

    private void retrivalTree(VCNode root) {
        HashMap<Integer, Long> vcnodeInLevelCounts = new HashMap<>();
        HashMap<Integer, Long> numberOfNodesCounts = new HashMap<>();
        HashMap<Integer, Long> numberofEdgesCounts = new HashMap<>();
        HashMap<Integer, Long> SkylineCounts = new HashMap<>();
        HashMap<Integer, Long> nodeinLevel = new HashMap<>();

        Stack<VCNode> q = new Stack<>();
        q.add(root);
        ArrayList<VCNode> visited = new ArrayList<>();

        while (!q.isEmpty()) {

            VCNode node = q.pop();


//            String tabStr = "\t";
//            System.out.println(new String(new char[node.level]).replace("\0", tabStr) + " " + node.level);
//            for (Node n : node.dg.nodes) {
//                System.out.println(new String(new char[node.level]).replace("\0", tabStr) + " " + n);
//            }
//            for (DistanceEdge de : node.dg.edges) {
//                System.out.println(new String(new char[node.level]).replace("\0", tabStr) + " " + de);
//                for (myPath mp : de.paths) {
//                    System.out.println(new String(new char[node.level]).replace("\0", tabStr) + "       " + mp + " " + mp.printCosts());
//
//                }
//            }


            if (vcnodeInLevelCounts.containsKey(node.level)) {
                long num = vcnodeInLevelCounts.get(node.level);
                vcnodeInLevelCounts.put(node.level, num + 1);
            } else {
                vcnodeInLevelCounts.put(node.level, 1L);
            }

            if (numberOfNodesCounts.containsKey(node.level)) {
                long num = numberOfNodesCounts.get(node.level);
                numberOfNodesCounts.put(node.level, num + node.dg.numberOfNodes());
            } else {
                numberOfNodesCounts.put(node.level, node.dg.numberOfNodes());
            }


            if (numberofEdgesCounts.containsKey(node.level)) {
                long num = numberofEdgesCounts.get(node.level);
                numberofEdgesCounts.put(node.level, num + node.dg.numberOfEdges());
            } else {
                numberofEdgesCounts.put(node.level, node.dg.numberOfEdges());
            }

            if (SkylineCounts.containsKey(node.level)) {
                long num = SkylineCounts.get(node.level);
                SkylineCounts.put(node.level, num + node.dg.totalPaths());
            } else {
                SkylineCounts.put(node.level, node.dg.totalPaths());
            }

            if (node.nodeInThisLevel != null) {
                if (nodeinLevel.containsKey(node.level)) {
                    long num = SkylineCounts.get(node.level);
                    nodeinLevel.put(node.level, num + node.nodeInThisLevel.size());
                } else {
                    nodeinLevel.put(node.level, (long) node.nodeInThisLevel.size());
                }
            } else {
                nodeinLevel.put(node.level, 0L);

            }


            visited.add(node);

            if (node.children != null) {
                for (VCNode vc : node.children) {
                    if (!visited.contains(vc))
                        q.add(vc);
                }
            }
        }

        StringBuffer sb = new StringBuffer();
        long totalNumberVCNodes = 0;
        long totalSotredNodes = 0;
        long totalSotredEdges = 0;
        long totalSotredSkyline = 0;

        long maxLevel = 0;
        Map<Integer, Long> map = new TreeMap<>(vcnodeInLevelCounts);
        for (Map.Entry<Integer, Long> vc : map.entrySet()) {
            int levelid = vc.getKey();
            System.out.println(" there are " + vc.getValue() + " vc nodes in the level " + vc.getKey());
            System.out.println("     there are total " + numberOfNodesCounts.get(levelid) + " nodes (" + numberOfNodesCounts.get(levelid) / vc.getValue() + ") and "
                    + numberofEdgesCounts.get(levelid) + " edges (" + numberofEdgesCounts.get(levelid) / vc.getValue() + ") stored in this level");
            System.out.println("     there are total " + SkylineCounts.get(levelid) + " skyline paths (" + (float) SkylineCounts.get(levelid) / numberofEdgesCounts.get(levelid) + ") stored in this level");
            System.out.println("     there are total " + nodeinLevel.get(levelid) + " nodes (" + (float) nodeinLevel.get(levelid) / numberofEdgesCounts.get(levelid) + ") left in this level");
            totalNumberVCNodes += vc.getValue();
            totalSotredEdges += numberofEdgesCounts.get(levelid);
            totalSotredNodes += numberOfNodesCounts.get(levelid);
            totalSotredSkyline += SkylineCounts.get(levelid);
            if (maxLevel < vc.getKey()) {
                maxLevel = vc.getKey();
            }
        }
        map.clear();
        sb.append("--| There are total ").append(totalNumberVCNodes).append(" VC nodes in the tree.\n");
        sb.append("--| There are total ").append(totalSotredNodes).append(" nodes in the tree.\n");
        sb.append("--| There are total ").append(totalSotredEdges).append(" edges in the tree.\n");
        sb.append("--| There are total ").append(totalSotredSkyline).append(" skyline in the tree.\n");
        System.out.print(sb);

    }

    private ArrayList<Node> getNodes(GraphDatabaseService graphdb) {
        ArrayList<Node> nodes = new ArrayList<>();
        int i = 0;
        try (Transaction tx = graphdb.beginTx();
             BufferedReader br = new BufferedReader(new FileReader(nodesPath))) {
            String line = null;
            while ((line = br.readLine()) != null) {
                i++;
                if (i % 5000 == 0) {
                    System.out.println(i + ".......");
                }
//                System.out.println(line.substring(0,line.indexOf(" ")));
                Node node = graphdb.findNode(BNode.BusNode, "name", line.substring(0, line.indexOf(" ")));
                nodes.add(node);
            }
            tx.success();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return nodes;
    }

    private ArrayList<Relationship> getEgdes(GraphDatabaseService graphdb) {
//        System.out.println(111);
        ArrayList<Relationship> Rels = new ArrayList<>();
        try (Transaction tx = graphdb.beginTx()) {
            ResourceIterable<Relationship> iter_Rels = graphdb.getAllRelationships();
            ResourceIterator<Relationship> iterator = iter_Rels.iterator();
//            System.out.println(iter_Rels.stream().count());
            int i = 0;
            try {
                while (iterator.hasNext()) {
                    i++;
                    Relationship item = iterator.next();
                    Rels.add(item);
                    if (i % 5000 == 0) {
                        System.out.println("-----------" + i);
                    }
                }

            } finally {
                iterator.close();
            }
            tx.success();
        }
        return Rels;

    }

}
