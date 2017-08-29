package Pindex.vctest;

import neo4jTools.Line;
import org.neo4j.graphdb.*;

import java.security.spec.DSAGenParameterSpec;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;

public class VCNode {
    private final Random r;
    VCNode parent = null;
    DistanceGraph dg;
    public GraphDatabaseService graphdb;
    private int threshold;
    public ArrayList<VCNode> children;
    int level;

    public VCNode(GraphDatabaseService graphdb) {
        this.graphdb = graphdb;
        this.r = new Random();

    }


    public void buildDistanceGraph(Graph graph, int threshold) {
        this.threshold = threshold;
        this.level = 0;
        try (Transaction tx = this.graphdb.beginTx()) {
            ArrayList<Node> VCNodes = getVCNodes(graph);
            System.out.println(VCNodes.size());
            ArrayList<DistanceEdge> VCEdges = CreateEdges(VCNodes);
            this.dg = new DistanceGraph(VCNodes, VCEdges);
            System.out.println("dg information " + dg.numberOfEdges() + " edges   " + dg.numberOfNodes() + " nodes");


//            for (DistanceEdge de : this.dg.edges) {
//                try {
//                    System.out.println(de.startNode.getId() + "  " + de.endNode.getId());
//                } catch (Exception e) {
//                    System.out.println("Exception");
//                }
//            }

            if (this.dg.numberOfNodes() > this.threshold) {
                System.out.println("generate subTree");
                System.out.println("there are " + this.dg.edges.size() + " edges in root distance graph");
                System.out.println("there are " + this.dg.nodes.size() + " edges in root distance graph");
                GrowTheTree(this);
            }
            tx.success();
        }


    }

    private void GrowTheTree(VCNode parent) {
//        System.out.println("Growth the tree");
        parent.children = new ArrayList<>();
        ArrayList<Node> S = new ArrayList<>(parent.dg.nodes);

        while (!S.isEmpty()) {
            VCNode child = new VCNode(parent.graphdb);
            child.parent = parent;
            child.level = parent.level + 1;
            child.buildDistanceGraph(parent.dg, parent.threshold, S);
//            if(child.level==3)
//            {
//                break;
//            }

            if (child.dg != null) {
                parent.children.add(child);
//                System.out.println(parent.level + " add children level " + child.level + " : " + child.dg.numberOfNodes() + " " + child.dg.numberOfEdges());
//              System.out.println(child.dg.numberOfNodes());
                if (child.dg.numberOfNodes() > this.threshold) {
                    GrowTheTree(child);
                }
            } else {
//                System.out.println("is null");
                S.clear();
            }

//            System.out.println("--------------");
//            System.out.println("S size:" + S.size());
//            for(Node n:S)
//            {
//                System.out.println("  DS:"+n);
//            }
//            System.out.println("--------------");

        }


    }

    private ArrayList<DistanceEdge> CreateEdges(ArrayList<Node> vcNodes) {
        ArrayList<DistanceEdge> result = new ArrayList<>();

        for (Node ns : vcNodes) {
            // Find neighbor nodes of ns.
//            Iterable<Relationship> rels = ns.getRelationships(Line.Linked, Direction.OUTGOING);
            Iterable<Relationship> rels = ns.getRelationships(Line.Linked, Direction.OUTGOING);
            Iterator<Relationship> rel_Iter = rels.iterator();

//            if (ns.getId() == 305) {
            while (rel_Iter.hasNext()) {

                DistanceEdge de = null;
                Relationship rel = rel_Iter.next();
                Node nextNode = rel.getEndNode();

                //if nextnode is a vc node
                if (vcNodes.contains(nextNode) && nextNode != ns) {
                    de = new DistanceEdge(ns, nextNode, rel);
//                        System.out.println(ns + "-----" + nextNode + "  " + (ns.getId()) + ":" + nextNode.getId() + "->" + (ns.getId() == nextNode.getId()));
                }//if next node is a non-vc-node,jump to next-next node, it should be a vc node.
                else if (!vcNodes.contains(nextNode) && nextNode.getId() != ns.getId()) {
                    Iterable<Relationship> next_rels = nextNode.getRelationships(Line.Linked, Direction.OUTGOING);
                    Iterator<Relationship> next_rel_Iter = next_rels.iterator();

                    while (next_rel_Iter.hasNext()) {
                        Relationship nextRel = next_rel_Iter.next();
                        Node tarNode = nextRel.getEndNode();
                        if (ns.getId() != tarNode.getId()) {
                            de = new DistanceEdge(ns, rel, nextNode, nextRel, tarNode);
//                                System.out.println(ns + "+++++" + tarNode + "  " + (ns.getId()) + ":" + tarNode.getId() + "->" + (ns.getId() == tarNode.getId()));
                        }
                    }
                }

                if (de != null)
                    CreateDisEdges(result, de);
            }
        }
//        }
//        System.out.println("Distance graph has " + result.size() + " Edges");


        return result;
    }

    private void CreateDisEdges(ArrayList<DistanceEdge> result, DistanceEdge de) {
        //if it all real has the edges
        if (result.contains(de)) {
            DistanceEdge tmpde = result.get(result.indexOf(de));
            createCombindDe(tmpde, de);
        } else {
            result.add(de);
        }
    }

    private DistanceEdge createCombindDe(DistanceEdge tmpde, DistanceEdge de) {
        for (myPath p : de.paths) {
            tmpde.addToSkylineResult(p);
        }
        return tmpde;
    }

    private ArrayList<Node> getVCNodes(Graph graph) {
        ArrayList<Relationship> copyRels = new ArrayList<>(graph.edges);
        HashSet<Node> vc_nodes = new HashSet<>();
        try (Transaction tx = graphdb.beginTx()) {
            while (!copyRels.isEmpty()) {
//                System.out.println("-------------");
//                System.out.println(copyRels.size());
                Relationship r = copyRels.get(0);
//                System.out.println(r);
                copyRels.remove(0);

                vc_nodes.add(r.getStartNode());
                vc_nodes.add(r.getEndNode());

                Iterable<Relationship> sRels = r.getStartNode().getRelationships(Line.Linked);
                Iterator<Relationship> sRels_iterator = sRels.iterator();
                removeEdges(copyRels, sRels_iterator);

                Iterable<Relationship> eRels = r.getEndNode().getRelationships(Line.Linked);
                Iterator<Relationship> eRels_iterator = eRels.iterator();
                removeEdges(copyRels, eRels_iterator);
            }
            tx.success();
        }
        return new ArrayList<>(vc_nodes);
    }

    private void removeEdges(ArrayList<Relationship> copyRels, Iterator<Relationship> sRels_iterator) {
        while (sRels_iterator.hasNext()) {
            Relationship r = sRels_iterator.next();
//            System.out.println("----remove:" + r);
            if (copyRels.contains(r)) {
                copyRels.remove(r);
            }
        }
    }

    public void buildDistanceGraph(DistanceGraph graph, int threshold, ArrayList<Node> Sor) {
        ArrayList<Node> S = new ArrayList<>();
        this.threshold = threshold;
        try (Transaction tx = this.graphdb.beginTx()) {

            ArrayList<Node> VCNodes = new ArrayList<>();
            ArrayList<DistanceEdge> VCEdges = new ArrayList<>();

            int trys = 0;
            while (trys != 10) {
                S.clear();
                S.addAll(Sor);
                VCNodes = getVCNodes(graph, S);
                if (VCNodes.size() != this.parent.dg.numberOfNodes()) {
                    break;
                }
                trys++;

            }

            if (trys != 10) {
                VCEdges = CreateEdgesDistance(VCNodes);
//                System.out.println("------------------");
//                System.out.println(VCNodes.size());
//                for (DistanceEdge de : VCEdges) {
//                    System.out.println("---- edges:" + de);
//                }
//                for (Node node : VCNodes) {
//                    System.out.println("---- node :" + node);
//
//                }
                this.dg = new DistanceGraph(VCNodes, VCEdges);
                Sor.clear();
                Sor.addAll(S);
            } else {
//                System.out.println("can not find a sub vc graph");
                this.dg = null;
            }
            tx.success();
        }
    }

    private ArrayList<DistanceEdge> CreateEdgesDistance(ArrayList<Node> vcNodes) {
        ArrayList<DistanceEdge> result = new ArrayList<>();
        for (Node n : vcNodes) {
            ArrayList<DistanceEdge> outging_edges = getOutGoingEdges(n);
            for (DistanceEdge de : outging_edges) {
                DistanceEdge new_edges = null;
                Node nextNode = de.endNode;
                if (vcNodes.contains(nextNode) && nextNode != n) {
//                    System.out.println("Create edge (next is vc)"+ n+" "+nextNode);
                    new_edges = new DistanceEdge(de);
                } else if (!vcNodes.contains(nextNode) && nextNode.getId() != n.getId()) {
                    ArrayList<DistanceEdge> nextList = getOutGoingEdges(nextNode);
                    for (DistanceEdge next_de : nextList) {
//                        System.out.println("Create edge (next is non-vc)"+ de.startNode+" "+de.endNode+" "+next_de.endNode);
                        if (n.getId() != next_de.endNode.getId()) {
                            new_edges = new DistanceEdge(de, next_de);
                        }
                    }
                }
                if (new_edges != null) {
                    CreateDisEdges(result, new_edges);
                }

            }

        }
        return result;
    }

    private ArrayList<DistanceEdge> getOutGoingEdges(Node n) {
        ArrayList<DistanceEdge> result = new ArrayList<>();
        for (DistanceEdge de : parent.dg.edges) {
            if (de.startNode.getId() == n.getId()) {
                result.add(de);
            }
        }
        return result;
    }

    private ArrayList<Node> getVCNodes(DistanceGraph graph, ArrayList<Node> s) {
        ArrayList<DistanceEdge> copyRels = new ArrayList<>(graph.edges);
//        System.out.println("get vc nodes ---------");
//        System.out.println(copyRels.size());
        HashSet<Node> vc_nodes = new HashSet<>();
//        int i = 0 ;
        try (Transaction tx = graphdb.beginTx()) {
            {
                while (!copyRels.isEmpty()) {
//                    System.out.println(copyRels.size()+" "+s.size());
//                    System.out.println("   -------------");
//
//                    for(DistanceEdge ee:copyRels)
//                    {
//                        System.out.println("   "+ee.startNode+ "  "+ee.endNode);
//
//                    }
//                    System.out.println("   -------------");

                    DistanceEdge deContainsSnode = null;
                    /**
                     * if s still has some node, find the edge start or end with this node
                     * if s is empty, read any of the edge from copy of Rels.
                     */
                    if (!s.isEmpty()) {
                        int sr = getRandomNumberInRange(0, s.size() - 1);
                        deContainsSnode = getEdgeContainSnode(graph, s.get(sr), copyRels);
//                        System.out.println("get from s "+s.get(sr));

//                        s.remove(0);
                    }

                    int Rnum = getRandomNumberInRange(0, copyRels.size() - 1);
                    if (deContainsSnode == null) {
                        deContainsSnode = copyRels.get(Rnum);
//                        System.out.println("found edges:"+deContainsSnode);

                    }


                    vc_nodes.add(deContainsSnode.startNode);
                    vc_nodes.add(deContainsSnode.endNode);
                    s.remove(deContainsSnode.startNode);
                    s.remove(deContainsSnode.endNode);
//                    System.out.println("----- add node " + deContainsSnode.startNode + "  " + deContainsSnode.endNode + "  " + s.size() + " " + copyRels.size()+ " "+Rnum);

                    ArrayList<DistanceEdge> sRels = getNeighborEdges(deContainsSnode.startNode);
                    ArrayList<DistanceEdge> eRels = getNeighborEdges(deContainsSnode.endNode);

                    removeEdges(copyRels, sRels, s);
                    removeEdges(copyRels, eRels, s);

                }
                tx.success();
            }
        }
//        System.out.println(i);
        return new ArrayList<>(vc_nodes);
    }

    private void removeEdges(ArrayList<DistanceEdge> copyRels, ArrayList<DistanceEdge> Rels, ArrayList<Node> s) {
        for (DistanceEdge de : Rels)
            if (copyRels.contains(de)) {
//                System.out.println("------removed edges" + de);
                copyRels.remove(de);
            }
    }

    private ArrayList<DistanceEdge> getNeighborEdges(Node node) {
        ArrayList<DistanceEdge> outgoingEdges = new ArrayList<>();
        try {
            for (DistanceEdge de : parent.dg.edges) {
                if (de.startNode.getId() == node.getId() || de.endNode.getId() == node.getId()) {
                    outgoingEdges.add(de);
                }
            }
        } catch (Exception e) {
            System.out.println(node + " " + this.dg.edges.size());
        }
        return outgoingEdges;
    }

    private DistanceEdge getEdgeContainSnode(DistanceGraph graph, Node n, ArrayList<DistanceEdge> copyRels) {
        ArrayList<DistanceEdge> tmpR = new ArrayList<>();
        for (DistanceEdge de : graph.edges) {
            if (copyRels.contains(de) && (de.endNode.getId() == n.getId() || de.startNode.getId() == n.getId())) {
                tmpR.add(de);
            }
        }
        if (tmpR.isEmpty()) {
            return null;
        }
        {
            return tmpR.get(getRandomNumberInRange(0, tmpR.size() - 1));
        }
    }

    private int getRandomNumberInRange(int min, int max) {

        if (min > max) {
            throw new IllegalArgumentException("max must be greater than min");
        }

        return r.nextInt((max - min) + 1) + min;
    }
}
