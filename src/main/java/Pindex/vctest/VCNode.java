package Pindex.vctest;

import neo4jTools.Line;
import org.neo4j.graphdb.*;

import java.security.spec.DSAGenParameterSpec;
import java.util.*;

public class VCNode {
    private final Random r;
    VCNode parent = null;
    DistanceGraph dg;
    public GraphDatabaseService graphdb;
    private int threshold;
    public ArrayList<VCNode> children;
    int level;
    public boolean isRoot = false;


    public VCNode(GraphDatabaseService graphdb) {
        this.graphdb = graphdb;
        this.r = new Random();

    }


    public void buildDistanceGraph(Graph graph, int threshold) {
        this.isRoot = true;
        this.threshold = threshold;
        this.level = 0;
        try (Transaction tx = this.graphdb.beginTx()) {
            ArrayList<Node> VCNodes = getVCNodes(graph);
//            System.out.println(VCNodes.size());
            ArrayList<DistanceEdge> VCEdges = CreateEdges(VCNodes, graph);
            this.dg = new DistanceGraph(VCNodes, VCEdges);
//            System.out.println("dg information " + dg.numberOfEdges() + " edges   " + dg.numberOfNodes() + " nodes");


//            for (DistanceEdge de : this.dg.edges) {
//                try {
//                    System.out.println(de.startNode.getId() + "  " + de.endNode.getId());
//                } catch (Exception e) {
//                    System.out.println("Exception");
//                }
//            }

            if (this.dg.numberOfNodes() > this.threshold) {
//                System.out.println("generate subTree");
//                System.out.println("there are " + this.dg.edges.size() + " edges in root distance graph");
//                System.out.println("there are " + this.dg.nodes.size() + " edges in root distance graph");
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
            int prv_size = S.size();
            child.buildDistanceGraph(parent.dg, parent.threshold, S);
//            if(child.level==3)
//            {
//                break;
//            }

            if (child.dg != null) {
                parent.children.add(child);
//                if (child.level % 1000 == 0) {
//                    System.out.println("Create child in level " + child.level);
//                }
//                System.out.println(parent.level + " add children level " + child.level + " : " + child.dg.numberOfNodes() + " " + child.dg.numberOfEdges());
//              System.out.println(child.dg.numberOfNodes());
                if (child.dg.numberOfNodes() > this.threshold) {
                    GrowTheTree(child);
                }

                int aft_size = S.size();
                S.clear();
//                if ((double) aft_size / prv_size > 0.8) {
//                    S.clear();
//                }
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

    private ArrayList<DistanceEdge> CreateEdges(ArrayList<Node> vcNodes, Graph graph) {
        ArrayList<DistanceEdge> result = new ArrayList<>();

        for (Node ns : vcNodes) {
            // Find neighbor nodes of ns.
//            Iterable<Relationship> rels = ns.getRelationships(Line.Linked, Direction.OUTGOING);
            LinkedList<Relationship> list = graph.getOutGoingRels(ns);

            if (list == null)
                continue;
//            if (ns.getId() == 97 || ns.getId() == 235) {
//                for (Relationship rel : list) {
//                    System.out.println(rel);
//                }
//                System.out.println("========");
//            }

//            if (ns.getId() == 305) {
            for (Relationship rel : list) {
                DistanceEdge de = null;
                Node nextNode = rel.getEndNode();

                //if nextnode is a vc node
                if (vcNodes.contains(nextNode) && nextNode != ns) {
                    de = new DistanceEdge(ns, nextNode, rel);
                    if (de != null)
                        CreateDisEdges(result, de);
                }//if next node is a non-vc-node,jump to next-next node, it should be a vc node.
                else if (!vcNodes.contains(nextNode) && nextNode.getId() != ns.getId()) {
                    LinkedList<Relationship> next_list = graph.getOutGoingRels(nextNode);

                    if (next_list == null)
                        continue;

                    for (Relationship nextRel : next_list) {

                        Node tarNode = nextRel.getEndNode();
                        if (ns.getId() != tarNode.getId()) {
                            de = new DistanceEdge(ns, rel, nextNode, nextRel, tarNode);
//                            if (nextNode.getId() == 97) {
//                                System.out.println("    " + nextRel);
//                                System.out.println("    " + ns + "+++++" + tarNode + "  " + (ns.getId()) + ":" + tarNode.getId() + "->" + (ns.getId() == tarNode.getId())+" "+result.contains(de));
//                                System.out.println("    " + de.paths.get(0));
//                            }
                            if (de != null)
                                CreateDisEdges(result, de);
                        }
                    }
                }


            }
        }
//        }
//        System.out.println("Distance graph has " + result.size() + " Edges");

//        for(DistanceEdge dd:result)
//        {
//            if(dd.startNode.getId()==235)
//            {
//                System.out.println(dd);
//            }
//        }

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
            ArrayList<DistanceEdge> VCEdges;

            int trys = 0;
            while (trys != 5) {
                S.clear();
                S.addAll(Sor);
                VCNodes = getVCNodes(graph, S);
                if ((VCNodes.size() < (parent.dg.numberOfNodes() * 0.9))) {
                    break;
                }
                trys++;

            }

            if (trys != 5) {
                VCEdges = CreateEdgesDistance(graph, VCNodes);
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

    private ArrayList<DistanceEdge> CreateEdgesDistance(DistanceGraph graph, ArrayList<Node> vcNodes) {
        ArrayList<DistanceEdge> result = new ArrayList<>();
        for (Node n : vcNodes) {
            ArrayList<DistanceEdge> outging_edges = getOutGoingEdges(n);
            if (outging_edges == null)
                continue;
            for (DistanceEdge de : outging_edges) {
//                if(de.startNode.getId()==22)
//                {
//                    System.out.println(this.level+":"+de);
//                }
                DistanceEdge new_edges = null;
                Node nextNode = de.endNode;
                if (vcNodes.contains(nextNode) && nextNode.getId() != n.getId()) {
//                    if(de.startNode.getId()==22)
//                    {
//                        System.out.println("Create edge (next is vc)"+ n+" "+nextNode);
//                    }
                    new_edges = new DistanceEdge(de);
//                    if(de.startNode.getId()==22)
//                    {
//                        System.out.println(new_edges==null);
//                        System.out.println(new_edges);
//                    }

                    if (new_edges != null) {
                        CreateDisEdges(result, new_edges);
                    }
                } else if (!vcNodes.contains(nextNode) && nextNode.getId() != n.getId()) {
                    ArrayList<DistanceEdge> nextList = getOutGoingEdges(nextNode);
                    for (DistanceEdge next_de : nextList) {
//                        if(de.startNode.getId()==22)
//                        {
//                            System.out.println("Create edge (next is non-vc)"+ de.startNode+" "+de.endNode+" "+next_de.endNode);
//                        }
                        if (n.getId() != next_de.endNode.getId()) {
                            new_edges = new DistanceEdge(de, next_de);
//                            if(de.startNode.getId()==22)
//                            {
//                                System.out.println(new_edges==null);
//                                System.out.println(new_edges);
//                            }

                            if (new_edges != null) {
                                CreateDisEdges(result, new_edges);
                            }
                        }
                    }
                }


            }

        }
        return result;
    }

    private ArrayList<DistanceEdge> getOutGoingEdges(Node n) {
        ArrayList<DistanceEdge> result = new ArrayList<>();
        result.addAll(this.parent.dg.getOutGoingRels(n));
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
        HashSet<DistanceEdge> tmpR = new HashSet<>();
        LinkedList<DistanceEdge> ogRels = this.parent.dg.getOutGoingRels(node);
        LinkedList<DistanceEdge> icRels = this.parent.dg.getIncommingRels(node);

        if (ogRels != null) {
            tmpR.addAll(ogRels);
        }

        if (icRels != null) {
            tmpR.addAll(icRels);
        }

        ArrayList<DistanceEdge> result = new ArrayList<>(tmpR);
        tmpR.clear();


        return result;
    }

    private DistanceEdge getEdgeContainSnode(DistanceGraph graph, Node n, ArrayList<DistanceEdge> copyRels) {
        HashSet<DistanceEdge> tmpR = new HashSet<>();
        LinkedList<DistanceEdge> ogRels = graph.getOutGoingRels(n);
        LinkedList<DistanceEdge> icRels = graph.getIncommingRels(n);

        if (ogRels != null) {
            tmpR.addAll(ogRels);
        }

        if (icRels != null) {
            tmpR.addAll(icRels);
        }

        ArrayList<DistanceEdge> result = new ArrayList<>();
        for (DistanceEdge de : tmpR) {
            if (copyRels.contains(de)) {
                result.add(de);
            }
        }
        tmpR.clear();

        if (result.isEmpty()) {
            return null;
        } else {
            return result.get(getRandomNumberInRange(0, result.size() - 1));
        }
    }

    private int getRandomNumberInRange(int min, int max) {

        if (min > max) {
            throw new IllegalArgumentException("max must be greater than min");
        }

        return r.nextInt((max - min) + 1) + min;
    }

    public ArrayList<myPath> expand_in_dg(myPath p) {
        Node n = p.endNode;
        ArrayList<myPath> result = new ArrayList<>();
        for (DistanceEdge de : this.dg.getOutGoingRels(n)) {
//                System.out.println(de);
            for (myPath sp : de.paths) {
                myPath new_p = new myPath(p, sp);
//                    System.out.println(new_p.hasCycle()+"    "+new_p);
                if (!new_p.hasCycle()) {
                    result.add(new_p);
                }
            }

        }
        return result;
    }


}
