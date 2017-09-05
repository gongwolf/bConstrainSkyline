package Pindex.vctest;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;

public class DistanceGraph {

    public ArrayList<DistanceEdge> edges;
    public ArrayList<Node> nodes;
    HashMap<Node, LinkedList<DistanceEdge>> Adj_out_List = new HashMap<>();
    HashMap<Node, LinkedList<DistanceEdge>> Adj_in_List = new HashMap<>();


    public DistanceGraph(ArrayList<Node> nodes, ArrayList<DistanceEdge> edges) {
        this.nodes = nodes;
        this.edges = edges;

        for (DistanceEdge r : edges) {
            Node startNode = r.startNode;
            Node endNode = r.endNode;
            if (Adj_out_List.containsKey(startNode)) {
                LinkedList<DistanceEdge> list = Adj_out_List.get(startNode);
                list.add(r);
                Adj_out_List.put(startNode, list);
            } else {
                LinkedList<DistanceEdge> list = new LinkedList<>();
                list.add(r);
                Adj_out_List.put(startNode, list);
            }

            if (Adj_in_List.containsKey(endNode)) {
                LinkedList<DistanceEdge> list = Adj_in_List.get(endNode);
                list.add(r);
                Adj_in_List.put(endNode, list);
            } else {
                LinkedList<DistanceEdge> list = new LinkedList<>();
                list.add(r);
                Adj_in_List.put(endNode, list);

            }
        }

    }


    public long numberOfNodes() {
        return (long) nodes.size();
    }


    public long numberOfEdges() {
        return (long) edges.size();
    }

    public long totalPaths() {
        long result = 0;
        for (DistanceEdge de : this.edges) {
            result += de.paths.size();
        }
        return result;
    }

    public ArrayList<myPath> getIncomingEdges(Node n) {
        ArrayList<myPath> result = new ArrayList<>();
        if (this.Adj_in_List.containsKey(n)) {
            for (DistanceEdge de : this.getIncommingRels(n)) {
                for (myPath p : de.paths) {
                    result.add(p);
                }
            }
        }
        return result;
    }

    public LinkedList<DistanceEdge> getOutGoingRels(Node startNode) {
        if (this.Adj_out_List.containsKey(startNode))
            return this.Adj_out_List.get(startNode);
        else
            return new LinkedList<>();
    }

    public LinkedList<DistanceEdge> getIncommingRels(Node endNode) {
        if (this.Adj_in_List.containsKey(endNode))
            return this.Adj_in_List.get(endNode);
        else
            return new LinkedList<>();
    }


}
