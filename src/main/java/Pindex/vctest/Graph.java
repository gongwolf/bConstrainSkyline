package Pindex.vctest;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;

public class Graph {
    ArrayList<Node> nodes = new ArrayList<>();
    ArrayList<Relationship> edges = new ArrayList<>();
    HashMap<Node, LinkedList<Relationship>> AdjList = new HashMap<>();

    public Graph(ArrayList<Node> nodes, ArrayList<Relationship> edges) {
        this.nodes = nodes;
        this.edges = edges;

        for (Relationship r : edges) {
            Node startNode = r.getStartNode();
            Node endNode = r.getEndNode();
            if (AdjList.containsKey(startNode)) {
                LinkedList<Relationship> list = AdjList.get(startNode);
                list.add(r);
                AdjList.put(startNode, list);
            } else {
                LinkedList<Relationship> list = new LinkedList<>();
                list.add(r);
                AdjList.put(startNode, list);
            }
        }
    }

    public LinkedList<Relationship> getOutGoingRels(Node startNode)
    {
        return this.AdjList.get(startNode);
    }
}
