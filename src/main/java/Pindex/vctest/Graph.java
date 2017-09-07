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
    HashMap<Node, LinkedList<Relationship>> Adj_out_List = new HashMap<>();
    HashMap<Node, LinkedList<Relationship>> Adj_in_List = new HashMap<>();


    public Graph(ArrayList<Node> nodes, ArrayList<Relationship> edges) {
        this.nodes = nodes;
        this.edges = edges;

        for (Relationship r : edges) {
            Node startNode = r.getStartNode();
            Node endNode = r.getEndNode();
            if (Adj_out_List.containsKey(startNode)) {
                LinkedList<Relationship> list = Adj_out_List.get(startNode);
                list.add(r);
                Adj_out_List.put(startNode, list);
            } else {
                LinkedList<Relationship> list = new LinkedList<>();
                list.add(r);
                Adj_out_List.put(startNode, list);
            }

            if (Adj_in_List.containsKey(endNode)) {
                LinkedList<Relationship> list = Adj_in_List.get(endNode);
                list.add(r);
                Adj_in_List.put(endNode, list);
            } else {
                LinkedList<Relationship> list = new LinkedList<>();
                list.add(r);
                Adj_in_List.put(endNode, list);

            }
        }
    }


    public LinkedList<Relationship> getOutGoingRels(Node startNode) {
        if (this.Adj_out_List.containsKey(startNode))
            return this.Adj_out_List.get(startNode);
        else
            return new LinkedList<>();
    }

    public LinkedList<Relationship> getIncommingRels(Node endNode) {
        if (this.Adj_in_List.containsKey(endNode))
            return this.Adj_in_List.get(endNode);
        else
            return new LinkedList<>();
    }
}
