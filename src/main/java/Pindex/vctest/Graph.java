package Pindex.vctest;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.util.ArrayList;

public class Graph {
    ArrayList<Node> nodes = new ArrayList<>();
    ArrayList<Relationship> edges = new ArrayList<>();

    public Graph(ArrayList<Node> nodes, ArrayList<Relationship> edges) {
        this.nodes = nodes;
        this.edges = edges;
    }
}
