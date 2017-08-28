package Pindex.vctest;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.util.ArrayList;

public class DistanceGraph  {

    public ArrayList<DistanceEdge> edges;
    public ArrayList<Node> nodes;

    public DistanceGraph(ArrayList<Node> nodes, ArrayList<DistanceEdge> edges) {
        this.nodes = nodes;
        this.edges = edges;
    }


    public long numberOfNodes(){
        return (long)nodes.size();
    }


    public long numberOfEdges(){
        return (long)edges.size();
    }
}
