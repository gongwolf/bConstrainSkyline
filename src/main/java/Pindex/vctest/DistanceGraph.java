package Pindex.vctest;

import org.neo4j.graphdb.Node;

import java.util.ArrayList;

public class DistanceGraph {

    public ArrayList<DistanceEdge> edges;
    public ArrayList<Node> nodes;

    public DistanceGraph(ArrayList<Node> nodes, ArrayList<DistanceEdge> edges) {
        this.nodes = nodes;
        this.edges = edges;
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
        for (DistanceEdge de : this.edges) {
            for (myPath p : de.paths) {
                if (p.endNode.getId() == n.getId())
                    result.add(p);
            }
        }
        return result;
    }
}
