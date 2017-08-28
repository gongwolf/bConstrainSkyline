package Pindex.vctest;

import neo4jTools.Line;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.util.ArrayList;

public class myPath {
    public Node startNode;
    public Node endNode;
    public int NumberOfProperties;
    public ArrayList<String> propertiesName = new ArrayList<>();
    public ArrayList<Node> Nodes;
    public ArrayList<Relationship> relationships;
    public boolean processed_flag = false;
    double[] cost;


    public myPath(Node src, Node dest, Relationship rel) {
        this.startNode = src;
        this.endNode = dest;
        this.NumberOfProperties = getNumberOfProperties();
        this.cost = new double[this.NumberOfProperties];
        // System.out.println("hahahha "+this.cost.length);
        this.Nodes = new ArrayList<>();
        this.relationships = new ArrayList<>();
        this.relationships.add(rel);
        this.Nodes.add(startNode);
        this.Nodes.add(endNode);
        calculateCosts();
    }

    public myPath(Node ns, Relationship rel1, Node nextNode, Relationship rel2, Node tarNode) {
        this.startNode = ns;
        this.endNode = tarNode;
        this.NumberOfProperties = getNumberOfProperties();
        this.cost = new double[this.NumberOfProperties];
        this.Nodes = new ArrayList<>();
        this.relationships = new ArrayList<>();
        this.relationships.add(rel1);
        this.relationships.add(rel2);
        this.Nodes.add(startNode);
        this.Nodes.add(nextNode);
        this.Nodes.add(endNode);
        calculateCosts();
    }

    public int getNumberOfProperties() {
        Iterable<Relationship> rels = this.startNode.getRelationships(Line.Linked, Direction.BOTH);
        if (rels.iterator().hasNext()) {
            Relationship rel = rels.iterator().next();
            return rel.getAllProperties().size();
            // return rel.size();
        } else {
            rels = this.startNode.getRelationships(Line.Linked, Direction.INCOMING);
            if (rels.iterator().hasNext()) {
                Relationship rel = rels.iterator().next();
                return rel.getAllProperties().size();
            } else {
                return -1;
            }
        }
    }

    private void calculateCosts() {
        if (this.startNode == this.endNode) {
            for (int i = 0; i < this.cost.length; i++) {
                this.cost[i] = 0;
            }
        } else {
            Relationship r = this.relationships.get(0);
            int i = 0;
            for (String pname : this.propertiesName) {
                this.cost[i] += Double.parseDouble(r.getProperty(pname).toString());
                i++;
            }
        }
    }

    public double[] getCosts() {
        return this.cost;
    }
}
