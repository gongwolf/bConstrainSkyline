package unDirectionalGraph;

import neo4jTools.Line;
import org.neo4j.graphalgo.WeightedPath;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

public class path {
    public Node startNode;
    public Node endNode;
    public int NumberOfProperties;
    public ArrayList<String> propertiesName = new ArrayList<>();
    public ArrayList<Node> Nodes;
    public ArrayList<Relationship> relationships;
    public boolean processed_flag = false;
    double[] cost;

    public path(Node startNode, Node endNode) {
        this.startNode = startNode;
        this.endNode = endNode;
        this.NumberOfProperties = getNumberOfProperties();
        this.setPropertiesName();
        this.cost = new double[this.NumberOfProperties];
        // System.out.println("hahahha "+this.cost.length);
        this.Nodes = new ArrayList<>();
        this.relationships = new ArrayList<>();
        this.Nodes.add(startNode);
        calculateCosts();
    }

    public path(path oldpath, Relationship rel) {
        this.startNode = oldpath.startNode;
        this.endNode = rel.getStartNode().getId()==oldpath.endNode.getId()?rel.getEndNode():rel.getStartNode();
        this.NumberOfProperties = oldpath.NumberOfProperties;
        this.cost = new double[this.NumberOfProperties];
        this.Nodes = new ArrayList<>(oldpath.Nodes);
        this.propertiesName = oldpath.propertiesName;
        this.relationships = new ArrayList<>(oldpath.relationships);
        this.relationships.add(rel);
        // System.out.println(startNode+"##"+endNode+"###"+
        // (startNode==endNode));
        this.Nodes.add(rel.getEndNode());
        System.arraycopy(oldpath.cost,0,this.cost,0,this.cost.length);
        calculateCosts(rel);
    }

    public path(WeightedPath paths) {
        this.startNode = paths.startNode();
        this.endNode = paths.endNode();

        this.NumberOfProperties = getNumberOfProperties();
        this.setPropertiesName();
        this.cost = new double[this.NumberOfProperties];

        this.Nodes = new ArrayList<>();
        Iterator<Node> nodes_iter = paths.nodes().iterator();
        while (nodes_iter.hasNext()) {
            this.Nodes.add(nodes_iter.next());
        }

        this.relationships = new ArrayList<>();
        Iterator<Relationship> rel_iter = paths.relationships().iterator();
        while (rel_iter.hasNext()) {
            this.relationships.add(rel_iter.next());
        }
        calculateCosts();
    }

    public path(Node startNode) {
        this(startNode, startNode);
    }

    public boolean containRelationShip(Relationship rel) {
        return this.relationships.contains(rel);

    }

    public Relationship getlastRelationship() {
        int n = this.relationships.size() - 1;
        return this.relationships.get(n);
    }

    public int getNumberOfProperties() {
        Iterable<Relationship> rels = this.startNode.getRelationships(Line.Linked, Direction.OUTGOING);
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

    public void setPropertiesName() {
        Iterable<Relationship> rels = this.startNode.getRelationships(Line.Linked, Direction.OUTGOING);
        if (rels.iterator().hasNext()) {
            Relationship rel = rels.iterator().next();
            Map<String, Object> pnamemap = rel.getAllProperties();
            for (Entry<String, Object> entry : pnamemap.entrySet()) {
                this.propertiesName.add(entry.getKey());
            }
        } else {
            rels = this.startNode.getRelationships(Line.Linked, Direction.INCOMING);
            if (rels.iterator().hasNext()) {
                Relationship rel = rels.iterator().next();
                Map<String, Object> pnamemap = rel.getAllProperties();
                for (Entry<String, Object> entry : pnamemap.entrySet()) {
                    this.propertiesName.add(entry.getKey());
                }
            }
        }
    }

    public ArrayList<String> getPropertiesName()
    {
        return this.propertiesName;
    } 

    private void calculateCosts() {
        if (this.startNode == this.endNode) {
            for (int i = 0; i < this.cost.length; i++) {
                this.cost[i] = 0;
            }
        } else {
            for (Relationship r : this.relationships) {
                int i = 0;
                // System.out.println("--------------------"+this.relationships.size());
                for (String pname : this.propertiesName) {
                    this.cost[i] += Double.parseDouble(r.getProperty(pname).toString());
                    // System.out.println(pname+"***"+r.getProperty(pname));
                    i++;
                }
            }
        }
    }

    private void calculateCosts(Relationship rel) {
        if (this.startNode == this.endNode) {
            for (int i = 0; i < this.cost.length; i++) {
                this.cost[i] = 0;
            }
        } else {
            int i = 0;
            // System.out.println("--------------------"+this.relationships.size());
            for (String pname : this.propertiesName) {
                this.cost[i] += Double.parseDouble(rel.getProperty(pname).toString());
                // System.out.println(pname+"***"+r.getProperty(pname));
                i++;
            }
        }
    }

    public double[] getCosts() {
        return this.cost;
    }

    public ArrayList<path> expand() {
        ArrayList<path> result = new ArrayList<>();

        Iterable<Relationship> rels = this.endNode.getRelationships(Line.Linked, Direction.BOTH);
        Iterator<Relationship> rel_Iter = rels.iterator();
        while (rel_Iter.hasNext()) {
            Relationship rel = rel_Iter.next();
            path nPath = new path(this, rel);
            result.add(nPath);
        }
        return result;
    }

    public String toString() {
        StringBuffer sb = new StringBuffer();
        if (this.relationships.isEmpty()) {
            sb.append("(" + this.startNode.getId() + ")");
        } else {
            int i;
            for (i = 0; i < this.Nodes.size() - 1; i++) {
                sb.append("(" + this.Nodes.get(i).getId() + ")");
                // sb.append("-[Linked," + this.relationships.get(i).getId() +
                // "]->");
                sb.append("-[" + this.relationships.get(i).getId() + "]-");
            }
            sb.append("(" + this.Nodes.get(i).getId() + ")");
        }
        return sb.toString();
    }

    public int getLenght()
    {
        return this.relationships.size();
    }

    public long totalDegree()
    { 
        long totalDegree = 0 ;
        for(Node n: this.Nodes)
        {
            totalDegree+=n.getDegree(Direction.OUTGOING);
        }
        return totalDegree;
    }

    public void setCosts(double[] upperbound) {
        for (int i = 0; i < this.NumberOfProperties; i++) {
            this.cost[i]=upperbound[i];
        }
    }

}
