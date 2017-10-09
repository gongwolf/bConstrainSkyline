package GraphPartition;

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
    public String startID, endID;
    public Node endNode;
    public int NumberOfProperties;
    public ArrayList<String> propertiesName = new ArrayList<>();
    public ArrayList<Node> Nodes;
    public ArrayList<Relationship> relationships;
    public boolean processed_flag = false;
    double[] cost;

    public path(Node startNode, Node startNode_dummy) {
        this.startNode = startNode;
        this.endNode = startNode_dummy;
        this.NumberOfProperties = getNumberOfProperties();
        this.setPropertiesName();
        this.cost = new double[this.NumberOfProperties];
        // System.out.println("hahahha "+this.cost.length);
        this.Nodes = new ArrayList<>();
        this.relationships = new ArrayList<>();
        this.Nodes.add(startNode);
        calculateCosts();
        this.startID = String.valueOf(startNode.getId());
        this.endID = String.valueOf(startNode_dummy.getId());
    }

    public path(path oldpath, Relationship rel) {
        this.startNode = oldpath.startNode;
        this.endNode = rel.getEndNode();
        this.NumberOfProperties = oldpath.NumberOfProperties;
        this.cost = new double[this.NumberOfProperties];
        this.Nodes = new ArrayList<>(oldpath.Nodes);
        this.propertiesName.addAll(oldpath.propertiesName);
        this.relationships = new ArrayList<>(oldpath.relationships);
        this.relationships.add(rel);
        // System.out.println(startNode+"##"+endNode+"###"+
        // (startNode==endNode));
        this.Nodes.add(rel.getEndNode());
        System.arraycopy(oldpath.cost, 0, this.cost, 0, this.cost.length);
        calculateCosts(rel);
        this.startID = String.valueOf(startNode.getId());
        this.endID = String.valueOf(endNode.getId());
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
        ;

        this.relationships = new ArrayList<>();
        Iterator<Relationship> rel_iter = paths.relationships().iterator();
        while (rel_iter.hasNext()) {
            this.relationships.add(rel_iter.next());
        }
        calculateCosts();
        this.startID = String.valueOf(startNode.getId());
        this.endID = String.valueOf(endNode.getId());
    }

    public path(Node startNode) {
        this(startNode, startNode);
    }

    public path(Relationship rel, path oldpath) {
//        System.out.println("-------------");
        this.startNode = rel.getStartNode();
        this.endNode = oldpath.endNode;

        this.NumberOfProperties = oldpath.NumberOfProperties;
        this.cost = new double[this.NumberOfProperties];
        //new nodes list
        this.Nodes = new ArrayList<>();
        this.Nodes.add(this.startNode);
        this.Nodes.addAll(oldpath.Nodes);
        //new rels list
        this.relationships = new ArrayList<>();
        this.relationships.add(rel);
        this.relationships.addAll(oldpath.relationships);
        this.propertiesName.addAll(oldpath.propertiesName);

        System.arraycopy(oldpath.cost, 0, this.cost, 0, this.cost.length);
        calculateCosts(rel);
        this.startID = String.valueOf(startNode.getId());
        this.endID = String.valueOf(endNode.getId());

    }

    public path(path p1, path p2) {
        this.startNode = p1.startNode;
        this.endNode = p2.endNode;
        this.NumberOfProperties = p1.NumberOfProperties;
        this.propertiesName = new ArrayList<>(p1.propertiesName);
        this.cost = new double[NumberOfProperties];

        System.arraycopy(p1.cost, 0, this.cost, 0, this.NumberOfProperties);
        for (int i = 0; i < this.NumberOfProperties; i++) {
            this.cost[i] = this.cost[i] + p2.cost[i];
        }

        this.Nodes = new ArrayList<>(p1.Nodes);
        this.Nodes.remove(this.Nodes.size() - 1);// p1.endNode is equal to
        // p2.startNode;
        this.Nodes.addAll(p2.Nodes);

        this.relationships = new ArrayList<>(p1.relationships);
        this.relationships.addAll(p2.relationships);

        this.startID = String.valueOf(startNode.getId());
        this.endID = String.valueOf(endNode.getId());
    }

    public path(double[] cost_upper_bound) {
        this.cost = new double[cost_upper_bound.length];
        System.arraycopy(cost_upper_bound, 0, this.cost, 0, cost_upper_bound.length);
    }

    public boolean containRelationShip(Relationship rel) {
        return this.relationships.contains(rel);

    }

    public boolean isCycle() {
        for (int i = 0; i < this.Nodes.size() - 2; i++) {
            if (this.Nodes.get(i).equals(this.endNode)) {
                return true;
            }
        }
        return false;
    }

    public Relationship getlastRelationship() {
        int n = this.relationships.size() - 1;
        return this.relationships.get(n);
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

    public void setPropertiesName() {
        Iterable<Relationship> rels = this.startNode.getRelationships(Line.Linked, Direction.BOTH);
        if (rels.iterator().hasNext()) {
            Relationship rel = rels.iterator().next();
            Map<String, Object> pnamemap = rel.getAllProperties();
            for (Entry<String, Object> entry : pnamemap.entrySet()) {
                this.propertiesName.add(entry.getKey());
            }
        }
    }

    public ArrayList<String> getPropertiesName() {
        return this.propertiesName;
    }

    private void calculateCosts() {
        if (this.startNode.getId() == this.endNode.getId()) {
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
        if (this.startNode.getId() == this.endNode.getId()) {
//            System.out.println("!!!!!");
            for (int i = 0; i < this.cost.length; i++) {
                this.cost[i] = 0;
            }
        } else {
            int i = 0;
            // System.out.println("--------------------"+this.relationships.size());
            for (String pname : this.propertiesName) {
                this.cost[i] += Double.parseDouble(rel.getProperty(pname).toString());
//                System.out.println(pname+"***"+rel.getProperty(pname));
                i++;
            }
        }
    }

    public double[] getCosts() {
        return this.cost;
    }

    public ArrayList<path> expand() {
        ArrayList<path> result = new ArrayList<>();
        Iterable<Relationship> rels = this.endNode.getRelationships(Line.Linked, Direction.OUTGOING);
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

        sb.append("     [");
        for (double d : this.cost) {
            sb.append(d).append(",");
        }
        sb.replace(sb.lastIndexOf(","), sb.length(), "");
        sb.append("]");
        return sb.toString();
    }

    public int getLenght() {
        return this.relationships.size();
    }

    public long totalDegree() {
        long totalDegree = 0;
        for (Node n : this.Nodes) {
            totalDegree += n.getDegree(Direction.OUTGOING);
        }
        return totalDegree;
    }

    public String printCosts() {
        StringBuffer sb = new StringBuffer();
        sb.append("[");
        int i = 0;
        for (; i < cost.length - 1; i++) {
            sb.append(cost[i] + ",");
        }
        sb.append(cost[i] + "]");
        return sb.toString();
    }

    public ArrayList<path> expand(block adjb) {
        ArrayList<path> result = new ArrayList<>();
        Iterable<Relationship> rels = this.endNode.getRelationships(Line.Linked, Direction.OUTGOING);
        Iterator<Relationship> rel_Iter = rels.iterator();
        while (rel_Iter.hasNext()) {
            Relationship rel = rel_Iter.next();
            path nPath = new path(this, rel);
            if (adjb.nodes.contains(String.valueOf(nPath.endNode.getId() - 1))) {
                result.add(nPath);
            }
        }
        return result;
    }
}
