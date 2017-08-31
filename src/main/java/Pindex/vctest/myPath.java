package Pindex.vctest;

import neo4jTools.Line;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.util.ArrayList;
import java.util.Map;

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
        this.setPropertiesName();
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
        this.setPropertiesName();
        calculateCosts();
    }

    public myPath(myPath p_de, myPath p_next) {
        this.startNode = p_de.startNode;
        this.endNode = p_next.endNode;
        this.NumberOfProperties = getNumberOfProperties();
        this.cost = new double[this.NumberOfProperties];

        this.Nodes = new ArrayList<>(p_de.Nodes);
        this.Nodes.remove(this.Nodes.size() - 1);
        this.Nodes.addAll(p_next.Nodes);


        this.relationships = new ArrayList<>(p_de.relationships);
        this.relationships.addAll(p_next.relationships);
        this.propertiesName = new ArrayList<>(p_de.propertiesName);


        for(int i=0;i<this.NumberOfProperties;i++)
        {
            this.cost[i]=p_de.getCosts()[i]+p_next.getCosts()[i];
        }

    }

    public myPath(Node s) {
        this.startNode = s;
        this.endNode = s;
        this.NumberOfProperties = getNumberOfProperties();
        this.cost = new double[this.NumberOfProperties];
        this.Nodes = new ArrayList<>();
        this.Nodes.add(s);
        this.relationships = new ArrayList<>();
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
        if (this.startNode.getId() == this.endNode.getId()) {
            for (int i = 0; i < this.cost.length; i++) {
                this.cost[i] = 0;
            }
        } else {
            for (Relationship r : this.relationships) {
                int i = 0;
                for (String pname : this.propertiesName) {
                    double value = Double.parseDouble(r.getProperty(pname).toString());
                    this.cost[i] += value;
                    i++;
                }
            }
        }
    }

    public double[] getCosts() {
        return this.cost;
    }

    public boolean hasCycle() {
//        System.out.println(this.startNode+" "+this.endNode);
//        for(Node n:this.Nodes)
//        {
//            System.out.println(n);
//        }
//
//        for(Node n1:this.Nodes)
//        {
//            if(n1.getId()!=this.startNode.getId() && n1.getId()!=this.endNode.getId()) {
//                for (Node n2 : this.Nodes) {
//                    if (n2.getId() == n1.getId()) {
//                        return true;
//                    }
//                }
//            }
//        }
        return  this.startNode.getId()==this.endNode.getId();
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

    public void setPropertiesName() {
        Iterable<Relationship> rels = this.startNode.getRelationships(Line.Linked, Direction.OUTGOING);
        if (rels.iterator().hasNext()) {
            Relationship rel = rels.iterator().next();
            Map<String, Object> pnamemap = rel.getAllProperties();
            for (Map.Entry<String, Object> entry : pnamemap.entrySet()) {
                this.propertiesName.add(entry.getKey());
            }
        } else {
            rels = this.startNode.getRelationships(Line.Linked, Direction.INCOMING);
            if (rels.iterator().hasNext()) {
                Relationship rel = rels.iterator().next();
                Map<String, Object> pnamemap = rel.getAllProperties();
                for (Map.Entry<String, Object> entry : pnamemap.entrySet()) {
                    this.propertiesName.add(entry.getKey());
                }
            }
        }
    }
}
