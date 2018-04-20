package BaseLine.approximate;

import BaseLine.constants;
import neo4jTools.Line;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

public class path {
    public double[] costs = new double[constants.path_dimension];
    public boolean expaned;
    public Node startNode, endNode;

    public ArrayList<Node> nodes = new ArrayList<>();
    public ArrayList<Relationship> rels = new ArrayList<>();
    public ArrayList<String> propertiesName = new ArrayList<>();


    public path(myNode current) {
        costs[0] = current.distance_q;
        costs[1] = costs[2] = costs[3] = 0;
//        constants.print(costs);
        this.startNode = current.node;
        this.endNode = current.node;
        this.expaned = false;

        this.nodes.add(startNode);

        this.setPropertiesName();
    }

    public path(path old_path, Relationship rel) {
        this.startNode = old_path.startNode;
        this.endNode = rel.getEndNode();
        expaned = false;

        this.nodes.addAll(old_path.nodes);
        this.nodes.add(rel.getEndNode());

        this.rels.addAll(old_path.rels);
        this.rels.add(rel);
        this.propertiesName = old_path.propertiesName;

        System.arraycopy(old_path.costs, 0, this.costs, 0, this.costs.length);

        calculateCosts(rel);
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


    private void calculateCosts(Relationship rel) {
        if (this.startNode.getId() != this.endNode.getId()) {
            int i = 1;
            for (String pname : this.propertiesName) {
                this.costs[i] = this.costs[i] + Double.parseDouble(rel.getProperty(pname).toString());
                i++;
            }
        }
    }


    public void setPropertiesName() {
        Iterable<Relationship> rels = this.startNode.getRelationships(Line.Linked, Direction.BOTH);
        if (rels.iterator().hasNext()) {
            Relationship rel = rels.iterator().next();
            Map<String, Object> pnamemap = rel.getAllProperties();
            for (Map.Entry<String, Object> entry : pnamemap.entrySet()) {
                this.propertiesName.add(entry.getKey());
            }
        } else {
            System.err.println("There is no edge from or to this node " + this.startNode.getId());
        }
    }

    public String toString() {
        StringBuffer sb = new StringBuffer();
        if (this.rels.isEmpty()) {
            sb.append("(" + this.startNode.getId() + ")");
        } else {
            int i;
            for (i = 0; i < this.nodes.size() - 1; i++) {
                sb.append("(" + this.nodes.get(i).getId() + ")");
                // sb.append("-[Linked," + this.relationships.get(i).getId() +
                // "]->");
                sb.append("-[" + this.rels.get(i).getId() + "]-");
            }
            sb.append("(" + this.nodes.get(i).getId() + ")");
        }

//        sb.append(",[");
//        for (double d : this.costs) {
//            sb.append(" " + d);
//        }
//        sb.append("]");
        return sb.toString();
    }

    @Override
    public boolean equals(Object obj) {

        if (obj == null && this == null) {
            return true;
        } else if((obj==null && this!=null) || (obj!=null&&this==null)){
            return false;
        }

        if (obj == this)
            return true;
        if (!(obj instanceof path))
            return false;


        path o_path = (path) obj;
        if (!o_path.nodes.equals(this.nodes) || !o_path.rels.equals(this.rels)) {
            return false;
        }
        return true;
    }
}
