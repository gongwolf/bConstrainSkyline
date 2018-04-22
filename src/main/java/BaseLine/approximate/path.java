package BaseLine.approximate;

import BaseLine.constants;
import neo4jTools.*;
import org.neo4j.graphdb.Relationship;
import java.util.ArrayList;


public class path {
    public double[] costs;
    public boolean expaned;
    public long startNode, endNode;

    public ArrayList<Long> nodes;
    public ArrayList<Long> rels;
    public ArrayList<String> propertiesName;


    public path(myNode current) {
        this.costs = new double[constants.path_dimension];
        costs[0] = current.distance_q;
        costs[1] = costs[2] = costs[3] = 0;
//        constants.print(costs);
        this.startNode = current.node;
        this.endNode = current.node;
        this.expaned = false;

        this.nodes = new ArrayList<>();
        this.rels = new ArrayList<>();
        this.propertiesName = new ArrayList<>();


        this.nodes.add(startNode);

        this.setPropertiesName();
//        System.out.println(this.propertiesName.size());
    }

    public path(path old_path, Relationship rel) {

        this.costs = new double[constants.path_dimension];
        this.nodes = new ArrayList<>(old_path.nodes);
        this.rels = new ArrayList<>(old_path.rels);
        this.propertiesName = new ArrayList<>(old_path.propertiesName);


        this.startNode = old_path.startNode;
        this.endNode = rel.getEndNodeId();

        expaned = false;

        this.nodes.add(this.endNode);
        this.rels.add(rel.getId());

        System.arraycopy(old_path.costs, 0, this.costs, 0, this.costs.length);

        calculateCosts(rel);
    }

    public ArrayList<path> expand() {
        ArrayList<path> result = new ArrayList<>();

        ArrayList<Relationship> outgoing_rels = connector.getOutgoutingEdges(this.endNode);
//        System.out.println(outgoing_rels.size());

        for (Relationship r : outgoing_rels) {
            path nPath = new path(this, r);
            result.add(nPath);
        }
        return result;
    }


//    public ArrayList<path> expand() {
//        ArrayList<path> result = new ArrayList<>();
//
//        try (Transaction tx = connector.graphDB.beginTx()) {
//            Iterable<Relationship> rels = connector.graphDB.getNodeById(this.endNode).getRelationships(Line.Linked, Direction.OUTGOING);
//            Iterator<Relationship> rel_Iter = rels.iterator();
//            while (rel_Iter.hasNext()) {
//                Relationship rel = rel_Iter.next();
//                path nPath = new path(this, rel);
//                result.add(nPath);
//            }
//            tx.success();
//        }
//        return result;
//    }


    private void calculateCosts(Relationship rel) {
//        System.out.println(this.propertiesName.size());
        if (this.startNode != this.endNode) {
            int i = 1;
            for (String pname : this.propertiesName) {
//                System.out.println(i+" "+this.costs[i]+"  "+Double.parseDouble(rel.getProperty(pname).toString()));

                this.costs[i] = this.costs[i] + (double) rel.getProperty(pname);
                i++;
            }
        }
    }


    public void setPropertiesName() {
        this.propertiesName = connector.propertiesName;

    }

    public String toString() {
        StringBuffer sb = new StringBuffer();
        if (this.rels.isEmpty()) {
            sb.append("(" + this.startNode + ")");
        } else {
            int i;
            for (i = 0; i < this.nodes.size() - 1; i++) {
                sb.append("(" + this.nodes.get(i) + ")");
                // sb.append("-[Linked," + this.relationships.get(i).getId() +
                // "]->");
                sb.append("-[" + this.rels.get(i) + "]-");
            }
            sb.append("(" + this.nodes.get(i) + ")");
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
        } else if ((obj == null && this != null) || (obj != null && this == null)) {
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
