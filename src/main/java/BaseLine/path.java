package BaseLine;

import javafx.util.Pair;
import neo4jTools.connector;
import org.neo4j.graphdb.Relationship;

import java.util.ArrayList;


public class path {
    public double[] costs;
    public boolean expaned;
    public long startNode, endNode;

//    public ArrayList<Long> nodes;
//    public ArrayList<Long> rels;

//    public long[] nodes;
//    public long[] rels;

//    public ArrayList<String> propertiesName;


    public path(myNode current) {
        this.costs = new double[constants.path_dimension];
        costs[0] = current.distance_q;
        costs[1] = costs[2] = costs[3] = 0;
//        constants.print(costs);
        this.startNode = current.node;
        this.endNode = current.node;
        this.expaned = false;

//        this.nodes = new ArrayList<>(100);
//        this.rels = new ArrayList<>(100);

//        this.nodes=new long[]{this.endNode};
//        this.rels = new long[]{};

//        this.propertiesName = new ArrayList<>(3);

//        this.setPropertiesName();

        //store the Long Objects
//        this.nodes.add(getLongObject_Node(this.endNode));
    }

    public path(path old_path, long rel_id, long end_id) {

        this.costs = new double[constants.path_dimension];
        this.startNode = old_path.startNode;
        this.endNode = end_id;


//        int n_nodes = old_path.nodes.length;
//        this.nodes = new long[n_nodes + 1];
//        System.arraycopy(old_path.nodes,0,this.nodes,0,n_nodes);
//
//
//
//        int n_rels = old_path.rels.length;
//        this.rels = new long[n_rels + 1];
//        System.arraycopy(old_path.rels,0,this.rels,0,n_rels);

//        this.propertiesName = new ArrayList<>(old_path.propertiesName);


        expaned = false;



//        this.nodes[n_nodes]=this.endNode;
//        this.rels[n_rels]=rel_id;

        System.arraycopy(old_path.costs, 0, this.costs, 0, this.costs.length);

//        this.nodes.add(getLongObject_Node(this.endNode));
//        this.rels.add(getLongObject_Edge(rel_id));
//        this.nodes = new ArrayList<>();
//        for (long n : old_path.nodes) {
//            this.nodes.add(getLongObject_Node(n));
//        }
//        this.rels = new ArrayList<>();
//        for (long e : old_path.rels) {
//            this.rels.add(getLongObject_Edge(e));
//        }

    }

//    public ArrayList<path> expand() {
//        ArrayList<path> result = new ArrayList<>();
//
//        ArrayList<Relationship> outgoing_rels = connector.getOutgoutingEdges(this.endNode);
////        System.out.println("  expand " +this.endNode+" "+outgoing_rels.size()+" | "+this.nodes.size()+" "+this.rels.size());
////        System.out.println(outgoing_rels.size());
//
//        for (Relationship r : outgoing_rels) {
//            path nPath = new path(this, r);
//            result.add(nPath);
//        }
//        return result;
//    }


    public ArrayList<path> expand() {
        ArrayList<path> result = new ArrayList<>();

        ArrayList<Pair<Pair<Long, Long>, double[]>> outgoingEdges = constants.edges.get(this.endNode);
        if (outgoingEdges != null) {
            for (Pair<Pair<Long, Long>, double[]> e : outgoingEdges) {
                long did = e.getKey().getKey();
                long rel_id = e.getKey().getValue();
                double[] add_costs = e.getValue();

                path nPath = new path(this, rel_id, did);
                nPath.calculateCosts(add_costs);

                result.add(nPath);
            }
        }

//        try (Transaction tx = connector.graphDB.beginTx()) {
//            ResourceIterable<Relationship> rels = (ResourceIterable<Relationship>) connector.graphDB.getNodeById(this.endNode).getRelationships(Line.Linked, Direction.OUTGOING);
//            Iterator<Relationship> rel_Iter = rels.iterator();
//            while (rel_Iter.hasNext()) {
//                Relationship rel = rel_Iter.next();
//                long rel_id = rel.getId();
//                long rel_endnode = rel.getEndNodeId();
//                path nPath = new path(this, rel_id, rel_endnode);
//                nPath.calculateCosts(rel);
//                result.add(nPath);
//            }
//
//            tx.success();
//        }
        return result;
    }


    public ArrayList<Pair<Pair<Long, Long>, double[]>> getNextEdges() {

        ArrayList<Pair<Pair<Long, Long>, double[]>> result = new ArrayList<>();

        ArrayList<Pair<Pair<Long, Long>, double[]>> outgoingEdges = constants.edges.get(this.endNode);
        if (outgoingEdges != null) {
            for (Pair<Pair<Long, Long>, double[]> e : outgoingEdges) {
                result.add(e);
            }
        }
        return result;
    }


//    private void calculateCosts(Relationship rel) {
////        System.out.println(this.propertiesName.size());
//        if (this.startNode != this.endNode) {
//            int i = 1;
//            for (String pname : this.propertiesName) {
////                System.out.println(i+" "+this.costs[i]+"  "+Double.parseDouble(rel.getProperty(pname).toString()));
//
//                this.costs[i] = this.costs[i] + (double) rel.getProperty(pname);
//                i++;
//            }
//        }
//    }


    public void calculateCosts(double add_costs[]) {
//        System.out.println(this.propertiesName.size());
        if (this.startNode != this.endNode) {
            int i = 1;
            for (;i<constants.path_dimension;i++) {
//                System.out.println(i+" "+this.costs[i]+"  "+Double.parseDouble(rel.getProperty(pname).toString()));
                this.costs[i] = this.costs[i] + add_costs[i - 1];
            }
        }
    }


//    public void setPropertiesName() {
//        this.propertiesName = connector.propertiesName;
//    }

//    public String toString() {
////        System.out.println("dasdasd:   "+this.nodes.size()+"  "+this.rels.size());
//        StringBuffer sb = new StringBuffer();
//        if (this.rels.isEmpty()) {
//            sb.append("(" + this.startNode + ")");
//        } else {
//            int i;
//            for (i = 0; i < this.nodes.size() - 1; i++) {
//                sb.append("(" + this.nodes.get(i) + ")");
//                // sb.append("-[Linked," + this.relationships.get(i).getId() +
//                // "]->");
//                sb.append("-[" + this.rels.get(i) + "]-");
//            }
//            sb.append("(" + this.nodes.get(i) + ")");
//        }
//
//        sb.append(",[");
//        for (double d : this.costs) {
//            sb.append(" " + d);
//        }
//        sb.append("]");
//        return sb.toString();
//    }

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
        if (o_path.endNode != endNode || o_path.startNode != startNode) {
            return false;
        }

        for (int i = 0; i < costs.length; i++) {
            if (o_path.costs[i] != costs[i]) {
                return false;
            }
        }

//        if (!o_path.nodes.equals(this.nodes) || !o_path.rels.equals(this.rels)) {
//            return false;
//        }
        return true;
    }

//    public Long getLongObject_Node(long id) {
//        Long id_obj = new Long(id);
//        Long Lobj;
//        if (!constants.accessedNodes.containsKey(id_obj)) {
//            Lobj = new Long(id);
//            constants.accessedNodes.put(id_obj, Lobj);
//        } else {
//            Lobj = constants.accessedNodes.get(id_obj);
//        }
//
//        return Lobj;
//    }
//
//
//    public Long getLongObject_Edge(long id) {
//        Long id_obj = new Long(id);
//        Long Lobj;
//        if (!constants.accessedEdges.containsKey(id_obj)) {
//            Lobj = new Long(id);
//            constants.accessedEdges.put(id_obj, Lobj);
//        } else {
//            Lobj = constants.accessedEdges.get(id_obj);
//        }
//
//        return Lobj;
//    }

    public boolean isDummyPath() {
        for (int i = 1; i < this.costs.length; i++) {
            if (this.costs[i] != 0) {
                return false;
            }
        }
        return true;
    }

//    public boolean hasCycle() {
//        for (int i = 0; i < nodes.length ; i++) {
//            if (this.endNode == nodes[i]) {
//                return true;
//            }
//        }
//        return false;
//    }
}
