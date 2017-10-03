package GraphPartition;

import neo4jTools.Line;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;


public class myNode {
    public String id;
    Node current;
    Node startNode;
    ArrayList<path> subRouteSkyline = new ArrayList<>();
    ArrayList<path> processedPaths = new ArrayList<>();
    ArrayList<Relationship> expansionRels = new ArrayList<>();
    boolean processed_flag = false;
    boolean processed_lower_bound = false;
    boolean processed[];
    boolean visited = false;
    double EduDist = 0.0;
    double[] lowerBound = null;
    String ComeFromRel = null;
    String ComeFromNode = null;
    public ArrayList<String> propertiesName = null;
    int degree;
    boolean inqueue = false;
    public double priority;
    public HashMap<String, path> shortestPaths = new HashMap<>();

    /**
     * @param startNode the start node of the query, that used to show the information is from this start node to current node.
     * @param current   The current node
     * @param flag      If it is a start node when we tries put it into a queue.
     */
    public myNode(Node startNode, Node current, boolean flag) {
        this.current = current;
        this.startNode = startNode;
        this.id = String.valueOf(this.current.getId());
        this.setEduDist();
        path p = new path(startNode, startNode);
        propertiesName = new ArrayList<String>(p.getPropertiesName());
        this.lowerBound = new double[p.NumberOfProperties];
        this.processed = new boolean[p.NumberOfProperties];
        this.degree = current.getDegree(Direction.OUTGOING);

        if (!flag) {
            for (int i = 0; i < lowerBound.length; i++) {
                lowerBound[i] = Double.POSITIVE_INFINITY;
            }
        }

        if (startNode.equals(current)) {
            this.subRouteSkyline.add(p);
        }

        for (int i = 0; i < processed.length; i++) {
            this.processed[i] = false;
        }

    }

    public myNode(Node startNode, Node current) {
        this.current = current;
        this.startNode = startNode;
        this.id = String.valueOf(this.current.getId());
        this.setEduDist();
        path p = new path(startNode, startNode);
        propertiesName = new ArrayList<String>(p.getPropertiesName());
        this.lowerBound = new double[p.NumberOfProperties];
        this.processed = new boolean[p.NumberOfProperties];

        if (startNode.equals(current)) {
            addToSkylineResult(p);
        } else {
            for (int i = 0; i < lowerBound.length; i++) {
                lowerBound[i] = Double.POSITIVE_INFINITY;
            }
        }

        for (int i = 0; i < processed.length; i++) {
            this.processed[i] = false;
        }

    }

    public myNode(Node startNode, Node current, int xlength) {
        this.current = current;
        this.startNode = startNode;
        this.id = String.valueOf(this.current.getId());
        this.setEduDist();
        this.lowerBound = new double[xlength];
    }

    public void setLowerBound(double[] tmplCosts) {
        for (int i = 0; i < tmplCosts.length; i++) {
            this.lowerBound[i] = tmplCosts[i];
        }

    }

    private void setEduDist() {
        double lats = Double.parseDouble(this.startNode.getProperty("lat").toString());
        double logs = Double.parseDouble(this.startNode.getProperty("log").toString());
        double latd = Double.parseDouble(this.current.getProperty("lat").toString());
        double logd = Double.parseDouble(this.current.getProperty("log").toString());
        double d1 = Math.pow(lats - latd, 2);
        double d2 = Math.pow(logs - logd, 2);
        this.EduDist = Math.sqrt(d1 + d2);
    }

    public void addToSkylineResult(path np) {
        int i = 0;
        if (this.subRouteSkyline.isEmpty()) {
            this.subRouteSkyline.add(np);
        } else {
            boolean alreadyinsert = false;
            boolean needToRemove = false;
            for (; i < subRouteSkyline.size(); ) {
                if (checkDominated(subRouteSkyline.get(i).getCosts(), np.getCosts())) {
                    if (alreadyinsert) {
                        needToRemove = true;
                    }
                    break;
                } else {
                    if (checkDominated(np.getCosts(), subRouteSkyline.get(i).getCosts())) {
                        this.subRouteSkyline.remove(i);
                    } else {
                        i++;
                    }
                    if (!alreadyinsert) {
                        alreadyinsert = true;
                    }
                }
            }

            if (!needToRemove && alreadyinsert) {
                this.subRouteSkyline.add(np);
            }
        }
    }

    private boolean checkDominated(double[] costs, double[] estimatedCosts) {
        int numberOfLessThan = 0;
        for (int i = 0; i < costs.length; i++) {
            //double c = (double) Math.round(costs[i] * 1000000d) / 1000000d;
            //double e = (double) Math.round(estimatedCosts[i] * 1000000d) / 1000000d;
            double c = costs[i];
            double e = estimatedCosts[i];
            if (c > e) {
                return false;
            }

            //if (numberOfLessThan == 0 && c < e) {
            //    numberOfLessThan = 1;
            //}

        }
        //if (numberOfLessThan == 1)
        return true;
        //else
        //    return false;
    }

    public void printSubRouteSkyline() {
        System.out.println("*******************");
        for (path p : this.subRouteSkyline) {
            System.out.println(p);
            System.out.println(printCosts(p.getCosts()));
        }
        System.out.println("*******************");
    }

    public String printCosts(double costs[]) {
        StringBuffer sb = new StringBuffer();
        sb.append("[");
        int i = 0;
        for (; i < costs.length - 1; i++) {
            sb.append(costs[i] + ",");
        }
        sb.append(costs[i] + "]");
        return sb.toString();
    }

    public double getCostFromSource(String property_type) {
        double result;
        int p_index = this.propertiesName.indexOf(property_type);
        result = lowerBound[p_index];
        return result;
    }

    /**
     * set the lower bound of the property, the position of this lower bound is the index of propertiesName.
     *
     * @param property_type the name of the property
     * @param value         the value of the lower bound
     */
    public void setCostFromSource(String property_type, double value) {
        int p_index = this.propertiesName.indexOf(property_type);
        lowerBound[p_index] = value;
    }

    public ArrayList<Relationship> getNeighbor(String property_type) {
        ArrayList<Relationship> result = new ArrayList<>();
        Iterable<Relationship> rels = this.current.getRelationships(Line.Linked, Direction.INCOMING);
        Iterator<Relationship> rel_Iter = rels.iterator();
        while (rel_Iter.hasNext()) {
            Relationship rel = rel_Iter.next();
//            Node nextNode = rel.getEndNode();
//            Double cost = Double.parseDouble(rel.getProperty(property_type).toString());
//            myNode mynextNode = new myNode(this.startNode, nextNode, false);
//            Pair<myNode, Double> pair = new Pair<>(mynextNode, cost);
            result.add(rel);
        }
        return result;
    }

    public ArrayList<Relationship> getAdjNodes(String property_type) {
//        ArrayList<Pair<myNode, Double>> result = new ArrayList<>();
        ArrayList<Relationship> result = new ArrayList<>();
        Iterable<Relationship> rels = this.current.getRelationships(Line.Linked, Direction.OUTGOING);
        Iterator<Relationship> rel_Iter = rels.iterator();
        while (rel_Iter.hasNext()) {
            Relationship rel = rel_Iter.next();
//            Node nextNode = rel.getEndNode();
//            Double cost = Double.parseDouble(rel.getProperty(property_type).toString());
//            myNode mynextNode = new myNode(this.startNode, nextNode, false);
//            Pair<myNode, Double> pair = new Pair<>(mynextNode, cost);
            result.add(rel);
        }
        return result;
    }

    public void setComeFromNode(String id) {
        this.ComeFromNode = id;
    }

    public void setComeFromRel(String id) {
        this.ComeFromRel = id;
    }
}
