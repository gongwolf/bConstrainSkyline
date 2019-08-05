package GPSkyline.BFS;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Direction;

import java.util.ArrayList;
import java.util.Iterator;
import javafx.util.Pair;
import neo4jTools.*;

public class myNode {
    public String id;
    Node current;
    Node startNode;
    ArrayList<path> subRouteSkyline = new ArrayList<>(500);
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

    public myNode(Node startNode, Node current, boolean flag) {
        this.current = current;
        this.startNode = startNode;
        this.id = String.valueOf(this.current.getId());
        this.setEduDist();
        path p = new path(startNode, startNode);
        propertiesName = new ArrayList<>(p.getPropertiesName());
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

    public int[] addToSkylineResult(path np) {
        int removedPath = 0;
        int insertedPath = 0;
        int i = 0;
        if (this.subRouteSkyline.isEmpty()) {
            insertedPath++;
            this.subRouteSkyline.add(np);
        } else if (this.subRouteSkyline.get(0).getCosts()[0] == 0 && this.subRouteSkyline.size() == 1) {
            this.subRouteSkyline.remove(0);
            this.subRouteSkyline.add(np);
            removedPath++;
            insertedPath++;
        } else {
            boolean alreadyinsert = false;
            boolean needToRemove = false;
            for (; i < subRouteSkyline.size();) {
                if (checkDominated(subRouteSkyline.get(i).getCosts(), np.getCosts())) {
                    // if (alreadyinsert && i != this.subRouteSkyline.size() -
                    // 1) {
                    if (alreadyinsert) {
                        removedPath++;
                        needToRemove = true;
                    }
                    break;
                } else {
                    if (checkDominated(np.getCosts(), subRouteSkyline.get(i).getCosts())) {
                        this.subRouteSkyline.remove(i);
                        removedPath++;
                    } else {
                        i++;
                    }
                    if (!alreadyinsert) {
                        insertedPath++;
                        alreadyinsert = true;
                    }
                }
            }

            if (!needToRemove && alreadyinsert) {
                this.subRouteSkyline.add(np);
            }
        }
        int[] result = new int[] { insertedPath, removedPath };
        return result;
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
        double result = 0.0;
        int p_index = this.propertiesName.indexOf(property_type);
        result = lowerBound[p_index];
        return result;
    }

    public void setCostFromSource(String property_type, double value) {
        int p_index = this.propertiesName.indexOf(property_type);
        lowerBound[p_index] = value;
    }

    public ArrayList<Pair<myNode, Double>> getNeighbor(String property_type) {
        ArrayList<Pair<myNode, Double>> result = new ArrayList<>();
        Iterable<Relationship> rels = this.current.getRelationships(Line.Linked, Direction.INCOMING);
        Iterator<Relationship> rel_Iter = rels.iterator();
        while (rel_Iter.hasNext()) {
            Relationship rel = rel_Iter.next();
            Node nextNode = rel.getStartNode();
            Double cost = Double.parseDouble(rel.getProperty(property_type).toString());
            myNode mynextNode = new myNode(this.startNode, nextNode, false);
            Pair<myNode, Double> pair = new Pair<>(mynextNode, cost);
            result.add(pair);
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
