package Pindex.vctest;

import org.neo4j.graphdb.Node;

import java.util.ArrayList;

public class myNode {
    public ArrayList<myPath> subRouteSkyline = new ArrayList<>();
    String id;
    public boolean inqueue;

    public myNode(Node s, boolean isSource) {
        this.id = String.valueOf(s.getId());
        if (isSource) {
            myPath initPath = new myPath(s);
            this.subRouteSkyline.add(initPath);
        }
    }

    public void addToSkylineResult(myPath np) {
//        int removedPath = 0;
//        int insertedPath = 0;
        int i = 0;
        if (this.subRouteSkyline.isEmpty()) {
//            insertedPath++;
            this.subRouteSkyline.add(np);
        } else if (this.subRouteSkyline.get(0).getCosts()[0] == 0 && this.subRouteSkyline.size() == 1) {
            this.subRouteSkyline.remove(0);
            this.subRouteSkyline.add(np);
//            removedPath++;
//            insertedPath++;
        } else {
            boolean alreadyinsert = false;
            boolean needToRemove = false;
            for (; i < subRouteSkyline.size(); ) {
                if (checkDominated(subRouteSkyline.get(i).getCosts(), np.getCosts())) {
                    // if (alreadyinsert && i != this.subRouteSkyline.size() -
                    // 1) {
                    if (alreadyinsert) {
//                        removedPath++;
                        needToRemove = true;
                    }
                    break;
                } else {
                    if (checkDominated(np.getCosts(), subRouteSkyline.get(i).getCosts())) {
                        this.subRouteSkyline.remove(i);
//                        removedPath++;
                    } else {
                        i++;
                    }
                    if (!alreadyinsert) {
//                        insertedPath++;
                        alreadyinsert = true;
                    }
                }
            }

            if (!needToRemove && alreadyinsert) {
                this.subRouteSkyline.add(np);
            }
        }
//        int[] result = new int[] { insertedPath, removedPath };
//        return result;
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
}
