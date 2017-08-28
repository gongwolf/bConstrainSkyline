package Pindex.inblockSkyline;

import Pindex.path;

import java.util.ArrayList;

public class Portal {
    double[] lowerBound = new double[3];
    ArrayList<path> skylinPaths = new ArrayList<>();


    public void addToSkylineResult(path np) {
        int i = 0;
        if (skylinPaths.isEmpty()) {
            this.skylinPaths.add(np);
        } else {
            boolean alreadyinsert = false;
            for (; i < skylinPaths.size(); ) {
                if (checkDominated(skylinPaths.get(i).getCosts(), np.getCosts())) {
                    if (alreadyinsert && i != this.skylinPaths.size() - 1) {
                        this.skylinPaths.remove(this.skylinPaths.size() - 1);
//                        this.removedPath++;
                    }
                    break;
                } else {
                    if (checkDominated(np.getCosts(), skylinPaths.get(i).getCosts())) {
                        this.skylinPaths.remove(i);
                    } else {
                        i++;
                    }
                    if (!alreadyinsert) {
                        this.skylinPaths.add(np);
                        alreadyinsert = true;
                    }

                }
            }
        }
    }

    private boolean checkDominated(double[] costs, double[] estimatedCosts) {
        int numberOfLessThan = 0;
        for (int i = 0; i < costs.length; i++) {
//            double c = (double) Math.round(costs[i] * 1000000d) / 1000000d;
//            double e = (double) Math.round(estimatedCosts[i] * 1000000d) / 1000000d;
            double c = costs[i];
            double e = estimatedCosts[i];
            if (c > e) {
                return false;
            }

//            if (numberOfLessThan == 0 && c < e) {
//                numberOfLessThan = 1;
//            }
        }
//        if (numberOfLessThan == 0) {
//            return false;
//        } else {
        return true;
//        }
    }

}
