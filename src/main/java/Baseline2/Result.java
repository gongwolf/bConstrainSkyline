package Baseline2;

import aRtree.Data;

public class Result implements Comparable<Result> {
    public Data start, end;
    public path p;
    public double[] costs = new double[constants.path_dimension + 3];


    public Result(Data queryD, Data destination, double[] c, path np) {
        this.start = queryD;
        this.end = destination;
        System.arraycopy(c, 0, this.costs, 0, c.length);
        p = np;
    }

    public Result(Data queryD, Object o, double[] c, path np) {
        this.start = queryD;
        this.end = null;
        System.arraycopy(c, 0, this.costs, 0, c.length);
        p = np;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Result)) {
            return false;
        }

        Result or = (Result) obj;

        if (!start.equals(or.start)) {
            return false;
        } else if (!end.equals(or.end)) {
            return false;
        } else if (!or.p.equals(this.p)) {
            return false;
        } else {
            return true;
        }

    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append(this.start.getPlaceId()).append(",").append(this.start.getData()[0]).append(",").append(this.start.getData()[1]).append(",");
        sb.append(this.end.getPlaceId()).append(",").append(this.end.getData()[0]).append(",").append(this.end.getData()[1]).append(",");
        sb.append(this.p);
        sb.append(",[");
        for (double c : this.costs) {
            sb.append(c).append(",");
        }
        sb.substring(0,sb.lastIndexOf(","));
        sb.append("]");
        return sb.toString();
    }


    @Override
    public int compareTo(Result o) {
        if (o.costs[0] == this.costs[0]) {
            return 0;
        } else if (o.costs[0] > this.costs[0]) {
            return -1;
        } else {
            return 1;
        }
    }
}
