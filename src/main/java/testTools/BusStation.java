package testTools;

import javafx.util.Pair;

import java.util.HashSet;

public class BusStation {
    long id;
    double[] center = new double[2];
    HashSet<Pair<Double, Double>> list = new HashSet<>();

    public BusStation(long id) {
        this.id = id;
    }

    public void recalculateCenter() {
        int n = list.size();

        double sum_latitude = 0;
        double sum_longitude = 0;

        for (Pair<Double, Double> p : this.list) {
            sum_latitude += p.getKey();
            sum_longitude += p.getValue();
        }

        this.center[0] = sum_latitude / n;
        this.center[1] = sum_longitude / n;
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append(id).append(" ").append(center[0]).append(" ").append(center[1]);
        return sb.toString();
    }
}
