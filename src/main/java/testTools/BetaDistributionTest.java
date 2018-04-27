package testTools;

import org.apache.commons.math3.distribution.BetaDistribution;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class BetaDistributionTest {
    public static void main(String args[]) {
        BetaDistribution bt = new BetaDistribution(1.89, 19);
        System.out.println(bt.density(0.5));
        System.out.println(bt.density(0.2));

        double[] s = bt.sample(300000);

        HashMap<Integer, Integer> rs = new HashMap<>();
        for (double d : s) {
            int id = (int)Math.floor(d*100);

            if (rs.containsKey(id)) {
                rs.put(id, rs.get(id) + 1);
            } else {
                rs.put(id, 1);
            }
        }

        TreeMap<Integer, Integer> map = new TreeMap<>();
        map.putAll(rs);
        for (Map.Entry<Integer, Integer> r : map.entrySet()) {
            System.out.println(r.getKey() + " " + r.getValue());
        }
    }
}
