package BaseLine;

import java.util.HashMap;

public class constants {
    public static final int path_dimension = 4; //1(edu_dis)+3(road net work attrs)+3(static node attrs);

    public static HashMap<Long,Long> accessedNodes = new HashMap<>();
    public static HashMap<Long, Long> accessedEdges = new HashMap<>();


    public static void print(double[] costs) {
        System.out.print("[");
        for (double c : costs) {
            System.out.print(c+" ");
        }
        System.out.println("]");
    }
}
