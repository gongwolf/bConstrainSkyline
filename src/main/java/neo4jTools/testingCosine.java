package neo4jTools;

public class testingCosine {
    public static void main(String args[]) {
        double[] x = new double[]{-0.5, -0.5};
        double[] y = new double[]{1,0};


        double r1 = x[0]*y[0]+x[1]*y[1];
        double r2 = Math.sqrt(x[0]*x[0]+x[1]*x[1]);
        double r3 = Math.sqrt(y[0]*y[0]+y[1]*y[1]);
        System.out.println(r1/(r2*r3));
        System.out.println(Math.toDegrees(Math.acos(r1/(r2*r3))));
    }
}
