package GPSkyline.landmark;

import neo4jTools.Line;
import neo4jTools.connector;
import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphalgo.WeightedPath;
import org.neo4j.graphdb.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.TreeSet;

public class landmarkValueTest {
    TreeSet<Long> landmarks = new TreeSet<>();
    File f = new File("/home/gqxwolf/mydata/projectData/testGraph4000_5/data/landmark");
    File to_f = new File(f.getAbsolutePath() + "/to");
    File from_f = new File(f.getAbsolutePath() + "/from");

    public static void main(String args[]) {
        landmarkValueTest lt = new landmarkValueTest();
        lt.runtest();
    }

    private void runtest() {
        long sid = 2792;
        long did = 2645;


        for (File fl : to_f.listFiles()) {
            String filename = fl.getName();
            filename = filename.substring(0, filename.indexOf("."));
            landmarks.add(Long.parseLong(filename));
        }


        getShortestPath(sid, did);
        System.out.println("=============================");
        getNeo4jDistance(sid, did);
        System.out.println("=============================");
        getLandMarkIndexValue(sid, did);
        System.out.println("=============================");
        getLandMarkUpperBound(sid, did);

    }

    private void getShortestPath(long sid, long did) {
        connector n = new connector("/home/gqxwolf/neo4j323/testdb4000_5/databases/graph.db");
        n.startDB();
        GraphDatabaseService graphdb = n.getDBObject();
        Node Source;
        Node Destination;
        try (Transaction tx = graphdb.beginTx()) {
            Source = graphdb.getNodeById(sid);
            Destination = graphdb.getNodeById(did);
            path fakePath = new path(Source);
            System.out.println(sid + " - >" + did);
            for (String propertyName : fakePath.propertiesName) {
                PathFinder<WeightedPath> finder = GraphAlgoFactory
                        .dijkstra(PathExpanders.forTypeAndDirection(Line.Linked, Direction.OUTGOING), propertyName);
                WeightedPath paths = finder.findSinglePath(Source, Destination);
                if (paths == null) {
                    System.out.print(propertyName + " " + -1 + " ");
                } else {
//                    System.out.println(paths);
                    System.out.print(propertyName + " " + paths.weight() + " ");
                }
            }
            System.out.println("\n------------------------------------");


            tx.success();
        }
        n.shutdownDB();
    }

    private void getLandMarkIndexValue(long sid, long did) {
        double estimatedCosts[] = new double[3];


        long spos = 32 * sid + 8;
        long dpos = 32 * did + 8;

        for (File to : to_f.listFiles()) {
            try {
                RandomAccessFile to_f = new RandomAccessFile(to, "r");
                to_f.seek(spos);
                double td1 = to_f.readDouble();
                if (td1 != -1) {
                    double td2 = to_f.readDouble();
                    double td3 = to_f.readDouble();

                    System.out.println(sid + " - > " + to.getName().replace(".lmk", "") + "  " + td1 + " " + td2 + " " + td3);

                    String ffff = this.from_f + "/" + to.getName();
                    RandomAccessFile from_f = new RandomAccessFile(ffff, "r");
                    from_f.seek(dpos);
                    double fd1 = from_f.readDouble();
                    double fd2 = from_f.readDouble();
                    double fd3 = from_f.readDouble();

                    //distance form landmark to source node
                    from_f.seek(spos);
                    double f_l_to_s_1 = from_f.readDouble();
                    double f_l_to_s_2 = from_f.readDouble();
                    double f_l_to_s_3 = from_f.readDouble();
                    if (fd1 != -1 && f_l_to_s_1 != -1) {
                        System.out.println(to.getName().replace(".lmk", "") + " - > " + did + "  " + fd1 + " " + fd2 + " " + fd3);
                        System.out.println(to.getName().replace(".lmk", "") + " - > " + sid + "  " + f_l_to_s_1 + " " + f_l_to_s_2 + " " + f_l_to_s_3);

                        //lower bound of the distance from s->l->t
                        double a_0_0 = Math.abs(td1 - fd1);
                        double a_0_1 = Math.abs(td2 - fd2);
                        double a_0_2 = Math.abs(td3 - fd3);

                        //lower bound of the distance l->s -- l->t
                        double a_1_0 = Math.abs(f_l_to_s_1 - fd1);
                        double a_1_1 = Math.abs(f_l_to_s_2 - fd2);
                        double a_1_2 = Math.abs(f_l_to_s_3 - fd3);


                        //chose the maximum of the minimum one of the lower bound
                        double a0, a1, a2;
                        a0 = a_0_0 > a_1_0 ? a_1_0 : a_0_0;
                        a1 = a_0_1 > a_1_1 ? a_1_1 : a_0_1;
                        a2 = a_0_2 > a_1_2 ? a_1_2 : a_0_2;

                        if (a0 > estimatedCosts[0]) {
                            estimatedCosts[0] = a0;
                        }
                        if (a1 > estimatedCosts[1]) {
                            estimatedCosts[1] = a1;
                        }
                        if (a2 > estimatedCosts[2]) {
                            estimatedCosts[2] = a2;
                        }
                    }
                    from_f.close();
                }
                to_f.close();
                System.out.println(estimatedCosts[0] + " " + estimatedCosts[1] + " " + estimatedCosts[2]);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    private void getLandMarkUpperBound(long sid, long did) {
        double estimatedCosts[] = new double[3];

        for (int i = 0; i < estimatedCosts.length; i++) {
            estimatedCosts[i] = Double.POSITIVE_INFINITY;
        }

        long spos = 32 * sid + 8;
        long dpos = 32 * did + 8;

        for (File to : to_f.listFiles()) {
            try {
                RandomAccessFile to_f = new RandomAccessFile(to, "r");
                to_f.seek(spos);
                double td1 = to_f.readDouble();
                if (td1 != -1) {
                    double td2 = to_f.readDouble();
                    double td3 = to_f.readDouble();

                    System.out.println(sid + " - > " + to.getName().replace(".lmk", "") + "  " + td1 + " " + td2 + " " + td3);

                    String ffff = this.from_f + "/" + to.getName();
                    RandomAccessFile from_f = new RandomAccessFile(ffff, "r");
                    from_f.seek(dpos);
                    double fd1 = from_f.readDouble();
                    if (fd1 != -1) {
                        double fd2 = from_f.readDouble();
                        double fd3 = from_f.readDouble();

                        System.out.println(to.getName().replace(".lmk", "") + " - > " + did + "  " + fd1 + " " + fd2 + " " + fd3);

                        double a0 = Math.abs(td1 + fd1);
                        double a1 = Math.abs(td2 + fd2);
                        double a2 = Math.abs(td3 + fd3);

                        if (a0 < estimatedCosts[0]) {
                            estimatedCosts[0] = a0;
                        }
                        if (a1 < estimatedCosts[1]) {
                            estimatedCosts[1] = a1;
                        }
                        if (a2 < estimatedCosts[2]) {
                            estimatedCosts[2] = a2;
                        }


                    }
                    from_f.close();
                }
                to_f.close();
                System.out.println(estimatedCosts[0] + " " + estimatedCosts[1] + " " + estimatedCosts[2]);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void getNeo4jDistance(long sid, long did) {
        connector n = new connector("/home/gqxwolf/neo4j323/testdb4000_5/databases/graph.db");
        n.startDB();
        GraphDatabaseService graphdb = n.getDBObject();
        path fakePath = null;
        Node Source;
        Node Destination;
        try (Transaction tx = graphdb.beginTx()) {
            Source = graphdb.getNodeById(sid);
            if (fakePath == null) {
                fakePath = new path(Source);
            }

            for (long lm : this.landmarks) {
                Destination = graphdb.getNodeById(lm);
                System.out.println(sid + " - >" + lm);
                for (String propertyName : fakePath.propertiesName) {
                    PathFinder<WeightedPath> finder = GraphAlgoFactory
                            .dijkstra(PathExpanders.forTypeAndDirection(Line.Linked, Direction.OUTGOING), propertyName);
                    WeightedPath paths = finder.findSinglePath(Source, Destination);
                    if (paths == null) {
                        System.out.print(propertyName + " " + -1 + " ");
                    } else {
                        System.out.print(propertyName + " " + paths.weight() + " ");
                    }
                }
                System.out.println();
            }

            tx.success();
        }


        try (Transaction tx = graphdb.beginTx()) {
            for (long lm : this.landmarks) {
                Source = graphdb.getNodeById(did);
                Destination = graphdb.getNodeById(lm);
                System.out.println(lm + "  - > " + did);
                for (String propertyName : fakePath.propertiesName) {
                    PathFinder<WeightedPath> finder = GraphAlgoFactory
                            .dijkstra(PathExpanders.forTypeAndDirection(Line.Linked, Direction.OUTGOING), propertyName);
                    WeightedPath paths = finder.findSinglePath(Destination, Source);
                    if (paths == null) {
                        System.out.print(propertyName + " " + -1 + " ");
                    } else {
                        System.out.print(propertyName + " " + paths.weight() + " ");
//                        System.out.println(paths);
                    }
                }
                System.out.println();
            }
            tx.success();
        }


        n.shutdownDB();
    }


}
