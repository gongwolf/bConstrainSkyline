package unDirectionalGraph;

import neo4jTools.Line;
import neo4jTools.connector;
import org.apache.commons.io.FileUtils;
import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphalgo.WeightedPath;
import org.neo4j.graphdb.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Random;
import java.util.TreeSet;

public class LandMark {
    private final Random r;
    long landmarkNumber;
    long graph_size = 0;
    String degree = "";
    TreeSet<Long> landmarks = new TreeSet<>();
    String Graph_path = "";

    public LandMark(long graph_size, String degree, long landmarkNumber) {
        this.graph_size = graph_size;
        this.degree = degree;
        this.landmarkNumber = landmarkNumber;
        r = new Random(System.nanoTime());
        this.Graph_path = "/home/gqxwolf/mydata/projectData/un_testGraph" + graph_size + "_" + degree + "/data/";
    }

    public void buildIndex() {
        try {
            String lm_index = this.Graph_path + "landmark";
            File lFile = new File(lm_index);
            System.out.println(lm_index);
            FileUtils.deleteDirectory(lFile);

            lFile.mkdirs();


            System.out.println("deleted old landmark index folder");
            randomChoseLandMark();
            System.out.println("-----------------------------------");
            System.out.println(landmarks.size());
            for (long lm : landmarks) {
                System.out.print(lm + ",");
            }
            System.out.println("\n-----------------------------------");

            buildLandMark();


        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void buildLandMark() {
        connector n = new connector("/home/gqxwolf/neo4j323/test_un_db" + graph_size + "_" + degree + "/databases/graph.db");
        String lm_index = this.Graph_path + "landmark/";
        long pos = 0;


        n.startDB();
        GraphDatabaseService graphdb = n.getDBObject();
        path fakePath = null;
        Node Source;
        Node Destination;
        try (Transaction tx = graphdb.beginTx()) {
            for (long i = 0; i < graph_size; i++) {

                if (i % 100 == 0) {
                    System.out.println(i + "........");
                }

                Source = graphdb.getNodeById(i);
                if (fakePath == null) {
                    fakePath = new path(Source);
                }

                for (long lm : this.landmarks) {
                    RandomAccessFile file = new RandomAccessFile(lm_index + lm + ".lmk", "rw");
                    file.seek(pos);
                    Destination = graphdb.getNodeById(lm);
                    file.writeLong(i);
                    for (String propertyName : fakePath.propertiesName) {
                        PathFinder<WeightedPath> finder = GraphAlgoFactory
                                .dijkstra(PathExpanders.forTypeAndDirection(Line.Linked, Direction.BOTH), propertyName);
                        WeightedPath paths = finder.findSinglePath(Source, Destination);
                        if (paths == null) {
                            for (String p : fakePath.propertiesName) {
                                file.writeDouble(-1);
                            }
                            break;
                        } else {
                            file.writeDouble(paths.weight());
                        }
                    }
                    file.close();
                }
                pos = pos + 32;

            }
            tx.success();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        n.shutdownDB();
    }

    private void randomChoseLandMark() {
        while (landmarks.size() != this.landmarkNumber) {
            long landmarkId = getRandomNumberInRange(0, graph_size - 1);
            while (landmarks.contains(landmarkId)) {
                landmarkId = getRandomNumberInRange(0, graph_size - 1);
            }
            landmarks.add(landmarkId);
        }
    }

    private int getRandomNumberInRange(long min, long max) {
        if (min >= max) {
            throw new IllegalArgumentException("max must be greater than min");
        }
        return (int) (this.r.nextInt((int) ((max - min) + 1)) + min);
    }
}
