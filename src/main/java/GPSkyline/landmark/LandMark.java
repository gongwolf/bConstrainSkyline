package GPSkyline.landmark;

import neo4jTools.BNode;
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
        this.Graph_path = "/home/gqxwolf/mydata/projectData/testGraph" + graph_size + "_" + degree + "/data/";
    }

    public void buildIndex() {
        try {
            String lm_index = this.Graph_path + "landmark";
            File lFile = new File(lm_index);
            System.out.println(lm_index);
            FileUtils.deleteDirectory(lFile);
            File l_from_File = new File(lm_index + "/from");
            File l_to_File = new File(lm_index + "/to");

            lFile.mkdirs();
            l_from_File.mkdirs();
            l_to_File.mkdirs();


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
        connector n = new connector("/home/gqxwolf/neo4j323/testdb" + graph_size + "_" + degree + "/databases/graph.db");

        String lm_index = this.Graph_path + "landmark/";
        long pos = 0;


        n.startDB();
        GraphDatabaseService graphdb = n.getDBObject();
        path fakePath = null;
        Node Source;
        Node Destination;
        try (Transaction tx = graphdb.beginTx()) {
            for (long i = 0; i < graph_size; i++) {
                Source = graphdb.getNodeById(i);
                if (fakePath == null) {
                    fakePath = new path(Source);
                }

                for (long lm : this.landmarks) {
                    RandomAccessFile file = new RandomAccessFile(lm_index + "/to/" + lm + ".lmk", "rw");
                    file.seek(pos);
                    Destination = graphdb.getNodeById(lm);
//                    System.out.println(i + " " + lm);
                    file.writeLong(i);
                    for (String propertyName : fakePath.propertiesName) {
                        PathFinder<WeightedPath> finder = GraphAlgoFactory
                                .dijkstra(PathExpanders.forTypeAndDirection(Line.Linked, Direction.OUTGOING), propertyName);
                        WeightedPath paths = finder.findSinglePath(Source, Destination);
                        if (paths == null) {
                            for (String p : fakePath.propertiesName) {
//                                System.out.println(p + " " + -1);
                                file.writeDouble(-1);
                            }
                            break;
                        } else {
                            file.writeDouble(paths.weight());
//                            System.out.println(propertyName + " " + paths.weight());
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


        try (Transaction tx = graphdb.beginTx()) {
            for (long lm : this.landmarks) {
                RandomAccessFile file = new RandomAccessFile(lm_index + "/from/" + lm + ".lmk", "rw");
                for (long i = 0; i < graph_size; i++) {
                    Source = graphdb.getNodeById(i);
                    Destination = graphdb.getNodeById(lm);
                    file.writeLong(i);
                    for (String propertyName : fakePath.propertiesName) {
                        PathFinder<WeightedPath> finder = GraphAlgoFactory
                                .dijkstra(PathExpanders.forTypeAndDirection(Line.Linked, Direction.OUTGOING), propertyName);
                        WeightedPath paths = finder.findSinglePath(Destination, Source);
                        if (paths == null) {
                            for (String p : fakePath.propertiesName) {
                                file.writeDouble(-1);
                            }
                            break;
                        } else {
                            file.writeDouble(paths.weight());
                        }
                    }
                }
                file.close();
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
