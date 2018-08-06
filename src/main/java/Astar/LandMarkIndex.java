package Astar;

import neo4jTools.Line;
import neo4jTools.connector;
import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphalgo.WeightedPath;
import org.neo4j.graphdb.*;
import org.neo4j.io.fs.FileUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;

public class LandMarkIndex {

    private String graphDB_path=null;
    String graphpath;
    private  Random r=null;
    int graph_size;
    int degree;
    int landmarkNumber;
    private ArrayList<Integer> refNodes;

    private ArrayList<String> property_names;
    public String landmark_folder;
//    private ArrayList<Integer> RefNodes;


    public LandMarkIndex(int graph_size, int degree, int landmarkNumber) {
        this.graph_size = graph_size;
        this.degree = degree;
        this.landmarkNumber = landmarkNumber;
        r = new Random(System.nanoTime());
        this.graphpath = "/home/gqxwolf/mydata/projectData/testGraph" + graph_size + "_" + degree + "/data/";
        this.graphDB_path = "/home/gqxwolf/neo4j334/testdb" + this.graph_size + "_" + this.degree + "/databases/graph.db";
        this.landmark_folder = graphpath + "landmarks/";
        System.out.println(this.graphpath);
    }

    public LandMarkIndex(int graph_size, String degree) {
        this.graph_size = graph_size;
        this.graphpath = "/home/gqxwolf/mydata/projectData/testGraph" + graph_size + "_" + degree + "/data/";
        this.landmark_folder = graphpath + "landmarks/";
        System.out.println(this.graphpath);
        this.getRefNodes();
    }


    public void buildIndex() {
        try {
            FileUtils.deleteRecursively(new File(landmark_folder));
            System.out.println("delete    " + landmark_folder);
            File landmark_parent_folder = new File(landmark_folder);
            landmark_parent_folder.mkdirs();
        } catch (IOException e) {
            e.printStackTrace();
        }

        connector conn = new connector(this.graphDB_path);
        conn.startDB();
        this.property_names = new ArrayList<>(connector.propertiesName);
        for (String p : property_names) {
            System.out.println(p);
        }
//        System.out.println(conn.getNumberofNodes()+"  "+conn.getNumberofEdges());
        conn.shutdownDB();


        //choose reference node
        this.refNodes = randomChooseRefNodes();
        System.out.println("=======================================");
        for (int ref_node : this.refNodes) {
            System.out.println(ref_node);
            CreateLandMarkIndex(ref_node);
//            break;
        }
        System.out.println("=======================================");


    }

    private void CreateLandMarkIndex(int ref_node) {
        connector conn = new connector(this.graphDB_path);
        conn.startBD_without_getProperties();
        GraphDatabaseService graphdb = conn.getDBObject();
//        for (String p : property_names) {
//            System.out.println(p);
//        }


        try (Transaction tx = graphdb.beginTx()) {
            RandomAccessFile file = new RandomAccessFile(this.landmark_folder + ref_node + ".lmk", "rw");
            Node Source = graphdb.getNodeById(ref_node);
            Node Destination;

            for (int dest_node_id = 0; dest_node_id < this.graph_size; dest_node_id++) {
                if (dest_node_id != ref_node) {
                    Destination = graphdb.getNodeById(dest_node_id);
                    for (String pname : this.property_names) {
                        PathFinder<WeightedPath> finder = GraphAlgoFactory
                                .dijkstra(PathExpanders.forTypeAndDirection(Line.Linked, Direction.BOTH), pname);
                        WeightedPath paths = finder.findSinglePath(Source, Destination);

                        if (paths == null) {
                            file.writeDouble(-2);
                        } else {
                            file.writeDouble(paths.weight());
                        }
                    }

                } else {
                    for (String pname : this.property_names) {
                        file.writeDouble(-1);
                    }
                }
            }

            file.close();
            tx.success();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }


        conn.shutdownDB();
    }

    private ArrayList<Integer> randomChooseRefNodes() {
        ArrayList<Integer> refNodes = new ArrayList<>();
        for (int i = 0; i < this.landmarkNumber; ) {
            int r_node_id = getRandomNumberInRange_int(0, this.graph_size);
            if (!refNodes.contains(refNodes)) {
                refNodes.add(r_node_id);
                i++;
            }
        }
        Collections.sort(refNodes);
        return refNodes;
    }

    public int getRandomNumberInRange_int(int min, int max) {
        if (min >= max) {
            throw new IllegalArgumentException("max must be greater than min");
        }
        Random r = new Random();
        return r.nextInt((max - min) + 1) + min;
    }


    public static void main(String args[]) {
        LandMarkIndex lmi = new LandMarkIndex(1000, 4, 10);
        lmi.buildIndex();
        lmi.getRefNodes();
        lmi.readLandMark(1, 83);
    }

    private void getRefNodes() {
        File f = new File(this.landmark_folder);
        this.refNodes = new ArrayList<>();
        for (File lmF : f.listFiles()) {
//            System.out.println(lmF.getName().split("\\.")[0]);
            String ref_id = lmF.getName().split("\\.")[0];
            this.refNodes.add(Integer.parseInt(ref_id));
        }
    }


    public double[] readLandMark(long snode, int enode) {
        double lowerbound[] = new double[3];
        for (int i = 0; i < lowerbound.length; i++) {
            lowerbound[i] = Double.NEGATIVE_INFINITY;
        }
        for (int landmark : this.refNodes) {
            try {
                RandomAccessFile file = new RandomAccessFile(this.landmark_folder + landmark + ".lmk", "r");
                for (int i = 0; i < lowerbound.length; i++) {
                    file.seek(snode * 24 + 8 * i);
                    double sTol = file.readDouble();

                    file.seek(enode * 24 + 8 * i);
                    double lToe = file.readDouble();

                    double abs_value = Math.abs(sTol - lToe);


                    if (sTol == -1) {
                        abs_value = Math.abs(lToe);
                    } else if (lToe == -1) {
                        abs_value = Math.abs(sTol);
                    }



                    if (abs_value > lowerbound[i] && sTol != -2 && lToe != -2) {
                        lowerbound[i] = abs_value;
                    }

//                    System.out.print(sTol+" "+lToe+" "+abs_value);
//
//
//                    System.out.println();


                }
//                System.out.println("\n-------------------------------------------_");


                file.close();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

//        System.out.println(lowerbound[0] + " " + lowerbound[1] + " " + lowerbound[2] + " ");


        return lowerbound;
    }
}
