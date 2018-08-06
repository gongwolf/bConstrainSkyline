package Astar;

import javafx.util.Pair;
import neo4jTools.connector;
import org.neo4j.graphdb.*;
import scala.Int;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class NearbyObjects {

    private final String dataPath;
    private final double range;
    private final int hotels_num;
    private final int graph_size;
    private final String degree;
    private final String graphDB_path;
    String home_folder = System.getProperty("user.home");
    HashMap<Integer, Pair<Double, Double>> hotels_locations_info;
    HashMap<Integer, double[]> hotels_attributes_info;

    public NearbyObjects(int graph_size, String degree, double range, int hotels_num) {
        this.range = range;
        this.hotels_num = hotels_num;
        this.graph_size = graph_size;
        this.degree = degree;
        this.dataPath = home_folder + "/shared_git/bConstrainSkyline/data/staticNode_" + this.graph_size + "_" + this.degree + "_" + range + "_" + hotels_num + ".txt";
        this.graphDB_path = "/home/gqxwolf/neo4j334/testdb" + this.graph_size + "_" + this.degree + "/databases/graph.db";

    }


    public int getAllHotels() {
        hotels_locations_info = new HashMap<>();
        hotels_attributes_info = new HashMap<>();
        int result = 0;
        File f = new File(this.dataPath);
        BufferedReader b = null;
        try {
            b = new BufferedReader(new FileReader(f));
            String readLine = "";

            while (((readLine = b.readLine()) != null)) {
                String[] infos = readLine.split(",");
                int id = Integer.valueOf(infos[0]);
                double latitude = Double.valueOf(infos[1]);
                double longitude = Double.valueOf(infos[2]);
                double valueofD1 = Double.valueOf(infos[3]);
                double valueofD2 = Double.valueOf(infos[4]);
                double valueofD3 = Double.valueOf(infos[5]);
                hotels_attributes_info.put(id, new double[]{valueofD1, valueofD2, valueofD3});
                hotels_locations_info.put(id, new Pair<>(latitude, longitude));
                result++;
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }


    public HashMap<Integer, ArrayList<Integer>> getNearbyList() {
        HashMap<Integer, ArrayList<Integer>> result = new HashMap<>(); //hotels_id d --> List of the hotels ids within the range from d (ArrayList<>)

        getAllHotels();

        connector conn = new connector(this.graphDB_path);
        conn.startBD_without_getProperties();
        GraphDatabaseService graphdb = conn.getDBObject();


        for (int i = 0; i < this.hotels_num; i++) {
            ArrayList<Integer> hotels_in_range = new ArrayList<>();

            double i_lat = this.hotels_locations_info.get(i).getKey();
            double i_longt = this.hotels_locations_info.get(i).getValue();


            try (Transaction tx = graphdb.beginTx()) {
                ResourceIterable<Node> r = graphdb.getAllNodes();
                ResourceIterator<Node> node_iter = r.iterator();

                while (node_iter.hasNext()) {
                    Node n = node_iter.next();
                    int id = Integer.valueOf((String) n.getProperty("name"));
                    double d_lat = (double) n.getProperty("lat");
                    double d_longt = (double) n.getProperty("log");

                    double d = Math.sqrt(Math.pow(i_lat - d_lat, 2) + Math.pow(i_longt - d_longt, 2));
                    if (d <= this.range) {
                        hotels_in_range.add(id);
                    }
                }


                tx.success();
            }

            Collections.sort(hotels_in_range);
            result.put(i, hotels_in_range);
//            System.out.println(i + "  " + hotels_in_range.size());
        }

        conn.shutdownDB();


        return result;
    }

    public static void main(String args[]) {
        NearbyObjects nb = new NearbyObjects(1000, "4", 14, 1000);
        nb.getNearbyList();
    }
}
