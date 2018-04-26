package testTools;

import javafx.util.Pair;
import neo4jTools.connector;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class statistic {

    String graphPath;
    String infoPath;
    String dataPath;

    int graph_size;
    int degree;

    connector conn;
    HashMap<Integer, Pair<Double, Double>> buses;
    HashMap<Integer, Pair<Double, Double>> hotels;


    public statistic(int graph_size, int degree) {
        this.graph_size = graph_size;
        this.degree = degree;
        this.graphPath = "/home/gqxwolf/neo4j334/testdb" + this.graph_size + "_" + this.degree + "/databases/graph.db";
        this.infoPath = "/home/gqxwolf/mydata/projectData/testGraph" + this.graph_size + "_" + this.degree + "/data";
        this.dataPath = "/home/gqxwolf/shared_git/bConstrainSkyline/data/staticNode.txt";


        buses = new HashMap<>();
        readBusData();
        hotels = new HashMap<>();
        readHotels();


//        conn = new connector(this.graphPath);
//        conn.startDB();


    }

    public static void main(String args[]) {
        statistic s = new statistic(2000, 4);
        s.HotelsToBuesWithinRange(10);

//        s.shutdown();
    }

    private void readHotels() {
        try {
            File file = new File(this.dataPath);
            FileReader fileReader = new FileReader(file);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                String[] infos = line.split(",");
                int id = Integer.parseInt(infos[0]);
                double latitude = Double.parseDouble(infos[1]);
                double longitude = Double.parseDouble(infos[2]);

                this.hotels.put(id, new Pair<>(latitude, longitude));
            }
            fileReader.close();
            System.out.println("Contents of file:" + dataPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void readBusData() {
        try {
            String path = this.infoPath + "/NodeInfo.txt";
            File file = new File(path);
            FileReader fileReader = new FileReader(file);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                String[] infos = line.split(" ");
                int id = Integer.parseInt(infos[0]);
                double latitude = Double.parseDouble(infos[1]);
                double longitude = Double.parseDouble(infos[2]);

                this.buses.put(id, new Pair<>(latitude, longitude));
            }
            fileReader.close();
            System.out.println("Contents of file:" + path);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void BusToHotelsWithinRange(int range) {
        HashMap<Integer, Integer> rs = new HashMap<>(); //# of bus stops that have # of hotels in range
        for (Map.Entry<Integer, Pair<Double, Double>> b : this.buses.entrySet()) {

            int b_id = b.getKey();
            double b_lat = b.getValue().getKey();
            double b_log = b.getValue().getValue();

            int counter = 0;

            for (Map.Entry<Integer, Pair<Double, Double>> h : this.hotels.entrySet()) {
                double h_lat = h.getValue().getKey();
                double h_log = h.getValue().getValue();

                double d = Math.sqrt(Math.pow(b_lat - h_lat, 2) + Math.pow(b_log - h_log, 2));
                if (d <= range) {
                    counter++;
                }
            }

            if (rs.containsKey(counter)) {
                rs.put(counter, rs.get(counter) + 1);
            } else {
                rs.put(counter, 1);
            }
        }

        //===========================================================//
        for (Map.Entry<Integer, Integer> r : rs.entrySet()) {
            System.out.println(r.getKey() + " " + r.getValue());
        }

    }



    private void HotelsToBuesWithinRange(int range) {
        HashMap<Integer, Integer> rs = new HashMap<>(); //# of bus stops that have # of hotels in range
        for (Map.Entry<Integer, Pair<Double, Double>> h : this.hotels.entrySet()) {
            double h_lat = h.getValue().getKey();
            double h_log = h.getValue().getValue();


            int counter = 0;

            for (Map.Entry<Integer, Pair<Double, Double>> b : this.buses.entrySet()) {

                double b_lat = b.getValue().getKey();
                double b_log = b.getValue().getValue();

                double d = Math.sqrt(Math.pow(b_lat - h_lat, 2) + Math.pow(b_log - h_log, 2));
                if (d <= range) {
                    counter++;
                }
            }

            if (rs.containsKey(counter)) {
                rs.put(counter, rs.get(counter) + 1);
            } else {
                rs.put(counter, 1);
            }
        }

        //===========================================================//

        TreeMap<Integer, Integer> map = new TreeMap<>();
        map.putAll(rs);
        for (Map.Entry<Integer, Integer> r : map.entrySet()) {
            System.out.println(r.getKey() + " " + r.getValue());
        }

    }


    public void shutdown() {
        conn.shutdownDB();

    }
}
