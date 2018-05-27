package testTools;

import BaseLine.Result;
import javafx.util.Pair;
import neo4jTools.connector;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class statistic {

    String graphPath;
    String infoPath;
    String dataPath;

    int graph_size;
    int degree;
    double range;

    connector conn;
    HashMap<Integer, Pair<Double, Double>> buses;
    HashMap<Integer, Pair<Double, Double>> hotels;


    public statistic(int graph_size, int degree, double range, int num_hotels) {
        this.graph_size = graph_size;
        this.degree = degree;
        this.range = range;


        this.graphPath = "/home/gqxwolf/neo4j334/testdb" + this.graph_size + "_" + this.degree + "/databases/graph.db";
        this.infoPath = "/home/gqxwolf/mydata/projectData/testGraph" + this.graph_size + "_" + this.degree + "/data";
        this.dataPath = "/home/gqxwolf/shared_git/bConstrainSkyline/data/staticNode_" + this.graph_size + "_" + this.degree + "_" + this.range + "_" + num_hotels + ".txt";


        buses = new HashMap<>();
        readBusData();
        hotels = new HashMap<>();
        readHotels();


//        conn = new connector(this.graphPath);
//        conn.startDB();


    }

    public static void main(String args[]) {
        statistic s = new statistic(20000, 4, 6, 5000);
        s.HotelsToBuesWithinRange();

//        s.shutdown();
    }

    public static double goodnessAnalyze(ArrayList<Result> all, ArrayList<Result> approx, String dist_measure) {

        HashSet<Integer> distinct_all = new HashSet<>();

        for (Result r : all) {
            distinct_all.add(r.end.getPlaceId());
        }

        int n_all = distinct_all.size();



        double[] all_max_array = getBoundsArray(all, "max");
        double[] all_min_array = getBoundsArray(all, "min");
        double[] approx_max_array = getBoundsArray(approx, "max");
        double[] approx_min_array = getBoundsArray(approx, "min");
        double max = Math.sqrt(7);


        //find common objects
        HashSet<Integer> common_set = new HashSet<>();
        for (Result r : all) {
            for (Result r1 : approx) {
                if (r.end.getPlaceId() == r1.end.getPlaceId()) {
                    common_set.add(r1.end.getPlaceId());
                }
            }
        }


        ArrayList<Integer> commonList = new ArrayList<>(common_set);

        double sum_up = 0;
        for (int idx : commonList) {
            double min_distance = Double.POSITIVE_INFINITY;
            for (Result r : all) {
                if (r.end.getPlaceId() == idx) {
                    for (Result r1 : approx) {
                        if (r1.end.getPlaceId() == idx) {
                            double d = 0;

                            switch (dist_measure) {
                                case "edu":
                                    d = EduclidianDist(r, r1, all_max_array, all_min_array, approx_max_array, approx_min_array);
                                    break;
                                case "cos":
                                    d = CosineSimilarity(r, r1, all_max_array, all_min_array, approx_max_array, approx_min_array);
                                    break;
                            }

                            if (d < min_distance) {
                                min_distance = d;
                            }
                        }

                    }

                }
            }

            switch (dist_measure) {
                case "edu":
                    sum_up += (max - min_distance);
                    break;
                case "cos":
                    sum_up += (1 - min_distance);
                    break;
            }
        }

        return sum_up/n_all;
    }


    private static double[] getBoundsArray(ArrayList<Result> all, String type) {
        int costs_length = 0;

        if (all.isEmpty()) {
            System.out.println("the result is empty");
        } else {
            costs_length = all.get(0).costs.length;
        }

        double result[] = new double[costs_length];
        for (int i = 0; i < result.length; i++) {
            if (type.equals("max")) {
                result[i] = Double.NEGATIVE_INFINITY;
            } else if (type.equals("min")) {
                result[i] = Double.POSITIVE_INFINITY;
            }
        }

        for (Result r : all) {
            for (int i = 0; i < costs_length; i++) {
                if (type.equals("max") && result[i] < r.costs[i]) {
                    result[i] = r.costs[i];
                } else if (type.equals("min") && result[i] > r.costs[i]) {
                    result[i] = r.costs[i];
                }
            }
        }

        return result;
    }


    public static double EduclidianDist(Result r, Result r1, double[] all_max_array, double[] all_min_array, double[] approx_max_array, double[] approx_min_array) {
        int cost_length = r.costs.length;
        double d = 0;
        for (int i = 0; i < cost_length; i++) {
            double i_max = all_max_array[i] > approx_max_array[i] ? all_max_array[i] : approx_max_array[i];
            double i_min = all_min_array[i] < approx_min_array[i] ? all_min_array[i] : approx_min_array[i];
            double v1 = (r.costs[i] - i_min) / (i_max - i_min);
            double v2 = (r1.costs[i] - i_min) / (i_max - i_min);

            if (v2 > 1 || v2 < 0) {
                System.out.println("Normalization error !!!!!!!");
                d = cost_length;
                break;
//                System.exit(0);
            }

            d += Math.pow(v1 - v2, 2);
        }
        d = Math.sqrt(d);
        return d;
    }

    private static double CosineSimilarity(Result r1, Result r, double[] all_max_array, double[] all_min_array, double[] approx_max_array, double[] approx_min_array) {
        int cost_length = r.costs.length;
        double d = 0;
        double d1 = 0;
        double d2 = 0;
        double d3 = 0;
        for (int i = 0; i < cost_length; i++) {
            double i_max = all_max_array[i] > approx_max_array[i] ? all_max_array[i] : approx_max_array[i];
            double i_min = all_min_array[i] < approx_min_array[i] ? all_min_array[i] : approx_min_array[i];
            double v1 = (r.costs[i] - i_min) / (i_max - i_min);
            double v2 = (r1.costs[i] - i_min) / (i_max - i_min);

            if (v2 > 1 || v2 < 0) {
                System.out.println("Normalization error !!!!!!!");
                d = 25 * cost_length;
                break;
//                System.exit(0);
            }

            d1 += v1 * v2;
            d2 += v1 * v1;
            d3 += v2 * v2;
//            d += Math.pow(v1 - v2, 2);
        }
        d = d1 / (Math.sqrt(d2) * Math.sqrt(d3));
        return (1 - d);
    }

    public static double goodnessAnalyze(ArrayList<Result> all, ArrayList<Result> approx, String dist_measure, int topK) {
        double[] all_max_array = getBoundsArray(all, "max");
        double[] all_min_array = getBoundsArray(all, "min");
        double[] approx_max_array = getBoundsArray(approx, "max");
        double[] approx_min_array = getBoundsArray(approx, "min");
        double max = Math.sqrt(7);


        //find common objects
        HashSet<Integer> common_set = new HashSet<>();
        for (Result r : all) {
            for (Result r1 : approx) {
                if (r.end.getPlaceId() == r1.end.getPlaceId()) {
                    common_set.add(r1.end.getPlaceId());
                }
            }
        }


        ArrayList<Integer> commonList = new ArrayList<>(common_set);
//        System.out.println(" c:"+common_set.size());

        if (commonList.size() < topK) {
            return goodnessAnalyze(all, approx, dist_measure);
        }

        HashMap<Integer, Double> selected = new HashMap<>();
        double sum_up = 0;
        for (int i = 0; i < topK; i++) {

            //random select object
            int idx = getRandomNumberInRange_int(0, common_set.size() - 1);
            while (selected.containsKey(idx)) {
                idx = getRandomNumberInRange_int(0, common_set.size() - 1);
            }

            double min_distance = Double.POSITIVE_INFINITY;
            for (Result r : all) {
                if (r.end.getPlaceId() == commonList.get(idx)) {
                    for (Result r1 : approx) {
                        if (r1.end.getPlaceId() == commonList.get(idx)) {
                            double d = 0;
                            switch (dist_measure) {
                                case "edu":
                                    d = EduclidianDist(r, r1, all_max_array, all_min_array, approx_max_array, approx_min_array);
                                    break;
                                case "cos":
                                    d = CosineSimilarity(r, r1, all_max_array, all_min_array, approx_max_array, approx_min_array);
                                    break;
                            }
//                            System.out.println(min_distance+" "+d);

                            if (d < min_distance) {
                                min_distance = d;
                            }
                        }

                    }

                }
            }

            switch (dist_measure) {
                case "edu":
                    sum_up += (max - min_distance);
                    selected.put(idx,(max - min_distance));
                    break;
                case "cos":
                    sum_up += (1 - min_distance);
                    selected.put(idx,(1 - min_distance));
                    break;
            }

//            System.out.println(sum_up);
        }

        return sum_up/topK;
    }


    public static int getRandomNumberInRange_int(int min, int max) {

        if (min >= max) {
            throw new IllegalArgumentException("max must be greater than min");
        }

        Random r = new Random();
        return r.nextInt((max - min) + 1) + min;
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

    private void HotelsToBuesWithinRange() {
        HashMap<Integer, Integer> rs = new HashMap<>(); //# of bus stops that have # of hotels in range
        for (Map.Entry<Integer, Pair<Double, Double>> h : this.hotels.entrySet()) {
            double h_lat = h.getValue().getKey();
            double h_log = h.getValue().getValue();


            int counter = 0;

            for (Map.Entry<Integer, Pair<Double, Double>> b : this.buses.entrySet()) {

                double b_lat = b.getValue().getKey();
                double b_log = b.getValue().getValue();

                double d = Math.sqrt(Math.pow(b_lat - h_lat, 2) + Math.pow(b_log - h_log, 2));
                if (d <= 7) {
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
