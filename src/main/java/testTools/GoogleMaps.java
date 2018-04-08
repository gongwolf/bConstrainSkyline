package testTools;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.maps.GeoApiContext;
import com.google.maps.GeocodingApi;
import com.google.maps.errors.ApiException;
import com.google.maps.model.AddressType;
import com.google.maps.model.GeocodingResult;
import com.google.maps.model.LatLng;
import javafx.util.Pair;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class GoogleMaps {

    HashSet<Pair<Double, Double>> busStations = new HashSet<>();

    public static void main(String args[]) {

        GoogleMaps g = new GoogleMaps();

        double lat1 = 40.90549300;
        double long1 = -73.84960400;
        double lat2 = 40.90364300;
        double long2 = -73.85031800;

        g.distanceInMeters(lat1, long1, lat2, long2);
        g.readBusInfo();
        System.out.println(g.busStations.size());
        g.findDistanceToBusStop(34.24741300, -118.41914200);
        g.findDetailsOfBusStation(34.249828,-118.422432);
    }

    private double distanceInMeters(double lat1, double long1, double lat2, double long2) {
        long R = 6371000;
        double r_lat1 = Math.PI / 180 * lat1;
        double r_lat2 = Math.PI / 180 * lat2;
        double delta_lat = Math.PI / 180 * (lat2 - lat1);
        double delta_long = Math.PI / 180 * (long2 - long1);
        double d;
//        double a = Math.sin(delta_lat / 2) * Math.sin(delta_lat / 2) + Math.cos(r_lat1) * Math.cos(r_lat2) * Math.sin(delta_long / 2) * Math.sin(delta_long / 2);
//        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
//        double d = R * c;
//        System.out.println(d);
//        double x = Math.PI / 180 * (long2 - long1) * Math.cos(Math.PI / 180 * (lat1 + lat2) / 2);
//        double y = Math.PI / 180 * (lat2 - lat1);
//        d = Math.sqrt(x * x + y * y) * R;
//        System.out.println(d);
        d = Math.acos(Math.sin(r_lat1) * Math.sin(r_lat2) + Math.cos(r_lat1) * Math.cos(r_lat2) * Math.cos(delta_long)) * R;
//        System.out.println(d);
        return d;
    }


    public void readBusInfo() {
        String bus_data = "data/Bus_data/output.txt";
        try {
            File f = new File(bus_data);
            BufferedReader b = new BufferedReader(new FileReader(f));
            String readLine = "";
//            System.out.println("Reading file using Buffered Reader");
            while (((readLine = b.readLine()) != null)) {
                if (readLine.startsWith("                           ")) {
                    String[] infos = readLine.trim().split("\\^");
                    String LatAndLong = infos[infos.length - 1];
                    double latitude = Double.valueOf(LatAndLong.substring(0, LatAndLong.indexOf(",")));
                    double longitude = Double.valueOf(LatAndLong.substring(LatAndLong.indexOf(",") + 1, LatAndLong.length()));
//                    System.out.println(LatAndLong+" "+latitude+" "+longitude);
                    this.busStations.add(new Pair<>(latitude, longitude));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void findDistanceToBusStop(double latitude, double longitude) {
        Iterator<Pair<Double, Double>> iter = this.busStations.iterator();
        HashMap<Integer, HashSet<Pair<Double, Double>>> BusStations_List = new HashMap<>();
        while (iter.hasNext()) {
            Pair<Double, Double> p = iter.next();
            double d = distanceInMeters(latitude, longitude, p.getKey(), p.getValue());
            int d_modular = (int) d / 1000;

            if (BusStations_List.containsKey(d_modular)) {
                HashSet<Pair<Double, Double>> set = BusStations_List.get(d_modular);
                set.add(p);
                BusStations_List.put(d_modular, set);

            } else {
                HashSet<Pair<Double, Double>> newSet = new HashSet<>();
                newSet.add(p);
                BusStations_List.put(d_modular, newSet);
            }

        }

        TreeMap<Integer, HashSet<Pair<Double, Double>>> map = new TreeMap<>();
        map.putAll(BusStations_List);

//        for (Map.Entry<Integer, HashSet<Pair<Double, Double>>> e : map.entrySet()) {
//            System.out.println(e.getKey() + "     " + e.getValue().size());
//        }

        System.out.println(BusStations_List.get(0));
        System.out.println(latitude+" "+longitude);

    }

    public void findDetailsOfBusStation(double latitude, double longitude) {
        System.out.println("========================");
        GeoApiContext context = new GeoApiContext.Builder()
                .apiKey("AIzaSyA8M13Xzf7XZH9hV3_E2L1FsA9ZcCqfYS0")
                .build();

        try {
            System.out.println(latitude+" "+longitude);
            GeocodingResult[] results = GeocodingApi.reverseGeocode(context,new LatLng(latitude,longitude)).resultType(AddressType.BUS_STATION,AddressType.).await();
            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            System.out.println(gson.toJson(results[0].formattedAddress));
            System.out.println(gson.toJson(results[0].geometry.location));
            System.out.println(results.length);


        } catch (ApiException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }


    }


}
