package testTools;

import BaseLine.Result;
import BaseLine.myNode;
import RstarTree.Data;
import com.google.maps.GeoApiContext;
import com.google.maps.PlacesApi;
import com.google.maps.errors.ApiException;
import com.google.maps.model.*;
import javafx.util.Pair;

import java.io.*;
import java.util.*;

public class GoogleMaps {

    HashMap<Pair<Double, Double>, String> busStations = new HashMap<>();
    HashSet<Pair<Double, Double>> busLocation = new HashSet<>();
    GeoApiContext context;

    public GoogleMaps() {
        this.context = new GeoApiContext.Builder()
                .apiKey("AIzaSyA8M13Xzf7XZH9hV3_E2L1FsA9ZcCqfYS0")
                .build();
    }

    public static void main(String args[]) {
        int range = 200;

        if (args.length == 1) {
            range = Integer.valueOf(args[0]);
        }

        GoogleMaps g = new GoogleMaps();

        double lat1 = 43.153826;
        double long1 = -77.05942;
        double lat2 = 43.173793;
        double long2 = -77.667754;

        System.out.println(g.distanceInMeters(lat1, long1, lat2, long2));
        System.out.println(Math.sqrt(Math.pow(lat1-lat2,2)+Math.pow(long1-long2,2)));
//        g.readBusInfo();
//        System.out.println(g.busStations.size());
//        g.statisticInRange(range);
//        g.averageDistance();
//        g.findDistanceToBusStop(37.75731290, -122.42150700);


//        int i = 0;
//        for (Map.Entry<Pair<Double, Double>, String> e : g.busStations.entrySet()) {
//            System.out.println("------------");
//            System.out.println(e.getValue());
//            g.findDetailsOfBusStation(e.getKey().getKey(), e.getKey().getValue());
//            if (i++ == 2) {
//                break;
//            }
//        }
//        g.getAddressInformation();
//        g.getAddressDetails();
    }

    public static double distanceInMeters(double lat1, double long1, double lat2, double long2) {
        long R = 6371000;
        double d;

        double r_lat1 = Math.PI / 180 * lat1;
        double r_lat2 = Math.PI / 180 * lat2;
        double delta_lat = Math.PI / 180 * (lat2 - lat1);
        double delta_long = Math.PI / 180 * (long2 - long1);
        double a = Math.sin(delta_lat / 2) * Math.sin(delta_lat / 2) + Math.cos(r_lat1) * Math.cos(r_lat2) * Math.sin(delta_long / 2) * Math.sin(delta_long / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        d = R * c;
//        System.out.println(d);
//        double x = Math.PI / 180 * (long2 - long1) * Math.cos(Math.PI / 180 * (lat1 + lat2) / 2);
//        double y = Math.PI / 180 * (lat2 - lat1);
//        d = Math.sqrt(x * x + y * y) * R;
//        System.out.println(d);
//        d = Math.acos(Math.sin(r_lat1) * Math.sin(r_lat2) + Math.cos(r_lat1) * Math.cos(r_lat2) * Math.cos(delta_long)) * R;
//        System.out.println(d);
        return d;
    }

    public static void distanceStatistic(ArrayList<Result> skyPaths, HashMap<Long, myNode> tmpStoreNodes) {
        HashSet<Long> startnodes = new HashSet<>();
        HashSet<Long> endNodes = new HashSet<>();
//        System.out.println(tmpStoreNodes.size());
        Data q = null;
        for (Result r : skyPaths) {

            if (q == null) {
                q = r.start;
            }

            int skylist[] = new int[]{2372, 1081, 2539, 8650, 8826, 510, 5046, 3840, 9350, 3144, 1692};

            boolean flag = false;
            for (long sid : skylist) {
                if (r.end.getPlaceId() == sid) {
                    flag = true;
//                    break;
                }
            }

            if (!flag) {
                if (r.p != null) {
                    double d1 = GoogleMaps.distanceInMeters(q.location[0], q.location[1], tmpStoreNodes.get(r.p.startNode).locations[0], tmpStoreNodes.get(r.p.startNode).locations[1]);
                    double d2 = GoogleMaps.distanceInMeters(q.location[0], q.location[1], tmpStoreNodes.get(r.p.endNode).locations[0], tmpStoreNodes.get(r.p.endNode).locations[1]);
                    System.out.println(q.PlaceId + " " + r.p.startNode + " " + r.p.endNode + " " + d1 + " " + d2);
                }
            }
        }


    }

    public void getAddressInformation() {
        String bs_path = "/home/gqxwolf/mydata/projectData/testGraph_real_50/data";

        String bus_data = bs_path + "/NodeInfo.txt";
        String path_node_with_id = bs_path + "/Node_with_placeID.txt";
        HashMap<Integer, Pair<Double, Double>> nodeList = new HashMap<>();
        try {
            File f = new File(bus_data);
            BufferedReader b = new BufferedReader(new FileReader(f));
            String readLine = "";
            while (((readLine = b.readLine()) != null)) {
                String[] infos = readLine.trim().split(" ");
                int id = Integer.parseInt(infos[0]);
                double latitude = Double.parseDouble(infos[1]);
                double longitude = Double.parseDouble(infos[2]);
                nodeList.put(id, new Pair<>(latitude, longitude));

            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println(nodeList.size());

        TreeMap<Integer, Pair<Double, Double>> sortedNodeList = new TreeMap<>(nodeList);
        BufferedWriter writer = null;
        try {

            File outputFile = new File(path_node_with_id);
            if (outputFile.exists()) {
                outputFile.delete();
            }
            writer = new BufferedWriter(new FileWriter(outputFile, true));

            for (Map.Entry<Integer, Pair<Double, Double>> e : sortedNodeList.entrySet()) {

                System.out.println(e.getKey() + " " + e.getValue().getKey() + " " + e.getValue().getValue());
                int id = e.getKey();
                double lat = e.getValue().getKey();
                double lng = e.getValue().getValue();

//                if (id == 10) {
//                    break;
//                }

                String name = "", placeid = "";
                PlacesSearchResponse resps = PlacesApi.nearbySearchQuery(this.context, new LatLng(lat, lng)).rankby(RankBy.DISTANCE).
                        type(PlaceType.BUS_STATION, PlaceType.FOOD, PlaceType.RESTAURANT, PlaceType.TRAIN_STATION).await();

//                PlacesSearchResponse resps = PlacesApi.radarSearchQuery(this.context, new LatLng(lat, lng),2000).await();
                PlacesSearchResult[] results = resps.results;

                if (results.length >= 1) {
////                        System.out.println(results[0].formattedAddress);
//                    System.out.println(results[0].name);
////                        System.out.println(results[0].vicinity);
//                    System.out.println(results[0].placeId);
                    name = results[0].name;
                    placeid = results[0].placeId;
                }

                writer.append(id + "," + lat + "," + lng + ", " + name + " ," + placeid + "\n");
            }

            writer.close();

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ApiException e) {
            e.printStackTrace();
        }
    }


    public void getAddressDetails() {
        String bs_path = "/home/gqxwolf/mydata/projectData/testGraph_real_50/data";

        String bus_data = bs_path + "/Node_with_placeID.txt";
        String path_node_with_details = bs_path + "/Node_with_placeDetails.txt";
        BufferedWriter writer = null;
        try {
            File outputFile = new File(path_node_with_details);
//            if (outputFile.exists()) {
//                outputFile.delete();
//            }

            writer = new BufferedWriter(new FileWriter(outputFile, true));


            File f = new File(bus_data);
            BufferedReader b = new BufferedReader(new FileReader(f));
            String readLine = "";
            while (((readLine = b.readLine()) != null)) {
                StringBuffer sb = new StringBuffer();
                System.out.println(readLine);
                String[] infos = readLine.trim().split(",");
                int id = Integer.parseInt(infos[0]);
                double latitude = Double.parseDouble(infos[1]);
                double longitude = Double.parseDouble(infos[2]);

//                String name = infos[3];
                String placeID = infos[infos.length - 1];

                sb.append(id).append(";");
                sb.append(latitude).append(";");
                sb.append(longitude).append(";");
//                sb.append(name).append(";");
                sb.append(placeID).append(";");

//                Thread.sleep(500);

                if (id >= 19129) {
//                    System.out.println("placeid:"+placeID+" "+(placeID==null)+" "+placeID.equals("  "));
                    if (placeID != null && !placeID.equals("  ")) {
                        PlaceDetails Details = PlacesApi.placeDetails(context, placeID).await();
                        sb.append(Details.formattedAddress).append(";");
//                System.out.println("    " + Details.name);
//                System.out.println("    " + Details.geometry.location);
                        for (AddressComponent c : Details.addressComponents) {
                            sb.append(" " + c.longName + " " + c.shortName).append("|");
//                    System.out.print("    " + c.longName + " "+c.shortName);
//                    for (AddressComponentType cty : c.types) {
//                        System.out.print(cty + ";");
//                    }
//                    System.out.println();

                        }
                    }
//                Arrays.stream(Details.addressComponents).forEach(c -> System.out.println(c.longName+" "+c.types));
//                Arrays.stream(Details.types).forEach(c -> System.out.print("    " + c + ","));
//                System.out.println();
                    writer.write(sb.substring(0, sb.length() - 1));
                    writer.write("\n");
//                break;
//                writer.append(id + "," + lat + "," + lng + ", " + name + " ," + placeid + "\n");

                }
            }
            writer.close();

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e1) {
            e1.printStackTrace();
        } catch (ApiException e1) {
            e1.printStackTrace();
        }

    }

    private void averageDistance() {

        String[] cities = new String[]{"Los Angeles", "New York", "San Francisco"};
        String[] types = new String[]{"restaurant", "lodging", "food"};

        HashSet<Pair<Double, Double>> poi_list = new HashSet<>();

        for (String city : cities) {
            for (String type : types) {
                String path = "/home/gqxwolf/shared_git/bConstrainSkyline/data/IOP_data/outfilename_" + type + "_" + city;
                System.out.println(path);
                try {
                    File f = new File(path);
                    BufferedReader b = new BufferedReader(new FileReader(f));
                    String line = "";
                    while (((line = b.readLine()) != null)) {
                        if (line.startsWith("   locations:")) {
                            double latitude = Double.valueOf(line.substring(line.indexOf(":") + 1).trim().split(",")[0]);
                            double longitude = Double.valueOf(line.substring(line.indexOf(":") + 1).trim().split(",")[1]);
                            poi_list.add(new Pair<>(latitude, longitude));

                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }
        }

        int counter = poi_list.size();
        System.out.println(counter);
        double result = 0;
        int i = 0;
        for (Pair<Double, Double> p : poi_list) {
            double distance = NearestBusStop(p.getKey(), p.getValue());
            result += distance;
            i++;
            if (i % 500 == 0) {
                System.out.println(i + "................");
            }
        }
        System.out.println(result + "/" + counter + "=" + result / counter);
    }

    private double NearestBusStop(double latitude, double longitude) {
        double result = Double.POSITIVE_INFINITY;
        for (Pair<Double, Double> p : this.busLocation) {
            double d = distanceInMeters(latitude, longitude, p.getKey(), p.getValue());
            if (d < result) {
                result = d;
            }
        }

        return result;
    }

    private void statisticInRange(int range) {
        HashMap<Integer, Integer> result = new HashMap<>();
        String[] cities = new String[]{"Los Angeles", "New York", "San Francisco"};
        String[] types = new String[]{"restaurant", "lodging", "food"};

        HashSet<Pair<Double, Double>> poi_list = new HashSet<>();

        for (String city : cities) {
            for (String type : types) {
                String path = "/home/gqxwolf/shared_git/bConstrainSkyline/data/IOP_data/outfilename_" + type + "_" + city;
                System.out.println(path);
                try {
                    File f = new File(path);
                    BufferedReader b = new BufferedReader(new FileReader(f));
                    String line = "";
                    while (((line = b.readLine()) != null)) {
                        if (line.startsWith("   locations:")) {
                            double latitude = Double.valueOf(line.substring(line.indexOf(":") + 1).trim().split(",")[0]);
                            double longitude = Double.valueOf(line.substring(line.indexOf(":") + 1).trim().split(",")[1]);
                            poi_list.add(new Pair<>(latitude, longitude));

                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }
        }

        System.out.println(poi_list.size());
        int i = 0;
        for (Pair<Double, Double> p : poi_list) {
            int numberInRange = getNumberInRange(p.getKey(), p.getValue(), range);

            if (result.containsKey(numberInRange)) {
                result.put(numberInRange, result.get(numberInRange) + 1);
            } else {
                result.put(numberInRange, 1);
            }

            i++;
            if (i % 500 == 0) {
                System.out.println(i + "................");
            }
        }

        TreeMap<Integer, Integer> map = new TreeMap<>();
        map.putAll(result);
        for (Map.Entry<Integer, Integer> e : map.entrySet()) {
            System.out.println(e.getKey() + " " + e.getValue());
        }
    }

    private int getNumberInRange(double latitude, double longitude, int range) {
        int result = 0;
        for (Pair<Double, Double> p : this.busLocation) {
            if (distanceInMeters(latitude, longitude, p.getKey(), p.getValue()) <= range) {
                result++;
            }
        }

        return result;

    }

    public void readBusInfo() {
        String bus_data = "/home/gqxwolf/shared_git/bConstrainSkyline/data/Bus_data/output.txt";
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
                    this.busStations.put(new Pair<>(latitude, longitude), readLine.trim());
                    this.busLocation.add(new Pair<>(latitude, longitude));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void findDistanceToBusStop(double latitude, double longitude) {
        Iterator<Pair<Double, Double>> iter = this.busLocation.iterator();
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

        System.out.println(map.size());

        System.out.println(BusStations_List.get(10));
        System.out.println(latitude + " " + longitude);

    }

    public void findDetailsOfBusStation(double latitude, double longitude) {
//        System.out.println("========================");
        LatLng queryLL = new LatLng(latitude, longitude);


        try {
//            System.out.println(latitude + " " + longitude);
//            GeocodingResult[] results = GeocodingApi.reverseGeocode(context, queryLL).resultType(AddressType.BUS_STATION,AddressType.TRANSIT_STATION).await();
//            Gson gson = new GsonBuilder().setPrettyPrinting().create();
//            System.out.println(results.length);
//            for (int i = 0; i < results.length; i++) {
//                System.out.println(gson.toJson(results[0].formattedAddress));
//                System.out.println(gson.toJson(results[0].geometry.location));
//                System.out.println(gson.toJson(results[0].types));
//                System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
//            }
//            System.out.println("=========================");
            PlacesSearchResponse place_response =
                    PlacesApi.nearbySearchQuery(context, queryLL).rankby(RankBy.DISTANCE).type(PlaceType.BUS_STATION).await();
            PlacesSearchResult[] places_results = place_response.results;
//            System.out.println(places_results.length);

            for (int i = 0; i < 1; i++) {
                String placeId = places_results[i].placeId;
                PlaceDetails Details = PlacesApi.placeDetails(context, placeId).await();
                System.out.println("    " + Details.formattedAddress);
                System.out.println("    " + Details.name);
                System.out.println("    " + Details.geometry.location);
                for (AddressComponent c : Details.addressComponents) {
                    System.out.print("    " + c.longName + " ");
                    for (AddressComponentType cty : c.types) {
                        System.out.print(cty + ";");
                    }
                    System.out.println();

                }
//                Arrays.stream(Details.addressComponents).forEach(c -> System.out.println(c.longName+" "+c.types));
                Arrays.stream(Details.types).forEach(c -> System.out.print("    " + c + ","));
                System.out.println();
            }

        } catch (ApiException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }


    }


}
