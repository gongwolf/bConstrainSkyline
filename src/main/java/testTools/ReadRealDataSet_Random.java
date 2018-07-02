package testTools;

import javafx.util.Pair;
import neo4jTools.BNode;
import neo4jTools.Line;
import neo4jTools.connector;
import org.apache.commons.io.FileUtils;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

import java.io.*;
import java.util.*;

public class ReadRealDataSet_Random {
    String DB_PATH = "";
    String path_base = "/home/gqxwolf/shared_git/bConstrainSkyline/data/";
    String bus_data = path_base + "Bus_data/output.txt";
    HashMap<Long, BusStation> stops_list = new HashMap<>();
    HashSet<SegObj> seg_list = new HashSet<>();
    long max_id;
    double distance_range;
    private String DBBase = "";
    private String EdgesPath = "";
    private String NodePath = "";
    private GraphDatabaseService graphdb;


    public ReadRealDataSet_Random(double distance_range, boolean deleteBefore) {
        this.max_id = 0;
        this.distance_range = distance_range;

        this.DBBase = "/home/gqxwolf/mydata/projectData/testGraph_real_int" + (int) distance_range + "_Random/data/";
        this.DB_PATH = "/home/gqxwolf/neo4j341/testdb_real_int" + (int) distance_range + "_Random/databases/graph.db";
        EdgesPath = DBBase + "SegInfo.txt";
        NodePath = DBBase + "NodeInfo.txt";

        System.out.println(this.DBBase);
        System.out.println(this.DB_PATH);
        System.out.println(this.EdgesPath);
        System.out.println(this.NodePath);

        if (deleteBefore) {
            File dataF = new File(DBBase);
            try {
                FileUtils.deleteDirectory(dataF);
                dataF.mkdirs();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    public static void main(String args[]) {
        double distance_range = 50;
        boolean deleteBefore = true;
        boolean createDB = false;

        if (args.length == 3) {
            distance_range = Double.parseDouble(args[0]);
            deleteBefore = Boolean.parseBoolean(args[1]);
            createDB = Boolean.parseBoolean(args[2]);
        }


        ReadRealDataSet_Random reReal_int = new ReadRealDataSet_Random(distance_range, deleteBefore);
        reReal_int.ReadBusStop();
//
//        if (createDB) {
//            reReal_int.createDatabase();
//        }

    }


    public void ReadBusStop() {
        String path = this.bus_data;
        System.out.println(path);
        try {
            File f = new File(path);
            BufferedReader b = new BufferedReader(new FileReader(f));
            String line = "";

            boolean start_new_line = false;
            boolean show = false;
            int counter = 0;

            ArrayList<String> stopsInLine = new ArrayList<>();

            while (((line = b.readLine()) != null)) {
                counter++;

                if (line.startsWith("           ") && line.indexOf("~") != -1) {
                    //process the bus line information, then clear the arraylist which stores the Busline Inforamtion
                    processBusLine(stopsInLine);
                    start_new_line = true;
                    stopsInLine.clear();
                } else if (line.startsWith("                           ")) {
                    stopsInLine.add(line.trim());

                    if (start_new_line) {
                        start_new_line = false;
                    }
                }

                if (counter % 10000 == 0) {
                    System.out.println((counter) + " .......................................");
                }
            }


            processBusLine(stopsInLine);
            writeToDisk();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void writeToDisk() {
        try (FileWriter fw = new FileWriter(NodePath, true);
             BufferedWriter bw = new BufferedWriter(fw);
             PrintWriter out = new PrintWriter(bw)) {
            for (Map.Entry<Long, BusStation> node : this.stops_list.entrySet()) {
                out.println(node.getValue().toString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }


        try (FileWriter fw = new FileWriter(EdgesPath, true);
             BufferedWriter bw = new BufferedWriter(fw);
             PrintWriter out = new PrintWriter(bw)) {
            for (SegObj seg : this.seg_list) {
                out.println(seg.toString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void processBusLine(ArrayList<String> stopsInLine) {
        if (!stopsInLine.isEmpty()) {
//            System.out.println("================================");

            ArrayList<tmpStop> list_t = new ArrayList<>();

            for (String line : stopsInLine) {
                String[] lineInfos = line.split("\\^");
                int n = lineInfos.length - 1;

                double latitude = Double.valueOf(lineInfos[n].split(",")[0]);
                double longitude = Double.valueOf(lineInfos[n].split(",")[1]);

                //if there is a bus stop within the given range
                boolean in_list = HasBusStationInfo(latitude, longitude);

                long id;
                String sub_name = lineInfos[0];
                int order = Integer.valueOf(lineInfos[3]);

//                System.out.println(line);

                if (in_list) {
                    id = getBusSationID(latitude, longitude, true);
                    BusStation bs = this.stops_list.get(id);
                    bs.list.add(new Pair<>(latitude, longitude));
                    bs.recalculateCenter();
                } else {
                    id = max_id;
                    max_id++;

                    BusStation bs = new BusStation(id);
                    bs.list.add(new Pair<>(latitude, longitude));
                    bs.recalculateCenter();
                    this.stops_list.put(id, bs);
                }
//                System.out.println(sub_name + " " + order + " " + lineInfos[n] + " " + id);

                tmpStop ts = new tmpStop(sub_name, id, order);
                list_t.add(ts);

            }


            Collections.sort(list_t, new Comparator<tmpStop>() {
                @Override
                public int compare(tmpStop o1, tmpStop o2) {
                    // -1 - less than, 1 - greater than, 0 - equal, all inversed for descending
                    if (o1.subName.equals(o2.subName)) {
                        if (o1.order > o2.order) {
                            return 1;
                        } else if (o1.order < o2.order) {
                            return -1;
                        } else {
                            return 0;
                        }
                    } else {
                        return o1.subName.compareTo(o2.subName);
                    }
                }
            });


            for (int i = 0; i < list_t.size() - 1; i++) {
                tmpStop t = list_t.get(i);
                tmpStop t_1 = list_t.get(i + 1);
                if (t.subName.equals(t_1.subName)) {
                    long sid = t.id;
                    long did = t_1.id;

                    double d = distanceInMeters(this.stops_list.get(t.id).center[0], this.stops_list.get(t.id).center[1], this.stops_list.get(t_1.id).center[0], this.stops_list.get(t_1.id).center[1]);

                    if (d >= distance_range) {
                        double d1 = getRandomNumberInRange_int(1,(int)(2*d));
                        double d2 = getRandomNumberInRange_int(1,(int)(2*d));
                        SegObj s = new SegObj(sid, did, (int) d, d1, d2);
                        this.seg_list.add(s);
                    }
                }
            }
        }
    }

    private boolean HasBusStationInfo(double latitude, double longitude) {
        boolean flag = false;

        if (this.stops_list.isEmpty()) {
            return flag;
        }


        for (Map.Entry<Long, BusStation> e : this.stops_list.entrySet()) {
            double c_latitude = e.getValue().center[0];
            double c_longitude = e.getValue().center[1];
            double d = distanceInMeters(latitude, longitude, c_latitude, c_longitude);


            if (d <= this.distance_range) {
                return true;
            }
        }
        return flag;
    }


    private long getBusSationID(double latitude, double longitude, boolean find_min) {
        long id = -1;

        if (this.stops_list.isEmpty()) {
            return -1;
        }


        double min_d = Double.POSITIVE_INFINITY;
        for (Map.Entry<Long, BusStation> e : this.stops_list.entrySet()) {
            double c_latitude = e.getValue().center[0];
            double c_longitude = e.getValue().center[1];
            double d = distanceInMeters(latitude, longitude, c_latitude, c_longitude);

            if (d <= this.distance_range) {
                if (!find_min) {
                    id = e.getKey();
                    return id;
                } else if (min_d > d) {
                    min_d = d;
                    id = e.getKey();
                }
            }
        }
        return id;
    }


    private double distanceInMeters(double lat1, double long1, double lat2, double long2) {
        long R = 6371000;
        double d;

        double r_lat1 = Math.PI / 180 * lat1;
        double r_lat2 = Math.PI / 180 * lat2;
        double delta_lat = Math.PI / 180 * (lat2 - lat1);
        double delta_long = Math.PI / 180 * (long2 - long1);
        double a = Math.sin(delta_lat / 2) * Math.sin(delta_lat / 2) + Math.cos(r_lat1) * Math.cos(r_lat2) * Math.sin(delta_long / 2) * Math.sin(delta_long / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        d = R * c;

        return d;
    }

    private double getGussianRandomValue(double mean, double sd) {
        Random r = new Random();
        double value = r.nextGaussian() * sd + mean;

        while (value <= 0) {
            value = r.nextGaussian() * sd + mean;
        }

        return value;
    }

    public int getRandomNumberInRange_int(int min, int max) {

        int value = 0;
        if (min >= max) {
            throw new IllegalArgumentException("max must be greater than min");
        }

        Random r = new Random();
        while (value <= 0) {
            value = r.nextInt((max - min) + 1) + min;
        }
        return value;
    }

    public void createDatabase() {
        //System.out.println(DB_PATH);
        //System.out.println(DBBase);
        //System.out.println("=============");

        connector nconn = new connector(this.DB_PATH);
        //delete the data base at first
        nconn.deleteDB();
        nconn.startDB();
        this.graphdb = nconn.getDBObject();


        try (Transaction tx = this.graphdb.beginTx()) {
            BufferedReader br = new BufferedReader(new FileReader(this.NodePath));
            String line = null;
            while ((line = br.readLine()) != null) {
                //System.out.println(line);
                String[] attrs = line.split(" ");

                String id = attrs[0];
                double lat = Double.parseDouble(attrs[1]);
                double log = Double.parseDouble(attrs[2]);
                Node n = createNode(id, lat, log);
            }

            br = new BufferedReader(new FileReader(this.EdgesPath));
            line = null;
            while ((line = br.readLine()) != null) {
                //System.out.println(line);
                String attrs[] = line.split(" ");
                String src = attrs[0];
                String des = attrs[1];
                double EDistence = Double.parseDouble(attrs[2]);
                double MetersDistance = Double.parseDouble(attrs[3]);
                double RunningTime = Double.parseDouble(attrs[4]);
                createRelation(src, des, EDistence, MetersDistance, RunningTime);
            }

            tx.success();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        nconn.shutdownDB();
        System.out.println("Database is created, the location of the db file is " + this.DB_PATH);
    }

    private void createRelation(String src, String des, double eDistence, double metersDistance, double runningTime) {
        try {
            Node srcNode = this.graphdb.findNode(BNode.BusNode, "name", src);
            Node desNode = this.graphdb.findNode(BNode.BusNode, "name", des);
            Relationship rel = srcNode.createRelationshipTo(desNode, Line.Linked);
            rel.setProperty("EDistence", eDistence);
            rel.setProperty("MetersDistance", metersDistance);
            rel.setProperty("RunningTime", runningTime);
        } catch (Exception e) {
            System.out.println(src + "-->" + des);
        }
    }

    private Node createNode(String id, double lat, double log) {
        Node n = this.graphdb.createNode(BNode.BusNode);
        n.setProperty("name", id);
        n.setProperty("lat", lat);
        n.setProperty("log", log);
        return n;
    }
}
