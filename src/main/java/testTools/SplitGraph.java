package testTools;

import javafx.util.Pair;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

public class SplitGraph {
    final int city_sf = 1, city_la = 2, city_ny = 3, city_none = -1;
    String[] sf_names = new String[]{"Alameda", "Albany", "American Canyon", "Antioch", "Atherton", "Belmont", "Belvedere",
            "Benicia", "Berkeley", "Brentwood", "Brisbane", "Burlingame", "Calistoga", "Campbell", "Clayton", "Cloverdale",
            "Colma", "Concord", "Corte Madera", "Cotati", "Cupertino", "Daly City", "Danville", "Dixon", "Dublin", "East Palo Alto",
            "El Cerrito", "Emeryville", "Fairfax", "Fairfield", "Foster City", "Fremont", "Gilroy", "Half Moon Bay", "Hayward", "Healdsburg",
            "Hercules", "Hillsborough", "Lafayette", "Larkspur", "Livermore", "Los Altos", "Los Altos Hills", "Los Gatos", "Martinez", "Menlo Park",
            "Mill Valley", "Millbrae", "Milpitas", "Monte Sereno", "Moraga", "Morgan Hill", "Mountain View", "Napa", "Newark", "Novato", "Oakland",
            "Oakley", "Orinda", "Pacifica", "Palo Alto", "Petaluma", "Piedmont", "Pinole", "Pittsburg", "Pleasant Hill", "Pleasanton", "Portola Valley",
            "Redwood City", "Richmond", "Rio Vista", "Rohnert Park", "Ross", "St. Helena", "San Anselmo", "San Bruno", "San Carlos", "San Francisco", "San Jose",
            "San Leandro", "San Mateo", "San Pablo", "San Rafael", "San Ramon", "Santa Clara", "Santa Rosa", "Saratoga", "Sausalito", "Sebastopol",
            "Sonoma", "South San Francisco", "Suisun City", "Sunnyvale", "Tiburon", "Union City", "Vacaville", "Vallejo", "Walnut Creek", "Windsor", "Woodside", "Yountville"};
    String[] sf_contries = new String[]{
            "Alameda", "Napa", "Contra Costa", "San Mateo", "Marin", "Solano", "Santa Clara", "Sonoma", "San Francisco"
    };
    String[] la_names = new String[]{
            "Agoura Hills", "Alhambra", "Arcadia", "Artesia", "Avalon", "Azusa", "Baldwin Park", "Bell", "Bell Gardens", "Bellflower", "Beverly Hills", "Bradbury", "Burbank",
            "Calabasas", "Carson", "Cerritos", "Claremont", "Commerce", "Compton", "Covina", "Cudahy", "Culver City", "Diamond Bar", "Downey", "Duarte", "El Monte", "El Segundo",
            "Gardena", "Glendale", "Glendora", "Hawaiian Gardens", "Hawthorne", "Hermosa Beach", "Hidden Hills", "Huntington Park", "Industry", "Inglewood", "Irwindale",
            "La Cañada Flintridge", "La Habra Heights", "La Mirada", "La Puente", "La Verne", "Lakewood", "Lancaster", "Lawndale", "Lomita", "Long Beach", "Los Angeles",
            "Lynwood", "Malibu", "Manhattan Beach", "Maywood", "Monrovia", "Montebello", "Monterey Park", "Norwalk", "Palmdale", "Palos Verdes Estates", "Paramount",
            "Pasadena", "Pico Rivera", "Pomona", "Rancho Palos Verdes", "Redondo Beach", "Rolling Hills", "Rolling Hills Estates", "Rosemead", "San Dimas", "San Fernando",
            "San Gabriel", "San Marino", "Santa Clarita", "Santa Fe Springs", "Santa Monica", "Sierra Madre", "Signal Hill", "South El Monte", "South Gate", "South Pasadena",
            "Temple City", "Torrance", "Vernon", "Walnut", "West Covina", "West Hollywood", "Westlake Village", "Whittier"
    };
    String[] ny_names = new String[]{
            "Albany", "Amsterdam", "Auburn", "Batavia", "Beacon", "Binghamton", "Buffalo", "Canandaigua", "Cohoes", "Corning", "Cortland", "Dunkirk", "Elmira", "Fulton", "Geneva",
            "Glen Cove", "Glens Falls", "Gloversville", "Hornell", "Hudson", "Ithaca", "Jamestown", "Johnstown", "Kingston", "Lackawanna", "Little Falls",
            "Lockport", "Long Beach", "Mechanicville", "Middletown", "Mount Vernon", "New Rochelle", "New York City", "Newburgh", "Niagara Falls", "North Tonawanda", "Norwich",
            "Ogdensburg", "Olean", "Oneida", "Oneonta", "Oswego", "Peekskill", "Plattsburgh", "Port Jervis", "Poughkeepsie", "Rensselaer", "Rochester", "Rome", "Rye", "Salamanca",
            "Saratoga Springs", "Schenectady", "Sherrill", "Syracuse", "Tonawanda", "Troy", "Utica", "Watertown", "Watervliet", "White Plains", "Yonkers"
    };
    String[] ny_contries = new String[]{
            "Albany", "Montgomery", "Cayuga", "Genesee", "Dutchess", "Broome", "Erie", "Ontario", "Steuben", "Cortland", "Chautauqua", "Chemung", "Oswego", "Seneca", "Nassau", "Warren",
            "Fulton", "Columbia", "Tompkins", "Ulster", "Herkimer", "Niagara", "Saratoga", "Orange", "Westchester", "Chenango", "St. Lawrence", "Cattaraugus", "Madison", "Otsego", "Clinton",
            "Rensselaer", "Monroe", "Oneida", "Schenectady", "Onondaga", "Jefferson"
    };
    HashMap<Integer, Integer> node_city_map = new HashMap<>();
    HashMap<Integer, Pair<Double, Double>> node_info = new HashMap<>();

    public static void main(String args[]) {
        SplitGraph sg = new SplitGraph();
        sg.readFile();
        sg.mappingNode();

    }


    public void readFile() {
        String bs_path = "/home/gqxwolf/mydata/projectData/testGraph_real_50_Random/data";
        String path_node_with_details = bs_path + "/Node_with_placeDetails.txt";
        try {
            int non_list = 0;

            File f = new File(path_node_with_details);
            BufferedReader b = new BufferedReader(new FileReader(f));
            String readLine = "";

            while (((readLine = b.readLine()) != null)) {
                String[] infors = readLine.split(";");
                int id = Integer.parseInt(infors[0]);
                double lat = Double.parseDouble(infors[1]);
                double lng = Double.parseDouble(infors[2]);


                StringBuffer sb = new StringBuffer();

                for (int i = 4; i < infors.length; i++) {
                    sb.append(infors[i]);
                }

                String s = sb.toString();
                int city_id = CheckInList(s.split("\\|")[0]);
                if (city_id == city_none) {
                    non_list++;
                } else {
                    node_city_map.put(id, city_id);
                    node_info.put(id, new Pair<>(lat, lng));

                }


            }
            System.out.println(non_list);
            b.close();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public int CheckInList(String s) {

        if (s.contains("CA")) {
            for (String s_name : s.split(",")) {
                for (String c_name : sf_contries) {
                    if (s_name.equals(c_name)) {
                        return city_sf;
                    }
                }


                for (String c_name : sf_names) {
                    if (s_name.equals(c_name)) {
                        return city_sf;
                    }
                }


                for (String c_name : la_names) {
                    if (s_name.equals(c_name)) {
                        return city_la;
                    }
                }
            }
        } else if (s.contains("NY") || s.contains("NJ") || s.contains("PA") || s.contains("CT")) {
            for (String s_name : s.split(",")) {
                for (String c_name : ny_contries) {
                    if (s_name.equals(c_name)) {
                        return city_ny;
                    }
                }


                for (String c_name : ny_names) {
                    if (s_name.equals(c_name)) {
                        return city_ny;
                    }
                }

            }

        }

        return city_none;
    }

    public void mappingNode() {
        TreeMap<Integer, Integer> sortedNodeList = new TreeMap<>(this.node_city_map); //original id --> city_id

        TreeMap<Integer, Integer> LA_Map = new TreeMap<>(); //original id --> new id
        TreeMap<Integer, Integer> SF_Map = new TreeMap<>(); //original id --> new id
        TreeMap<Integer, Integer> NY_Map = new TreeMap<>(); //original id --> new id

        for (Map.Entry<Integer, Integer> e : sortedNodeList.entrySet()) {

            switch (e.getValue()) {
                case city_sf:
                    SF_Map.put(e.getKey(), SF_Map.size());
                    break;
                case city_la:
                    LA_Map.put(e.getKey(), LA_Map.size());
                    break;
                case city_ny:
                    NY_Map.put(e.getKey(), NY_Map.size());
                    break;

            }
        }

        System.out.println(LA_Map.size() + "  " + SF_Map.size() + "  " + NY_Map.size());
        mappingEdges(SF_Map, "SF");
        mappingEdges(LA_Map, "LA");
        mappingEdges(NY_Map, "NY");
    }

    private void mappingEdges(TreeMap<Integer, Integer> node_Map, String city_name) {
        TreeMap<Integer, Integer> sortedNodeList = new TreeMap<>(node_Map); //original id --> new id


        String bs_path = "/home/gqxwolf/mydata/projectData/testGraph_real_50_Random/data";
        String path_Edge = bs_path + "/SegInfo.txt";

        String target_edge = bs_path + "/" + city_name + "_SegInfo.txt";
        String target_node = bs_path + "/" + city_name + "_NodeInfo.txt";

        BufferedWriter writer_node = null;
        BufferedWriter writer_edge = null;
        try {
            int non_list = 0;


            if (new File(target_edge).exists()) {
                new File(target_edge).delete();
            }
            if (new File(target_node).exists()) {
                new File(target_node).delete();
            }

            writer_edge = new BufferedWriter(new FileWriter(target_edge, true));


            File f = new File(path_Edge);
            BufferedReader b = new BufferedReader(new FileReader(f));
            String readLine = "";

            while (((readLine = b.readLine()) != null)) {
                int sid, did;
                double[] costs = new double[3];
                sid = Integer.parseInt(readLine.split(" ")[0]);
                did = Integer.parseInt(readLine.split(" ")[1]);

                if (node_Map.containsKey(sid) && node_Map.containsKey(did)) { //if the start node and the end node are in the same city
                    costs[0] = Double.parseDouble(readLine.split(" ")[2]);
                    costs[1] = Double.parseDouble(readLine.split(" ")[3]);
                    costs[2] = Double.parseDouble(readLine.split(" ")[4]);

//                    costs[0] = randomFloatInRange(1,20);
//                    costs[1] = randomFloatInRange(1,20);
//                    costs[2] = randomFloatInRange(1,20);
                    writer_edge.write(node_Map.get(sid) + " " + node_Map.get(did) + " " + costs[0] + " " + costs[1] + " " + costs[2] + "\n");
                }
            }

            b.close();
            writer_edge.close();


            writer_node = new BufferedWriter(new FileWriter(target_node, true));
            for (Map.Entry<Integer, Integer> e : sortedNodeList.entrySet()) {
                writer_node.write(e.getValue() + " " + this.node_info.get(e.getKey()).getKey() + " " + this.node_info.get(e.getKey()).getValue() + "\n");

            }
            writer_node.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public float randomFloatInRange(float min, float max) {
        Random r = new Random();
        float random = min + r.nextFloat() * (max - min);
        return random;
    }
}
