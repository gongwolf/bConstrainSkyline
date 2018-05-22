package neo4jTools;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

public class mimicBusLine {
    String DBBase = "/home/gqxwolf/mydata/projectData/busline/data/";
    String EdgesPath = DBBase + "SegInfo.txt";
    String NodePath = DBBase + "NodeInfo.txt";
    //direction
    final int righttop = 1;
    final int rightbuttom = 2;
    final int leftbuttom = 3;
    final int lefttop = 4;
    private final int numofNode;
    HashMap<Integer, node> Nodes = new HashMap<>();
    int max_node_id;

    public mimicBusLine(int numofNode) {
        this.numofNode = numofNode;
        this.max_node_id = 0;
    }

    public static void main(String args[]) {
        mimicBusLine m = new mimicBusLine(100);
        m.generateBusline();
    }

    public void generateBusline() {
        int num_bus_inLine;
        if (numofNode - Nodes.size() < 10) {
            num_bus_inLine = numofNode - Nodes.size();
        } else {
            num_bus_inLine = getRandomNumberInRange_int(10, 20);
        }
        System.out.println(num_bus_inLine);

        int[] bus_ids = new int[num_bus_inLine];

        for (int i = 0; i < num_bus_inLine; i++) {
            System.out.println(i);
            node new_n = new node();

            if (i < 2) {
                new_n.latitude = getRandomNumberInRange(0, 360);
                new_n.longitude = getRandomNumberInRange(0, 360);
                System.out.println(new_n.latitude+" "+new_n.longitude);
            } else {
                int not_go_direction = 0;
                if (Nodes.get(bus_ids[i - 1]).latitude - Nodes.get(bus_ids[i - 2]).latitude >= 0 && Nodes.get(bus_ids[i - 1]).longitude - Nodes.get(bus_ids[i - 2]).longitude >= 0) {
                    not_go_direction = this.leftbuttom;
                } else if (Nodes.get(bus_ids[i - 1]).latitude - Nodes.get(bus_ids[i - 2]).latitude >= 0 && Nodes.get(bus_ids[i - 1]).longitude - Nodes.get(bus_ids[i - 2]).longitude <= 0) {
                    not_go_direction = this.lefttop;
                } else if (Nodes.get(bus_ids[i - 1]).latitude - Nodes.get(bus_ids[i - 2]).latitude <= 0 && Nodes.get(bus_ids[i - 1]).longitude - Nodes.get(bus_ids[i - 2]).longitude <= 0) {
                    not_go_direction = this.righttop;
                } else {
                    not_go_direction = this.rightbuttom;
                }

                int next_direction = getRandomDirection(not_go_direction);

                double p_l = Nodes.get(bus_ids[i - 1]).latitude;
                double p_g = Nodes.get(bus_ids[i - 1]).longitude;

                switch (next_direction) {
                    case righttop:
                        new_n.latitude = getRandomNumberInRange(p_l, p_l + 10);
                        new_n.longitude = getRandomNumberInRange(p_g, p_g + 10);
                        break;
                    case rightbuttom:
                        new_n.latitude = getRandomNumberInRange(p_l, p_l + 10);
                        new_n.longitude = getRandomNumberInRange(p_g - 10, p_g);
                        break;
                    case leftbuttom:
                        new_n.latitude = getRandomNumberInRange(p_l - 10, p_l);
                        new_n.longitude = getRandomNumberInRange(p_g - 10, p_g);
                        break;
                    case lefttop:
                        new_n.latitude = getRandomNumberInRange(p_l - 10, p_l);
                        new_n.longitude = getRandomNumberInRange(p_g, p_g + 10);
                        break;
                }


            }

            int newid;
            if ((newid = hasNodes(new_n)) == -1) {
                newid = max_node_id;
                max_node_id++;
                this.Nodes.put(newid, new_n);
            } else {
                new_n.latitude = this.Nodes.get(newid).latitude;
                new_n.longitude = this.Nodes.get(newid).longitude;
            }
            new_n.id = newid;
            bus_ids[i] = newid;
        }


    }

    private int hasNodes(node node) {
        int flag = -1;
        for (Map.Entry<Integer, neo4jTools.node> n : this.Nodes.entrySet()) {
            if (Math.sqrt(Math.pow(n.getValue().latitude - node.latitude, 2) + Math.pow(n.getValue().longitude - node.longitude, 2)) < 2) {
                flag = n.getKey();
                System.out.println("find a node "+flag);
            }
        }
        return flag;
    }

    private double getRandomNumberInRange(double min, double max) {

        if (min >= max) {
            throw new IllegalArgumentException("max must be greater than min");
        }

        Random r = new Random();
        return r.nextDouble() * (max - min) + min;
    }


    private int getRandomNumberInRange_int(int min, int max) {

        if (min >= max) {
            throw new IllegalArgumentException("max must be greater than min");
        }

        Random r = new Random(System.currentTimeMillis());
        return r.nextInt((max - min) + 1) + min;
    }

    private int getRandomDirection(int not_go_dirct) {
        int direction = getRandomNumberInRange_int(1, 4);
        while (direction == not_go_dirct) {
            direction = getRandomNumberInRange_int(1, 4);
        }
        return direction;
    }

    private double getGaussian(double mean, double sd) {
        Random r = new Random(System.currentTimeMillis());
        double value = r.nextGaussian() * sd + mean;

        while (value <= 0) {
            value = r.nextGaussian() * sd + mean;
        }

        return value;
    }


    private void writeNodeToDisk(HashMap<String, String[]> nodes) {
        try (FileWriter fw = new FileWriter(NodePath, true);
             BufferedWriter bw = new BufferedWriter(fw);
             PrintWriter out = new PrintWriter(bw)) {
            System.out.println(NodePath);
            TreeMap<String, String[]> tm = new TreeMap<String, String[]>(new StringComparator());
            tm.putAll(nodes);
            for (Map.Entry<String, String[]> node : tm.entrySet()) {
                StringBuffer sb = new StringBuffer();
                String nodeId = node.getKey();
                sb.append(nodeId).append(" ");
                for (String cost : node.getValue()) {
                    sb.append(cost).append(" ");
                }
                out.println(sb.toString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

class node {
    int id;
    double latitude, longitude;
}
