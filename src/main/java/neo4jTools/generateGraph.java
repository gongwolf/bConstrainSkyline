package neo4jTools;

import javafx.util.Pair;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;

public class generateGraph {
    String DBBase = "/home/gqxwolf/mydata/projectData/testGraph10_2/data/";
    String EdgesPath = DBBase + "SegInfo.txt";
    String NodePath = DBBase + "NodeInfo.txt";

    int numberNodes, numberofEdges, numberofDimens;


    public generateGraph(int graphsize, int degree, int dimensions) {
        this.numberNodes = graphsize;
        this.numberofEdges = (int) Math.round(numberNodes * (degree));
        this.numberofDimens = dimensions;

        this.DBBase = "/home/gqxwolf/mydata/projectData/testGraph" + graphsize + "_" + degree + "/data/";
        EdgesPath = DBBase + "SegInfo.txt";
        NodePath = DBBase + "NodeInfo.txt";

    }


    public static void main(String args[]) {
        int numberNodes = 1000;
        int numberofDegree = 2;
        int numberofDimen = 3;
        generateGraph g = new generateGraph(numberNodes, numberofDegree, numberofDimen);
        g.generateG();

    }

    public void generateG() {
        //File dataF = new File(DBBase);
        //try {
        //    FileUtils.deleteDirectory(dataF);
        //    dataF.mkdirs();
        //} catch (IOException e) {
        //    e.printStackTrace();
        //}

        HashMap<Pair<String, String>, String[]> Edges = new HashMap<>();
        HashMap<String, String[]> Nodes = new HashMap<>();


        //生成经度和纬度
        for (int i = 0; i < numberNodes; i++) {
            String cost1 = String.valueOf(getRandomNumberInRange(1, 180));
            String cost2 = String.valueOf(getRandomNumberInRange(1, 180));
            Nodes.put(String.valueOf(i), new String[]{cost1, cost2});
        }

        //Create the Edges information.
        for (int i = 0; i < numberofEdges; i++) {
            String startNode = String.valueOf(getRandomNumberInRange(0, numberNodes - 1));
            String endNode = String.valueOf(getRandomNumberInRange(0, numberNodes - 1));
            while (startNode.equals(endNode)) {
                endNode = String.valueOf(getRandomNumberInRange(0, numberNodes - 1));
            }

            String[] costs = new String[numberofDimens];
            for (int j = 0; j < numberofDimens; j++) {
                costs[j] = String.valueOf(getRandomNumberInRange(1, 500));
            }

            Edges.put(new Pair(startNode, endNode), costs);
        }

        //stores all of the start node of the created edges
        HashSet<String> containedNodes = new HashSet<>();
        for (Pair<String, String> p : Edges.keySet()) {
            containedNodes.add(p.getKey());
        }

        for (String node : Nodes.keySet()) {
            //if there is the node does not have any edge start with it
            //Create edge for it
            if (!containedNodes.contains(node)) {
                String startNode = String.valueOf(node);
                String endNode = String.valueOf(getRandomNumberInRange(0, numberNodes - 1));
                while (startNode.equals(endNode)) {
                    endNode = String.valueOf(getRandomNumberInRange(0, numberNodes - 1));
                }

                String[] costs = new String[numberofDimens];
                for (int j = 0; j < numberofDimens; j++) {
                    costs[j] = String.valueOf(getRandomNumberInRange(1, 500));
                }

                Edges.put(new Pair(startNode, endNode), costs);
            }
        }
        writeNodeToDisk(Nodes);
        writeEdgeToDisk(Edges);

        System.out.println("Graph is created, there are "+Nodes.size()+" nodes and "+Edges.size()+" edges");
    }

    private void writeEdgeToDisk(HashMap<Pair<String, String>, String[]> edges) {
        try (FileWriter fw = new FileWriter(EdgesPath, true);
             BufferedWriter bw = new BufferedWriter(fw);
             PrintWriter out = new PrintWriter(bw)) {
            for (Map.Entry<Pair<String, String>, String[]> node : edges.entrySet()) {
//                System.out.println(EdgesPath);
                StringBuffer sb = new StringBuffer();
                String snodeId = node.getKey().getKey();
                String enodeId = node.getKey().getValue();
                sb.append(snodeId).append(" ");
                sb.append(enodeId).append(" ");
                for (String cost : node.getValue()) {
                    sb.append(cost).append(" ");
                }
                out.println(sb.toString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
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

    private int getRandomNumberInRange(int min, int max) {

        if (min >= max) {
            throw new IllegalArgumentException("max must be greater than min");
        }

        Random r = new Random();
        return r.nextInt((max - min) + 1) + min;
    }


}
