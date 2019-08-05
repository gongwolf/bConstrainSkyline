package unDirectionalGraph;

import javafx.util.Pair;
import neo4jTools.StringComparator;
import org.apache.commons.io.FileUtils;

import java.io.*;
import java.util.*;

public class generateGraph {
    String DBBase = "/home/gqxwolf/mydata/projectData/un_testGraph2000_5/data/";
    String EdgesPath = DBBase + "SegInfo.txt";
    String NodePath = DBBase + "NodeInfo.txt";

    public generateGraph(int numberNodes, int numberofDegree, int numberofDimen) {
        this.DBBase = "/home/gqxwolf/mydata/projectData/un_testGraph" + numberNodes + "_" + numberofDegree + "/data/";
        System.out.println(this.DBBase);
    }

    public static void main(String args[]) {
        int numberNodes = 2000;
        int numberofDegree = 5;
        int numberofEdges = (int) Math.round(numberNodes * (numberofDegree));
        int numberofDimen = 3;
        generateGraph g = new generateGraph(numberNodes, numberofDegree, numberofDimen);
        g.generateG(numberNodes, numberofEdges, numberofDimen);

    }

    private void generateG(int numberNodes, int numberofEdges, int numberofDimens) {
        File dataF = new File(DBBase);
        try {
            FileUtils.deleteDirectory(dataF);
            dataF.mkdirs();
        } catch (IOException e) {
            e.printStackTrace();
        }
        HashMap<Pair<String, String>, String[]> Edges = new HashMap<>();
        HashMap<String, String[]> Nodes = new HashMap<>();


        //生成经度和纬度
        for (int i = 0; i < numberNodes; i++) {
            String cost1 = String.valueOf(getRandomNumberInRange(1, 180));
            String cost2 = String.valueOf(getRandomNumberInRange(1, 180));
            Nodes.put(String.valueOf(i), new String[]{cost1, cost2});
        }

        //Create the Edges information.
        for (int i = 0; i < numberNodes; i++) {
            int degree_count = 0;
            String startNode = String.valueOf(i);
            while (degree_count != 5) {
                //randomly pick one other nodes that not equal to i.
                int endNodeId = getRandomNumberInRange(0, numberNodes - 1, i);
                String endNode = String.valueOf(endNodeId);


                //If already has one same edge in the set, regenerate a new edge
                while (Edges.keySet().contains(new Pair<>(startNode, endNode)) || Edges.keySet().contains(new Pair<>(endNode, startNode))) {
                    endNodeId = getRandomNumberInRange(0, numberNodes - 1, i);
                    endNode = String.valueOf(endNodeId);
                }


                String[] costs = new String[numberofDimens];
                for (int j = 0; j < numberofDimens; j++) {
                    costs[j] = String.valueOf(getRandomNumberInRange(1, 500));
                }
                Edges.put(new Pair<>(startNode,endNode),costs);
                degree_count++;
            }
        }

        writeNodeToDisk(Nodes);
        writeEdgeToDisk(Edges);

        System.out.println(Nodes.size());
        System.out.println(Edges.size());


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

    private int getRandomNumberInRange(int min, int max, int DontWant) {

        if (min >= max) {
            throw new IllegalArgumentException("max must be greater than min");
        }

        Random r = new Random();
        int result = r.nextInt((max - min) + 1) + min;
        while (result == DontWant) {
            result = r.nextInt((max - min) + 1) + min;
        }
        return result;
    }


}
