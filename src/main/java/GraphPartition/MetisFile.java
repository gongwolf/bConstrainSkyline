package GraphPartition;

import neo4jTools.StringComparator;

import java.io.*;
import java.util.*;

public class MetisFile {
    String NodePath = "/home/gqxwolf/mydata/projectData/testGraph/data/NodeInfo.txt";
    String EdgePath = "/home/gqxwolf/mydata/projectData/testGraph/data/SegInfo.txt";
    String metisGraphFile = "/home/gqxwolf/mydata/projectData/testGraph/data/metisFormatFile.csv";
    String mappingPath = "/home/gqxwolf/mydata/projectData/testGraph/data/mapping/";


    HashMap<String, ArrayList<String[]>> NodeList = new HashMap<>();
    HashMap<String, Boolean> remainNodes = new HashMap<>();
    ArrayList<HashMap<String, ArrayList<String[]>>> connectionSets = new ArrayList<>();

    public static void main(String args[]) {
        MetisFile mf = new MetisFile();
        mf.generateMetisFile(-1);
        mf.checkingConnection("1");
        System.out.println("======================");
        mf.countEdges("/home/gqxwolf/mydata/projectData/testGraph/data/metisFormatFile.csv");
    }

    public void generateMetisFile(int costid) {
        try {
            FileWriter fw = new FileWriter(metisGraphFile, true);
            BufferedWriter bw = new BufferedWriter(fw);
            BufferedReader br = new BufferedReader(new FileReader(NodePath));
            String line = null;
            int i = 0;

            while ((line = br.readLine()) != null) {
                String Nodeid = line.split(" ")[0];
                System.out.println(Nodeid);
                String content = readSegMentInfo(Nodeid, costid);
//                 System.out.println(content);
                // bw.write((Long.parseLong(Nodeid) + 1) + " " + content);
//                break;
                bw.write(content);
                i++;
                if (i % 10000 == 0) {
                    System.out.println(i + "..............");
                    bw.flush();
                }
            }
            System.out.println("done");
            br.close();
            bw.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public String readSegMentInfo(String Nodeid, int costid) {
        // System.out.println(Nodeid);
        StringBuffer infos = new StringBuffer();
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(EdgePath));
            String line = null;
            while ((line = br.readLine()) != null) {
                String[] segInfo = line.split(" ");
                // System.out.println(segInfo[0]+"
                // "+segInfo[1]+Nodeid.equals(segInfo[0]));
                if (Nodeid.equals(segInfo[0])) {
                    infos.append(Long.parseLong(segInfo[1]) + 1).append(" ");
                } else if (Nodeid.equals(segInfo[1])) {
                    infos.append(Long.parseLong(segInfo[0]) + 1).append(" ");
                } else {
                    continue;
                }
                if (costid != -1) {
                    infos.append(segInfo[costid + 2]).append(" ");
                }
            }
            br.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        // System.out.println(infos.toString());
        return infos.append("\n").toString();
    }

    public void checkingConnection(String StartNodeId) {
        this.loadNodeInfo();

        File mappingDir = new File(mappingPath);
        if (mappingDir.exists()) {
            mappingDir.delete();
        }
        mappingDir.mkdirs();
//
        int id = 1;
        while (!remainNodes.isEmpty()) {
            String nodeid = getRandomStartNode();
            HashMap<String, ArrayList<String[]>> set = runDFS(nodeid);
            System.out.println(set.size());
            this.connectionSets.add(set);
            mappingToMetisFormat(set, id);


//            if (set.size() < 10000) {
//                mappingToMetisFormat(set, id);
//                id++;
//                System.out.println(set.size());
////                break;
//            }
        }
        System.out.println("===========================");
        System.out.println(connectionSets.size());
    }

    public void loadNodeInfo() {
        try {
            BufferedReader br = new BufferedReader(new FileReader(metisGraphFile));
            String line = null;
            int i = 1;
            while ((line = br.readLine()) != null) {
                String nodeid = String.valueOf(i);
                this.remainNodes.put(String.valueOf(i), false);
                String[] adjInfos = line.split(" ");
                //Store the adj nodes of one of the node.
                ArrayList<String[]> n_infos = new ArrayList<>();

                for (int j = 0; j < adjInfos.length; j++) {
                    String n_node = adjInfos[j];
                    n_infos.add(new String[]{n_node});
                }

                this.NodeList.put(nodeid, n_infos);
                i++;

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private String getRandomStartNode() {
        Random r = new Random();
        List<String> rList = new ArrayList<>(this.remainNodes.keySet());
        String randomKey = rList.get(r.nextInt(rList.size()));
        return randomKey;
    }

    private HashMap<String, ArrayList<String[]>> runDFS(String nodeid) {
        HashMap<String, ArrayList<String[]>> sub_NodeList = new HashMap<>();

        Stack<String> st = new Stack<>();
//        Queue<String> queue = new LinkedList();

        st.add(nodeid);
        while (!st.isEmpty()) {
            String cNode = st.pop();
            ArrayList<String[]> s = this.NodeList.get(cNode);
            if (!sub_NodeList.containsKey(cNode)) {
                sub_NodeList.put(cNode, s);
            }

            this.remainNodes.remove(cNode);

//            if (this.remainNodes.containsKey(cNode)) {
            for (int i = 0; i < s.size(); i++) {
                String n_nodeid = s.get(i)[0];
                if (this.remainNodes.containsKey(n_nodeid)) {
                    st.add(n_nodeid);
                    this.remainNodes.remove(n_nodeid);
                }
            }
//            }
        }
        return sub_NodeList;
    }


    private void mappingToMetisFormat(HashMap<String, ArrayList<String[]>> set, int id) {
        TreeMap<String, ArrayList<String[]>> tSet = new TreeMap<>(new StringComparator());
        tSet.putAll(set);

        int mapping_id = 1;
        HashMap<String, String> node_mapping = new HashMap<>();

        for (Map.Entry<String, ArrayList<String[]>> entry : tSet.entrySet()) {
            String n_id = entry.getKey();
            node_mapping.put(n_id, String.valueOf(mapping_id));
//            System.out.println(n_id + "--->" + mapping_id);
            mapping_id++;
        }
//        System.out.println("==========================================");
//        System.out.println(tSet.size());
//        printGraphInfo(set);
        HashMap<String, ArrayList<String[]>> mapped_set = createMappingEdgeInfo(set, node_mapping);
//        System.out.println(mapped_set.size());
//        printGraphInfo(mapped_set);

//        CompareSet(randomNodeid(set.keySet()), set, mapped_set, node_mapping);
        writeToDisk(node_mapping, set, mapped_set, id);
    }

    private HashMap<String, ArrayList<String[]>> createMappingEdgeInfo(HashMap<String, ArrayList<String[]>> set, HashMap<String, String> node_mapping) {
        HashMap<String, ArrayList<String[]>> Mapped_node_list = new HashMap<>();

        for (Map.Entry<String, ArrayList<String[]>> entry : set.entrySet()) {
            String n_id = entry.getKey();
            String mapped_id = node_mapping.get(n_id);
            ArrayList<String[]> mapped_adj_infos = new ArrayList<>();
            for (String[] adj_infos : entry.getValue()) {
                String mapped_adj[] = new String[1];
                mapped_adj[0] = node_mapping.get(adj_infos[0]);
//                mapped_adj[1] = adj_infos[1];
                mapped_adj_infos.add(mapped_adj);
            }
            Mapped_node_list.put(mapped_id, mapped_adj_infos);
        }

        return Mapped_node_list;
    }

    private void writeToDisk(HashMap<String, String> node_mapping, HashMap<String, ArrayList<String[]>> set, HashMap<String, ArrayList<String[]>> mapped_set, int id) {
        String node_mapping_path = this.mappingPath + "node_mapping_" + id + ".txt";
        String origin_path = this.mappingPath + "origin_metis_" + id + ".graph";
        String mapped_path = this.mappingPath + "mapped_metis_" + id + ".graph";

        File mapped_file = new File(mapped_path);
        File node_mapping_file = new File(node_mapping_path);
        if (mapped_file.exists()) {
            mapped_file.delete();
        }

        if (node_mapping_file.exists()) {
            node_mapping_file.delete();
        }

        try (FileWriter fw = new FileWriter(mapped_file.getAbsoluteFile(), true);
             BufferedWriter bw = new BufferedWriter(fw);
             PrintWriter out = new PrintWriter(bw)) {
            int c_id = 1;
            int max_id = node_mapping.size();
//        printGraphInfo(mapped_set);

            for (int i = 1; i <= max_id; i++) {
                StringBuffer sb = new StringBuffer();
                ArrayList<String[]> n_infos = mapped_set.get(String.valueOf(i));
                for (String[] adj_infos : n_infos) {
                    String n_id = adj_infos[0];
                    sb.append(n_id).append(" ");

//                    String n_cost = adj_infos[1];
//                    sb.append(n_id).append(" ").append(n_cost).append(" ");
                }
                out.println(sb);
            }

        } catch (IOException e) {
            //exception handling left as an exercise for the reader
        }


        //write the mapping of the original node to mapped node
        try (FileWriter fw = new FileWriter(node_mapping_file.getAbsoluteFile(), true);
             BufferedWriter bw = new BufferedWriter(fw);
             PrintWriter out = new PrintWriter(bw)) {

            StringBuffer sb = new StringBuffer();

            for (Map.Entry<String, String> e : node_mapping.entrySet()) {
                String o_node = e.getKey();
                String m_node = e.getValue();
                sb.append(o_node).append(" ").append(m_node).append("\n");
            }


            out.println(sb);


        } catch (IOException e) {
            //exception handling left as an exercise for the reader
        }
    }

    public int countEdges(String path) {
        int EdgesNum = 0;
        int nodeNum = 0;
        try {
            BufferedReader br = new BufferedReader(new FileReader(path));
            String line = null;
            while ((line = br.readLine()) != null) {
                EdgesNum = EdgesNum + (line.split(" ").length);
                nodeNum++;
            }
            System.out.println(nodeNum);
            br.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println(EdgesNum/2);

        return EdgesNum/2;
    }
}
