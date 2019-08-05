package GraphPartition;

import neo4jTools.StringComparator;

import java.io.*;
import java.util.*;

public class MetisFile {
    String DBBase = "/home/gqxwolf/mydata/projectData/testGraph40000/data/";
    String NodePath = DBBase + "NodeInfo.txt";
    String EdgePath = DBBase + "SegInfo.txt";
    String metisGraphFile = DBBase + "metisFormatFile.csv";
    String mappingPath = DBBase + "mapping/";
//    String mappedGraphFileName = "mapped_metis_10000.graph";


    //Node_id, the adj nodes of this node id.
    HashMap<String, ArrayList<String[]>> NodeList = new HashMap<>();


    HashMap<String, Boolean> remainNodes = new HashMap<>();
    ArrayList<HashMap<String, ArrayList<String[]>>> connectionSets = new ArrayList<>();

    public MetisFile() {

    }


    public static void main(String args[]) {
        MetisFile mf = new MetisFile();
//        mf.generateMetisFile(-1);
        mf.checkingConnection("1");
//        System.out.println("======================");
        String mappingfile = "mapped_metis_1.graph";
        mf.countEdges(mappingfile);
    }

    /**
     * transfer the graph information of the graph from nodepath and edgepath, to the metis formed file.
     * original node id becomes the id+1 in metis csv file
     *
     * @param costid the i-th id to include into the metis file
     */
    public void generateMetisFile(int costid) {
        try {
            FileWriter fw = new FileWriter(metisGraphFile, true);
            BufferedWriter bw = new BufferedWriter(fw);
            BufferedReader br = new BufferedReader(new FileReader(NodePath));
            String line = null;
            int i = 0;

            while ((line = br.readLine()) != null) {
                // read the node
                String Nodeid = line.split(" ")[0];
                String content = readSegMentInfo(Nodeid, costid);
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

    /**
     * find the edges start or end with the give node.
     * cost is -1, means take the graph as a unweighted graph, don't consider the weight.
     * else return the i-th cost of the edge as well.
     *
     * @param Nodeid the node id that find the edges that directly connect to id
     * @param costid the i-th cost.
     * @return: the string of connection information of NodeId.
     * Such as t1 cost1 t2 cost2 ...... tn costn
     */
    public String readSegMentInfo(String Nodeid, int costid) {
        StringBuffer infos = new StringBuffer();
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(EdgePath));
            String line = null;
            while ((line = br.readLine()) != null) {
                String[] segInfo = line.split(" ");

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
        //while there is still some node does not cover by some of the connection component
        while (!remainNodes.isEmpty()) {
            //get a random node to start the dfs
            String nodeid = getRandomStartNode();
            //Node nid belong to this connection component
            //Adj nodes of the nid, String[]{adj nid, cost1, cost2....}
            HashMap<String, ArrayList<String[]>> set = runDFS(nodeid);
            System.out.println(id + " " + set.size());
            this.connectionSets.add(set);
            mappingToMetisFormat(set, id);
            id++;

//            if (set.size() < 10000) {
//                mappingToMetisFormat(set, id);
//                id++;
//                System.out.println(set.size());
//            }
        }
        System.out.println("===========================");
        System.out.println(connectionSets.size());
    }

    /**
     * load node information from metis graph file.
     * all nodes in remainNodes are <nodeid, false>.
     * all the adj information are put in NodeList
     * <nodeId, ArrayList<
     * String{adj_node_1,cost_i_of_node_1},
     * String{adj_node_2,cost_i_of_node_2},
     * ........
     * String{adj_node_n,cost_i_of_node_n},
     * >
     * >
     */
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


    /**
     * Get a random node from remainNodes.
     *
     * @return a node in the have not been processed randomly.
     */
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
            //neighbor of the cNode
            ArrayList<String[]> s = this.NodeList.get(cNode);

            if (!sub_NodeList.containsKey(cNode)) {
                sub_NodeList.put(cNode, s);
            }

            this.remainNodes.remove(cNode);

            for (int i = 0; i < s.size(); i++) {
                //n_nodeid is the adj node id of cNode
                String n_nodeid = s.get(i)[0];
                //if the n_nodeid is un-visited, add to stack and remove from remainNodes.
                if (this.remainNodes.containsKey(n_nodeid)) {
                    st.add(n_nodeid);
                }
            }
        }
        return sub_NodeList;
    }

    /**
     * @param set the node and its adj information in this connection component
     * @param id  the id of the connection component
     */
    private void mappingToMetisFormat(HashMap<String, ArrayList<String[]>> set, int id) {
        //sort the nodes in this connection component by id
        TreeMap<String, ArrayList<String[]>> tSet = new TreeMap<>(new StringComparator());
        tSet.putAll(set);

        int mapping_id = 1;
        HashMap<String, String> node_mapping = new HashMap<>();

        //Given a mapped id to each node in this connection component from 1
        for (Map.Entry<String, ArrayList<String[]>> entry : tSet.entrySet()) {
            String n_id = entry.getKey();
            node_mapping.put(n_id, String.valueOf(mapping_id));
            mapping_id++;
        }

        HashMap<String, ArrayList<String[]>> mapped_set = createMappingEdgeInfo(set, node_mapping);
//        System.out.println(mapped_set.size());
//        printGraphInfo(mapped_set);

//        CompareSet(randomNodeid(set.keySet()), set, mapped_set, node_mapping);
        writeToDisk(node_mapping, set, mapped_set, id);
    }

    /**
     * @param set          the node and its adj information in this connection component
     * @param node_mapping the mapping of the origin node id to the mapped node id that is in this connection component
     * @return all the mapped adj information are put in NodeList
     * <mapped_node_id, ArrayList<
     * String{mappedID_of_adj_node_1,cost_i_of_node_1},
     * String{mappedID_of_adj_node_2,cost_i_of_node_2},
     * ........
     * String{mappedID_of_adj_node_n,cost_i_of_node_n},
     * >
     * >
     */
    private HashMap<String, ArrayList<String[]>> createMappingEdgeInfo(HashMap<String, ArrayList<String[]>> set, HashMap<String, String> node_mapping) {
        HashMap<String, ArrayList<String[]>> Mapped_node_list = new HashMap<>();

        for (Map.Entry<String, ArrayList<String[]>> entry : set.entrySet()) {
            String n_id = entry.getKey();
            //get the mapped id by the original node id
            String mapped_id = node_mapping.get(n_id);
            ArrayList<String[]> mapped_adj_infos = new ArrayList<>();
            for (String[] adj_infos : entry.getValue()) {
                String mapped_adj[] = new String[1];
                //get the mapped id from the original node id of the adj node of n_id
                mapped_adj[0] = node_mapping.get(adj_infos[0]);
//                mapped_adj[1] = adj_infos[1];
                mapped_adj_infos.add(mapped_adj);
            }
            Mapped_node_list.put(mapped_id, mapped_adj_infos);
        }

        return Mapped_node_list;
    }

    /**
     * write the connection component information.
     * write the mapped adj information of mapped node id from 1 to n to mapped_metis_ +id+.graph.
     * write the mapping information from original id to mapped id in the file node_mapping_ + id + .txt
     *
     * @param node_mapping the mapping of the origin node id to the mapped node id that is in this connection component
     * @param set          the original node id and its adj information in this connection component
     * @param mapped_set   the mapped node id and its mapped adj node id information in this connection component
     * @param id           the id of the connection component
     */
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
            int max_id = node_mapping.size();

            for (int i = 1; i <= max_id; i++) {
                StringBuffer sb = new StringBuffer();
                ArrayList<String[]> n_infos = mapped_set.get(String.valueOf(i));
                for (String[] adj_infos : n_infos) {
                    String n_id = adj_infos[0];
                    sb.append(n_id).append(" ");
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

    public int countEdges(String mappedGraphFileName) {
        int EdgesNum = 0;
        int nodeNum = 0;
        try {
            BufferedReader br = new BufferedReader(new FileReader(this.DBBase + "/mapping/" + mappedGraphFileName));
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
        System.out.println(EdgesNum / 2);

        return EdgesNum / 2;
    }
}
