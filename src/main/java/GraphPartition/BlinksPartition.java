package GraphPartition;

import javafx.util.Pair;
import neo4jTools.StringComparator;
import org.neo4j.cypher.internal.frontend.v2_3.ast.In;
import org.neo4j.cypher.internal.frontend.v2_3.ast.functions.Sin;

import java.io.*;
import java.util.*;

public class BlinksPartition {
    private final String lowerboundSelector;
    private final String portalSelector;
    String PathBase = "/home/gqxwolf/mydata/projectData/testGraph10000/data/";
    String EdgesInfoPath = PathBase + "SegInfo.txt";
    String nodeMappingBase = PathBase + "mapping/";
    private ArrayList<Pair<String, String>> connectionInfos = new ArrayList<>();
    private HashMap<String, Pair<String, String>> partitionInfos = new HashMap<>();//node id --> pair<cid,pid>
    private HashMap<String, HashMap<String, Integer>> numberOfPartitions = new HashMap<>();

    public long NodeNum = 10000;

    blocks prts = new blocks();
    int out_port = 1;
    int in_port = 2;
    private int num_parts = 200;

    public BlinksPartition(int num_parts, long graphsize, String portalSelector, String lowerboundSelector) {
        this.num_parts = num_parts;
        this.PathBase = "/home/gqxwolf/mydata/projectData/testGraph" + graphsize + "/data/";
        this.NodeNum = graphsize;
        this.portalSelector = portalSelector;
        this.lowerboundSelector = lowerboundSelector;
        System.out.println(num_parts+" "+this.NodeNum+" "+this.portalSelector+" "+this.lowerboundSelector);
        System.out.println(this.PathBase);
    }

    public static void main(String args[]) {
        int num_parts = 100;
        long graphsize = 10000;
        String portalSelector = "Blinks";
        String lowerboundSelector = "landmark";
        if (args.length == 4) {
            num_parts = Integer.parseInt(args[0]);
            graphsize = Long.parseLong(args[1]);
            portalSelector = args[2];
            lowerboundSelector = args[3];
        }
        BlinksPartition bp = new BlinksPartition(num_parts, graphsize, portalSelector, lowerboundSelector);

        ArrayList<String> portals=null;
        if (portalSelector.equals("Blinks")) {
            portals = bp.getPortalsBlinks();
        } else if (portalSelector.equals("VC")) {
            portals = bp.getPortalsVertexCover();
        }
        System.out.println("===========================");
        System.out.println(portals.size());
        bp.cleanFadePortal(portals);
        bp.createBlocks(portals);

        System.out.println(bp.prts.blocks.size());


        bp.prts.randomSelectLandMark(3);
        long buildlandmark = System.currentTimeMillis();
        if (lowerboundSelector.equals("landmark")) {
            System.out.println("run landmark");
        } else if (lowerboundSelector.equals("dijkstra")) {
            System.out.println("run dijkstra");
        }
        bp.prts.buildIndexes(graphsize,lowerboundSelector);
        System.out.println("The time usage to build the landmark index " + (System.currentTimeMillis() - buildlandmark) + " ms");

        for (String pid : bp.prts.blocks.keySet()) {
            block b = bp.prts.blocks.get(pid);
            System.out.print(pid + "  " + b.nodes.size() + " " + b.iportals.size() + " " + b.oportals.size()
                    + " " + b.landMarks.size() + " " + b.fromLandMarkIndex.size() + " " + b.toLandMarkIndex.size()
                    + "  " + b.innerIndex.size());
            int count = 0;

            for (Map.Entry<Pair<String, String>, ArrayList<path>> p : b.innerIndex.entrySet()) {
                count += p.getValue().size();
            }

            System.out.print("  " + count + "\n");
        }


//        bp.writePoralsToDisk(portals);
//        bp.portalsMapping(portals);

    }


    private void createBlocks(ArrayList<String> portals) {
        TreeMap<String, Pair<String, String>> infoTM = new TreeMap<>(new StringComparator());
        infoTM.putAll(this.partitionInfos);
        for (Map.Entry<String, Pair<String, String>> node : infoTM.entrySet()) {

            int node_id = Integer.parseInt(node.getKey());

            if (portals.contains(node.getKey())) {
                ArrayList<String> in_nodes = getIncomingNodeToNode(node_id);
                for (String inode : in_nodes) {
                    if (!portals.contains(inode)) {
                        String pid = this.partitionInfos.get(inode).getValue();
                        //node is a out-going portal of the partition where the inode is located.
                        addToBlock(node_id, pid, out_port);
                    }
                }

                ArrayList<String> out_nodes = getOutGoingFromNode(node_id);
                for (String onode : out_nodes) {
                    if (!portals.contains(onode)) {
                        //get the pid of the onode
                        String pid = this.partitionInfos.get(onode).getValue();
                        //node is a in-coming portal of the partition where the onode is located.
                        addToBlock(node_id, pid, in_port);
                    }
                }
            } else {
                String pid = node.getValue().getValue();
                addToBlock(node_id, pid, 0);
            }

        }
        infoTM.clear();
    }

    private void addToBlock(int node_id, String pid, int node_type) {
        String str_nodeID = String.valueOf(node_id);
        block b = prts.blocks.get(pid);
        if (b == null) {
            b = new block(pid);
            prts.blocks.put(pid, b);
        }

        b.nodes.add(str_nodeID);

        if (node_type == in_port) {
            b.iportals.add(str_nodeID);
        } else if (node_type == out_port) {
            b.oportals.add(str_nodeID);
        }
    }

    private void cleanFadePortal(ArrayList<String> portals) {
        int count = 0;
        int count1 = 0;
        for (String portal : portals) {

            boolean allInportal = true, alloutportal = true;
            ArrayList<String> outNodes = getOutGoingFromNode(Integer.parseInt(portal));
            for (String n : outNodes) {
                if (!portals.contains(n)) {
                    alloutportal = false;
                    break;
                }
            }
            int numberOfOut = outNodes.size();

            ArrayList<String> inNodes = getIncomingNodeToNode(Integer.parseInt(portal));
            int numberOfIn = inNodes.size();
            for (String n : inNodes) {
                if (!portals.contains(n)) {
                    allInportal = false;
                    break;
                }
            }

//            System.out.println(portal+":"+numberOfOut+"  "+numberOfIn+" "+alloutportal+" "+allInportal);

            if (numberOfIn == 0 || numberOfOut == 0) {
                count++;
            }

            //if the node connect two non-portals
            if (!allInportal && !alloutportal) {
                count1++;
            }
        }
//        System.out.println(count+"  "+count1);
    }

    public void writePoralsToDisk(ArrayList<String> portals) {
        String portalFile = PathBase + "portalList.txt";
        try (FileWriter fw = new FileWriter(portalFile, true);
             BufferedWriter bw = new BufferedWriter(fw);
             PrintWriter out = new PrintWriter(bw)) {
            for (String portal : portals) {
                out.println(portal);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private ArrayList<String> getPortalsBlinks() {
        System.out.println("read the " + this.num_parts + " partition file");
        loadEdgesInfo();
        loadPartitionsInfo(this.num_parts);
        loadnumberinPartition();
        System.out.println(partitionInfos.size());
        System.out.println(this.connectionInfos.size());
//
        ArrayList<Pair<String, String>> S = new ArrayList<>();
        HashSet<String> P = new HashSet<>();
//
        for (Pair<String, String> ep : connectionInfos) {
//            System.out.println(ep);

            if (isCutterEdge(ep)) {
                S.add(ep);
            }
        }


        System.out.println("======");
        ArrayList<Pair<String, String>> copyOfS = new ArrayList<>(S);
//        System.out.println(copyOfS.size());

        for (Pair<String, String> sp : S) {


            if (copyOfS.contains(sp)) {
                String startNode = sp.getKey();
                String endNode = sp.getValue();

                ArrayList<Pair<String, String>> Sincs = getIncidentTo(copyOfS, startNode);
                ArrayList<Pair<String, String>> Eincs = getIncidentTo(copyOfS, endNode);

//            if (sp.getKey().equals("3") || sp.getValue().equals("3")) {
//                System.out.println(Ss + "   " + Se);
//            }

                String Scid = this.partitionInfos.get(sp.getKey()).getKey();
                String Spid = this.partitionInfos.get(sp.getKey()).getValue();
                String Ecid = this.partitionInfos.get(sp.getValue()).getKey();
                String Epid = this.partitionInfos.get(sp.getValue()).getValue();


                int SNumOfBlocks = this.numberOfPartitions.get(Scid).get(Spid);
                int ENumOfBlocks = this.numberOfPartitions.get(Ecid).get(Epid);
//                System.out.println("   :" + sp + "  --> " + Sincs.size() + "+" + SNumOfBlocks + "=" + (Sincs.size() + SNumOfBlocks) + "  ===>  " + Eincs.size() + "+" + ENumOfBlocks + "=" + (Eincs.size() + ENumOfBlocks));

                if ((Sincs.size() + 0.1 * SNumOfBlocks) >= (0.1 * ENumOfBlocks + Eincs.size())) {
                    P.add(startNode);
                    removeFromSpeEdges(copyOfS, Sincs);
                } else {
                    P.add(endNode);
                    removeFromSpeEdges(copyOfS, Eincs);
                }
//                System.out.println(copyOfS.size());
            }
        }
        return new ArrayList<>(P);
    }


    private ArrayList<String> getPortalsVertexCover() {
        loadEdgesInfo();
        loadPartitionsInfo(this.num_parts);
        loadnumberinPartition();
        System.out.println(partitionInfos.size());
        System.out.println(this.connectionInfos.size());
//
        ArrayList<Pair<String, String>> S = new ArrayList<>();
        HashSet<String> P = new HashSet<>();
//
        for (Pair<String, String> ep : connectionInfos) {
            if (isCutterEdge(ep)) {
                S.add(ep);
            }
        }


        System.out.println("======");
        ArrayList<Pair<String, String>> copyOfS = new ArrayList<>(S);
        while (!copyOfS.isEmpty()) {
            int index = 0;
            if (copyOfS.size() != 1)
                index = getRandomNumberInRange(0, copyOfS.size() - 1);
            Pair<String, String> cuttingEdge = copyOfS.get(index);
            String startNode = cuttingEdge.getKey();
            String endNode = cuttingEdge.getValue();

            ArrayList<Pair<String, String>> Sincs = getIncidentTo(copyOfS, startNode);
            ArrayList<Pair<String, String>> Eincs = getIncidentTo(copyOfS, endNode);

            String Scid = this.partitionInfos.get(cuttingEdge.getKey()).getKey();
            String Spid = this.partitionInfos.get(cuttingEdge.getKey()).getValue();
            String Ecid = this.partitionInfos.get(cuttingEdge.getValue()).getKey();
            String Epid = this.partitionInfos.get(cuttingEdge.getValue()).getValue();


            int SNumOfBlocks = this.numberOfPartitions.get(Scid).get(Spid);
            int ENumOfBlocks = this.numberOfPartitions.get(Ecid).get(Epid);
//                System.out.println("   :" + sp + "  --> " + Sincs.size() + "+" + SNumOfBlocks + "=" + (Sincs.size() + SNumOfBlocks) + "  ===>  " + Eincs.size() + "+" + ENumOfBlocks + "=" + (Eincs.size() + ENumOfBlocks));

            if ((Sincs.size() + 0.1 * SNumOfBlocks) >= (0.1 * ENumOfBlocks + Eincs.size())) {
                P.add(startNode);
            } else {
                P.add(endNode);
            }

            removeFromSpeEdges(copyOfS, Sincs);
            removeFromSpeEdges(copyOfS, Eincs);


        }
        return new ArrayList<>(P);
    }

    private void removeFromSpeEdges(ArrayList<Pair<String, String>> source, ArrayList<Pair<String, String>> sub) {
        for (Pair<String, String> p : sub) {
            int index = source.indexOf(p);
            if (index != -1) {
                source.remove(index);
            }
        }
    }


    private void loadnumberinPartition() {
        for (Map.Entry<String, Pair<String, String>> e : this.partitionInfos.entrySet()) {
            String nodeID = e.getKey();
            String cid = e.getValue().getKey();
            String pid = e.getValue().getValue();

            if (numberOfPartitions.containsKey(cid)) {
                HashMap<String, Integer> parinf = numberOfPartitions.get(cid);
                if (parinf.containsKey(pid)) {
                    int n = parinf.get(pid) + 1;
                    parinf.put(pid, n);
                    numberOfPartitions.put(cid, parinf);
                } else {
                    parinf.put(pid, 1);
                    numberOfPartitions.put(cid, parinf);
                }

            } else {
                HashMap<String, Integer> parinf = new HashMap<>();
                parinf.put(pid, 1);
                numberOfPartitions.put(cid, parinf);
            }

        }

//        System.out.println(numberOfPartitions.size());
//        for (Map.Entry<String, HashMap<String, Integer>> parInfos : numberOfPartitions.entrySet()) {
//            for (Map.Entry<String, Integer> e : parInfos.getValue().entrySet()) {
//                System.out.println(e.getKey() + " ====== " + e.getValue());
//
//            }
//
//        }

    }

    private void loadEdgesInfo() {
        try {
            BufferedReader br = new BufferedReader(new FileReader(EdgesInfoPath));
            String line = null;
            while ((line = br.readLine()) != null) {
                String StartNode = String.valueOf(Integer.parseInt(line.split(" ")[0]) + 1);
                String EndNode = String.valueOf(Integer.parseInt(line.split(" ")[1]) + 1);
//                System.out.println(StartNode+ " -> "+ EndNode+ "     "+line);
                Pair<String, String> p = new Pair<>(StartNode, EndNode);
                this.connectionInfos.add(p);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void loadPartitionsInfo(int num_parts) {
        String paritionFile = PathBase + "partitions_info.txt";
        System.out.println(paritionFile);
        File pFile = new File(paritionFile);
        if (pFile.exists()) {
            System.out.println("delete partition infos");
            pFile.delete();
        }

        System.out.println("write and read partition infos");
        writePartionsInfo(paritionFile, num_parts);
    }

//    private void readPartionsInfo(String paritionFile) {
//        try {
//            BufferedReader br = new BufferedReader(new FileReader(paritionFile));
//            String line = null;
//            while ((line = br.readLine()) != null) {
//                String NodeId = line.split(" ")[0];
//                String Cid = line.split(" ")[1];
//                String Pid = line.split(" ")[2];
//                Pair<String, String> p = new Pair<>(Cid, Pid);
//                this.partitionInfos.put(NodeId, p);
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }

    private void writePartionsInfo(String paritionFile, int num_parts) {
        try (FileWriter fw = new FileWriter(paritionFile, true);
             BufferedWriter bw = new BufferedWriter(fw);
             PrintWriter out = new PrintWriter(bw)) {
            int i = 1;
            for (; i <= this.NodeNum; i++) {
                long infos[] = this.getC_id(i);
                int cid = (int) (infos[0]);
                long mapped_id = infos[1];
                int pid = -1;
                if (cid == -1) {
                    System.out.println("Find a node not in all the node_mapping files");
                    break;
                    //To-do
                } else if (cid > 3) {
                    pid = 0;
//                } else {
//                    pid = this.getPid_inC(cid, mapped_id);
                } else {
                    pid = this.getPid_inC(cid, mapped_id, num_parts);
                }

                Pair<String, String> p = new Pair<>(String.valueOf(cid), String.valueOf(pid));
                this.partitionInfos.put(String.valueOf(i), p);

                out.println(i + " " + cid + " " + pid);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * @param nodeid the original node id
     * @return value: cid, mapped id in the connection component.
     */
    private long[] getC_id(long nodeid) {
        int C_id = -1;
        File nodeDir = new File(nodeMappingBase);
        for (File f : nodeDir.listFiles(new textFileFilter())) {
            try {
                BufferedReader br = new BufferedReader(new FileReader(f));
                String line = null;
                while ((line = br.readLine()) != null) {
//                    System.out.println(line.split(" ")[0]);
                    String org_id = line.split(" ")[0];
                    if (org_id.equals(String.valueOf(nodeid))) {
                        int sIndex = f.getName().lastIndexOf("_") + 1;
                        int eIndex = f.getName().lastIndexOf(".");
                        C_id = Integer.parseInt(f.getName().substring(sIndex, eIndex));
                        //To-Do
                        if (C_id <= 3)
                            return new long[]{C_id, Long.parseLong(line.split(" ")[1])};
                        else
                            return new long[]{C_id, 0};
                    }
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        return new long[]{-1, -1};
    }

    /**
     * return the partition id of the node in the specific connection component
     *
     * @param cid       the connection component id
     * @param mapped_id the mapped node id
     * @return the partition id
     */
    private int getPid_inC(int cid, long mapped_id, int num_parts) {
        int pid = -1;
        String partFile = nodeMappingBase + "mapped_metis_" + cid + ".graph.part." + num_parts;
//        String partFile = PathBase + "mapped_metis_" + cid + ".graph.part.10";
        try {
            BufferedReader br = new BufferedReader(new FileReader(partFile));
            String line = null;
            long i = 1;
            while ((line = br.readLine()) != null) {
                if (i == mapped_id) {
                    pid = Integer.parseInt(line);
                    return pid;
                }
                i++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }


        return pid;
    }

    private boolean isCutterEdge(Pair<String, String> ep) {
        String startNode = ep.getKey();
        String endNode = ep.getValue();

        String s_cid = this.partitionInfos.get(startNode).getKey();
        String s_pid = this.partitionInfos.get(startNode).getValue();

        String e_cid = this.partitionInfos.get(endNode).getKey();
        String e_pid = this.partitionInfos.get(endNode).getValue();

        if (s_cid.equals(e_cid) && !s_pid.equals(e_pid)) {
            return true;
        }

        return false;
    }

    /**
     * @param s    the collections that contains edges information
     * @param node specific the node want to find the edges incident to it
     * @return the collections that contains the edges that incident to the node
     */

    private ArrayList<Pair<String, String>> getIncidentTo(ArrayList<Pair<String, String>> s, String node) {
        ArrayList<Pair<String, String>> incPairs = new ArrayList<>();
        for (Pair<String, String> p : s) {
            if (node.equals(p.getValue())) {
                incPairs.add(p);
            }
        }
        return incPairs;
    }


    private void portalsMapping(ArrayList<String> p) {
        int count = 0;
        //cid, pair<pid, portal node id>
        HashMap<String, HashMap<String, HashSet<String>>> pMapping = new HashMap<>();
        HashSet<String> cids = new HashSet<>();

        //find the partition id that each portal id need to belong
        //The portal node need to be the same partition of its neighbor nodes.
        for (String n : p) {
            int nid = Integer.parseInt(n);
            //get the incomming or outgoing nodes of nid
            ArrayList<String> neighborNodes = getNeighborNodes(nid);
            for (String Nnode : neighborNodes) {
                //get the cid and pid of the neighborhood nodes
                String C_id = partitionInfos.get(Nnode).getKey();
                String P_id = partitionInfos.get(Nnode).getValue();

//                cids.add(C_id);

                if (pMapping.containsKey(C_id)) {
                    HashMap<String, HashSet<String>> partitions = pMapping.get(C_id);
                    if (partitions.containsKey(P_id)) {
                        HashSet<String> portalsInPars = partitions.get(P_id);
                        portalsInPars.add(n);
                    } else {
                        HashSet<String> portalsInPars = new HashSet<>();
                        portalsInPars.add(n);
                        partitions.put(P_id, portalsInPars);
                    }

                } else {
                    HashMap<String, HashSet<String>> partitions = new HashMap<>();
                    HashSet<String> portalsInPars = new HashSet<>();
                    portalsInPars.add(n);
                    partitions.put(P_id, portalsInPars);
                    pMapping.put(C_id, partitions);
                }

            }
        }
//        System.out.println(cids);

        for (String cid : pMapping.keySet()) {
            for (String pid : pMapping.get(cid).keySet()) {
                for (String portalNode : pMapping.get(cid).get(pid)) {
                    String bits = "";
                    boolean ip = isIncomingPortal(Integer.parseInt(portalNode), pid);
                    boolean op = isOutGoingPortal(Integer.parseInt(portalNode), pid);
                    if (!ip && !op) {
                        bits = "00";
//                        System.out.println(portalNode);
                    } else if (ip && !op) {
                        bits = "10";
                    } else if (!ip && op) {
                        bits = "01";
                    } else {
                        bits = "11";
                    }
                    writeToDisk(cid, pid, portalNode, bits);
                }
            }

        }

    }


    private boolean isIncomingPortal(int nodeid, String pid) {
        boolean hectmfob = hasEdgesComeToMeFromOtherBlock(nodeid, pid);
//        System.out.println("------");
        boolean hegtsb = hasEdgesGoToSameBlock(nodeid, pid);
        return hectmfob && hegtsb;
    }

    private boolean isOutGoingPortal(int nodeid, String pid) {
        boolean hetob = hasEdgesToOtherBlock(nodeid, pid);
//        System.out.println(hetob);
//        System.out.println("-----");
        boolean hectmfsb = hasEdgesComeToMeFromSameBlock(nodeid, pid);
//        System.out.println(hectmfsb);
        return hetob && hectmfsb;
    }


    private boolean hasEdgesGoToSameBlock(int nodeid, String pid) {
        boolean flag = false;
        String N = String.valueOf(nodeid);
        String N_cid = this.partitionInfos.get(N).getKey();
        String N_pid = this.partitionInfos.get(N).getValue();


        ArrayList<String> outgoingNodes = getOutGoingFromNode(nodeid);
//        System.out.println(outgoingNodes.size());
        for (String outgo_node : outgoingNodes) {
            String I_cid = this.partitionInfos.get(outgo_node).getKey();
            String I_pid = this.partitionInfos.get(outgo_node).getValue();
//            System.out.println(outgo_node + " "+ I_cid+ " "+I_pid);

            if (I_cid.equals(N_cid) && I_pid.equals(pid)) {
                flag = true;
                break;

            }
        }
        return flag;
    }


    private boolean hasEdgesComeToMeFromOtherBlock(int nodeid, String pid) {
        boolean flag = false;
        String N = String.valueOf(nodeid);
        String N_cid = this.partitionInfos.get(N).getKey();
        String N_pid = this.partitionInfos.get(N).getValue();

//        System.out.println(N + " "+ N_cid+ " "+N_pid);

        ArrayList<String> incomingNodes = getIncomingNodeToNode(nodeid);
//        System.out.println(incomingNodes.size());
        for (String income_node : incomingNodes) {
            String I_cid = this.partitionInfos.get(income_node).getKey();
            String I_pid = this.partitionInfos.get(income_node).getValue();
//            System.out.println(income_node + " "+ I_cid+ " "+I_pid);

            if (I_cid.equals(N_cid) && !I_pid.equals(pid)) {
                flag = true;
                break;

            }
        }
        return flag;
    }


    private boolean hasEdgesToOtherBlock(int nodeid, String pid) {
        boolean flag = false;
        String N = String.valueOf(nodeid);
        String N_cid = this.partitionInfos.get(N).getKey();
        String N_pid = this.partitionInfos.get(N).getValue();

//        System.out.println(N + " "+ N_cid+ " "+N_pid);

        ArrayList<String> outgoingNodes = getOutGoingFromNode(nodeid);
//        System.out.println(incomingNodes.size());
        for (String outgo_node : outgoingNodes) {
            String I_cid = this.partitionInfos.get(outgo_node).getKey();
            String I_pid = this.partitionInfos.get(outgo_node).getValue();
//            System.out.println(outgo_node + " "+ I_cid+ " "+I_pid);

            if (I_cid.equals(N_cid) && !I_pid.equals(pid)) {
                flag = true;
                break;

            }
        }
        return flag;
    }


    private boolean hasEdgesComeToMeFromSameBlock(int nodeid, String pid) {
        boolean flag = false;
        String N = String.valueOf(nodeid);
        String N_cid = this.partitionInfos.get(N).getKey();
        String N_pid = this.partitionInfos.get(N).getValue();

//        System.out.println(N + " "+ N_cid+ " "+N_pid);

        ArrayList<String> incomingNodes = getIncomingNodeToNode(nodeid);
//        System.out.println(incomingNodes.size());
        for (String income_node : incomingNodes) {
            String I_cid = this.partitionInfos.get(income_node).getKey();
            String I_pid = this.partitionInfos.get(income_node).getValue();
//            System.out.println(income_node + " "+ I_cid+ " "+I_pid);

            if (I_cid.equals(N_cid) && I_pid.equals(pid)) {
                flag = true;
                break;

            }
        }
        return flag;
    }

    private ArrayList<String> getOutGoingFromNode(int nodeid) {
        String N = String.valueOf(nodeid);
        ArrayList<String> outgoingNodes = new ArrayList<>();
        for (Pair<String, String> p : this.connectionInfos) {
            String startNode = p.getKey();
            String endNode = p.getValue();
            if (startNode.equals(N)) {
                outgoingNodes.add(endNode);
            }
        }
        return outgoingNodes;
    }


    private ArrayList<String> getIncomingNodeToNode(int nodeid) {
        String N = String.valueOf(nodeid);
//        System.out.println(N);
        ArrayList<String> incomingNodes = new ArrayList<>();
        for (Pair<String, String> p : this.connectionInfos) {
            String startNode = p.getKey();
            String endNode = p.getValue();
            if (endNode.equals(N)) {
//                System.out.println("s");
                incomingNodes.add(startNode);
            }
        }
        return incomingNodes;
    }

    public ArrayList<String> getNeighborNodes(int nid) {
        String N = String.valueOf(nid);
        HashSet<String> neighborNodes = new HashSet<>();
        for (Pair<String, String> p : this.connectionInfos) {
            String startNode = p.getKey();
            String endNode = p.getValue();
            if (startNode.equals(N)) {
                neighborNodes.add(endNode);
            } else if (endNode.equals(N)) {
                neighborNodes.add(startNode);
            }
        }

        return new ArrayList<>(neighborNodes);
    }

    private void writeToDisk(String cid, String pid, String portalNode, String bits) {
        String cidPath = PathBase + "portals/" + cid;
        String pidPath = cidPath + "/" + pid;


        File cidDir = new File(cidPath);
        if (!cidDir.exists()) {
            cidDir.mkdirs();
        }

        File pidFile = new File(pidPath);
        try (FileWriter fw = new FileWriter(pidPath, true);
             BufferedWriter bw = new BufferedWriter(fw);
             PrintWriter out = new PrintWriter(bw)) {
            out.println(portalNode + " " + bits);
        } catch (Exception e) {
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
