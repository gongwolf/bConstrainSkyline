package GraphPartition;

import javafx.util.Pair;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

public class BlinksPartition {
    String EdgesInfoPath = "/home/gqxwolf/mydata/projectData/ConstrainSkyline/data/SegInfo.txt";
    String nodeMappingBase = "/home/gqxwolf/mydata/projectData/ConstrainSkyline/data/mapping/";
    String PathBase = "/home/gqxwolf/mydata/projectData/ConstrainSkyline/data/";
    private ArrayList<Pair<String, String>> connectionInfos = new ArrayList<>();
    private HashMap<String, Pair<String, String>> partitionInfos = new HashMap<>();
    public int NodeNum = 67393;

    public static void main(String args[]) {
        BlinksPartition bp = new BlinksPartition();
//        long[] infos = bp.getC_id(55);
//        System.out.println(infos[0]);
//        System.out.println(infos[1]);
//        int pid = bp.getPid_inC((int) infos[0], infos[1]);
//        System.out.println(pid);
        ArrayList<String> portals = bp.getPortals();
        System.out.println(portals.size());
//        bp.writePoralsToDisk(portals);
        bp.portalsMapping(portals);

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

    private ArrayList<String> getPortals() {
        loadEdgesInfo();
        loadPartitionsInfo();
        System.out.println(partitionInfos.size());
        System.out.println(this.connectionInfos.size());
        System.out.println(this.partitionInfos.size());
//
        ArrayList<Pair<String, String>> S = new ArrayList<>();
        HashSet<String> P = new HashSet<>();
//
        for (Pair<String, String> ep : connectionInfos) {

            if (isCutterEdge(ep)) {
                S.add(ep);
                if(ep.getKey().equals("60409")||ep.getValue().equals("60409"))
                {
                    System.out.println(ep);
                    System.out.println(this.partitionInfos.get(ep.getKey()));
                    System.out.println(this.partitionInfos.get(ep.getValue()));
                }
            }

        }

        for (Pair<String, String> sp : S) {
            String startNode = sp.getKey();
            String endNode = sp.getValue();

            int Ss = getIncidentTo(S, startNode);
            int Se = getIncidentTo(S, endNode);
//            System.out.println(Ss + "   "+ Se);

            if (Ss >= Se) {
                P.add(startNode);
            } else {
                P.add(endNode);
            }
        }
        return new ArrayList<>(P);
    }

    private void loadEdgesInfo() {
        try {
            BufferedReader br = new BufferedReader(new FileReader(EdgesInfoPath));
            String line = null;
            while ((line = br.readLine()) != null) {
                String StartNode = String.valueOf(Integer.parseInt(line.split(",")[0]) + 1);
                String EndNode = String.valueOf(Integer.parseInt(line.split(",")[1]) + 1);
//                System.out.println(StartNode+ " -> "+ EndNode+ "     "+line);
                Pair<String, String> p = new Pair<>(StartNode, EndNode);
                this.connectionInfos.add(p);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void loadPartitionsInfo() {
        String paritionFile = PathBase + "partitions_info.txt";
        File pFile = new File(paritionFile);
        if (pFile.exists()) {
            readPartionsInfo(paritionFile);
        } else {
            writePartionsInfo(paritionFile);
        }

    }

    private void readPartionsInfo(String paritionFile) {
        try {
            BufferedReader br = new BufferedReader(new FileReader(paritionFile));
            String line = null;
            while ((line = br.readLine()) != null) {
                String NodeId = line.split(" ")[0];
                String Cid = line.split(" ")[1];
                String Pid = line.split(" ")[2];
                Pair<String, String> p = new Pair<>(Cid, Pid);
                this.partitionInfos.put(NodeId, p);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void writePartionsInfo(String paritionFile) {
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
                    pid = this.getPid_inC(cid, mapped_id);
                }

                Pair<String, String> p = new Pair<>(String.valueOf(cid), String.valueOf(pid));
                this.partitionInfos.put(String.valueOf(i), p);

                out.println(i + " " + cid + " " + pid);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

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
                        if (C_id <=3)
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

    private int getPid_inC(int cid, long mapped_id) {
        int pid = -1;
        String partFile = nodeMappingBase + "mapped_metis_"+cid+".graph.part.150";
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


    private int getIncidentTo(ArrayList<Pair<String, String>> s, String startNode) {
        int counter = 0;
        for (Pair<String, String> p : s) {
            if (startNode.equals(p.getValue())) {
                counter++;
            }

        }
        return counter;
    }


    private void portalsMapping(ArrayList<String> p) {
        int count = 0;
        HashMap<String, HashMap<String, HashSet<String>>> pMapping = new HashMap<>();
        HashSet<String> cids = new HashSet<>();
        for (String n : p) {
            int nid = Integer.parseInt(n);
            ArrayList<String> neighborNodes = getNeighborNodes(nid);
            for (String Nnode : neighborNodes) {
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
            System.out.println(cid);
            for (String pid : pMapping.get(cid).keySet()) {
                System.out.println("   " + pid + "   " + pMapping.get(cid).get(pid).size());
                for (String portalNode : pMapping.get(cid).get(pid)) {
                    if(portalNode.equals("60409"))
                    {
                        System.out.println("!!!");
                    }
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
//                    writeToDisk(cid, pid, portalNode, bits);
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


        ArrayList<String> outgoingNodes = getOutGoingNodes(nodeid);
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

        ArrayList<String> incomingNodes = getInComingNodeList(nodeid);
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

        ArrayList<String> outgoingNodes = getOutGoingNodes(nodeid);
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

        ArrayList<String> incomingNodes = getInComingNodeList(nodeid);
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

    private ArrayList<String> getOutGoingNodes(int nodeid) {
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


    private ArrayList<String> getInComingNodeList(int nodeid) {
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

}
