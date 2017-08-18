package Pindex;

import javafx.util.Pair;
import neo4jTools.StringComparator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeMap;

public class statistic {
    //nodeID,Pair<Cid,Pid>
    public HashMap<String, Pair<String, String>> partitionInfos = new HashMap<>();
    //HashMap<Cid,HashMap<pid,HashMap<nodeid,bits>>>
    HashMap<String, HashMap<String, HashMap<String, String>>> pMapping = new HashMap<>();
    //HashMap<Cid,HashMap<pid,#ofNodes>>
    HashMap<String, HashMap<String, Integer>> NodesSta = new HashMap<>();
    public static String PathBase = "/home/gqxwolf/mydata/projectData/ConstrainSkyline/data/";
    public static String partitionFile = PathBase + "partitions_info.txt";
    String NodePath = "/home/gqxwolf/mydata/projectData/ConstrainSkyline/data/NodeInfo.txt";

    public HashSet<String> nodes = new HashSet<>();

    public static void main(String args[]) {
        statistic s = new statistic();
        s.portalStatistic();

    }

    public void portalStatistic() {
        readPartionsInfo(this.partitionFile);
        readPartitionInfo();
        loadNodes();
        System.out.println(nodes.contains("67393"));
        System.out.println(this.partitionInfos.keySet().contains("67393"));
        System.out.println("there are " + pMapping.size() + " connection components!!");
        for (Map.Entry<String, HashMap<String, HashMap<String, String>>> e : pMapping.entrySet()) {
            System.out.println("For connection component id " + e.getKey() + " :");
            System.out.println("    there are " + e.getValue().size() + " partitions");
            TreeMap<String, HashMap<String, String>>t = new TreeMap(new StringComparator());
            t.putAll(e.getValue());
            for (Map.Entry<String, HashMap<String, String>> pe : t.entrySet())
            {
//                System.out.println("            pid:"+pe.getKey()+"  - size:"+pe.getValue().size());
                if(pe.getValue().get("0")!=null)
                {
                    System.out.println("found it");
                }
            }
        }


        for (String nodeid : partitionInfos.keySet()) {
            String cid = this.partitionInfos.get(nodeid).getKey();
            String pid = this.partitionInfos.get(nodeid).getValue();
            if (!isPortal(nodeid, cid, pid)) {
                countNodesNumber(nodeid, cid, pid);
            }
        }


//        System.out.println("there are " + NodesSta.size() + " connection components!!");

        for (Map.Entry<String, HashMap<String, Integer>> e : NodesSta.entrySet()) {
            int cid = Integer.parseInt(e.getKey());
            if (cid <= 3) {
//                System.out.println("For connection component id " + e.getKey() + " :");
//                System.out.println("    there are " + e.getValue().size() + " partitions");
                TreeMap<String, Integer> t = new TreeMap(new StringComparator());
                t.putAll(e.getValue());
                for (Map.Entry<String, Integer> pe : t.entrySet()) {
                    String pid = pe.getKey();
//                    int numberofPortal = this.pMapping.get(String.valueOf(cid)).get(pid).size();
                    int numberofNodes = pe.getValue();
//                    System.out.println("            pid:" + pe.getKey() + "  - size:" + numberofNodes +" "+ (double)numberofPortal/numberofNodes );
                    System.out.println("            pid:" + pe.getKey() + "  - size:" + numberofNodes );
                }
            }
        }

    }

    private void countNodesNumber(String nodeid, String cid, String pid) {
        // System.out.println(" " + portalNode + " " + bits);
        if (NodesSta.containsKey(cid)) {
            HashMap<String, Integer> partitions = NodesSta.get(cid);
            if (partitions.containsKey(pid)) {
                int numberOfNodes = partitions.get(pid) + 1;
                partitions.put(pid, numberOfNodes);
            } else {
                partitions.put(pid, 1);
            }
        } else {
            HashMap<String, Integer> partitions = new HashMap<>();
            partitions.put(pid, 1);
            NodesSta.put(cid, partitions);
        }

    }


    private boolean isPortal(String nodeid, String cid, String pid) {
        HashMap<String, HashMap<String, String>> pe = this.pMapping.get(cid);
        if (pe != null) {
            HashMap<String, String> ne = pe.get(pid);
            if (ne != null) {
                String node = ne.get(nodeid);
                if (node != null) {
                    return true;
                } else {
                    return false;
                }

            }
            return false;
        }
        return false;
    }


    public void readPartionsInfo(String paritionFile) {
        try (BufferedReader br = new BufferedReader(new FileReader(paritionFile))) {
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

    public void readPartitionInfo() {
        this.pMapping.clear();
        String partitionInfoPath = "/home/gqxwolf/mydata/projectData/ConstrainSkyline/data/portals/";
        File parFile = new File(partitionInfoPath);
        for (File cFile : parFile.listFiles()) {
            // System.out.println(cFile.getName());
            String Cid = cFile.getName();
            for (File pFile : cFile.listFiles()) {
                // System.out.println(" " + pFile.getName());
                String Pid = pFile.getName();
                try (BufferedReader br = new BufferedReader(new FileReader(pFile))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        String portalNode = line.split(" ")[0];
                        String bits = line.split(" ")[1];
                        // System.out.println(" " + portalNode + " " + bits);
                        if (pMapping.containsKey(Cid)) {
                            HashMap<String, HashMap<String, String>> partitions = pMapping.get(Cid);
                            if (partitions.containsKey(Pid)) {
                                HashMap<String, String> portalsInPars = partitions.get(Pid);
                                portalsInPars.put(portalNode, bits);
                            } else {
                                HashMap<String, String> portalsInPars = new HashMap<>();
                                portalsInPars.put(portalNode, bits);
                                partitions.put(Pid, portalsInPars);
                            }

                        } else {
                            HashMap<String, HashMap<String, String>> partitions = new HashMap<>();
                            HashMap<String, String> portalsInPars = new HashMap<>();
                            portalsInPars.put(portalNode, bits);
                            partitions.put(Pid, portalsInPars);
                            pMapping.put(Cid, partitions);
                        }

                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
        }

    }

    private void loadNodes() {
        try {
            BufferedReader br = new BufferedReader(new FileReader(NodePath));
            String line = null;
            while ((line = br.readLine()) != null) {
                String StartNode = String.valueOf(Integer.parseInt(line.split(",")[0]) + 1);
//                System.out.println(StartNode+ " -> "+ EndNode+ "     "+line);
                this.nodes.add(StartNode);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
