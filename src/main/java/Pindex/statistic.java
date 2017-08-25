package Pindex;

import javafx.util.Pair;
import neo4jTools.StringComparator;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.*;

public class statistic {
    //nodeID,Pair<Cid,Pid>
    public HashMap<String, Pair<String, String>> partitionInfos = new HashMap<>();
    //HashMap<Cid,HashMap<pid,HashMap<nodeid,bits>>>
    HashMap<String, HashMap<String, HashMap<String, String>>> pMapping = new HashMap<>();
    //HashMap<Cid,HashMap<pid,#ofNodes>>
    HashMap<String, HashMap<String, HashSet<String>>> NodesSta = new HashMap<>();

    HashSet<String> a = new HashSet<>();
    ArrayList<String> b = new ArrayList<>();

    ArrayList<String> portals = new ArrayList<>();
    public static String PathBase = "/home/gqxwolf/mydata/projectData/ConstrainSkyline/data/";
    public static String partitionFile = PathBase + "partitions_info.txt";
    String NodePath = "/home/gqxwolf/mydata/projectData/ConstrainSkyline/data/NodeInfo.txt";
    String PortalsPath = "/home/gqxwolf/mydata/projectData/ConstrainSkyline/data/portalList.txt";

    public HashSet<String> nodes = new HashSet<>();

    public static void main(String args[]) {
        statistic s = new statistic();
        s.portalStatistic();
//        s.countIncommingPortal(1,"0");

    }

    public void portalStatistic() {
        readPartionsInfo(this.partitionFile);
        readPartitionInfo();
        loadNodes();
        loadPortals();
//        System.out.println(nodes.contains("67393"));
//        System.out.println(this.partitionInfos.keySet().contains("67393"));


        for (String nodeid : partitionInfos.keySet()) {
            String cid = this.partitionInfos.get(nodeid).getKey();
            String pid = this.partitionInfos.get(nodeid).getValue();
            if (!isPortal(nodeid, cid, pid)) {
                countNodesNumber(nodeid, cid, pid);
            }
        }

//        System.out.println(isPortal("60409", "2", "60"));
//
//        for (String bb : b) {
//            if (this.portals.contains(bb)) {
//                System.out.println(bb);
//            }
//        }

        for (Map.Entry<String, HashMap<String, HashSet<String>>> e : NodesSta.entrySet()) {
            int cid = Integer.parseInt(e.getKey());
            if (cid > 3) {
                TreeMap<String, HashSet<String>> t = new TreeMap(new StringComparator());
                t.putAll(e.getValue());
                System.out.println(cid);
                for (Map.Entry<String, HashSet<String>> pe : t.entrySet()) {
                    String pid = pe.getKey();
                    HashSet<String> numberofNodes = pe.getValue();
//                    int countinfos[] = countIncommingPortal(cid,pid);
//                    int number = countinfos[0];
//                    int numberOfIn = countinfos[1];
//                    int numberOfOut = countinfos[2];
//                    System.out.println("            pid:" + pe.getKey() + "  - size:" + numberofNodes.size()+" "+number+" "+numberOfIn+" "+numberOfOut );
                    System.out.println("            pid:" + pe.getKey() + "  - size:" + numberofNodes.size()+" ");
                }
            }
        }

    }

    private int[] countIncommingPortal(int cid, String pid) {
        int numberofIn = 0;
        int numberofOut = 0;
        int number=0;
        String filename = PathBase+"portals/"+cid+"/"+pid;

        try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
            String line = null;
            while ((line = br.readLine()) != null) {

                line = line.trim().split(" ")[1];
                if(line.startsWith("1"))
                {
//                    System.out.println("In");
                    numberofIn++;
                }

                if(line.endsWith("1"))
                {
//                    System.out.println("Out");
                    numberofOut++;
                }

                number++;
//                System.out.println(line+"  "+line.endsWith("1"));
//                System.out.println(number+" "+numberofIn+" "+numberofOut);
//                System.out.println("----------");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return new int[]{number,numberofIn,numberofOut};
    }

    private boolean isContains(String nodeid, String cid, String pid) {
        return this.NodesSta.get(cid).get(pid).contains(nodeid);
    }

    private void countNodesNumber(String nodeid, String cid, String pid) {
        // System.out.println(" " + portalNode + " " + bits);
        if (NodesSta.containsKey(cid)) {
            HashMap<String, HashSet<String>> partitions = NodesSta.get(cid);
            if (partitions.containsKey(pid)) {
                HashSet<String> numberOfNodes = partitions.get(pid);
                if (!numberOfNodes.contains(nodeid)) {
                    numberOfNodes.add(nodeid);
                    partitions.put(pid, numberOfNodes);
                    a.add(nodeid);
                    b.add(nodeid);
                }
            } else {
                HashSet<String> numberOfNodes = new HashSet<>();
                numberOfNodes.add(nodeid);
                partitions.put(pid, numberOfNodes);
                a.add(nodeid);
                b.add(nodeid);
            }
        } else {
            HashMap<String, HashSet<String>> partitions = new HashMap<>();
            HashSet<String> numberOfNodes = new HashSet<>();
            numberOfNodes.add(nodeid);
            partitions.put(pid, numberOfNodes);
            NodesSta.put(cid, partitions);
            a.add(nodeid);
            b.add(nodeid);
        }

    }


    private boolean isPortal(String nodeid, String cid, String pid) {
        return this.portals.contains(nodeid);
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

    private void loadPortals() {
        try {
            BufferedReader br = new BufferedReader(new FileReader(PortalsPath));
            String line = null;
            while ((line = br.readLine()) != null) {
                String StartNode = line.trim();
//                System.out.println(StartNode+ " -> "+ EndNode+ "     "+line);
                this.portals.add(StartNode);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static boolean checkPathResultEquation(ArrayList<path> r1, ArrayList<path> r2) {
        boolean[] results = new boolean[r1.size()];
        if (r1.size() != r2.size())
            return false;
        int i = 0;
        for (path p1 : r1) {
            for (path p2 : r2) {
                if (isEqualPath(p1, p2)) {
                    results[i] = true;
                    continue;
                }
            }
            i++;
        }

        for (boolean f : results) {
            if (!f) {
                return false;
            }
        }


        return true;
    }

    private static boolean isEqualPath(path p1, path p2) {
        return isEqualNodes(p1.Nodes, p2.Nodes) && isEqualEdges(p1.relationships, p2.relationships);
    }

    private static boolean isEqualEdges(ArrayList<Relationship> relationships1, ArrayList<Relationship> relationships2) {
        if (relationships1.size() != relationships2.size()) {
            return false;
        }


        for (int i = 0; i < relationships1.size(); i++) {
            if (relationships1.get(i).getId() != relationships2.get(i).getId()) {
                return false;
            }

        }
        return true;
    }


    private static boolean isEqualNodes(ArrayList<Node> nodes1, ArrayList<Node> nodes2) {
        if (nodes1.size() != nodes2.size()) {
            return false;
        }


        for (int i = 0; i < nodes1.size(); i++) {
            if (nodes1.get(i).getId() != nodes2.get(i).getId()) {
                return false;
            }

        }
        return true;
    }
}
