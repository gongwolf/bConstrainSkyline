package Pindex.inblockSkyline;

import Pindex.myshortestPathUseNodeFinal;
import Pindex.path;
import javafx.util.Pair;
import neo4jTools.BNode;
import neo4jTools.connector;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;

public class test {
    public HashMap<String, Pair<String, String>> partitionInfos = new HashMap<>();
    HashMap<String, HashMap<String, HashMap<String, String>>> pMapping = new HashMap<>();


    public static String PathBase = "/home/gqxwolf/mydata/projectData/testGraph/data/";
    public static String paritionFile = PathBase + "partitions_info.txt";
    public static String portalListFile = PathBase+"portalList.txt";
    ArrayList<String> portals = new ArrayList<>();


    public static void main(String args[])
    {
        connector n = new connector("/home/gqxwolf/neo4j/neo4j-community-3.2.3/testdb/databases/graph.db");
        // connector n = new connector();
        n.startDB();
        GraphDatabaseService graphdb = n.getDBObject();
        String src = "0";
        String dest = "62";
        test t = new test();
        t.runUseNode(src,dest,graphdb);
        t.readPartionsInfo(paritionFile);
        t.loadPortals();

        t.readPartitionInfo();


//        HashMap<String, String> qq = t.pMapping.get("1").get("0");
//        System.out.println(0+" "+(qq==null));
        t.runInBlock(src, dest,graphdb);

        n.shutdownDB();

    }

    private void runUseNode(String sid, String eid, GraphDatabaseService graphdb) {
        long run1 = System.nanoTime();
        ArrayList<path> r1 = runUseNodeFinal(sid, eid, graphdb);
        System.out.println(r1.size());
        run1 = (System.nanoTime()-run1)/1000000;
        System.out.println(run1);
        System.out.println("==============================");
    }

    private void runInBlock(String sid, String did, GraphDatabaseService graphdb) {
        long run1 = System.nanoTime();

        //Long BeforePages = getFromManagementBean("Page cache","Faults",graphdb);
        Node Source;
        Node Destination;
        try (Transaction tx = graphdb.beginTx()) {
            Source = graphdb.findNode(BNode.BusNode, "name", sid);
            Destination = graphdb.findNode(BNode.BusNode, "name", did);
            tx.success();
        }


        myskylinePartition mspNode = new myskylinePartition(graphdb,partitionInfos,portals,pMapping);
        mspNode.getSkylinePath(Source, Destination);
        run1 = (System.nanoTime()-run1)/1000000;
        System.out.println(run1);


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

    private void loadPortals() {
        try {
            BufferedReader br = new BufferedReader(new FileReader(portalListFile));
            String line = null;
            while ((line = br.readLine()) != null) {
                String StartNode = line.trim();
//                String nodeid = String.valueOf(Long.valueOf(StartNode)-1);
                this.portals.add(StartNode);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public ArrayList<path> runUseNodeFinal(String sid, String did, GraphDatabaseService graphdb) {
//        connector n = new connector("/home/gqxwolf/neo4j/csldb/databases/graph.db");
        Node Source;
        Node Destination;
        try (Transaction tx = graphdb.beginTx()) {
            Source = graphdb.findNode(BNode.BusNode, "name", sid);
            Destination = graphdb.findNode(BNode.BusNode, "name", did);
            tx.success();
        }
        myshortestPathUseNodeFinal mspNode = new myshortestPathUseNodeFinal(graphdb);
        ArrayList<path> r = mspNode.getSkylinePath(Source, Destination);
        return r;
    }

    public void removePathNotWithinBlock(String pid, ArrayList<path> paths) {

        int i = 0;
        for (; i < paths.size(); ) {
            path p = paths.get(i);

            // System.out.println(p);
            // System.out.println(printCosts(p.getCosts()));

            long sid = p.startNode.getId();
            long eid = p.endNode.getId();

            boolean flag = true;
            for (Node n : p.Nodes) {
                if (n.getId() != sid && n.getId() != eid ) {
                    String nid = String.valueOf(n.getId() + 1);
                    String n_pid = this.partitionInfos.get(nid).getValue();
                    if (!n_pid.equals(pid) || this.portals.contains(nid)) {
                        flag = false;
                        break;
                    }

                }
            }
            // System.out.println(flag);
            if (!flag) {
                paths.remove(i);
            } else {
                i++;
            }
        }

    }

    public void readPartitionInfo() {
//        this.pMapping.clear();
        String partitionInfoPath = this.PathBase+"portals/";
        File parFile = new File(partitionInfoPath);
        for (File cFile : parFile.listFiles()) {
//             System.out.println(cFile.getName());
            String Cid = cFile.getName();
            for (File pFile : cFile.listFiles()) {
//                System.out.println(" " + pFile.getName());
                String Pid = pFile.getName();
                try (BufferedReader br = new BufferedReader(new FileReader(pFile))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        String portalNode = line.split(" ")[0];
                        String bits = line.split(" ")[1];
//                         System.out.println(" " + portalNode + " " + bits);
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
}
