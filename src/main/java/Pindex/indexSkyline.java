package Pindex;

import javafx.util.Pair;
import neo4jTools.BNode;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class indexSkyline {
    public static String PathBase = "/home/gqxwolf/mydata/projectData/testGraph/data/";
    public static String paritionFile = PathBase + "partitions_info.txt";
    private final GraphDatabaseService graphdb;
    //NodeId, pair<cid,pid>
    public HashMap<String, Pair<String, String>> partitionInfos = new HashMap<>();
    //Cid, Hashmap<pid, HashMap<portalNode, bits>>
    HashMap<String, HashMap<String, HashMap<String, String>>> pMapping = new HashMap<>();

    ArrayList<path> skylinPaths = new ArrayList<>();


    public indexSkyline(GraphDatabaseService graphdb) {
        this.readPartitionInfo();
        this.readPartionsInfo(paritionFile);
        this.graphdb = graphdb;
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
        String partitionInfoPath = "/home/gqxwolf/mydata/projectData/testGraph/data/portals/";
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

        // for (String cid : pMapping.keySet()) {
        // System.out.println(cid);
        // for (String pid : pMapping.get(cid).keySet()) {
        // System.out.println(" " + pid + " " +
        // pMapping.get(cid).get(pid).size());
        // HashMap<String, String> portalNode = pMapping.get(cid).get(pid);
        // }
        // }

    }


    public ArrayList<path> getSkylinePath(Node source, Node destination) {
        String sid = String.valueOf(source.getId());
        String eid = String.valueOf(destination.getId());

        String cid_sid = this.partitionInfos.get(sid).getKey();
        String cid_eid = this.partitionInfos.get(eid).getKey();
        String pid_sid = this.partitionInfos.get(sid).getValue();
        String pid_eid = this.partitionInfos.get(eid).getValue();
        ArrayList<path> skylinPaths = new ArrayList<>();

        if (!cid_eid.equals(cid_sid)) {
            //startNode and endNode in different connection component
            return null;
        } else {
            if (pid_eid.equals(pid_sid)) {
                ArrayList<path> skyR = this.runUseNodeFinal(sid, eid, graphdb);
                return skyR;
            } else {
                ArrayList<String> sidPortals = getPortals(cid_sid, pid_sid);
                System.out.println(sidPortals.size());
                ArrayList<String> eidPortals = getPortals(cid_eid, pid_eid);
                System.out.println(eidPortals.size());


                for(String pnode:sidPortals)
                {
                    ArrayList<path> tmpR = runUseNodeFinal(sid, pnode, this.graphdb);
                    if(tmpR!=null)
                    System.out.println("     "+tmpR.size());
                }


            }
        }


        return null;
    }

    private ArrayList<String> getPortals(String cid, String pid) {
        HashMap<String, String> cand_portals = pMapping.get(cid).get(pid);
        ArrayList<String> portals  = new ArrayList<>();
        for(Map.Entry<String,String> e:cand_portals.entrySet())
        {
            if(e.getValue().endsWith("1"))
            {
                portals.add(e.getKey());
            }
        }

        return portals;
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
}
