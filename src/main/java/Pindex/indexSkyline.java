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
    public static String portalListFile = PathBase + "portalList.txt";
    public static String paritionFile = PathBase + "partitions_info.txt";
    private final GraphDatabaseService graphdb;
    //NodeId, pair<cid,pid>
    public HashMap<String, Pair<String, String>> partitionInfos = new HashMap<>();
    //Cid, Hashmap<pid, HashMap<portalNode, bits>>
    HashMap<String, HashMap<String, HashMap<String, String>>> pMapping = new HashMap<>();
    //Portal ID
    ArrayList<String> portalList = new ArrayList<>();

    ArrayList<path> skylinPaths = new ArrayList<>();


    public indexSkyline(GraphDatabaseService graphdb) {
        this.readPartitionInfo();
        this.readPartionsInfo(paritionFile);
        this.readPortalList();
        this.graphdb = graphdb;
    }

    private void readPortalList() {
        try (BufferedReader br = new BufferedReader(new FileReader(portalListFile))) {
            String line = null;
            while ((line = br.readLine()) != null) {
                this.portalList.add(line);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
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
        String orig_sid = String.valueOf(source.getId());
        String orig_eid = String.valueOf(destination.getId());
        String mapped_sid = String.valueOf(source.getId() + 1);
        String mapped_eid = String.valueOf(destination.getId() + 1);

        String cid_sid = this.partitionInfos.get(mapped_sid).getKey();
        String cid_eid = this.partitionInfos.get(mapped_eid).getKey();
        String pid_sid = this.partitionInfos.get(mapped_sid).getValue();
        String pid_eid = this.partitionInfos.get(mapped_eid).getValue();
        ArrayList<path> TskylinPaths = new ArrayList<>();

        if (!cid_eid.equals(cid_sid)) {
            //startNode and endNode in different connection component
            return null;
        } else {
            System.out.println(orig_sid + ":" + isPortal(mapped_sid));
            System.out.println(orig_eid + ":" + isPortal(mapped_eid));


            //If the start node is a portal node, directly create a portalObject.
            //If the end Node is a portal, do not run  runUseNodeFinal function to calculate the skyline path from the in-coming portal to the end Node
            if (isPortal(mapped_sid)) {

            } else if (isPortal(mapped_eid)) {

            } else {
                if (pid_eid.equals(pid_sid)) {
                    ArrayList<path> skyR = this.runUseNodeFinal(orig_sid, orig_sid, graphdb);
                    return skyR;
                } else {
                    ArrayList<String> sidPortals = getPortals(cid_sid, pid_sid);
                    System.out.println(sidPortals.size());
                    ArrayList<String> eidPortals = getPortals(cid_eid, pid_eid);
                    System.out.println(eidPortals.size());

                    for (String pnode : sidPortals) {
                        ArrayList<path> tmpR = runUseNodeFinal(orig_sid, pnode, this.graphdb);
                        if (tmpR != null) {
                            System.out.println("     " + tmpR.size());
                            removePathNotWithinBlock(pid_sid, tmpR);
                            int aftersize = tmpR.size();
                            if (aftersize != 0) {
                                System.out.println(1111 + "    " + tmpR.size());
                                for (path p : tmpR) {
                                    TskylinPaths.add(p);
                                }
                            }
                        }
                    }

                    System.out.println("===============");
                    System.out.println(TskylinPaths.size());


                }
            }


        }


        return null;
    }

    private boolean isPortal(String mapped_sid) {
//        this.pMapping
        return this.portalList.contains(mapped_sid);
    }

    private ArrayList<String> getPortals(String cid, String pid) {
        HashMap<String, String> cand_portals = pMapping.get(cid).get(pid);
        ArrayList<String> portals = new ArrayList<>();
        for (Map.Entry<String, String> e : cand_portals.entrySet()) {
            if (e.getValue().endsWith("1")) {
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
                if (n.getId() != sid && n.getId() != eid) {
                    String nid = String.valueOf(n.getId() + 1);
                    String n_pid = this.partitionInfos.get(nid).getValue();
                    if (!n_pid.equals(pid)) {
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
}
