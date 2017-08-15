package Pindex;

import neo4jTools.*;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphalgo.WeightedPath;
import org.neo4j.graphdb.*;

import javafx.util.Pair;

import Pindex.path;

public class test {
    HashMap<String, HashMap<String, HashMap<String, String>>> pMapping = new HashMap<>();
    public HashMap<String, Pair<String, String>> partitionInfos = new HashMap<>();
    public static String PathBase = "/home/gqxwolf/mydata/projectData/ConstrainSkyline/data/";
    public static String paritionFile = PathBase + "partitions_info.txt";

    public static void main(String args[]) {
        test t = new test();
//         String sid = "1";
//         String eid = "99";
        t.readPartitionInfo();
        t.readPartionsInfo(paritionFile);
//         ArrayList<path> r = t.runUseNodeFinal(sid,eid);
//         System.out.println(r.size());
//         t.removePathNotWithinBlock("0", r);
//         System.out.println(r.size());
//
//         double[] lowerb = t.getShortestCost(sid, eid);
//         System.out.println(t.printCosts(lowerb));
//
////         pairSer ps = new pairSer(sid, eid, r, lowerb);
//
//        // t.writeToDisk("19", "0", ps);


//        t.generateInnerPair();
//        t.generateInterPair();

//        connector n = new connector();

        connector n = new connector("/home/gqxwolf/neo4j/csldb/databases/graph.db");
        n.startDB();
        GraphDatabaseService graphdb = n.getDBObject();
        t.buildIndex(graphdb);
        n.shutdownDB();
    }

    private void buildIndex(GraphDatabaseService graphdb) {
        String innerP = "/home/gqxwolf/mydata/projectData/ConstrainSkyline/data/Pairs/pairs.inner";
        String interP = "/home/gqxwolf/mydata/projectData/ConstrainSkyline/data/Pairs/pairs.inter";

        int i = 1;
        System.out.println("Building inner index is processing");
         try (BufferedReader br = new BufferedReader(new FileReader(innerP))) {
             String line = null;
             while ((line = br.readLine()) != null) {
                 String[] infos = line.split(" ");
                 String cid = infos[0];
                 String pid = infos[1];
                 String sid = infos[2];
                 String eid = infos[3];
                 ArrayList<path> skyR = this.runUseNodeFinal(sid, eid,graphdb,pid);
                 if (skyR!=null && skyR.size() != 0 ) {
                     removePathNotWithinBlock(pid, skyR);
                     if (skyR.size() != 0) {
                         double[] costs = getShortestCost(sid, eid,graphdb);
                         if (costs[0] != -1) {
                             pairSer ps = new pairSer(sid, eid, skyR, costs);
                             writeToDisk(cid, pid, ps);
                         }
                     }
                 }

                 if (i % 10000 == 0) {
                     System.out.println(i + "..........");
                 }
                 i++;

             }
         } catch (Exception e) {
             e.printStackTrace();
         }

        long i_ter = 1;
        System.out.println("Building inter index is processing");
        try (BufferedReader br = new BufferedReader(new FileReader(interP))) {
            String line = null;
            while ((line = br.readLine()) != null) {
                String[] infos = line.split(" ");
                String cid = infos[0];
                String pid = infos[1];
                String sid = infos[2];
                String eid = infos[3];

                double[] costs = getShortestCost(sid, eid,graphdb);
                writeToDisk(cid, pid, sid, eid, costs);
                if (i_ter % 400000 == 0) {
                    System.out.println(i_ter + "..........");
                }
                i_ter++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void writeToDisk(String cid, String pid, String sid, String eid, double[] costs) {
        String fpath = "/home/gqxwolf/mydata/projectData/ConstrainSkyline/data/indexes/inter/" + cid + "/";
        File f = new File(fpath);
        if (!f.exists()) {
            f.mkdirs();
        }
        String interpath = fpath + "pid.inter.idx";
        try (FileWriter fw = new FileWriter(interpath, true);
                BufferedWriter bw = new BufferedWriter(fw);
                PrintWriter out = new PrintWriter(bw)) {
            String result = "";
            result = result + cid + " ";
            result = result + pid + " ";
            result = result + sid + " ";
            result = result + eid + " ";
            for (double d : costs) {
                result = result + d + " ";
            }
            out.println(result);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void writeToDisk(String cid, String pid, pairSer ps) {
        String fpath = "/home/gqxwolf/mydata/projectData/ConstrainSkyline/data/indexes/inner/" + cid + "/" + pid
                + "_idx/";
        File f = new File(fpath);
        if (!f.exists()) {
            f.mkdirs();
        }
        try {
            String indexFilePath = fpath + "/" + ps.startNode + "_" + ps.endNode + ".inner.idx";
            File idxF = new File(indexFilePath);

            if (idxF.exists()) {
                idxF.delete();
            }

            FileOutputStream fos = new FileOutputStream(indexFilePath);
            ObjectOutputStream oos = new ObjectOutputStream(fos);
            oos.writeObject(ps);
            oos.close();
            fos.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void writePairToDisk(String cid, String pid, String sid, String eid, String indexType) {
        String fpath = "/home/gqxwolf/mydata/projectData/ConstrainSkyline/data/Pairs/";
        File fFile = new File(fpath);
        if (!fFile.exists()) {
            fFile.mkdirs();
        }
        if (indexType.equals("inner")) {
            fpath = fpath + "pairs.inner";
        } else if (indexType.equals("inter")) {
            fpath = fpath + "pairs.inter";
        }

        try (FileWriter fw = new FileWriter(fpath, true);
                BufferedWriter bw = new BufferedWriter(fw);
                PrintWriter out = new PrintWriter(bw)) {
            String result = "";
            result = result + cid + " ";
            result = result + pid + " ";
            result = result + sid + " ";
            result = result + eid;
            out.println(result);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private void generateInnerPair() {
        int counter = 0;
        System.out.println("inner pair Starting......");
        for (String cid : this.pMapping.keySet()) {
            for (String pid : this.pMapping.get(cid).keySet()) {
                HashMap<String, String> portalNodes = this.pMapping.get(cid).get(pid);
                for (Map.Entry<String, String> portal : portalNodes.entrySet()) {
                    String portalId = portal.getKey();
                    String bits = portal.getValue();
                    // if this portal is a in-coming portal.
                    if (bits.startsWith("1")) {
                        for (Map.Entry<String, String> otherP : portalNodes.entrySet()) {
                            // if there is a out-going portal node in the same
                            // block, and their id is not same;
                            if (!otherP.equals(portalId) && otherP.getValue().endsWith("1")) {
                                String sid = String.valueOf(Integer.parseInt(portalId) - 1);
                                String eid = String.valueOf(Integer.parseInt(otherP.getKey()) - 1);
                                writePairToDisk(cid, pid, sid, eid, "inner");
                                counter++;
                                if (counter % 5000 == 0) {
                                    System.out.println(counter + "........");
                                }
                            }
                        }
                    }
                }
            }
        }
        System.out.println("done !!!! " + counter);

    }

    private void generateInterPair() {
        int counter = 0;
        System.out.println("inner pair Starting......");
        for (String cid : this.pMapping.keySet()) {
            for (String pid : this.pMapping.get(cid).keySet()) {
                HashMap<String, String> portalNodes = this.pMapping.get(cid).get(pid);
                for (Map.Entry<String, String> portal : portalNodes.entrySet()) {
                    String portalId = portal.getKey();
                    String bits = portal.getValue();
                    // if this portal is a out-going portal.
                    if (bits.endsWith("1")) {

                        for (String o_cid : this.pMapping.keySet()) {
                            // if there is a in-coming in same connection
                            // component.
                            // and it is in the different partition
                            if (o_cid.equals(cid)) {
                                for (String o_pid : pMapping.get(cid).keySet()) {
                                    if (!pid.equals(o_pid)) {
                                        HashMap<String, String> o_portalNodes = pMapping.get(cid).get(o_pid);
                                        for (Map.Entry<String, String> o_portal : o_portalNodes.entrySet()) {

                                            if (!portalId.equals(o_portal.getKey())
                                                    && o_portal.getValue().startsWith("1")) {
                                                String sid = String.valueOf(Integer.parseInt(portalId) - 1);
                                                String eid = String.valueOf(Integer.parseInt(o_portal.getKey()) - 1);
                                                writePairToDisk(cid, pid, sid, eid, "inter");
                                                counter++;
                                                if (counter % 10000 == 0) {
                                                    System.out.println(counter + "........");
                                                }
                                            }
                                        }
                                    }

                                }
                            }
                        }

                    }
                }
            }
        }

        System.out.println("done !!!! " + counter);

    }

    public ArrayList<path> runUseNodeFinal(String sid, String did, GraphDatabaseService graphdb, String pid) {
//        connector n = new connector("/home/gqxwolf/neo4j/csldb/databases/graph.db");
        Node Source;
        Node Destination;
        try (Transaction tx = graphdb.beginTx()) {
            Source = graphdb.findNode(BNode.BusNode, "name", sid);
            Destination = graphdb.findNode(BNode.BusNode, "name", did);
            tx.success();
        }
        myshortestPathUseNodeFinal mspNode = new myshortestPathUseNodeFinal(graphdb);
        ArrayList<path> r = mspNode.getSkylinePath(Source, Destination,pid,this.partitionInfos);
        return r;
    }

    public double[] getShortestCost(String sid, String did, GraphDatabaseService graphdb) {
//        connector n = new connector("/home/gqxwolf/neo4j/csldb/databases/graph.db");

        Node Source;
        Node Destination;
        double[] iniLowerBound;
        try (Transaction tx = graphdb.beginTx()) {
            Source = graphdb.findNode(BNode.BusNode, "name", sid);
            Destination = graphdb.findNode(BNode.BusNode, "name", did);
            path dummyP = new path(Source);
            int i = 0;
            iniLowerBound = new double[dummyP.NumberOfProperties];

            for (int j = 0; j < iniLowerBound.length; j++) {
                iniLowerBound[j] = -1;
            }

            for (String property_name : dummyP.propertiesName) {
                PathFinder<WeightedPath> finder = GraphAlgoFactory
                        .dijkstra(PathExpanders.forTypeAndDirection(Line.Linked, Direction.OUTGOING), property_name);
                WeightedPath paths = finder.findSinglePath(dummyP.endNode, Destination);
                if (paths != null) {
                    iniLowerBound[i++] = paths.weight();
                } else {
                    break;
                }
            }
            tx.success();
            // System.out.println(printCosts(iniLowerBound));
        }

        return iniLowerBound;
    }

    public String printCosts(double costs[]) {
        StringBuffer sb = new StringBuffer();
        sb.append("[");
        int i = 0;
        for (; i < costs.length - 1; i++) {
            sb.append(costs[i] + ",");
        }
        sb.append(costs[i] + "]");
        return sb.toString();
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

        // for (String cid : pMapping.keySet()) {
        // System.out.println(cid);
        // for (String pid : pMapping.get(cid).keySet()) {
        // System.out.println(" " + pid + " " +
        // pMapping.get(cid).get(pid).size());
        // HashMap<String, String> portalNode = pMapping.get(cid).get(pid);
        // }
        // }

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

    public void removePathNotWithinBlock(String pid, ArrayList<path> paths) {

        int i = 0;
        for (; i < paths.size();) {
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
