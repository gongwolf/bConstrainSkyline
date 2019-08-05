package Pindex;

import javafx.util.Pair;
import neo4jTools.BNode;
import neo4jTools.connector;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;

public class indexChecking {
    public static String PathBase = "/home/gqxwolf/mydata/projectData/ConstrainSkyline/data/";
    static String backupInnerIndex = PathBase + "/backup/indexes/inner";
    public static String paritionFile = PathBase + "partitions_info.txt";
    public static String portalListFile = PathBase + "portalList.txt";

    public HashMap<String, Pair<String, String>> partitionInfos = new HashMap<>();
    ArrayList<String> portals = new ArrayList<>();

    public long Prt;
    public long Srt;


    public static void main(String args[]) {
        indexChecking inc = new indexChecking();
        inc.checking(backupInnerIndex);


//        inc.readPartionsInfo(paritionFile);
//        inc.loadPortals();
//        connector n = new connector("/home/gqxwolf/neo4j323/csldb/databases/graph.db");
//        n.startDB();
//        GraphDatabaseService graphdb = n.getDBObject();
//        System.out.println("connected to neo4j success");
//        String pid = "49";
//        String cid = "1";
//        String startNode = "24427";
//        String endNode="24083";
//        File iFile = new File(backupInnerIndex + "/" + cid + "/" + pid + "_idx/" + startNode + "_" + endNode + ".inner.idx");
//        pairSer p = inc.DesTopairSer(iFile,pid);
//        System.out.println(inc.checkingPaths(p,pid,graphdb));
//        n.shutdownDB();
    }

    private void checking(String backupInnerIndex) {
        connector n = new connector("/home/gqxwolf/neo4j323/csldb/databases/graph.db");
        n.startDB();
        GraphDatabaseService graphdb = n.getDBObject();
        System.out.println("connected to neo4j success");

        readPartionsInfo(paritionFile);
        loadPortals();


        int i = 0;
        File f = new File(backupInnerIndex);
        for (File cfile : f.listFiles()) {
            String cid = cfile.getName();
            for (File pfile : cfile.listFiles()) {
                String pid = pfile.getName().substring(0, pfile.getName().indexOf("_"));
                for (File iFile : pfile.listFiles(new indexFilter())) {
                    i++;

                    long a1 = System.nanoTime();
                    pairSer p = DesTopairSer(iFile, pid);
                    this.Prt += ((System.nanoTime() - a1) / 1000000);
                    if (p == null) {
                        System.out.println(iFile.getName());
                    } else {
                        boolean rf = checkingPaths(p, pid, graphdb);
                        if (!rf) {
                            System.out.println(cid + " " + pid + " " + p.startNode + " " + p.endNode);
                        }
                    }

                    if (i % 1000 == 0) {
                        System.out.println(i + ".................");
                    }

                }

            }
        }

        System.out.println("Total files:" + i);
        System.out.println("Total time to read index file :" + (this.Prt / 1000) + "s    avg time:" + ((double) this.Prt / i) + "ms");
        System.out.println("Total time to get the skyline in node :" + (this.Srt / 1000) + "s   avg time:" + ((double) this.Srt / i) + "ms");
        n.shutdownDB();
        System.out.println("shut down success");

    }


    private boolean checkingPaths(pairSer p, String pid, GraphDatabaseService graphdb) {
        String sid = p.startNode;
        String eid = p.endNode;
        long a1 = System.nanoTime();
        ArrayList<path> r2 = runSkylineInBlock(sid, eid, pid, graphdb);
        removePathNotWithinBlockOrig(pid, r2);
//        System.out.println(r2.size());
        removePathNotWithinBlockDisk(pid, p.pathinfos);
        this.Srt += ((System.nanoTime() - a1) / 1000000);

        return checkPathResultEquation(r2, p.pathinfos);
    }

    private pairSer DesTopairSer(File iFile, String pid) {
        pairSer r = null;
        try {
            // Reading the object from a file
            FileInputStream file = new FileInputStream(iFile);
            ObjectInputStream in = new ObjectInputStream(file);

            // Method for deserialization of object
            r = (pairSer) in.readObject();

            in.close();
            file.close();

//            System.out.println("Object has been deserialized ");
//            System.out.println(r.startNode+"  "+r.endNode);
//            System.out.println(r.pathinfos.size());

        } catch (IOException ex) {
            ex.printStackTrace();
            return null;
        } catch (ClassNotFoundException ex) {
            System.out.println("ClassNotFoundException is caught");
            return null;
        }
        return r;
    }

    public ArrayList<path> runSkylineInBlock(String sid, String did, String pid, GraphDatabaseService graphdb) {
//        connector n = new connector("/home/gqxwolf/neo4j/csldb/databases/graph.db");
        Node Source;
        Node Destination;
        try (Transaction tx = graphdb.beginTx()) {
            Source = graphdb.findNode(BNode.BusNode, "name", sid);
            Destination = graphdb.findNode(BNode.BusNode, "name", did);
            tx.success();
        }
        mySkylineInBlock ibNode = new mySkylineInBlock(graphdb, this.partitionInfos, this.portals);
        ArrayList<path> r = ibNode.getSkylinePath(Source, Destination, pid);
        return r;
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


    public void removePathNotWithinBlockOrig(String pid, ArrayList<path> paths) {

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


    public void removePathNotWithinBlockDisk(String pid, ArrayList<pathSer> paths) {

        int i = 0;
        for (; i < paths.size(); ) {
            pathSer p = paths.get(i);

            // System.out.println(p);
            // System.out.println(printCosts(p.getCosts()));

            String sid = p.nodes.get(0);
            String eid = p.nodes.get(p.nodes.size() - 1);

            boolean flag = true;
            for (String n : p.nodes) {
                if (!n.equals(sid) && !n.equals(eid)) {
                    String mapped_nID = String.valueOf(Long.parseLong(n) + 1);
                    String n_pid = this.partitionInfos.get(mapped_nID).getValue();
                    if (!n_pid.equals(pid) || this.portals.contains(mapped_nID)) {
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


    private void loadPortals() {
        try {
            BufferedReader br = new BufferedReader(new FileReader(portalListFile));
            String line = null;
            while ((line = br.readLine()) != null) {
                String StartNode = line.trim();
                this.portals.add(StartNode);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static boolean checkPathResultEquation(ArrayList<path> r1, ArrayList<pathSer> r2) {
//        System.out.println(r2.size()+" "+(r1.size() > r2.size()));
        boolean[] results = new boolean[r2.size()];
        if (r1.size() < r2.size())
            return false;
        int i = 0;
        for (pathSer p2 : r2) {
            for (path p1 : r1) {
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

    private static boolean isEqualPath(path p1, pathSer p2) {
        return isEqualNodes(p1.Nodes, p2.nodes) && isEqualEdges(p1.relationships, p2.rels);
    }

    private static boolean isEqualEdges(ArrayList<Relationship> relationships1, ArrayList<String> relationships2) {
        if (relationships1.size() != relationships2.size()) {
            return false;
        }


        for (int i = 0; i < relationships1.size(); i++) {
            if (relationships1.get(i).getId() != Long.parseLong(relationships2.get(i))) {
                return false;
            }

        }
        return true;
    }


    private static boolean isEqualNodes(ArrayList<Node> nodes1, ArrayList<String> nodes2) {
        if (nodes1.size() != nodes2.size()) {
            return false;
        }


        for (int i = 0; i < nodes1.size(); i++) {
            if (nodes1.get(i).getId() != Long.parseLong(nodes2.get(i))) {
                return false;
            }

        }
        return true;
    }

}
