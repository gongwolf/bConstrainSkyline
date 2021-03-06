package Pindex.inblockSkyline;

import Pindex.myNodePriorityQueue;
import Pindex.mySkylineInBlock;
import Pindex.path;
import javafx.util.Pair;
import neo4jTools.BNode;
import org.neo4j.cypher.internal.frontend.v2_3.ast.functions.Str;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import javax.xml.transform.Source;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class myskylinePartition {

    private GraphDatabaseService graphdb;
    private PortalPriorityQueue mqueue;
    private HashMap<String, Pair<String, String>> partitionInfos;
    private ArrayList<String> portals;
    private mySkylineInBlock msib;
    private HashMap<String, HashMap<String, HashMap<String, String>>> pMapping;
    private Portal[] portalsObj;
    ArrayList<path> skylinPaths = new ArrayList<>();


    public myskylinePartition(GraphDatabaseService graphdb, HashMap<String, Pair<String, String>> partitionInfos, ArrayList<String> portals, HashMap<String, HashMap<String, HashMap<String, String>>> pMapping) {
        this.graphdb = graphdb;
        mqueue = new PortalPriorityQueue();
        this.partitionInfos = partitionInfos;
        this.portals = portals;
        this.pMapping = pMapping;
        this.msib = new mySkylineInBlock(graphdb, partitionInfos, portals);
        this.portalsObj = new Portal[16];
        for (int i = 0; i < portalsObj.length; i++) {
            portalsObj[i] = new Portal();
        }
    }

    private void initailPortals(String mapped_did) {
        HashMap<Pair<String, String>, Double[]> costMap = new HashMap<>();
        String pFile = "/home/gqxwolf/mydata/projectData/testGraph/data/indexes/inter/1/pid.inter.idx";
        int lines = 0;
        try (BufferedReader br = new BufferedReader(new FileReader(pFile))) {
            String line;
            while ((line = br.readLine()) != null) {
                lines++;
                String infos[] = line.split(" ");
                String sid = infos[2];
                String eid = infos[3];

                int n = infos.length - 4;
                Double cost[] = new Double[n];
                for (int i = 4; i < infos.length; i++) {
                    cost[i - 4] = Double.parseDouble(infos[i]);
                }
                costMap.put(new Pair<>(sid, eid), cost);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(lines + "  " + costMap.size());
        Integer pid = Integer.parseInt(this.partitionInfos.get(mapped_did).getValue());
        for (int i = 0; i < this.portalsObj.length; i++) {
            if (pid != i) {
                double[] cost = getLowerBound(pid, i, costMap);
            }
        }
    }

    private double[] getLowerBound(Integer epid, int i, HashMap<Pair<String, String>, Double[]> costMap) {
        double[] result = new double[3];

        for (int j = 0; j < result.length; j++) {
            result[j] = -Double.MAX_VALUE;
        }

//        HashMap<String, String> qq = this.pMapping.get("1").get(i);
        System.out.println(i + " " + epid);
        String iid = String.valueOf(i);
        String str_epid = String.valueOf(epid);
        for (Map.Entry<String, String> iportal : this.pMapping.get("1").get(iid).entrySet()) {
            if (iportal.getValue().endsWith("1")) {
                for (Map.Entry<String, String> eportal : this.pMapping.get("1").get(str_epid).entrySet()) {
                    if (eportal.getValue().startsWith("1")) {
                        String sid = String.valueOf(Long.parseLong(iportal.getKey()) - 1);
                        String eid = String.valueOf(Long.parseLong(eportal.getKey()) - 1);
                        if (!sid.equals(eid)) {
                            Pair<String, String> key = new Pair<>(sid, eid);
//                            System.out.println("--->" + sid + "   " + eid);
                            Double[] cost = costMap.get(key);
                            for (int j = 0; j < cost.length; j++) {
                                if (cost[j] < result[j]) {
                                    result[j] = cost[j];
                                }
                            }
                        }
                    }

                }
            }
        }

        return result;
    }

    public void getSkylinePath(Node source, Node destination) {
        String sid = String.valueOf(source.getId());
        String did = String.valueOf(destination.getId());


        String mapped_sid = String.valueOf(source.getId() + 1);
        String mapped_did = String.valueOf(destination.getId());

        initailPortals(mapped_did);

        //mapped_sid is not a portal.
        String spid = this.partitionInfos.get(mapped_sid).getValue();
        int int_pid = Integer.valueOf(spid);
        if (!isPortals(mapped_sid)) {

            System.out.println(spid);
            findPathToPortals(source, spid);
        } else {
            path iniPath = new path(source, source);
            portalsObj[int_pid].addToSkylineResult(iniPath);
        }


        this.mqueue.add(this.portalsObj[int_pid]);
        System.out.println("find " + portalsObj[int_pid].skylinPaths.size() + " paths from " + sid + " to portals");

        while (!this.mqueue.isEmpty()) {
            Portal por = mqueue.pop();

            for (path p : por.skylinPaths) {
                System.out.println(p.endNode);

            }
        }


    }

    private void findPathToPortals(Node source, String spid) {
        int int_pid = Integer.parseInt(spid);
        for (Map.Entry<String, String> portals : this.pMapping.get("1").get(spid).entrySet()) {
            if (portals.getValue().endsWith("1")) {
                try (Transaction tx = this.graphdb.beginTx()) {
                    Node destination = graphdb.findNode(BNode.BusNode, "name", portals.getKey());
                    ArrayList<path> iniSkylinePath = msib.getSkylinePath(source, destination, spid);
//                    System.out.println(iniSkylinePath.size());
                    for (path p : iniSkylinePath) {
                        this.portalsObj[int_pid].addToSkylineResult(p);
//                    System.out.println(p);
                    }
                }
            }
        }
    }

    private boolean isPortals(String mapped_sid) {
        return this.portals.contains(mapped_sid);
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

    private void addToSkylineResult(path np) {
        int i = 0;
        if (skylinPaths.isEmpty()) {
            this.skylinPaths.add(np);
        } else {
            boolean alreadyinsert = false;
            for (; i < skylinPaths.size(); ) {
                if (checkDominated(skylinPaths.get(i).getCosts(), np.getCosts())) {
                    if (alreadyinsert && i != this.skylinPaths.size() - 1) {
                        this.skylinPaths.remove(this.skylinPaths.size() - 1);
//                        this.removedPath++;
                    }
                    break;
                } else {
                    if (checkDominated(np.getCosts(), skylinPaths.get(i).getCosts())) {
                        this.skylinPaths.remove(i);
                    } else {
                        i++;
                    }
                    if (!alreadyinsert) {
                        this.skylinPaths.add(np);
                        alreadyinsert = true;
                    }

                }
            }
        }
    }

    private boolean checkDominated(double[] costs, double[] estimatedCosts) {
        int numberOfLessThan = 0;
        for (int i = 0; i < costs.length; i++) {
//            double c = (double) Math.round(costs[i] * 1000000d) / 1000000d;
//            double e = (double) Math.round(estimatedCosts[i] * 1000000d) / 1000000d;
            double c = costs[i];
            double e = estimatedCosts[i];
            if (c > e) {
                return false;
            }

//            if (numberOfLessThan == 0 && c < e) {
//                numberOfLessThan = 1;
//            }
        }
//        if (numberOfLessThan == 0) {
//            return false;
//        } else {
        return true;
//        }
    }
}
