package twoHop;

import javafx.util.Pair;
import neo4jTools.Line;
import neo4jTools.connector;
import org.apache.commons.io.FileUtils;
import org.neo4j.graphdb.*;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

public class twoHopIndex {

    String basePath = "/home/gqxwolf/mydata/projectData/testGraph4000_5/data";
    String graphDBPath = "/home/gqxwolf/neo4j323/testdb4000_5/databases/graph.db";
    boolean isDirectional = false;
    private GraphDatabaseService graphdb;
    private connector nconn;
    private int graphSize;
    private int degree;
    private long usedInQuery;


    public twoHopIndex(int graphSize, int degree, boolean isDirectional) {
        if (isDirectional) {
            this.basePath = "/home/gqxwolf/mydata/projectData/testGraph" + graphSize + "_" + degree + "/data";
            this.graphDBPath = "/home/gqxwolf/neo4j323/testdb" + graphSize + "_" + degree + "/databases/graph.db";
        } else {
            this.basePath = "/home/gqxwolf/mydata/projectData/un_testGraph" + graphSize + "_" + degree + "/data";
            this.graphDBPath = "/home/gqxwolf/neo4j323/test_un_db" + graphSize + "_" + degree + "/databases/graph.db";
        }

        this.isDirectional = isDirectional;
        this.graphSize = graphSize;
        this.degree = degree;

        this.nconn = new connector(this.graphDBPath);
        nconn.startDB();
        //System.out.println(this.graphDBPath);
        //System.out.println(this.basePath);
        this.graphdb = nconn.getDBObject();
    }

    public static void main(String args[]) {
        twoHopIndex thi = new twoHopIndex(1000, 5, true);
        long r1  = System.nanoTime();
        thi.buildIndex();
        System.out.println((System.nanoTime()-r1)/1000000);
        thi.closeDB();
    }

    public void closeDB() {
        this.nconn.shutdownDB();
    }

    public void buildIndex() {
        if (this.isDirectional) {
            buildFromIndex();
        }
    }

    private void buildFromIndex() {
        String target_From_Path = this.basePath + "/twoHop/FromIndex/";
        String target_To_Path = this.basePath + "/twoHop/ToIndex/";
        //System.out.println(target_From_Path);
        //System.out.println(target_To_Path);
        File FromFolder = new File(target_From_Path);
        File ToFolder = new File(target_To_Path);
        try {
            FileUtils.deleteDirectory(FromFolder);
            FileUtils.deleteDirectory(ToFolder);
            FromFolder.mkdirs();
            ToFolder.mkdirs();
        } catch (IOException e) {
            e.printStackTrace();
        }


        LabelsIndex L = new LabelsIndex(this.graphSize);
        ArrayList<String> pNames = getPropertiesName();

        for (int vi = 0; vi < this.graphSize; vi++) {
            long rt1 = System.nanoTime();
//            LabelsIndex L1 = new LabelsIndex(L);
            long copyTime = System.nanoTime() - rt1;
            long QueryTime_before = this.usedInQuery;
            long pt = 0, rpt = 0;
            for (String propertyName : pNames) {
                //System.out.println(propertyName);
                int pIndex = pNames.indexOf(propertyName);
                long st = System.nanoTime();
                PrunedDijkstra(vi, L, L, pIndex, propertyName);
                pt += System.nanoTime() - st;

                st = System.nanoTime();
                reversedPrunedDijkstra(vi, L, L, pIndex, propertyName);
                rpt += System.nanoTime() - st;
            }
            long qt = this.usedInQuery - QueryTime_before;
//            L = L1;

            long rt_Iter = System.nanoTime() - rt1;
            if (vi % 500 == 0) {
                System.out.println(vi + " " + rt_Iter + "  " + qt + " " + copyTime+" "+pt+" "+rpt);
//                System.out.println("====================================");
            }
        }

        writeToDisk(L, FromFolder, ToFolder);

    }

    private void writeToDisk(LabelsIndex l, File fromFolder, File toFolder) {
        for (int vi = 0; vi < this.graphSize; vi++) {
            String vi_from_path = fromFolder.getAbsolutePath() + "/" + vi + ".idx";
            String vi_to_path = toFolder.getAbsolutePath() + "/" + vi + ".idx";

            BufferedWriter f_bw = null;
            BufferedWriter t_bw = null;
            FileWriter f_fw = null;
            FileWriter t_fw = null;

            Labels la = l.labels[vi];
            try {
                f_fw = new FileWriter(vi_from_path, true);
                f_bw = new BufferedWriter(f_fw);


                t_fw = new FileWriter(vi_to_path, true);
                t_bw = new BufferedWriter(t_fw);

                for (int fromid : la.fromLabelSet.keySet()) {
                    String line = String.valueOf(fromid) + ",";
                    line += la.fromLabelSet.get(fromid)[0] + ",";
                    line += la.fromLabelSet.get(fromid)[1] + ",";
                    line += la.fromLabelSet.get(fromid)[2];
                    f_bw.write(line + "\n");

                }

                for (int toid : la.toLabelSet.keySet()) {
                    String line = String.valueOf(toid) + ",";
                    line += la.toLabelSet.get(toid)[0] + ",";
                    line += la.toLabelSet.get(toid)[1] + ",";
                    line += la.toLabelSet.get(toid)[2];
                    t_bw.write(line + "\n");

                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {

                try {

                    if (f_bw != null)
                        f_bw.close();

                    if (f_fw != null)
                        f_fw.close();

                    if (t_bw != null)
                        t_bw.close();

                    if (t_fw != null)
                        t_fw.close();

                } catch (IOException ex) {

                    ex.printStackTrace();

                }
            }

        }
    }

    private LabelsIndex reversedPrunedDijkstra(int vi, LabelsIndex L, LabelsIndex L1, int pIndex, String propertyName) {

        //distance from other nodes to vi
        double[] P = new double[this.graphSize];
        boolean[] visited = new boolean[this.graphSize];

        for (int i = 0; i < this.graphSize; i++) {
            if (vi == i) {
                P[i] = 0;
            } else {
                P[i] = Double.POSITIVE_INFINITY;
            }
        }

        myPriorityQueue Q = new myPriorityQueue();
        Q.add(new Pair(vi, P[vi]));
        while (!Q.isEmpty()) {
            Pair<Integer, Double> u = Q.pop();

            //get uid and distance so far
            int uid = u.getKey();
            double costSoFar = u.getValue();

            //set uid is already visited
            visited[uid] = true;


//            if the distance from uid to vi which queried from the previous level already less than the distance on current Dijkstra.
            if (Query(uid, vi, L, pIndex) <= P[uid]) {
                continue;
            }

            //get the costs of the lable of uid, which store the cost from uid to vid on each dimension
            Double[] costs = L1.labels[uid].toLabelSet.get(vi);
            if (costs == null) {
                costs = new Double[3];
            }
            costs[pIndex] = costSoFar;
            L1.labels[uid].toLabelSet.put(vi, costs);

            ArrayList<Pair<Integer, Double>> neigbors = getInNeigbors(uid, propertyName);
            for (Pair<Integer, Double> w_i : neigbors) {
                if (!visited[w_i.getKey()]) {
                    double alt_cost = costSoFar + w_i.getValue();
                    if (alt_cost < P[w_i.getKey()]) {
                        P[w_i.getKey()] = alt_cost;
                        Q.add(new Pair<>(w_i.getKey(), alt_cost));
                    }
                }
            }
        }

        return L1;

    }

    private LabelsIndex PrunedDijkstra(int vi, LabelsIndex L, LabelsIndex L1, int pIndex, String propertyName) {

        double[] P = new double[this.graphSize];
        boolean[] visited = new boolean[this.graphSize];

        //System.out.println(P.length+" "+visited.length);

        for (int i = 0; i < this.graphSize; i++) {
            if (vi == i) {
                P[i] = 0;
            } else {
                P[i] = Double.POSITIVE_INFINITY;
            }
        }

        myPriorityQueue Q = new myPriorityQueue();
        Q.add(new Pair(vi, P[vi]));
        while (!Q.isEmpty()) {
            Pair<Integer, Double> u = Q.pop();
            //System.out.println(u);

            //get uid and distance so far
            int uid = u.getKey();
            double costSoFar = u.getValue();

            //set uid is already visited
            visited[uid] = true;


            //if the distance from vi to uid which queried from the previous level already less than the distance on current Dijkstra.
            if (Query(vi, uid, L, pIndex) <= P[uid]) {
                continue;
            }


            //get the costs of the lable of uid, which store the cost from v_i to uid on each dimension
            Double[] costs = L1.labels[uid].fromLabelSet.get(vi);
            //if there is no label before, create a new array to store the distance from vi to uid
            if (costs == null) {
                costs = new Double[3];
            }
            // In level L1, the distance from vi to u
            costs[pIndex] = costSoFar;
            L1.labels[uid].fromLabelSet.put(vi, costs);

            //Dijkstra relax
            ArrayList<Pair<Integer, Double>> neigbors = getOutNeigbors(uid, propertyName);
            for (Pair<Integer, Double> w_i : neigbors) {
                //System.out.println(w_i);
                if (!visited[w_i.getKey()]) {
                    double alt_cost = costSoFar + w_i.getValue();
                    if (alt_cost < P[w_i.getKey()]) {
                        P[w_i.getKey()] = alt_cost;
                        Q.add(new Pair<>(w_i.getKey(), alt_cost)); //node w_i -> distance from vi to w_i so far
                    }
                }
            }
        }

        return L1;
    }

    private ArrayList<Pair<Integer, Double>> getOutNeigbors(int uid, String pName) {
        ArrayList<Pair<Integer, Double>> result = new ArrayList<>();
        try (Transaction tx = this.graphdb.beginTx()) {
            Node n = this.graphdb.getNodeById(uid);
            //System.out.println(uid);
            Iterable<Relationship> rels = n.getRelationships(Line.Linked, Direction.OUTGOING);
            Iterator<Relationship> relIters = rels.iterator();
            while (relIters.hasNext()) {
                Relationship rel = relIters.next();
                int id = (int) rel.getEndNodeId();
                double cost = (double) rel.getProperty(pName);
                result.add(new Pair(id, cost));
                //System.out.println(id);
            }
            tx.success();
        }

        return result;
    }

    private ArrayList<Pair<Integer, Double>> getInNeigbors(int uid, String pName) {
        ArrayList<Pair<Integer, Double>> result = new ArrayList<>();
        try (Transaction tx = this.graphdb.beginTx()) {
            Node n = this.graphdb.getNodeById(uid);
            Iterable<Relationship> rels = n.getRelationships(Line.Linked, Direction.INCOMING);
            Iterator<Relationship> relIters = rels.iterator();
            while (relIters.hasNext()) {
                Relationship rel = relIters.next();
                int id = (int) rel.getStartNodeId();
                double cost = (double) rel.getProperty(pName);
                result.add(new Pair(id, cost));
            }
            tx.success();
        }

        return result;
    }


    /**
     * get the property names and return it as a ArrayList<String>
     *
     * @return the list of the property names
     */
    public ArrayList<String> getPropertiesName() {
        ArrayList<String> propertiesName = new ArrayList<>();
        try (Transaction tx = this.graphdb.beginTx()) {
            Node n = this.graphdb.getNodeById(0);
            Iterable<Relationship> rels = n.getRelationships(Line.Linked, Direction.BOTH);
            if (rels.iterator().hasNext()) {
                Relationship rel = rels.iterator().next();
                Map<String, Object> pnamemap = rel.getAllProperties();
                for (Map.Entry<String, Object> entry : pnamemap.entrySet()) {
                    propertiesName.add(entry.getKey());
                }
            }
            tx.success();
        }
        return propertiesName;
    }

    private double Query(int vi, int uid, LabelsIndex l, int pIndex) {
        long qt1 = System.nanoTime();
        double shortestDis = Double.POSITIVE_INFINITY;

        //all the node can be reached from vi
        for (int intermedia_id : l.labels[vi].toLabelSet.keySet()) {
            Double[] costs1 = l.labels[vi].toLabelSet.get(intermedia_id);
            if (costs1 != null) {
                Double cost1 = costs1[pIndex];
                if (cost1 != null) {
                    //the nodes could reached uid
                    Double[] costs2 = l.labels[uid].fromLabelSet.get(intermedia_id);
                    if (costs2 != null) {
                        Double cost2 = costs2[pIndex];
                        if (cost2 != null && cost1 + cost2 < shortestDis) {
                            shortestDis = cost1 + cost2;
                        }
                    }
                }
            }
        }

        this.usedInQuery += System.nanoTime() - qt1;
        return shortestDis;
    }
}
