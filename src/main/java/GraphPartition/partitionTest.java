package GraphPartition;

import javafx.util.Pair;
import org.neo4j.cypher.internal.frontend.v2_3.ast.In;
import org.neo4j.cypher.internal.frontend.v2_3.ast.InequalityExpression;
import scala.Int;

import javax.swing.*;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class partitionTest {
    String path = "/home/gqxwolf/mydata/projectData/testGraph40000/data";
    String EdgesInfoPath = path + "/SegInfo.txt";
    String partitionFile = path + "/mapping/mapped_metis_1.graph.part.40";

    private ArrayList<Pair<Integer, Integer>> connectionInfos = new ArrayList<>();
    private HashMap<Integer, Integer> nodes = new HashMap<>();
    private HashMap<Integer, Integer> partitionNNumber = new HashMap<>();
    private HashMap<Integer, Integer> partitionBNumber = new HashMap<>();

    public static void main(String args[]) {
        partitionTest pt = new partitionTest();
        pt.loadEdgesInfo();
        System.out.println(pt.connectionInfos.size());
        pt.partitionInformation();
        System.out.println(pt.nodes.size());
        pt.findBounderyNodes();


        int count=0;
        for (Map.Entry<Integer, Integer> p : pt.partitionBNumber.entrySet()) {
            System.out.println(p.getKey()+" "+pt.partitionNNumber.get(p.getKey())+" "+p.getValue());
            count+=p.getValue();
        }

        System.out.println(count);


    }

    private void findBounderyNodes() {
        for (Map.Entry<Integer, Integer> node : this.nodes.entrySet()) {
            int nodeid = node.getKey();
            int pid = node.getValue();
            ArrayList<Pair<Integer, Integer>> connectedEdge = getConnectNode(nodeid);
            for (Pair<Integer, Integer> e : connectedEdge) {
                if (e.getValue() == nodeid) {
                    if (this.nodes.get(e.getKey()) != pid) {
                        if (partitionBNumber.get(pid) != null) {
                            partitionBNumber.put(pid, partitionBNumber.get(pid) + 1);
                        } else {
                            partitionBNumber.put(pid, 1);
                        }

                    }
                }

                if (e.getKey() == nodeid) {
                    if (this.nodes.get(e.getValue()) != pid) {
                        if (partitionBNumber.get(pid) != null) {
                            partitionBNumber.put(pid, partitionBNumber.get(pid) + 1);
                        } else {
                            partitionBNumber.put(pid, 1);
                        }
                    }
                }


            }
        }
    }

    public void partitionInformation() {
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(partitionFile));
            String line = null;
            int i = 0;
            while ((line = br.readLine()) != null) {
                int pid = Integer.parseInt(line);
                this.nodes.put(i, pid);
                if (partitionNNumber.get(pid) != null) {
                    partitionNNumber.put(pid, partitionNNumber.get(pid) + 1);
                } else {
                    partitionNNumber.put(pid, 1);
                }
                i++;
            }
            br.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private void loadEdgesInfo() {
        try {
            BufferedReader br = new BufferedReader(new FileReader(EdgesInfoPath));
            String line = null;
            while ((line = br.readLine()) != null) {
                int StartNode = Integer.parseInt(line.split(" ")[0]);
                int EndNode = Integer.parseInt(line.split(" ")[1]);
//                System.out.println(StartNode+ " -> "+ EndNode+ "     "+line);
                Pair<Integer, Integer> p = new Pair<>(StartNode, EndNode);
                this.connectionInfos.add(p);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public ArrayList<Pair<Integer, Integer>> getConnectNode(int nodeid) {
        ArrayList<Pair<Integer, Integer>> result = new ArrayList<>();
        for (Pair<Integer, Integer> e : this.connectionInfos) {
            if (e.getKey() == nodeid || e.getValue() == nodeid) {
                result.add(e);
            }
        }

        return result;
    }
}
