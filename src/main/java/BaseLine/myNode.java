package BaseLine;

import RstarTree.Data;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import java.util.ArrayList;

public class myNode {
    public long id;
    public Node node;
    public Data qNode;
    public ArrayList<path> skyPaths;
    public double distance_q;
    double[] locations = new double[2];
    public ArrayList<Data> d_list;

    public myNode(Data startNode, Node current, GraphDatabaseService graphdb) {
        this.node = current;
        this.qNode = startNode;
        this.id = current.getId();
        skyPaths = new ArrayList<>();
        setLocations(graphdb);
        path dp = new path(this);
        this.skyPaths.add(dp);
    }

    public double[] getLocations() {
        return locations;
    }

    public void setLocations(GraphDatabaseService graphdb) {
        try (Transaction tx = graphdb.beginTx()) {
            locations[0] = (double) this.node.getProperty("lat");
            locations[1] = (double) this.node.getProperty("log");
            this.distance_q = Math.sqrt(Math.pow(locations[0] - qNode.location[0], 2) + Math.pow(locations[1] - qNode.location[1], 2));
            tx.success();
        }
        this.locations = locations;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public boolean addToSkyline(path np) {
        int i = 0;
        if (skyPaths.isEmpty()) {
            this.skyPaths.add(np);
        } else {
            boolean can_insert_np = true;
            for (; i < skyPaths.size(); ) {
                if (checkDominated(skyPaths.get(i).costs, np.costs)) {
                    can_insert_np = false;
                    break;
                } else {
                    if (checkDominated(np.costs, skyPaths.get(i).costs)) {
                        this.skyPaths.remove(i);
                    } else {
                        i++;
                    }
                }
            }

            if (can_insert_np) {
                this.skyPaths.add(np);
                return true;
            }
        }
        return false;
    }

    private boolean checkDominated(double[] costs, double[] estimatedCosts) {
        for (int i = 0; i < costs.length; i++) {
            if (costs[i]*(1) > estimatedCosts[i]) {
                return false;
            }
        }
        return true;
    }
}
