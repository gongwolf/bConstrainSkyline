package GPSkyline.BFS;

import neo4jTools.*;

import javax.management.ObjectName;

import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphdb.PathExpanders;
import org.neo4j.graphalgo.WeightedPath;
import org.neo4j.graphdb.Transaction;
import org.neo4j.jmx.*;

import java.util.ArrayList;
import java.util.Date;
import java.util.Random;

public class mytest {
    public static void main(String args[]) {

        int graph_size = 4000;
        String degree = "5";

        if (args.length == 2) {
            graph_size = Integer.parseInt(args[0]);
            degree = args[1];
        }


        System.out.println("/home/gqxwolf/neo4j323/testdb"+graph_size+"_"+degree+"/databases/graph.db");
        mytest t = new mytest();
        int num = 20;
        int i = 0;
        while (i != num) {
            String sid = String.valueOf(t.getRandomNumberInRange(0, (int) graph_size - 1));
            String did = String.valueOf(t.getRandomNumberInRange(0, (int) graph_size - 1));
            String r = t.runUseNodeFinal(sid, did,graph_size,degree);
            if (r!=null && !r.split(":")[3].split(",")[3].equals("0")) {
                System.out.println(r);
                i++;
            }
        }
    }

    public void test(String sid, String did) {
        connector n = new connector("/home/gqxwolf/neo4j323/testdb4000_5/databases/graph.db");
        // connector n = new connector();
        n.startDB();
        GraphDatabaseService graphdb = n.getDBObject();
        System.out.println(getStartdate(graphdb));
        Node Source;
        Node Destination;
        try (Transaction tx = graphdb.beginTx()) {
            Long BeforePages = getFromManagementBean("Page cache", "Faults", graphdb);
            Source = graphdb.findNode(BNode.BusNode, "name", sid);
            System.out.println("Source Node:" + Source.getProperty("lat"));
            Destination = graphdb.findNode(BNode.BusNode, "name", did);
            System.out.println("Destination Node:" + Destination.getProperty("lat"));

            PathFinder<WeightedPath> finder = GraphAlgoFactory
                    .dijkstra(PathExpanders.forTypeAndDirection(Line.Linked, Direction.OUTGOING), "RunningTime");
            WeightedPath paths = finder.findSinglePath(Source, Destination);
            Long AfterPages = getFromManagementBean("Page cache", "Faults", graphdb);
            System.out.println(paths);
            System.out.println(AfterPages - BeforePages);
            System.out.println(getStartdate(graphdb));
            tx.success();
        }

        n.shutdownDB();
    }

    public String runUseNodeFinal(String sid, String did,int graph_size,String degree) {
        connector n = new connector("/home/gqxwolf/neo4j323/testdb"+graph_size+"_"+degree+"/databases/graph.db");
        n.startDB();
        GraphDatabaseService graphdb = n.getDBObject();
        Node Source;
        Node Destination;
        try (Transaction tx = graphdb.beginTx()) {
            Source = graphdb.findNode(BNode.BusNode, "name", sid);
            Destination = graphdb.findNode(BNode.BusNode, "name", did);
            tx.success();
        }
        myshortestPathUseNodeFinal mspNode = new myshortestPathUseNodeFinal(graphdb);
        String r = mspNode.getSkylinePath(Source, Destination);
//        System.out.println(r);
        n.shutdownDB();
        return r;
    }

    private Long getFromManagementBean(String Object, String Attribuite, GraphDatabaseService graphDb) {
        ObjectName objectName = JmxUtils.getObjectName(graphDb, Object);
        Long value = JmxUtils.getAttribute(objectName, Attribuite);

        return value;
    }

    private Date getStartdate(GraphDatabaseService graphdb) {
        ObjectName objectName = JmxUtils.getObjectName(graphdb, "Kernel");
        Date date = JmxUtils.getAttribute(objectName, "KernelStartTime");
        return date;
    }

    private static int getRandomNumberInRange(int min, int max) {
        if (min >= max) {
            throw new IllegalArgumentException("max must be greater than min");
        }
        Random r = new Random(System.nanoTime());
        return r.nextInt((max - min) + 1) + min;
    }
}
