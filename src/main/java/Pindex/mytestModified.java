package Pindex;

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

import java.util.Date;

public class mytestModified {
    public static void main(String args[]) {
        //System.out.println("start  ........");
        String src = "1";
        String dest = "5";

        src = args[0];
        dest = args[1];
        //System.out.println(src + "  to   " + dest);

        mytestModified t = new mytestModified();
        //t.runUseNode(src,dest);
        //System.out.println("=======================================================");
        t.runUseNodeFinal(src, dest);
        //t.test(src,dest);
    }

    public void runUseNode(String sid, String did) {
        connector n = new connector("/home/gqxwolf/neo4j/csldb/databases/graph.db");
        n.startDB();
        GraphDatabaseService graphdb = n.getDBObject();
        Node Source;
        Node Destination;
        try (Transaction tx = graphdb.beginTx()) {
            Source = graphdb.findNode(BNode.BusNode, "name", sid);
            Destination = graphdb.findNode(BNode.BusNode, "name", did);
            tx.success();
        }

        myshortestPathUseNodeModified mspNode = new myshortestPathUseNodeModified(graphdb);
        mspNode.getSkylinePath(Source, Destination);
        n.shutdownDB();
    }


    public void runUseNodeFinal(String sid, String did) {
        connector n = new connector("/home/gqxwolf/neo4j/csldb/databases/graph.db");
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
        mspNode.getSkylinePath(Source, Destination);
        n.shutdownDB();
    }

    public void test(String sid, String did) {
        connector n = new connector("/home/gqxwolf/neo4j/csldb/databases/graph.db");
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
            long startTime = System.nanoTime();
            PathFinder<WeightedPath> finder = GraphAlgoFactory
                    .dijkstra(PathExpanders.forTypeAndDirection(Line.Linked, Direction.OUTGOING), "RunningTime");
            WeightedPath paths = finder.findSinglePath(Source,Destination);
            System.out.println(System.nanoTime()-startTime);
            Long AfterPages = getFromManagementBean("Page cache", "Faults", graphdb);
            System.out.println(paths.nodes());
            System.out.println(AfterPages - BeforePages);
            System.out.println(getStartdate(graphdb));
            
            
            System.out.println("========================");
            BeforePages = getFromManagementBean("Page cache", "Faults", graphdb);
            myshortestPathUseNodeFinal mspNode = new myshortestPathUseNodeFinal(graphdb);
            startTime = System.nanoTime();
            Double weight = mspNode.myDijkstra(Source,Destination,"RunningTime");
            System.out.println(System.nanoTime()-startTime);
            AfterPages = getFromManagementBean("Page cache", "Faults", graphdb);
            System.out.println(paths.weight());
            System.out.println(weight);
            System.out.println(AfterPages-BeforePages);
            System.out.println("result is right ?"+ (paths.weight()==weight));
            tx.success();
        }

        n.shutdownDB();

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
}
