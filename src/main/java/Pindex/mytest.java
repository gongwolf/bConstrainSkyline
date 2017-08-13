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

public class mytest {
    public static void main(String args[]) {
        //System.out.println("start  ........");
        String src = "1";
        String dest = "5";

        src = args[0];
        dest = args[1];
        //System.out.println(src + "  to   " + dest);

        mytest t = new mytest();
        t.runUseNode(src, dest);
        // t.test(src, dest);
    }

    public void runUseNode(String sid, String did) {
        connector n = new connector("/home/gqxwolf/neo4j/csldb/databases/graph.db");
        // connector n = new connector();
        n.startDB();
        GraphDatabaseService graphdb = n.getDBObject();
        //Long BeforePages = getFromManagementBean("Page cache","Faults",graphdb);
        Node Source;
        Node Destination;
        try (Transaction tx = graphdb.beginTx()) {
            Source = graphdb.findNode(BNode.BusNode, "name", sid);
            //System.out.println("Source Node:" + Source.getId());
            Destination = graphdb.findNode(BNode.BusNode, "name", did);
            //System.out.println("Destination Node:" + Destination.getId());
            tx.success();
        }

        //Long MiddlePages = getFromManagementBean("Page cache", "Faults", graphdb);
        //System.out.println("Pins : "+getFromManagementBean("Page cache", "Pins",graphdb));
        //System.out.println(MiddlePages - BeforePages);
        
        myshortestPathUseNode mspNode = new myshortestPathUseNode(graphdb);
        mspNode.getSkylinePath(Source, Destination);
        
        //Long AfterPages = getFromManagementBean("Page cache", "Faults", graphdb);
        //System.out.println(AfterPages - MiddlePages);
        //System.out.println(getStartdate(graphdb));
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
