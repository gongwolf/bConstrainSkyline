package twoHop;

import GPSkyline.landmark.path;
import neo4jTools.CreateDB;
import neo4jTools.Line;
import neo4jTools.connector;
import neo4jTools.generateGraph;
import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphalgo.WeightedPath;
import org.neo4j.graphdb.*;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.jmx.JmxUtils;
import twoHop.Cached.CachedBlocks;
import twoHop.Cached.writeBinaryFile;

import javax.management.ObjectName;
import java.io.*;
import java.util.HashMap;
import java.util.Random;

public class TestShortestPath {
    CachedBlocks cb_cache;
    int graphsize, degree;
    int cachesize, pagesize;
    int neo4j_page_missed = 0;
    int neo4j_page_accessed = 0;
    private String dbPath;
    private String indexPath;
    private String dataPath;

    public TestShortestPath(int graphsize, int degree, int cachesize, int pagesize, boolean deleteBefore, boolean buildIndex) {
        this.graphsize = graphsize;
        this.degree = degree;
        this.cachesize = cachesize;
        this.pagesize = pagesize;


        this.dbPath = "/home/gqxwolf/neo4j323/testdb" + this.graphsize + "_" + this.degree + "/databases/graph.db";
        this.indexPath = "/home/gqxwolf/mydata/projectData/testGraph" + this.graphsize + "_" + this.degree + "/data/twoHop/";
        this.dataPath = "/home/gqxwolf/mydata/projectData/testGraph" + this.graphsize + "_" + this.degree + "/data";


        if (deleteBefore) {
            deleteGraphData();
            CreateGraph();

            twoHopIndex thi = new twoHopIndex(graphsize, degree, true);
            thi.buildIndex();
            thi.closeDB();
            System.out.println("Created two-hop index is done !");

            writeBinaryFile w = new writeBinaryFile(graphsize, degree, pagesize * 1024);
            w.writeToDisk();
            System.out.println("Transfer the two-hop index to binary index is done");

        } else if (!deleteBefore && buildIndex) {
            twoHopIndex thi = new twoHopIndex(graphsize, degree, true);
            thi.buildIndex();
            thi.closeDB();
            System.out.println("Created two-hop index is done !");

            writeBinaryFile w = new writeBinaryFile(graphsize, degree, pagesize * 1024);
            w.writeToDisk();
            System.out.println("Transfer the two-hop index to binary index is done");
        }


        this.cb_cache = new CachedBlocks(this.graphsize, this.degree, this.cachesize, this.pagesize);
    }

    public static void main(String args[]) throws IOException {

        int graphsize = 100;
        int degree = 5;
        int cachesize = 2048;
        int pagesize = 2;
        boolean deleteBefore = false;
        boolean buildIndex = false;
        long number_query = 200;
        long t_r1 = 0, t_r2 = 0, t_r3 = 0;

        if (args.length == 7) {
            graphsize = Integer.parseInt(args[0]);
            degree = Integer.valueOf(args[1]);
            cachesize = Integer.valueOf(args[2]);
            pagesize = Integer.valueOf(args[3]);
            deleteBefore = Boolean.parseBoolean(args[4]);
            buildIndex = Boolean.parseBoolean(args[5]);
            number_query = Long.valueOf(args[6]);
            System.out.println("Read the parameter done!!");
        }

        TestShortestPath t = new TestShortestPath(graphsize, degree, cachesize, pagesize, deleteBefore, buildIndex);


        int i = 0;


        while (i < number_query) {
            int sid = getRandomNumberInRange(0, t.graphsize - 1);
            int did = getRandomNumberInRange(0, t.graphsize - 1);
//
//            int sid = 893;
//            int did = 115;

            long r1 = System.nanoTime();
            connector n = new connector(t.dbPath);
            n.startDB();
            GraphDatabaseService graphdb = n.getDBObject();
            double[] result1 = t.getShortestPath(sid, did, graphdb);
            n.shutdownDB();
            long dr1 = (System.nanoTime() - r1);
            t_r1 += dr1;


//            System.out.println(sid+"   "+did);
            long r2 = System.nanoTime();
            double[] result2 = t.query(sid, did);
            double dr2 = (System.nanoTime() - r2);
            t_r2 += dr2;

//            System.out.println(ArrayToString(result2));


            long r3 = System.nanoTime();
            double[] result3 = t.query_cached(sid, did);
            double dr3 = (System.nanoTime() - r3);
            t_r3 += dr3;

//            System.out.println(ArrayToString(result3));

            System.out.println(sid + "-->" + did + " : " + dr1 + "," + dr2 + "," + dr3 + " : " + compareResult(result1, result2) + "  |  " + compareResult(result2, result3));

            i++;
            if(i%200==0)
            {
                System.out.println(i+".........");
            }
        }

        StringBuffer sb = new StringBuffer();
        sb.append(t.cb_cache.tosize/t.pagesize/1024).append(" , ");
        sb.append(t.cb_cache.fromsize/t.pagesize/1024).append(" , ");
        sb.append(t_r1/1000000.0/number_query).append(" , ");
        sb.append(t_r2/1000000.0/number_query).append(" , ");
        sb.append(t_r3/1000000.0/number_query).append(" , ");
        sb.append(t.neo4j_page_accessed).append(" , ");
        sb.append(t.neo4j_page_missed).append(" , ");
        sb.append(t.cb_cache.cache_accessed).append(" , ");
        sb.append(t.cb_cache.cache_missed);

        System.out.println(sb.toString());


    }

    private static String ArrayToString(double[] result) {
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < result.length; i++) {
            sb.append(result[i]);
            if (i != result.length - 1) {
                sb.append(",");
            }
        }
        return sb.toString();
    }

    private static String compareResult(double[] result1, double[] result2) {
        StringBuffer sb = new StringBuffer();
        boolean flag = true;
        for (int i = 0; i < result1.length; i++) {
            if (result1[i] > result2[i]) {
                sb.append(0).append("|");
                flag = false;
            } else if (result1[i] == result2[i]) {
                sb.append(1).append("|");
            } else {
                sb.append(2).append("|");
                flag = false;
            }
        }

        String result = sb.substring(0, sb.lastIndexOf("|"));
        return result + " : " + flag;
    }

    private static int getRandomNumberInRange(int min, int max) {
        if (min >= max) {
            throw new IllegalArgumentException("max must be greater than min");
        }
        Random r = new Random(System.nanoTime());
        return r.nextInt((max - min) + 1) + min;
    }

    private void CreateGraph() {
        generateGraph gg = new generateGraph(this.graphsize, this.degree, 3);
        gg.generateG();
        CreateDB db = new CreateDB(this.graphsize, this.degree);
        db.createDatabase();

    }

    private void deleteGraphData() {
        try {
            FileUtils.deleteRecursively(new File(this.dataPath)); //delete the whole folder of the data file.
            System.out.println("Deleted the data file:" + this.dataPath);
            FileUtils.deleteRecursively(new File(this.dbPath)); //delete the database file.
            System.out.println("Deleted the database file:" + this.dbPath);
            File DataFile = new File(this.dataPath);
            DataFile.mkdirs();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private double[] query_cached(int sid, int did) throws IOException {
        double[] result = new double[3];
        for (int i = 0; i < 3; i++) {
            result[i] = Double.POSITIVE_INFINITY;
        }

        HashMap<Integer, double[]> to_map = this.cb_cache.read_toIndex_blocks(sid);
//        System.out.println(to_map.size());
        HashMap<Integer, double[]> from_map = this.cb_cache.read_FromIndex_blocks(did);
//        System.out.println(from_map.size());
        for (int id : to_map.keySet()) {
//            System.out.println("---"+id);
            double[] cost1 = to_map.get(id);
//            System.out.println(ArrayToString(cost1));
            double[] cost2 = from_map.get(id);

            if (cost2 != null && cost1 != null) {
//                System.out.println("-----");
                for (int i = 0; i < 3; i++) {
                    if (cost1[i] != -1 && cost2[i] != -1 && ((cost1[i] + cost2[i]) < result[i])) {
                        result[i] = cost1[i] + cost2[i];
                    }
                }
            }
        }
        return result;
    }

    private double[] getShortestPath(long sid, long did, GraphDatabaseService graphdb) {
        double[] result = new double[3];
        Node Source;
        Node Destination;
        long pg1, pg2, pin1, pin2;
        //pg1 = getFromManagementBean("Page cache", "Faults", graphdb);
        //pin1 = getFromManagementBean("Page cache", "Pins", graphdb);

        try (Transaction tx = graphdb.beginTx()) {
            Source = graphdb.getNodeById(sid);
            Destination = graphdb.getNodeById(did);
            path fakePath = new path(Source);

            int pIndex = 0;

            for (String propertyName : fakePath.propertiesName) {
                PathFinder<WeightedPath> finder = GraphAlgoFactory
                        .dijkstra(PathExpanders.forTypeAndDirection(Line.Linked, Direction.OUTGOING), propertyName);
                WeightedPath paths = finder.findSinglePath(Source, Destination);
                if (paths == null) {
                    result[0] = result[1] = result[2] = -1.0;
                    break;
                } else {
                    result[pIndex++] = paths.weight();
                }
            }

            tx.success();
        }
        pg2 = getFromManagementBean("Page cache", "Faults", graphdb);
        pin2 = getFromManagementBean("Page cache", "Pins", graphdb);
        //System.out.println(pg1 + " " + pg2 + " " + pin1 + " " + pin2 + " " + (pg2 - pg1) + " " + (pin2 - pin1));
        this.neo4j_page_missed += pg2;
        this.neo4j_page_accessed += pin2;
        //System.out.println(result[0] + " " + result[1] + " " + result[2]);
        return result;
    }

    public double[] query(long sid, long did) {
        double result[] = new double[3];
        for (int i = 0; i < 3; i++) {
            result[i] = Double.POSITIVE_INFINITY;
        }
        String fPath = this.indexPath + "FromIndex/";
        String tPath = this.indexPath + "ToIndex/";

        HashMap<Long, double[]> to_map = new HashMap<>();
        HashMap<Long, double[]> from_map = new HashMap<>();

        try (BufferedReader br = new BufferedReader(new FileReader(tPath + "" + sid + ".idx"));
             BufferedReader fbr = new BufferedReader(new FileReader(fPath + "" + did + ".idx"))) {
            String line;
            String tline;
            while ((line = br.readLine()) != null) {
                Long interId = Long.valueOf(line.split(",")[0]);
                double[] costsTo = new double[3];
                for (int i = 0; i < costsTo.length; i++) {
                    if (!line.split(",")[i + 1].equals("null")) {
                        costsTo[i] = Double.valueOf(line.split(",")[i + 1]);
                    } else {
                        costsTo[i] = Double.NEGATIVE_INFINITY;
                    }
                }
//                System.out.println(interId);
                to_map.put(interId, costsTo);
//                System.out.println(line);
            }

            while ((tline = fbr.readLine()) != null) {
                long targId = Long.valueOf(tline.split(",")[0]);
//                System.out.println(tline);
                double[] costsFrom = new double[3];

                for (int i = 0; i < costsFrom.length; i++) {
                    if (!tline.split(",")[i + 1].equals("null")) {
                        costsFrom[i] = Double.valueOf(tline.split(",")[i + 1]);
                    } else {
                        costsFrom[i] = Double.NEGATIVE_INFINITY;
                    }
                }
//                System.out.println(interId);
                from_map.put(targId, costsFrom);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

//        System.out.println(to_map.size()+"  ---");
//        System.out.println(from_map.size()+"  ---");

        for (long id : to_map.keySet()) {
            double[] cost1 = to_map.get(id);
            double[] cost2 = from_map.get(id);

            if (cost2 != null && cost1 != null) {
                for (int i = 0; i < 3; i++) {
                    if (cost1[i] != Double.NEGATIVE_INFINITY && cost2[i] != Double.NEGATIVE_INFINITY && ((cost1[i] + cost2[i]) < result[i])) {
                        result[i] = cost1[i] + cost2[i];
                    }
                }
            }
        }

//        for (int i = 0; i < 3; i++) {
//            System.out.print(result[i] + ",");
//        }
//        System.out.println(result[0] + " " + result[1] + " " + result[2]);

        return result;
    }

    private Long getFromManagementBean(String Object, String Attribuite, GraphDatabaseService graphDb) {
        ObjectName objectName = JmxUtils.getObjectName(graphDb, Object);
        Long value = JmxUtils.getAttribute(objectName, Attribuite);
        return value;
    }
}
