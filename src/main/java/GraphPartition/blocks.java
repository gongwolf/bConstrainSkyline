package GraphPartition;

import neo4jTools.StringComparator;
import neo4jTools.connector;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;

import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

public class blocks {
    public TreeMap<String, block> blocks; // block_id -> block

    public blocks() {
        blocks = new TreeMap<>(new StringComparator());
    }

    /**
     * @param numberofLandmard the number of landmarks in each blocks
     */
    public void randomSelectLandMark(int numberofLandmard) {
        for (Map.Entry<String, block> b_obj : blocks.entrySet()) {
            block b = b_obj.getValue();

            for (int i = 0; i < numberofLandmard; i++) {
                String node_id = b.nodes.get(getRandomNumberInRange(0, b.nodes.size() - 1));
                while (b.landMarks.contains(node_id)) {
                    node_id = b.nodes.get(getRandomNumberInRange(0, b.nodes.size() - 1));
                }
                b.landMarks.add(node_id);
            }

        }
    }

    public void buildIndexes() {
        connector n = new connector("/home/gqxwolf/neo4j323/testdb/databases/graph.db");
        n.startDB();
        GraphDatabaseService graphDB = n.getDBObject();
        try (Transaction tx = graphDB.beginTx()) {
            for (Map.Entry<String, block> b_obj : blocks.entrySet()) {
                block b = b_obj.getValue();
                System.out.println("Build index for partition "+b_obj.getKey());
                b.buildLandmarkIndex(graphDB);
                b.buildInnerSkylineIndex(graphDB);
            }
            tx.success();
        }
        n.shutdownDB();

    }

    private int getRandomNumberInRange(int min, int max) {

        if (min >= max) {
            throw new IllegalArgumentException("max must be greater than min");
        }

        Random r = new Random();
        return r.nextInt((max - min) + 1) + min;
    }


}