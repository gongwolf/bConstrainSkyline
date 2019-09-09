package TestCase;

import RstarTree.Data;
import neo4jTools.connector;
import testTools.Index;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;

public class IndexTestClass {

    String home_folder = System.getProperty("user.home");


    public static void main(String args[]) {
        IndexTestClass itc = new IndexTestClass();
        itc.findCandidatesObjects("LA");
        itc.findCandidatesObjects("SF");
        itc.findCandidatesObjects("NY");
    }


    public void findCandidatesObjects(String city) {
        HashSet<Integer> object_ids = new HashSet<>();
        Index idx = new Index(city, -1);
        String graph_db_path = home_folder + "/neo4j334/testdb_" + city + "_Random/databases/graph.db";
        connector n = new connector(graph_db_path);
        n.startDB();
        long nodes_num = n.getNumberofNodes();
        for(long nodeid = 0; nodeid < nodes_num;nodeid++){
            ArrayList<Data> dlist = idx.read_d_list_from_disk(nodeid);
            for(Data d : dlist){
                object_ids.add(d.getPlaceId());
            }
        }
        System.out.println(nodes_num+" graph nodes in city "+city +"  and "+object_ids.size()+" number of candidate objects ");
        n.shutdownDB();
    }

    private int getRandomNumberInRange_int(int min, int max) {

        if (min >= max) {
            throw new IllegalArgumentException("max must be greater than min");
        }

        Random r = new Random();
        return r.nextInt((max - min) +1) + min;
    }
}
