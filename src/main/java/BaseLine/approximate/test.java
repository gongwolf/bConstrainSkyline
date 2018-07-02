package BaseLine.approximate;

import BaseLine.BaseMethod1;
import BaseLine.BaseMethod5;
import BaseLine.approximate.mixed.BaseMethod_mixed;
import BaseLine.approximate.mixed.BaseMethod_mixed_index;
import BaseLine.approximate.range.BaseMethod_approx;
import BaseLine.approximate.range.BaseMethod_approx_index;
import BaseLine.approximate.subpath.BaseMethod_subPath;
import RstarTree.Data;
import neo4jTools.connector;
import org.apache.commons.cli.*;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;

public class test {
    public static void main(String args[]) throws ParseException {
        test t = new test();
        int graph_size, query_num, hotels_num;
        String degree;
        double range;
        String city;

        Options options = new Options();
        options.addOption("g", "grahpsize", true, "number of nodes in the graph");
        options.addOption("de", "degree", true, "degree of the graphe");
        options.addOption("qn", "querynum", true, "number of querys");
        options.addOption("hn", "hotelsnum", true, "number of hotels in the graph");
        options.addOption("r", "range", true, "range of the distance to be considered");
        options.addOption("h", "help", false, "print the help of this command");
        options.addOption("c", "city", true, "the city name");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        String g_str = cmd.getOptionValue("g");
        String de_str = cmd.getOptionValue("de");
        String qn_str = cmd.getOptionValue("qn");
        String hn_str = cmd.getOptionValue("hn");
        String r_str = cmd.getOptionValue("r");
        String c_str = cmd.getOptionValue("c");


        if (cmd.hasOption("h")) {
            HelpFormatter formatter = new HelpFormatter();
            String header = "Run the code of testing approximate methods 5 :";
            formatter.printHelp("java -jar approx_test.jar", header, options, "", false);
        } else {

            if (g_str == null) {
                graph_size = 2000;
            } else {
                graph_size = Integer.parseInt(g_str);
            }

            if (de_str == null) {
                degree = "4";
            } else {
                degree = de_str;
            }

            if (qn_str == null) {
                query_num = 5;
            } else {
                query_num = Integer.parseInt(qn_str);
            }

            if (hn_str == null) {
                hotels_num = 1000;
            } else {
                hotels_num = Integer.parseInt(hn_str);
            }

            if (r_str == null) {
                range = 12;
            } else {
                range = Integer.parseInt(r_str);
            }


            if (c_str == null) {
                city = "SF";
            } else {
                city = c_str;
            }


//            String home_folder = System.getProperty("user.home");
//            String graph = home_folder + "/neo4j334/testdb_" + city + "_Random/databases/graph.db";
            Data[] queryList = new Data[query_num];
//
//            connector n = new connector(graph);
//            n.startDB();
//            GraphDatabaseService graphdb = n.getDBObject();
//            try (Transaction tx = graphdb.beginTx()) {
//                for (int i = 0; i < query_num; i++) {
//                    BaseMethod5 bm5 = new BaseMethod5(city);
//                    bm5.graphdb = graphdb;
//                    int random_place_id = bm5.getRandomNumberInRange_int(0, bm5.getNumberOfHotels() - 1);
//
//                    Data queryD = bm5.getDataById(random_place_id);
//                    bm5.nearestNetworkNode(queryD);
//                    double distance = bm5.nn_dist;
////                    System.out.println(distance);
//                    while (distance > 0.02115) {
//                        random_place_id = bm5.getRandomNumberInRange_int(0, bm5.getNumberOfHotels() - 1);
//                        queryD = bm5.getDataById(random_place_id);
//                        bm5.nearestNetworkNode(queryD);
//                        distance = bm5.nn_dist;
////                        System.out.println(distance);
//                    }
//                    queryList[i] = queryD;
//                }
//
//                tx.success();
//            }
//
//            n.shutdownDB();

            for (int i = 0; i < query_num; i++) {

                BaseMethod5 bm5 = new BaseMethod5(graph_size, degree, range, hotels_num);
                int random_place_id = bm5.getRandomNumberInRange_int(0, bm5.getNumberOfHotels() - 1);
                Data queryD = bm5.getDataById(random_place_id);
                queryList[i] = queryD;
            }


            /*
             *
             * For new york test generater
             *
             * */

//            int[] random_id = new int[]{8883, 5080, 5120, 5175, 4032, 4090, 5073, 8935, 5140, 9358, 5088, 5159};
//            query_num = random_id.length;
//
//            Data[] queryList = new Data[query_num];
//
//            connector n = new connector(graph);
//            n.startDB();
//            GraphDatabaseService graphdb = n.getDBObject();
//            try (Transaction tx = graphdb.beginTx()) {
//                for (int i = 0; i < query_num; i++) {
//                    BaseMethod5 bm5 = new BaseMethod5(city);
//                    bm5.graphdb = graphdb;
//
//                    Data queryD = bm5.getDataById(random_id[i]);
//                    bm5.nearestNetworkNode(queryD);
//
//
//                    queryList[i] = queryD;
//                }
//
//                tx.success();
//            }
//
//            n.shutdownDB();

            for (Data d : queryList) {
                t.testing1(graph_size, degree, range, hotels_num, d);

//                t.test_real(d, city);
//                System.out.println("===============================================");
            }
        }

    }


    public void testing(int graph_size, String degree, double range, int hotels_num, Data queryD) {


        BaseMethod5 bm5 = new BaseMethod5(graph_size, degree, range, hotels_num);
        BaseMethod_approx bs_approx = new BaseMethod_approx(graph_size, degree, range, range, hotels_num);
        BaseMethod_approx_index bs_approx_index = new BaseMethod_approx_index(graph_size, degree, range, range, hotels_num);

        BaseMethod_subPath bs_sub = new BaseMethod_subPath(graph_size, degree, range, range, hotels_num);
        BaseMethod_mixed bs_mix = new BaseMethod_mixed(graph_size, degree, range, range, hotels_num);
        BaseMethod_mixed_index bs_mix_index = new BaseMethod_mixed_index(graph_size, degree, range, range, hotels_num);

        bm5.baseline(queryD);
        bs_approx.baseline(queryD);
        bs_approx_index.baseline(queryD);
        bs_sub.baseline(queryD);
        bs_mix.baseline(queryD);
        bs_mix_index.baseline(queryD);


        System.out.print(testTools.statistic.goodnessAnalyze(bm5.skyPaths, bs_approx.skyPaths, "cos"));
        System.out.print(" " + testTools.statistic.goodnessAnalyze(bm5.skyPaths, bs_approx.skyPaths, "cos", 10));
        System.out.println(" " + testTools.statistic.goodnessAnalyze(bm5.skyPaths, bs_approx.skyPaths, "cos", 100));


        System.out.print(testTools.statistic.goodnessAnalyze(bm5.skyPaths, bs_sub.skyPaths, "cos"));
        System.out.print(" " + testTools.statistic.goodnessAnalyze(bm5.skyPaths, bs_sub.skyPaths, "cos", 10));
        System.out.println(" " + testTools.statistic.goodnessAnalyze(bm5.skyPaths, bs_sub.skyPaths, "cos", 100));


        System.out.print(testTools.statistic.goodnessAnalyze(bm5.skyPaths, bs_mix.skyPaths, "cos"));
        System.out.print(" " + testTools.statistic.goodnessAnalyze(bm5.skyPaths, bs_mix.skyPaths, "cos", 10));
        System.out.println(" " + testTools.statistic.goodnessAnalyze(bm5.skyPaths, bs_mix.skyPaths, "cos", 100));
//        testTools.statistic.goodnessAnalyze(bm5.skyPaths, bs_approx.skyPaths, "edu");
////        testTools.statistic.goodnessAnalyze(bm5.skyPaths, bs_approx.skyPaths, "cos");
////        testTools.statistic.goodnessAnalyze(bm5.skyPaths, bs_approx_index.skyPaths, "edu");
//        testTools.statistic.goodnessAnalyze(bm5.skyPaths, bs_approx_index.skyPaths, "cos");
//        testTools.statistic.goodnessAnalyze(bm5.skyPaths, bs_approx_index.skyPaths, "cos",10);
////        testTools.statistic.goodnessAnalyze(bm5.skyPaths, bs_approx_index.skyPaths, "cos");
////        testTools.statistic.goodnessAnalyze(bm5.skyPaths, bs_sub.skyPaths, "edu");
//        testTools.statistic.goodnessAnalyze(bm5.skyPaths, bs_sub.skyPaths, "cos");
////        testTools.statistic.goodnessAnalyze(bm5.skyPaths, bs_mix.skyPaths, "edu");
////        testTools.statistic.goodnessAnalyze(bm5.skyPaths, bs_mix.skyPaths, "cos");
////        testTools.statistic.goodnessAnalyze(bm5.skyPaths, bs_mix_index.skyPaths, "edu");
//        testTools.statistic.goodnessAnalyze(bm5.skyPaths, bs_mix_index.skyPaths, "cos");
////        testTools.statistic.goodnessAnalyze(bm5.skyPaths, bm5.skyPaths, "edu");
////        testTools.statistic.goodnessAnalyze(bm5.skyPaths, bm5.skyPaths, "cos");
        System.out.println("=================================");

    }



    public void testing1(int graph_size, String degree, double range, int hotels_num, Data queryD) {


        BaseMethod5 bm5 = new BaseMethod5(graph_size, degree, range, hotels_num);
        BaseMethod1 bm1 = new BaseMethod1(graph_size, degree, range, range, hotels_num);


        bm1.baseline(queryD);
        bm5.baseline(queryD);


        System.out.println("=================================");

    }

    public void test_real(Data queryD, String city) {
//        System.out.println(queryD);
        String home_folder = System.getProperty("user.home");
        String graph = home_folder + "/neo4j334/testdb_" + city + "_Random/databases/graph.db";
        String tree = home_folder + "/shared_git/bConstrainSkyline/data/real_tree_" + city + ".rtr";
        String data = home_folder + "/shared_git/bConstrainSkyline/data/staticNode_real_" + city + ".txt";


//        if (!city.equals("LA")) {
//            BaseMethod1 bm1 = new BaseMethod1(city);
//            bm1.baseline(queryD);
//        }
        BaseMethod5 bm5 = new BaseMethod5(city);
        bm5.baseline(queryD);


        BaseMethod_approx bs_range = new BaseMethod_approx(tree, data, graph, 0.02115);
        BaseMethod_approx_index bs_range_indexed = new BaseMethod_approx_index(tree, data, graph, 0.02115);
        bs_range.baseline(queryD);
        bs_range_indexed.baseline(queryD);


        BaseMethod_subPath bs_sub = new BaseMethod_subPath(tree, data, graph);
        bs_sub.baseline(queryD);

        BaseMethod_mixed bs_mix = new BaseMethod_mixed(tree, data, graph, 0.02115);
        BaseMethod_mixed_index bs_mix_indexed = new BaseMethod_mixed_index(tree, data, graph, 0.02115);
        bs_mix.baseline(queryD);
        bs_mix_indexed.baseline(queryD);

        System.out.print(testTools.statistic.goodnessAnalyze(bm5.skyPaths, bs_range.skyPaths, "cos"));
        System.out.print(" " + testTools.statistic.goodnessAnalyze(bm5.skyPaths, bs_range.skyPaths, "cos", 10));
        System.out.print(" " + testTools.statistic.goodnessAnalyze(bm5.skyPaths, bs_range.skyPaths, "cos", 20));
        System.out.print(" " + testTools.statistic.goodnessAnalyze(bm5.skyPaths, bs_range.skyPaths, "cos", 50));
        System.out.println(" " + testTools.statistic.goodnessAnalyze(bm5.skyPaths, bs_range.skyPaths, "cos", 100));


        System.out.print(testTools.statistic.goodnessAnalyze(bm5.skyPaths, bs_sub.skyPaths, "cos"));
        System.out.print(" " + testTools.statistic.goodnessAnalyze(bm5.skyPaths, bs_sub.skyPaths, "cos", 10));
        System.out.print(" " + testTools.statistic.goodnessAnalyze(bm5.skyPaths, bs_sub.skyPaths, "cos", 20));
        System.out.print(" " + testTools.statistic.goodnessAnalyze(bm5.skyPaths, bs_sub.skyPaths, "cos", 50));
        System.out.println(" " + testTools.statistic.goodnessAnalyze(bm5.skyPaths, bs_sub.skyPaths, "cos", 100));

        System.out.print(testTools.statistic.goodnessAnalyze(bm5.skyPaths, bs_mix.skyPaths, "cos"));
        System.out.print(" " + testTools.statistic.goodnessAnalyze(bm5.skyPaths, bs_mix.skyPaths, "cos", 10));
        System.out.print(" " + testTools.statistic.goodnessAnalyze(bm5.skyPaths, bs_mix.skyPaths, "cos", 20));
        System.out.print(" " + testTools.statistic.goodnessAnalyze(bm5.skyPaths, bs_mix.skyPaths, "cos", 50));
        System.out.println(" " + testTools.statistic.goodnessAnalyze(bm5.skyPaths, bs_mix.skyPaths, "cos", 100));

//        BaseMethod_subPath bs_sub = new BaseMethod_subPath(tree, data, graph,1000);
//        bs_sub.baseline(queryD);
//        BaseMethod_mixed bs_mix = new BaseMethod_mixed(tree, data, graph, 0.01);
//        bs_mix.baseline(queryD);


    }
}
