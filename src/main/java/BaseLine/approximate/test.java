package BaseLine.approximate;

import BaseLine.BaseMethod1;
import BaseLine.BaseMethod5;
import BaseLine.Result;
import BaseLine.approximate.mixed.BaseMethod_mixed;
import BaseLine.approximate.mixed.BaseMethod_mixed_index;
import BaseLine.approximate.range.BaseMethod_approx;
import BaseLine.approximate.range.BaseMethod_approx_index;
import BaseLine.approximate.subpath.BaseMethod_subPath;
import RstarTree.Data;
import neo4jTools.connector;
import org.apache.commons.cli.*;
import org.neo4j.graphdb.GraphDatabaseService;

import java.lang.reflect.Array;
import java.util.ArrayList;

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
        options.addOption("c", "city", false, "the city name");

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
                query_num = 4;
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

            String home_folder = System.getProperty("user.home");
            String graph = home_folder + "/neo4j334/testdb" + graph_size + "_" + degree + "/databases/graph.db";
//            System.out.println(graph);
            connector n = new connector(graph);
            n.startDB();
            GraphDatabaseService graphdb = n.getDBObject();

            Data[] queryList = new Data[query_num];
            for (int i = 0; i < query_num; i++) {
                BaseMethod5 bm5 = new BaseMethod5(graph_size, degree, range, hotels_num);
                bm5.graphdb = graphdb;
                int random_place_id = bm5.getRandomNumberInRange_int(0, bm5.getNumberOfHotels() - 1);
                Data queryD = bm5.getDataById(random_place_id);
                bm5.nearestNetworkNode(queryD);
                double distance = bm5.nn_dist;
                while (distance > range) {
                    random_place_id = bm5.getRandomNumberInRange_int(0, bm5.getNumberOfHotels() - 1);
                    queryD = bm5.getDataById(random_place_id);
                    bm5.nearestNetworkNode(queryD);
                    distance = bm5.nn_dist;
                }
//                System.out.println(distance+"    "+range);
                queryList[i] = queryD;
            }

            n.shutdownDB();

//            for (Data d : queryList) {
//                t.testing(graph_size, degree, range, hotels_num, d);
//                System.out.println("===============================================");
//            }
//
            NewTesting(queryList,graph_size, degree, range, hotels_num);


            /**
             * Testing of real dataset
             */
//            String home_folder = System.getProperty("user.home");
//            String graph = home_folder + "/neo4j334/testdb_"+city+"/databases/graph.db";
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
//                    while (distance > 0.0105) {
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

//            for (Data d : queryList) {
////                t.testing(graph_size, degree, range, hotels_num, d);
//                t.test_real(d,city);
//                System.out.println("===============================================");
//            }
        }

    }

    private static void NewTesting(Data[] queryList, int graph_size, String degree, double range, int hotels_num) {

        ArrayList<ArrayList<Result>> rs_baseline=new ArrayList<>();
        ArrayList<ArrayList<Result>> rs_range=new ArrayList<>();
        ArrayList<ArrayList<Result>> rs_sub=new ArrayList<>();
        ArrayList<ArrayList<Result>> rs_mix=new ArrayList<>();
        for(Data queryD:queryList)
        {
            BaseMethod5 bm5 = new BaseMethod5(graph_size, degree, range, hotels_num);
            bm5.baseline(queryD);
            rs_baseline.add(new ArrayList<>(bm5.skyPaths));
        }
        System.out.println("===============================================");


        for(Data queryD:queryList)
        {
            BaseMethod_approx bs_approx = new BaseMethod_approx(graph_size, degree, range, range, hotels_num);
            bs_approx.baseline(queryD);
        }
        System.out.println("===============================================");


        for(Data queryD:queryList)
        {
            BaseMethod_approx_index bs_approx_index = new BaseMethod_approx_index(graph_size, degree, range, range, hotels_num);
            bs_approx_index.baseline(queryD);
            rs_range.add(new ArrayList<>(bs_approx_index.skyPaths));
        }
        System.out.println("===============================================");


        for(Data queryD:queryList)
        {
            BaseMethod_subPath bs_sub = new BaseMethod_subPath(graph_size, degree, range, range, hotels_num);
            bs_sub.baseline(queryD);
            rs_sub.add(new ArrayList<>(bs_sub.skyPaths));
        }

        System.out.println("===============================================");

        for(Data queryD:queryList) {
            BaseMethod_mixed bs_mix = new BaseMethod_mixed(graph_size, degree, range, range, hotels_num);
            bs_mix.baseline(queryD);
        }

        System.out.println("===============================================");


        for(Data queryD:queryList)
        {
            BaseMethod_mixed_index bs_mix_index = new BaseMethod_mixed_index(graph_size, degree, range, range, hotels_num);
            bs_mix_index.baseline(queryD);
            rs_mix.add(new ArrayList<>(bs_mix_index.skyPaths));
        }

        System.out.println("===============================================");

        for(int i = 0 ; i < queryList.length;i++)
        {
            System.out.print(testTools.statistic.goodnessAnalyze(rs_baseline.get(i), rs_range.get(i), "cos"));
            System.out.print(" " + testTools.statistic.goodnessAnalyze(rs_baseline.get(i), rs_range.get(i), "cos", 10));
            System.out.println(" " + testTools.statistic.goodnessAnalyze(rs_baseline.get(i), rs_range.get(i), "cos", 100));


            System.out.print(testTools.statistic.goodnessAnalyze(rs_baseline.get(i), rs_sub.get(i), "cos"));
            System.out.print(" " + testTools.statistic.goodnessAnalyze(rs_baseline.get(i), rs_sub.get(i), "cos", 10));
            System.out.println(" " + testTools.statistic.goodnessAnalyze(rs_baseline.get(i), rs_sub.get(i), "cos", 100));


            System.out.print(testTools.statistic.goodnessAnalyze(rs_baseline.get(i), rs_mix.get(i), "cos"));
            System.out.print(" " + testTools.statistic.goodnessAnalyze(rs_baseline.get(i), rs_mix.get(i), "cos", 10));
            System.out.println(" " + testTools.statistic.goodnessAnalyze(rs_baseline.get(i), rs_mix.get(i), "cos", 100));
            System.out.println("----------------------------------------------------");
        }
    }


    public void testing(int graph_size, String degree, double range, int hotels_num, Data queryD) {



        BaseMethod_approx bs_approx = new BaseMethod_approx(graph_size, degree, range, range, hotels_num);
        bs_approx.baseline(queryD);

        BaseMethod_approx_index bs_approx_index = new BaseMethod_approx_index(graph_size, degree, range, range, hotels_num);
        bs_approx_index.baseline(queryD);


        BaseMethod_subPath bs_sub = new BaseMethod_subPath(graph_size, degree, range, range, hotels_num);
        bs_sub.baseline(queryD);

        BaseMethod_mixed bs_mix = new BaseMethod_mixed(graph_size, degree, range, range, hotels_num);
        bs_mix.baseline(queryD);

        BaseMethod_mixed_index bs_mix_index = new BaseMethod_mixed_index(graph_size, degree, range, range, hotels_num);
        bs_mix_index.baseline(queryD);

        BaseMethod5 bm5 = new BaseMethod5(graph_size, degree, range, hotels_num);
        bm5.baseline(queryD);


        System.out.print(testTools.statistic.goodnessAnalyze(bm5.skyPaths, bs_approx.skyPaths, "cos"));
        System.out.print(" " + testTools.statistic.goodnessAnalyze(bm5.skyPaths, bs_approx.skyPaths, "cos", 10));
        System.out.println(" " + testTools.statistic.goodnessAnalyze(bm5.skyPaths, bs_approx.skyPaths, "cos", 100));


        System.out.print(testTools.statistic.goodnessAnalyze(bm5.skyPaths, bs_sub.skyPaths, "cos"));
        System.out.print(" " + testTools.statistic.goodnessAnalyze(bm5.skyPaths, bs_sub.skyPaths, "cos", 10));
        System.out.println(" " + testTools.statistic.goodnessAnalyze(bm5.skyPaths, bs_sub.skyPaths, "cos", 100));


        System.out.print(testTools.statistic.goodnessAnalyze(bm5.skyPaths, bs_mix.skyPaths, "cos"));
        System.out.print(" " + testTools.statistic.goodnessAnalyze(bm5.skyPaths, bs_mix.skyPaths, "cos", 10));
        System.out.println(" " + testTools.statistic.goodnessAnalyze(bm5.skyPaths, bs_mix.skyPaths, "cos", 100));

//          testTools.statistic.goodnessAnalyze(bm5.skyPaths, bs_approx.skyPaths, "edu");
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

    public void test_real(Data queryD, String city) {
//        System.out.println(queryD);
        String home_folder = System.getProperty("user.home");
        String graph = home_folder + "/neo4j334/testdb_" + city + "_Random/databases/graph.db";
        String tree = home_folder + "/shared_git/bConstrainSkyline/data/real_tree_" + city + ".rtr";
        String data = home_folder + "/shared_git/bConstrainSkyline/data/staticNode_real_" + city + ".txt";


        BaseMethod1 bm1 = new BaseMethod1(city);
        BaseMethod5 bm5 = new BaseMethod5(city);
        bm1.baseline(queryD);
        bm5.baseline(queryD);


        BaseMethod_approx bs_range = new BaseMethod_approx(tree, data, graph, 0.0105);
        BaseMethod_approx_index bs_range_indexed = new BaseMethod_approx_index(tree, data, graph, 0.0105);
        bs_range.baseline(queryD);
        bs_range_indexed.baseline(queryD);


        BaseMethod_subPath bs_sub = new BaseMethod_subPath(tree, data, graph);
        bs_sub.baseline(queryD);

        BaseMethod_mixed bs_mix = new BaseMethod_mixed(tree, data, graph, 0.0105);
        BaseMethod_mixed_index bs_mix_indexed = new BaseMethod_mixed_index(tree, data, graph, 0.0105);
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
