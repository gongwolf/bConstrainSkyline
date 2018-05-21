package BaseLine.approximate;

import BaseLine.BaseMethod5;
import BaseLine.Result;
import BaseLine.approximate.mixed.BaseMethod_mixed;
import BaseLine.approximate.mixed.BaseMethod_mixed_index;
import BaseLine.approximate.range.BaseMethod_approx;
import BaseLine.approximate.range.BaseMethod_approx_index;
import BaseLine.approximate.subpath.BaseMethod_subPath;
import RstarTree.Data;
import org.apache.commons.cli.*;

public class test {
    public static void main(String args[]) throws ParseException {
        test t = new test();
        int graph_size, query_num, hotels_num;
        String degree;
        double range;

        Options options = new Options();
        options.addOption("g", "grahpsize", true, "number of nodes in the graph");
        options.addOption("de", "degree", true, "degree of the graphe");
        options.addOption("qn", "querynum", true, "number of querys");
        options.addOption("hn", "hotelsnum", true, "number of hotels in the graph");
        options.addOption("r", "range", true, "range of the distance to be considered");
        options.addOption("h", "help", false, "print the help of this command");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        String g_str = cmd.getOptionValue("g");
        String de_str = cmd.getOptionValue("de");
        String qn_str = cmd.getOptionValue("qn");
        String hn_str = cmd.getOptionValue("hn");
        String r_str = cmd.getOptionValue("r");


        if (cmd.hasOption("h")) {
            HelpFormatter formatter = new HelpFormatter();
            String header = "Run the code of testing approximate methods 5 :";
            formatter.printHelp("java -jar approx_test.jar", header, options, "", false);
        } else {

            if (g_str == null) {
                graph_size = 4000;
            } else {
                graph_size = Integer.parseInt(g_str);
            }

            if (de_str == null) {
                degree = "4";
            } else {
                degree = de_str;
            }

            if (qn_str == null) {
                query_num = 1;
            } else {
                query_num = Integer.parseInt(qn_str);
            }

            if (hn_str == null) {
                hotels_num = 1000;
            } else {
                hotels_num = Integer.parseInt(hn_str);
            }

            if (r_str == null) {
                range = 10;
            } else {
                range = Integer.parseInt(r_str);
            }


            Data[] queryList = new Data[query_num];


            for (int i = 0; i < query_num; i++) {
                BaseMethod5 bm5 = new BaseMethod5(graph_size, degree, range, hotels_num);
                int random_place_id = bm5.getRandomNumberInRange_int(0, hotels_num - 1);
                Data queryD = bm5.getDataById(random_place_id);
                queryList[i] = queryD;
            }

            for (Data d : queryList) {
                t.testing(graph_size, degree, range, hotels_num, d);
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
//        testTools.statistic.goodnessAnalyze(bm5.skyPaths, bs_approx.skyPaths, "edu");
//        testTools.statistic.goodnessAnalyze(bm5.skyPaths, bs_approx.skyPaths, "cos");
        testTools.statistic.goodnessAnalyze(bm5.skyPaths, bs_approx_index.skyPaths, "edu");
        testTools.statistic.goodnessAnalyze(bm5.skyPaths, bs_approx_index.skyPaths, "cos");
        testTools.statistic.goodnessAnalyze(bm5.skyPaths, bs_approx_index.skyPaths, "cos");
        testTools.statistic.goodnessAnalyze(bm5.skyPaths, bs_sub.skyPaths, "edu");
        testTools.statistic.goodnessAnalyze(bm5.skyPaths, bs_sub.skyPaths, "cos");
//        testTools.statistic.goodnessAnalyze(bm5.skyPaths, bs_mix.skyPaths, "edu");
//        testTools.statistic.goodnessAnalyze(bm5.skyPaths, bs_mix.skyPaths, "cos");
        testTools.statistic.goodnessAnalyze(bm5.skyPaths, bs_mix_index.skyPaths, "edu");
        testTools.statistic.goodnessAnalyze(bm5.skyPaths, bs_mix_index.skyPaths, "cos");
//        testTools.statistic.goodnessAnalyze(bm5.skyPaths, bm5.skyPaths, "edu");
//        testTools.statistic.goodnessAnalyze(bm5.skyPaths, bm5.skyPaths, "cos");
        System.out.println("=================================");


    }
}
