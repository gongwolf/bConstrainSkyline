package testTools;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class analyzeResult {
    public static void main(String args[]) {
        analyzeResult ar = new analyzeResult();
        ar.anaylze_exp5();

    }

    private void anaylze_exp5() {
        String folder_path = "/home/gqxwolf/shared_git/bConstrainSkyline/target/output/complementary/exp6";
        File base_f = new File(folder_path);
        int divider = 11;

        for (File output_f : base_f.listFiles()) {
//            System.out.println(output_f.getName());
            try {
                BufferedReader b = new BufferedReader(new FileReader(output_f));
                String readLine = "";
                int counter = 1;


                //running time
                double rt_imporved = 0;
                double rt_range = 0;
                double rt_range_index = 0;
                double rt_path = 0;
                double rt_mix = 0;
                double rt_mix_index = 0;

                //visited number
                long v_imporved = 0;
                long v_range = 0;
                long v_range_index = 0;
                long v_path = 0;
                long v_mix = 0;
                long v_mix_index = 0;

                long f_imporved = 0;
                long f_range = 0;
                long f_range_index = 0;
                long f_path = 0;
                long f_mix = 0;
                long f_mix_index = 0;

                //skyline checking
                long sk_imporved = 0;
                long sk_range = 0;
                long sk_range_index = 0;
                long sk_path = 0;
                long sk_mix = 0;
                long sk_mix_index = 0;

                //edu similar score
                double f_t_range_edu = 0;
                double range_t_f_edu = 0;
                double f_t_path_edu = 0;
                double path_t_f_edu = 0;
                double f_t_mix_edu = 0;
                double mix_t_f_edu = 0;


                //edu similar score
                double range_cos =0;
                double range_cos_10 = 0;
                double range_cos_100 =0;

                double sub_cos =0;
                double sub_cos_10 = 0;
                double sub_cos_100 =0;

                double mix_cos =0;
                double mix_cos_10 = 0;
                double mix_cos_100 =0;


                int query_num = 0;

                while ((readLine = b.readLine()) != null) {
//                    System.out.println(readLine);
                    if (counter % divider == 1) {
                        String[] split_info = readLine.split(" ");
                        try {
                            rt_imporved += (double) Long.parseLong(readLine.split("\\|")[1]) / 1000;
                        }catch (Exception e)
                        {
                            System.out.println(readLine);
                        }
//                        f_imporved += Long.parseLong(split_info[split_info.length - 5].split(",")[0]);
//                        v_imporved += Long.parseLong(split_info[split_info.length - 5].split(",")[1]);
                        sk_imporved += Long.parseLong(split_info[split_info.length - 3]);
//                        System.out.println((double) Long.parseLong(readLine.split("\\|")[1]) / 1000 + " " + split_info[split_info.length - 5] + " " + split_info[split_info.length - 2]);
                    } else if (counter % divider == 2) {
                        String[] split_info = readLine.split(" ");
                        rt_range += (double) Long.parseLong(readLine.split("\\|")[1]) / 1000;
//                        f_range += Long.parseLong(split_info[split_info.length - 4].split(",")[0]);
//                        v_range += Long.parseLong(split_info[split_info.length - 4].split(",")[1]);
                        sk_range += Long.parseLong(split_info[split_info.length - 1]);
//                        System.out.println((double) Long.parseLong(readLine.split("\\|")[1]) / 1000 + " " + split_info[split_info.length - 4] + " " + split_info[split_info.length - 1]);
                    } else if (counter % divider == 3) {
                        String[] split_info = readLine.split(" ");
                        rt_range_index += (double) Long.parseLong(readLine.split("\\|")[1]) / 1000;
//                        f_range_index += Long.parseLong(split_info[split_info.length - 4].split(",")[0]);
//                        v_range_index += Long.parseLong(split_info[split_info.length - 4].split(",")[1]);
                        sk_range_index += Long.parseLong(split_info[split_info.length - 1]);
//                        System.out.println((double) Long.parseLong(readLine.split("\\|")[1]) / 1000 + " " + split_info[split_info.length - 4] + " " + split_info[split_info.length - 1]);
                    } else if (counter % divider == 4) {
                        String[] split_info = readLine.split(" ");
                        rt_path += (double) Long.parseLong(readLine.split("\\|")[1]) / 1000;
//                        f_path += Long.parseLong(split_info[split_info.length - 4].split(",")[0]);
//                        v_path += Long.parseLong(split_info[split_info.length - 4].split(",")[1]);
                        sk_path += Long.parseLong(split_info[split_info.length - 1]);
//                        System.out.println((double) Long.parseLong(readLine.split("\\|")[1]) / 1000 + " " + split_info[split_info.length - 4] + " " + split_info[split_info.length - 1]);
                    } else if (counter % divider == 5) {
                        String[] split_info = readLine.split(" ");
                        rt_mix += (double) Long.parseLong(readLine.split("\\|")[1]) / 1000;
//                        f_mix += Long.parseLong(split_info[split_info.length - 4].split(",")[0]);
//                        v_mix += Long.parseLong(split_info[split_info.length - 4].split(",")[1]);
                        sk_mix += Long.parseLong(split_info[split_info.length - 1]);
//                        System.out.println((double) Long.parseLong(readLine.split("\\|")[1]) / 1000 + " " + split_info[split_info.length - 4] + " " + split_info[split_info.length - 1]);
                    } else if (counter % divider == 6) {
                        String[] split_info = readLine.split(" ");
                        rt_mix_index += (double) Long.parseLong(readLine.split("\\|")[1]) / 1000;
//                        f_mix_index += Long.parseLong(split_info[split_info.length - 4].split(",")[0]);
//                        v_mix_index += Long.parseLong(split_info[split_info.length - 4].split(",")[1]);
                        sk_mix_index += Long.parseLong(split_info[split_info.length - 1]);
//                        System.out.println((double) Long.parseLong(readLine.split("\\|")[1]) / 1000 + " " + split_info[split_info.length - 4] + " " + split_info[split_info.length - 1]);
                    } else if (counter % divider == 7) {
                        range_cos += Double.parseDouble(readLine.split(" ")[0]);
                        range_cos_10 += Double.parseDouble(readLine.split(" ")[1]);
                        range_cos_100 += Double.parseDouble(readLine.split(" ")[2]);
//                        System.out.println(readLine.split(" ")[6] + " " + readLine.split(" ")[18]);
                    } else if (counter % divider == 8) {
                        sub_cos += Double.parseDouble(readLine.split(" ")[0]);
                        sub_cos_10 += Double.parseDouble(readLine.split(" ")[1]);
                        sub_cos_100 += Double.parseDouble(readLine.split(" ")[2]);
                    } else if (counter % divider == 9) {
                        mix_cos += Double.parseDouble(readLine.split(" ")[0]);
                        mix_cos_10 += Double.parseDouble(readLine.split(" ")[1]);
                        mix_cos_100 += Double.parseDouble(readLine.split(" ")[2]);
                    } else if (counter % divider == 10) {
                        query_num++;
                    }
                    counter++;
                }

//                System.out.println(mix_cos+" "+query_num);

                StringBuffer sb = new StringBuffer();
                //graph_size, degree, hotels_num
                sb.append(output_f.getName().split("_")[1] + " " + output_f.getName().split("_")[2] + " " + output_f.getName().split("_")[3]+" "+query_num +"|");
                //running time in second
                sb.append(" " + String.format("%.2f", rt_imporved / query_num) + " " + String.format("%.2f", rt_range / query_num) + " " + String.format("%.2f", rt_path / query_num)
                        + " " + String.format("%.2f", rt_mix / query_num) + " " + String.format("%.2f", rt_range_index / query_num) + " " + String.format("%.2f", rt_mix_index / query_num) +"|");
                //speed up ratio
                sb.append(String.format("%.2f", rt_imporved / rt_range) + " " + String.format("%.2f", rt_imporved / rt_path) + " " + String.format("%.2f", rt_imporved / rt_mix)
                        + " " + String.format("%.2f", rt_imporved / rt_range_index) + " " + String.format("%.2f", rt_imporved / rt_mix_index)  +"|");
                //skyline checking
                sb.append(sk_imporved / query_num + " " + sk_range / query_num + " " + sk_path / query_num + " " + sk_mix / query_num  +"|");

                //
                sb.append(String.format("%.4f", range_cos/query_num)+" "+String.format("%.4f", range_cos_10/query_num)+" "+String.format("%.4f", range_cos_100/query_num)+"|");
                sb.append(String.format("%.4f", sub_cos/query_num)+" "+String.format("%.4f", sub_cos_10/query_num)+" "+String.format("%.4f", sub_cos_100/query_num)+"|");
                sb.append(String.format("%.4f", mix_cos/query_num)+" "+String.format("%.4f", mix_cos_10/query_num)+" "+String.format("%.4f", mix_cos_100/query_num));
//                sb.append(String.format("%.6f", f_t_range_edu / query_num) + " " + String.format("%.6f", f_t_path_edu / query_num) + " " + String.format("%.6f", f_t_mix_edu / query_num) + " ");
//                sb.append(String.format("%.6f", range_t_f_edu / query_num) + " " + String.format("%.6f", path_t_f_edu / query_num) + " " + String.format("%.6f", mix_t_f_edu / query_num) + " ");
                System.out.println(sb);


            } catch (IOException e) {
                e.printStackTrace();
            }

//            break;
        }
    }
}
