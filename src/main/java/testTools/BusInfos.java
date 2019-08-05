package testTools;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class BusInfos {
    String path = "/home/gqxwolf/mydata/projectData/ConstrainSkyline/data/backup/data/Bus_data/output.txt";

    public static void main(String args[]) {
        BusInfos bi = new BusInfos();
        bi.readAreaId();

    }


    public void readAreaId() {
        try (BufferedReader br = new BufferedReader(new FileReader(this.path))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (!line.startsWith(" ")) {
                    System.out.println(line);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }


    }
}
