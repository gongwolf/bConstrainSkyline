package twoHop.Cached;

import org.apache.commons.io.FileUtils;

import java.io.*;

public class writeBinaryFile {
    private final int graphSize;
    private final int degree;
    private final int pageSize;
    private final int NumRecordsInPage;
    String path, BinaryLocation;

    public writeBinaryFile(int graphSize, int degree, int pageSize) {

        this.graphSize = graphSize;
        this.degree = degree;
        this.pageSize = pageSize;
        this.NumRecordsInPage = pageSize / 28; //one int, three double, 28=4+8*3

        this.path = "/home/gqxwolf/mydata/projectData/testGraph" + graphSize + "_" + degree + "/data/twoHop/";
        this.BinaryLocation = this.path + "/binary/";
    }


    public static void main(String[] args) {
        int graphSize = 1000;
        int degree = 5;
        int PageSize = 2048;
        writeBinaryFile w = new writeBinaryFile(graphSize, degree, PageSize);
        w.writeToDisk();
    }

    public void writeToDisk() {
        File BFile = new File(this.BinaryLocation);
        try {
            if (BFile.exists()) {
                FileUtils.deleteDirectory(BFile);
            }
            BFile.mkdirs();
        } catch (IOException e) {
            e.printStackTrace();
        }

        String toPath = this.path + "ToIndex/";
        String FromPath = this.path + "FromIndex/";


        String Binary_to_header = this.BinaryLocation + this.graphSize + "_" + this.degree + "_to.header";
        String Binary_From_header = this.BinaryLocation + this.graphSize + "_" + this.degree + "_From.header";

        String Binary_to_idx = this.BinaryLocation + this.graphSize + "_" + this.degree + "_to.idx";
        String Binary_From_idx = this.BinaryLocation + this.graphSize + "_" + this.degree + "_From.idx";

        //if the header and the index file already exist, remove them first.
        if (new File(Binary_to_header).exists()) {
            new File(Binary_to_header).delete();
        }

        if (new File(Binary_From_header).exists()) {
            new File(Binary_From_header).delete();
        }

        if (new File(Binary_to_idx).exists()) {
            new File(Binary_to_idx).delete();
        }

        if (new File(Binary_From_idx).exists()) {
            new File(Binary_From_idx).delete();
        }


        RandomAccessFile idx_to_fp = null, header_to_fp = null;
        RandomAccessFile idx_from_fp = null, header_from_fp = null;

        try {

            int idx_to_pos = 0;
            int idx_to_page_number = 0;
            int idx_to_num_records_in_page = -1;
            int idx_from_pos = 0;
            int idx_from_page_number = 0;
            int idx_from_num_records_in_page = -1;

            idx_to_fp = new RandomAccessFile(Binary_to_idx, "rw");
            header_to_fp = new RandomAccessFile(Binary_to_header, "rw");


            idx_from_fp = new RandomAccessFile(Binary_From_idx, "rw");
            header_from_fp = new RandomAccessFile(Binary_From_header, "rw");

            for (int vi = 0; vi < this.graphSize; vi++) {
                int started_to_page = idx_to_page_number;
                int started_from_page = idx_from_page_number;
                int TolineNum = 0;
                int FromlineNum = 0;
                String To_indexFile = toPath + vi + ".idx";
                String From_indexFile = FromPath + vi + ".idx";

                BufferedReader br = new BufferedReader(new FileReader(To_indexFile));
                String line;
                while ((line = br.readLine()) != null) {
                    TolineNum++;
                    String[] infos = line.split(",");

                    /**if the page is full, write in next page.
                     * page number ++;
                     * current number of records in the new page is 0;
                     * the pointer move to the start of next page
                     **/
                    if ((idx_to_num_records_in_page + 1) >= this.NumRecordsInPage) {
//                        System.out.println(idx_to_page_number+" "+idx_to_fp.getFilePointer());
                        fillWithZero(idx_to_fp, idx_to_page_number);
                        idx_to_num_records_in_page = 0;
                        idx_to_page_number++;
                        idx_to_pos += this.pageSize;
//                        System.out.println(idx_to_page_number+" "+idx_to_fp.getFilePointer());
                    } else {
                        idx_to_num_records_in_page++;
                    }
                    long aa = idx_to_fp.getFilePointer();
                    writeBytesToFile(idx_to_fp, infos);
//                    System.out.println("----"+idx_to_num_records_in_page+" "+idx_to_page_number+" "+idx_to_fp.getFilePointer()+"  "+(idx_to_fp.getFilePointer()-aa));

                }

//                System.out.println(idx_to_page_number+" "+TolineNum+" "+this.NumRecordsInPage);
                fillWithZero(idx_to_fp, idx_to_page_number);

                //after read one node, next node need to be wrote in a new page
                idx_to_page_number++;
                idx_to_num_records_in_page = 0;
                idx_to_pos += this.pageSize;


                WriteToHeader(header_to_fp, TolineNum, started_to_page);


                br = new BufferedReader(new FileReader(From_indexFile));
                line = "";
                while ((line = br.readLine()) != null) {
                    FromlineNum++;
                    String[] infos = line.split(",");

                    /**if the page is full, write in next page.
                     * page number ++;
                     * current number of records in the new page is 0;
                     * the pointer move to the start of next page
                     **/
                    if ((idx_from_num_records_in_page + 1) >= this.NumRecordsInPage) {
                        //System.out.println(idx_from_page_number + " " + idx_from_fp.getFilePointer());
                        fillWithZero(idx_from_fp, idx_from_page_number);
                        idx_from_num_records_in_page = 0;
                        idx_from_page_number++;
                        idx_from_pos += this.pageSize;
                        //System.out.println(idx_from_page_number + " " + idx_from_fp.getFilePointer());

                    } else {
                        idx_from_num_records_in_page++;
                    }
                    writeBytesToFile(idx_from_fp, infos);
                }

                //after read one node, next node need to be wrote in a new page
                fillWithZero(idx_from_fp, idx_from_page_number); // set the rest bytes of the last page to be 0.

                idx_from_page_number++;
                idx_from_num_records_in_page = 0;
                idx_from_pos += this.pageSize;
                WriteToHeader(header_from_fp, FromlineNum, started_from_page);

            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (header_to_fp != null) {
                    header_to_fp.close();
                }

                if (idx_to_fp != null) {
                    idx_to_fp.close();
                }

                if (header_from_fp != null) {
                    header_from_fp.close();
                }

                if (idx_from_fp != null) {
                    idx_from_fp.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    private void WriteToHeader(RandomAccessFile header_to_fp, int lineNum, int started_to_page) {
        try {
            header_to_fp.writeInt(lineNum);
            header_to_fp.writeInt(started_to_page);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void fillWithZero(RandomAccessFile fp, int idx_to_page_number) {
        try {
            long i = fp.getFilePointer();
            for (; i <= (idx_to_page_number + 1) * this.pageSize - 1; i++) {
                fp.writeByte(-2);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private void writeBytesToFile(RandomAccessFile fp, String[] infos) {

        int NodeId = Integer.valueOf(infos[0]);
        double v1 = infos[1].equals("null") ? -1 : Double.valueOf(infos[1]);
        double v2 = infos[2].equals("null") ? -1 : Double.valueOf(infos[2]);
        double v3 = infos[3].equals("null") ? -1 : Double.valueOf(infos[3]);

        try {
            fp.writeInt(NodeId);
            fp.writeDouble(v1);
            fp.writeDouble(v2);
            fp.writeDouble(v3);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
