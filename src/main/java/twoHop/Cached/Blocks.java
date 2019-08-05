package twoHop.Cached;


import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

public class Blocks {
    final int toIndex_type = 1;
    final int fromIndex_type = 2;

    RandomAccessFile to_header, from_header;
    RandomAccessFile to_index, from_index;
    int records_in_page;
    private int pagesize = 2048;
    public long tosize,fromsize;

    public Blocks(int graphSize, int degree, int pagesize) {
        this.pagesize = pagesize * 1024;
        records_in_page = this.pagesize / 28;
        String index_path = "/home/gqxwolf/mydata/projectData/testGraph" + graphSize + "_" + degree + "/data/twoHop/binary";
        try {
            to_header = new RandomAccessFile(index_path + "/" + graphSize + "_" + degree + "_to.header", "r");
            from_header = new RandomAccessFile(index_path + "/" + graphSize + "_" + degree + "_From.header", "r");

            to_index = new RandomAccessFile(index_path + "/" + graphSize + "_" + degree + "_to.idx", "r");
            from_index = new RandomAccessFile(index_path + "/" + graphSize + "_" + degree + "_From.idx", "r");
            tosize = to_index.length();
            fromsize = from_index.length();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public int neededPage(int node_id, int header_type) {
        long seek_position = node_id * 8;
        try {
            if (header_type == toIndex_type) {

                to_header.seek(seek_position);
                int node_idx_record_num = to_header.readInt();
                return (int) Math.ceil(node_idx_record_num * 1.0 / records_in_page);

            } else if (header_type == fromIndex_type) {
                from_header.seek(seek_position);
                int node_idx_record_num = from_header.readInt();
                return (int) Math.ceil(node_idx_record_num * 1.0 / records_in_page);

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return -1;
    }


    public int getStartPage(int node_id, int Index_type) {
        long seek_position = node_id * 8 + 4;
        try {
            if (Index_type == toIndex_type) {

                to_header.seek(seek_position);
                int start_page_num = to_header.readInt();
                return start_page_num;

            } else if (Index_type == fromIndex_type) {
                from_header.seek(seek_position);
                int start_page_num = from_header.readInt();
                return start_page_num;

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return -1;
    }

    public byte[] read_block_from_disk(long seek_pos, int Index_type) {
        byte[] data = new byte[this.pagesize];
        try {
            if (Index_type == toIndex_type) {
                to_index.seek(seek_pos);
                to_index.readFully(data);
            } else if (Index_type == fromIndex_type) {
                from_index.seek(seek_pos);
                from_index.readFully(data);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return data;

    }
}
