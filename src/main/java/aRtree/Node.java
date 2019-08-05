package aRtree;

import java.io.IOException;

public interface Node {
    public abstract int get_num_of_data();  // returns number of data entries
    public abstract Data get(int i);      // returns the i-th object in the
    public abstract void read_from_buffer(byte buffer[]) throws IOException; // reads data from buffer
    public abstract void write_to_buffer(byte buffer[]) throws IOException; // writes data to buffer
    public abstract boolean is_data_node();           // returns TRUE, if "this" is RTDataNode
    public abstract float[] get_mbr();       // returns mbr enclosing whole page
    public abstract void nodes(int nodes_a[]);
    public abstract int insert(Data d, aRTNode sn[]);
    public abstract void delete();
    public abstract float[] getAttr_lower();
    public abstract float[] getAttr_upper();
    public abstract void print(String prefix, int times);
}
