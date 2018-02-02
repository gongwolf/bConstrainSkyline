package aRtree;

import java.io.IOException;

public interface Node {
    public abstract int get_num_of_data();  // returns number of data entries

    //    // behind that node
    public abstract Data get(int i);      // returns the i-th object in the
    // tree lying behind that node

    public abstract void read_from_buffer(byte buffer[]) throws IOException; // reads data from buffer

    public abstract void write_to_buffer(byte buffer[]) throws IOException; // writes data to buffer

    public abstract boolean is_data_node();           // returns TRUE, if "this" is RTDataNode

    public abstract float[] get_mbr();       // returns mbr enclosing whole page

//    public abstract void print();                // prints rectangles


    //public abstract void overlapping(float p[], int nodes_t[]);

    public abstract void nodes(int nodes_a[]);


    public abstract int insert(Data d, aRTNode sn[]);
    // inserts d recursively, if there
    // occurs a split, FALSE will be
    // returned and in sn a
    // pointer to the new node

    public abstract void delete();
}
