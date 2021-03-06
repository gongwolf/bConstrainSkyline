package aRtree;

////////////////////////////////////////////////////////////////////////
// DirEntry
////////////////////////////////////////////////////////////////////////

/**
 * DirEntry implements the entries of a directory node (RTDirNode)
 * <p>
 * the info of the DirEntry in a RTDirNode block is organised as follows:
 * +-------------+-----+------------------------+-----+-------------+
 * | bounces[0]  | ... | bounces[2*dimension-1] | son | num_of_data |
 * +-------------+-----+------------------------+-----+-------------+
 */


import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class DirEntry {

    public aRTNode son_ptr;                     // pointer to son if in main mem.
    aRTree my_tree;                      // pointer to my R-tree
    int son;                            // block # of son
    boolean son_is_data;                // TRUE, if son is a data page
    int dimension;                      // dimension of the box
    float bounces[];                    // the mbr of the box
    int son_level;                      // level of the node pointed to
    int num_of_data;                    // amount of data entries behind the son of this entry

    float attr_lower[];
    float attr_upper[];

    DirEntry(int _dimension, boolean son_is_data, aRTree rt) {
        dimension = _dimension;
        this.son_is_data = son_is_data;
        my_tree = rt;
        bounces = new float[2 * dimension];
        son_ptr = null;
        num_of_data = 0;
        attr_lower = new float[Constants.attrs_length];
        attr_upper = new float[Constants.attrs_length];
    }

    /**
     * copy the contents of this to another Direntry object
     */
    void copyTo(DirEntry target) {
        target.dimension = dimension;
        target.son = son;
        target.son_ptr = son_ptr;
        target.son_level = son_level;
        target.son_is_data = son_is_data;
        target.num_of_data = num_of_data;
        System.arraycopy(bounces, 0, target.bounces, 0, 2 * dimension);
    }

    /**
     * Checks if point v is inside the entry's MBR
     */
    boolean is_inside(float v[]) {
        int i;

        for (i = 0; i < dimension; i++) {
            if (v[i] <= bounces[2 * i]
                    || // upper limit
                    v[i] >= bounces[2 * i + 1]) // lower limit
            {
                return false;
            }
        }
        return true;
    }

    /**
     * Tests if the parameter mbr is inside or overlaps the MBR of the entry
     */
    int section(float mbr[]) {
        boolean inside;
        boolean overlap;
        int i;

        overlap = true;
        inside = true;
        for (i = 0; i < dimension; i++) {
            if (mbr[2 * i] >= bounces[2 * i + 1]
                    || mbr[2 * i + 1] <= bounces[2 * i]) {
                overlap = false;
            }
            if (mbr[2 * i] <= bounces[2 * i]
                    || mbr[2 * i + 1] >= bounces[2 * i + 1]) {
                inside = false;
            }
        }
        if (inside) {
            return Constants.INSIDE;
        } else if (overlap) {
            return Constants.OVERLAP;
        } else {
            return Constants.S_NONE;
        }
    }

    /**
     * reads from the input stream the object's info used by
     * RTDirNode.read_from_buffer()
     */
    public void read_from_buffer(DataInputStream in) throws IOException {
        for (int i = 0; i < 2 * dimension; ++i) {
            bounces[i] = in.readFloat();
        }
        son = in.readInt();
        num_of_data = in.readInt();

        for (int i = 0; i < Constants.attrs_length; i++) {
            this.attr_lower[i] = in.readFloat();
        }

        for (int i = 0; i < Constants.attrs_length; i++) {
            this.attr_upper[i] = in.readFloat();
        }
    }

    /**
     * writes to the output stream the object's info used by
     * RTDirNode.write_to_buffer()
     */
    public void write_to_buffer(DataOutputStream out) throws IOException {
        for (int i = 0; i < 2 * dimension; ++i) {
            out.writeFloat(bounces[i]);
        }
        out.writeInt(son);
        out.writeInt(num_of_data);

        for (int i = 0; i < Constants.attrs_length; i++) {
            out.writeFloat(this.attr_lower[i]);
        }

        for (int i = 0; i < Constants.attrs_length; i++) {
            out.writeFloat(this.attr_upper[i]);
        }
    }

    int get_size() {
        return 2 * dimension * Constants.SIZEOF_FLOAT // mbr
                + Constants.SIZEOF_INT // block id
                + Constants.SIZEOF_INT // num of data
                + 2 * Constants.attrs_length * Constants.SIZEOF_FLOAT ;//upper and lower

    }

    /**
     * returns the son_ptr (the node this entry points to if the node is not in
     * main memory, it it read from disk (see RTDirNode/RTDataNode constructor)
     */
    public aRTNode get_son() {
        if (son_ptr == null) {
            if (son_is_data) {
                son_ptr = new aRTDataNode(my_tree, son);
            } else {
                son_ptr = new aRTDirNode(my_tree, son);
            }
        }
        return son_ptr;
    }

    public void delete() {
        if (son_ptr != null) {
            ((Node) son_ptr).delete();
        }
    }
}
