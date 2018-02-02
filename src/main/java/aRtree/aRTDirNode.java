package aRtree;

import java.io.*;

public final class aRTDirNode extends aRTNode implements Node {

    public DirEntry entries[];            // array of entries in the directory
    public boolean son_is_data;           // true, if son is a data page


    public aRTDirNode(aRTree rt) // create a brand new directory node
    // zugehoeriger Plattenblock muss erst erzeugt werden
    {
        super(rt);
        byte b[];
        int header_size;
        DirEntry d;

        // Al briefly create a data entry and look how big that is ..
        d = new DirEntry(dimension, son_is_data, rt);

        header_size = Constants.SIZEOF_BOOLEAN //son_is_data
                + Constants.SIZEOF_SHORT //level
                + Constants.SIZEOF_INT;  //num_entries
        capacity = (rt.file.get_blocklength() - header_size) / d.get_size();

        entries = new DirEntry[capacity];

        // initialize block for this node
        b = new byte[rt.file.get_blocklength()];
        // append block to the rtree's blockfile
        try {
            block = rt.file.append_block(b);
        } catch (IOException e) {
            Constants.error("RTDirnode creation: error in block appending", true);
        }

        rt.num_of_inodes++;

        // If removed from memory, this node has to be written back to disk
        dirty = true;
    }

    public aRTDirNode(aRTree rt, int _block) {
        super(rt);

        byte b[];
        int header_size;
        DirEntry d;

        // Times briefly create a data entry and look how big the ..
        d = new DirEntry(dimension, son_is_data, rt);

        header_size = Constants.SIZEOF_BOOLEAN //son_is_data
                + Constants.SIZEOF_SHORT //level
                + Constants.SIZEOF_INT;  //num_entries
        capacity = (rt.file.get_blocklength() - header_size) / d.get_size();

        entries = new DirEntry[capacity];

        block = _block;
        b = new byte[rt.file.get_blocklength()];


        try {
            rt.file.read_block(b, block);
            read_from_buffer(b);
        } catch (IOException e) {
            Constants.error("RTDirnode initialization: error in block reading", true);
        }

        dirty = false;
    }

    public void read_from_buffer(byte buffer[]) throws IOException {
        ByteArrayInputStream byte_in = new ByteArrayInputStream(buffer);
        DataInputStream in = new DataInputStream(byte_in);

        // read header info
        son_is_data = in.readBoolean();
        level = in.readShort();
        num_entries = in.readInt();

        //System.out.println("RTDirNode.read_from_buffer(): num_entries =" +num_entries);
        // read directory entries
        for (int i = 0; i < num_entries; i++) {
            entries[i] = new DirEntry(dimension, son_is_data, my_tree);
            entries[i].read_from_buffer(in);
            entries[i].son_is_data = son_is_data;
        }

        in.close();
        byte_in.close();
    }

    public void delete() {
        if (dirty) {
            byte b[] = new byte[my_tree.file.get_blocklength()];
            try {
                write_to_buffer(b);
                my_tree.file.write_block(b, block);
            } catch (IOException e) {
                Constants.error("RTDirNode delete: Error in writing block", true);
            }
        }
        for (int i = 0; i < num_entries; i++) {
            entries[i].delete();
        }
    }

    public void write_to_buffer(byte buffer[]) throws IOException {
        ByteArrayOutputStream byte_out = new ByteArrayOutputStream(buffer.length);
        DataOutputStream out = new DataOutputStream(byte_out);

        // write header info
        out.writeBoolean(son_is_data);
        out.writeShort(level);
        out.writeInt(num_entries);

        // write directory entries
        for (int i = 0; i < num_entries; i++) {
            entries[i].write_to_buffer(out);
        }

        byte[] bytes = byte_out.toByteArray();

        int bl = bytes.length;
        for (int i = 0; i < bytes.length; ++i) {
            buffer[i] = bytes[i];
        }

        out.close();
        byte_out.close();
    }
}