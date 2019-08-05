package aRtree;


import java.io.*;

public class aRTree {

    public aRTNode root_ptr;       // root-node
    int root;                     // block # of root node
    boolean root_is_data;         // true, if root is a data page
    int dimension;                // dimension of the data's

    int num_of_data;              // # of stored data
    int num_of_dnodes;            // # of stored data pages
    int num_of_inodes;            // # of stored directory pages


    boolean re_level[];           // if re_level[i] is true,
    LinList re_data_cands = new LinList();

    float node_weight[];
    CachedBlockFile file;
    byte header[];

    public aRTree(String fname, int _b_length, int cache_size, int _dimension) // neuen R-Baum konstruieren
    {
        node_weight = new float[20];

        try {
            //create blokc file
            file = new CachedBlockFile(fname, _b_length, cache_size);
        } catch (IOException e) {
            Constants.error("RTree creation: error in block file initialization", true);
        }

        // allocate and read header
        header = new byte[file.get_blocklength()];


        // this is nonsense in itself, but that's the variable
        // set user_header correctly so that when writing the header the
        // caller has no problem
        try {
            read_header(header);
        } catch (IOException e) {
            e.printStackTrace();
        }

        //because the tree is just creating, here, we initialize these parameter
        dimension = _dimension;
        root = 0;
        root_ptr = null;
        root_is_data = true;
        num_of_data = num_of_inodes = num_of_dnodes = 0;

        root_ptr = new aRTDataNode(this);
        System.out.println(root_ptr);
        root = root_ptr.block;
    }


    public aRTree(String fname, int cache_size) // bereits vorhandenen R-Baum laden
    {
        node_weight = new float[20];

        try {
            file = new CachedBlockFile(fname, 0, cache_size);
        } catch (IOException e) {
            Constants.error("RTree reading: error in block file initialization", true);
        }

        // Header allocate and read
        header = new byte[file.get_blocklength()];
        try {
            file.read_header(header);
        } catch (IOException e) {
            Constants.error("RTree header reading: error in block file initialization", true);
        }

        try {
            read_header(header);
        } catch (Exception e) {
        }

        root_ptr = null;

        if (get_num() == 0) // Baum war leer -> Datenknoten anlegen und d einfuegen
        {
//            System.out.println("aaaaaaa");
            root_ptr = new aRTDataNode(this);
            root = root_ptr.block;
            root_ptr.level = 0;
        } else {
            load_root();
        }
    }


    //Read the tree information from the header block
    void read_header(byte buffer[]) throws IOException {
        ByteArrayInputStream byte_in = new ByteArrayInputStream(buffer);
        DataInputStream in = new DataInputStream(byte_in);
        dimension = in.readInt();
        num_of_data = in.readInt();
        num_of_dnodes = in.readInt();
        num_of_inodes = in.readInt();
        root_is_data = in.readBoolean();
        root = in.readInt();
        in.close();
        byte_in.close();
    }

    public void insert(Data d) {

        int i, j;                                            // counters
        aRTNode sn[] = new aRTNode[1];            // potential new node when SPLIT takes place
        aRTDirNode nroot_ptr;                    // new root when the root is SPLIT
        int nroot;                                        // block # of nroot_ptr
        DirEntry de;                                    // temp Object used to consruct new root dir entries when SPLIT takes place
        int split_root = Constants.NONE;  // return of root_ptr.insert(d)
        Data d_cand, dc;                            // temp duplicates of d
        float nmbr[],lower[],upper[];

        load_root();

        re_level = new boolean[root_ptr.level + 1];
        for (i = 0; i <= root_ptr.level; i++) {
            re_level[i] = false;
        }

        dc = (Data) d.clone(); //duplicate the data into dc
        re_data_cands.insert(dc); //insert the datacopy into the
        //list of pending to be inserted data
        j = -1;


        while (re_data_cands.get_num() > 0) {
            d_cand = (Data) re_data_cands.get_first();
            if (d_cand != null) {
                dc = (Data) d_cand.clone();
                re_data_cands.erase();
                split_root = ((Node) root_ptr).insert(dc, sn);
            } else {
                Constants.error("RTree::insert: inconsistent list re_data_cands", true);
            }

//            System.out.println(split_root);
            /*
             * insert has lead to split --> create new root having as sons
             * old root and sn
             */

//            System.out.println(split_root);
            if (split_root == Constants.SPLIT) {

                //Todo: new root is created,so the aggregate value of this new node show be update
                //initialize new root
                nroot_ptr = new aRTDirNode(this);


                System.out.println("new root "+nroot_ptr);

                nroot_ptr.son_is_data = root_is_data;
                nroot_ptr.level = (short) (root_ptr.level + 1);
                nroot = nroot_ptr.block;
                //System.out.println("nroot "+nroot);

                // a new direntry is introduced having as son the old root
                de = new DirEntry(dimension, root_is_data, this);
                nmbr = ((Node) root_ptr).get_mbr();
                Constants.print(nmbr);
                // store the mbr of the root to the direntry
                System.arraycopy(nmbr, 0, de.bounces, 0, 2 * dimension);

                lower = ((Node) root_ptr).getAttr_lower();
                upper = ((Node) root_ptr).getAttr_upper();
                System.arraycopy(lower, 0, de.attr_lower,0, Constants.attrs_length);
                System.arraycopy(upper, 0, de.attr_upper,0, Constants.attrs_length);


                de.son = root_ptr.block;
                de.son_ptr = root_ptr;
                de.son_is_data = root_is_data;
                de.num_of_data = ((Node) root_ptr).get_num_of_data();
                // add de to the new root
                nroot_ptr.enter(de);
                Constants.print(((Node) root_ptr).getAttr_lower());
                Constants.print(((Node) root_ptr).getAttr_upper());
                System.out.println(nroot_ptr);
                System.out.println("-------" + ((Node) root_ptr).get_num_of_data());


                // a new direntry is introduced having as son the brother(split) of the old root
                de = new DirEntry(dimension, root_is_data, this);
//                System.out.println(sn[0].getClass().getName());

                nmbr = ((Node) sn[0]).get_mbr();
                Constants.print(nmbr);
                System.arraycopy(nmbr, 0, de.bounces, 0, 2 * dimension);

                lower = ((Node) sn[0]).getAttr_lower();
                upper = ((Node) sn[0]).getAttr_upper();
                System.arraycopy(lower, 0, de.attr_lower,0, Constants.attrs_length);
                System.arraycopy(upper, 0, de.attr_upper,0, Constants.attrs_length);

                de.son = sn[0].block;
                de.son_ptr = sn[0];
                de.son_is_data = root_is_data;
                de.num_of_data = ((Node) sn[0]).get_num_of_data();
                nroot_ptr.enter(de);
                Constants.print(((Node) sn[0]).getAttr_lower());
                Constants.print(((Node) sn[0]).getAttr_upper());
                System.out.println(nroot_ptr);
                System.out.println("-------" + ((Node) sn[0]).get_num_of_data());

                System.out.println("==============");
                Constants.print(nroot_ptr.getAttr_lower());
                Constants.print(nroot_ptr.getAttr_upper());
                System.out.println(nroot_ptr);


                // replace the root of the tree with the new node
                root = nroot;
                root_ptr = nroot_ptr;

                //System.out.println("New root direntries:");
                //for (int l = 0; l < root_ptr.get_num(); l++)
                //{
                //    for (int k=0; k<2*dimension; k++)
                //        System.out.print(((RTDirNode)root_ptr).entries[l].bounces[k] + " ");
                //    System.out.println(" ");
                //}
                // the new root is a directory node
                root_is_data = false;
            }
            // go to the next data object to be (re)inserted
            j++;
        }

        // increase number of data in the tree after insertion
//        System.out.println(root_ptr);
        num_of_data++;
        System.out.println(num_of_data + " " + num_of_dnodes + " " + num_of_inodes);
    }

    /**
     * Return to nodes_a[] the # of nodes at each level of the tree. -->
     * Propagate to the root node.
     */
    void nodes(int nodes_a[]) {
        load_root();

        ((Node) root_ptr).nodes(nodes_a);
    }


    void print()
    {
        load_root();
        ((Node) root_ptr).print("   ",0);
    }

    void load_root() {
        if (root_ptr == null) {
            if (root_is_data) {
                root_ptr = new aRTDataNode(this, root);
            } else {
                root_ptr = new aRTDirNode(this, root);
            }
        }
    }


    int get_num() // returns # of stored data
    {
        return num_of_data;
    }

    public void delete() {
        try {
            write_header(header);
            file.set_header(header);
        } catch (IOException e) {
            Constants.error("RTree.delete: error in writing header", true);
        }

        if (root_ptr != null) {
            ((Node) root_ptr).delete();
        }
        try {
            file.flush();
        } catch (IOException e) {
            Constants.error("RTree.delete: error in flushing file", true);
        }

        System.out.println("Rtree saved: num_of_data=" + num_of_data
                + " num_of_inodes=" + num_of_inodes
                + " num_of_dnodes=" + num_of_dnodes);
    }

    void write_header(byte buffer[]) throws IOException {
        ByteArrayOutputStream byte_out = new ByteArrayOutputStream(buffer.length);
        DataOutputStream out = new DataOutputStream(byte_out);

        out.writeInt(dimension);
        out.writeInt(num_of_data);
        out.writeInt(num_of_dnodes);
        out.writeInt(num_of_inodes);
        out.writeBoolean(root_is_data);
        out.writeInt(root);
        out.writeInt(dimension);

        //user_header = &buffer[i];
        byte[] bytes = byte_out.toByteArray();
//        System.out.println(bytes.length+"!!!!!!!");
        //copy bytes[] to buffer[]
        for (int i = 0; i < bytes.length; ++i) {
            buffer[i] = bytes[i];
        }

        out.close();
        byte_out.close();
    }

}
