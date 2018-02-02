package aRtree;

import java.io.*;

public class aRTDataNode extends aRTNode implements Node {
    public Data data[];

    public aRTDataNode(aRTree rt) // create a brand new RTDataNode
    {
        super(rt);

        byte b[];
        int header_size;
        Data d;

        level = 0;

        d = new Data(dimension);

        header_size = Constants.SIZEOF_SHORT //level
                + Constants.SIZEOF_INT; //num_entries

        capacity = (rt.file.get_blocklength() - header_size) / d.get_size();//how many data can be stored in one data node

        data = new Data[capacity];

        // create a new block for the node
        b = new byte[rt.file.get_blocklength()];
        try {
            block = rt.file.append_block(b);
        } catch (IOException e) {
            Constants.error("RTDatanode creation: error in block appending", true);
        }

        rt.num_of_dnodes++;
        // Must be written to disk --> Set dirty bit
        dirty = true;
    }

    // this constructor reads an existing RTDataNode from the disk
    public aRTDataNode(aRTree rt, int _block) {
        super(rt);

        byte b[];
        int header_size;

        Data d;
        d = new Data(dimension);

        header_size = Constants.SIZEOF_SHORT //level
                + Constants.SIZEOF_INT;  //num_entries
        capacity = (rt.file.get_blocklength() - header_size) / d.get_size();
        data = new Data[capacity];
        block = _block;

        try {
            b = new byte[rt.file.get_blocklength()];
            rt.file.read_block(b, block);
            read_from_buffer(b);
        } catch (Exception e) {
            Constants.error("RTDataNode initialization: error in block reading", true);
        }

        // Block block does not have to be written for the time being
        dirty = false;
    }

    public void read_from_buffer(byte buffer[]) throws IOException {
        ByteArrayInputStream byte_in = new ByteArrayInputStream(buffer);
        DataInputStream in = new DataInputStream(byte_in);

        // read header
        level = in.readShort();
        level = 0; // level should be 0 anyway
        num_entries = in.readInt();

        // read data
        for (int i = 0; i < num_entries; i++) {
            data[i] = new Data(dimension);
            data[i].read_from_buffer(in);
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
                Constants.error("RTDataNode delete: Error in writing block", true);
            }
        }
    }

    public void write_to_buffer(byte buffer[]) throws IOException {
        ByteArrayOutputStream byte_out = new ByteArrayOutputStream(buffer.length);
        DataOutputStream out = new DataOutputStream(byte_out);

        // write header
        level = 0;  // level should be 0 anyway
        out.writeShort(level);
        out.writeInt(num_entries);

        // write data
        for (int i = 0; i < num_entries; i++) {
            data[i].write_to_buffer(out);
        }

        byte[] bytes = byte_out.toByteArray();
        for (int i = 0; i < bytes.length; ++i) {
            buffer[i] = bytes[i];
        }

        out.close();
        byte_out.close();
    }

    public int insert(Data d, aRTNode sn[]) // liefert false, wenn die Seite gesplittet werden muss
    {
        int i, last_cand;
        float mbr[], center[];
        SortMbr sm[]; //used for REINSERT
        Data nd, new_data[];


        if (get_num() == capacity) {
            Constants.error("RTDataNode.insert: maximum capacity violation", true);
        }

        // insert data into the node
//        System.out.println("there are " + this.get_num() + " entries before insert !!!!");
        data[get_num()] = d;
        num_entries++;

        // Plattenblock zum Schreiben markieren
        // Mark the block for writing
        dirty = true;
//        System.out.println(get_num()+" "+capacity+" "+(get_num() == (capacity - 1))+" "+num_entries);
        if (get_num() == (capacity - 1)) // overflow
        {
            if (my_tree.re_level[0] == false) // there was no reinsert on level 0 during this insertion
            // --> reinsert 30% of the data
            {
                // calculate center of page
                mbr = get_mbr();
                center = new float[dimension];
                for (i = 0; i < dimension; i++) {
                    center[i] = (mbr[2 * i] + mbr[2 * i + 1]) / (float) 2.0;
                }

                // neues Datenarray erzeugen
                // -. siehe Konstruktor
                //RTDataNode__dimension = dimension;
                //construct a new array to hold the data sorted according to their distance
                //from the node mbr's center
                new_data = new Data[capacity];

                // initialize array that will sort the mbrs
                sm = new SortMbr[num_entries];
                for (i = 0; i < num_entries; i++) {
                    sm[i] = new SortMbr();
                    sm[i].index = i;
                    sm[i].dimension = dimension;
                    sm[i].mbr = data[i].get_mbr();
                    sm[i].center = center;
                }

                //keep 70% entries still in this node
                //insert 30% entries to reinsertion list
                // sort by distance of each center to the overall center
                Constants.quickSort(sm, 0, sm.length - 1, Constants.SORT_CENTER_MBR);

                last_cand = (int) ((float) num_entries * 0.3);

//                System.out.format("0.30 * %d = last_cand to reinsertion list: %d \n", num_entries, last_cand);
//                System.out.format("%d entries is keep in this node\n", num_entries - last_cand);
                // copy the nearest 70% candidates to new array.
                // If the last_cand is equal to 0, it will keep all the data[] to the new array new_data[].
                // Then there will no re-insert appended.
                for (i = 0; i < num_entries - last_cand; i++) {
                    new_data[i] = data[sm[i].index];
                }

                // insert last 30% candidates into reinsertion list
                for (; i < num_entries; i++) {
                    nd = new Data(dimension);
                    nd = data[sm[i].index];
                    my_tree.re_data_cands.insert(nd);
                }

                data = new_data;

                my_tree.re_level[0] = true;

                // correct # of entries
                num_entries -= last_cand;

                // must write page
                dirty = true;
//                System.out.println(last_cand);

                return Constants.REINSERT;
            } else {
                // reinsert was applied before
                // --> split the node
                sn[0] = new aRTDataNode(my_tree);
//                System.out.println("sn"+sn[0].getClass().getName());
                sn[0].level = level;
                split((aRTDataNode) sn[0]);
//                for (int q = 0; q < ((RTDataNode) sn[0]).num_entries; q++) {
//                    Data d1 = ((RTDataNode) sn[0]).data[q];
//                    System.out.println(d1.getPlaceId());
//                }
                return Constants.SPLIT;
            }
        } else {
            return Constants.NONE;
        }
    }

    public int get_num_of_data() {
        return get_num();
    }

    public void nodes(int nodes_a[]) // see RTree.nodes()
    {
        nodes_a[0]++;
    }

    /*
     * split this to this and the new node brother
     * called when the node overflows and a split has to take place.
     * This function splits the data into two partitions and assigns the
     * first partition as data of this node, and the second as data of
     * the new split node.
     * invokes RTNode.split() to calculate the split distribution
     */
    public void split(aRTDataNode splitnode) // splits the current node so that m mbr's moves to splitnode
    // werden
    {
        int i, distribution[][], dist, n;
        float mbr_array[][]; // to be passed as parameter to RTNode.split()
        Data new_data1[], new_data2[];

        //#ifdef SHOWMBR
        //    split_000++;
        //#endif
        // initialize n, distribution[0]
        n = get_num();
        distribution = new int[1][];

        // allocate mbr_array to contain the mbrs of all entries
        //copy all data in DataNode to mbr_array （include old node and reinstall node)
        mbr_array = new float[n][2 * dimension];
        for (i = 0; i < n; i++) {
            mbr_array[i] = data[i].get_mbr();
        }

        // calculate distribution[0], dist by calling RTNode.split()
        dist = super.split(mbr_array, distribution);

        // neues Datenarray erzeugen
        // -. siehe Konstruktor
        //RTDataNode__dimension = dimension;
        // create new Data arrays to store the split results
        new_data1 = new Data[capacity];
        new_data2 = new Data[capacity];

        for (i = 0; i < dist; i++) {
            new_data1[i] = data[distribution[0][i]];
        }

        for (i = dist; i < n; i++) {
            new_data2[i - dist] = data[distribution[0][i]];
        }

        // set the new arrays as data arrays of this and splitnode's data
        data = new_data1;
        splitnode.data = new_data2;

        // Anzahl der Eintraege berichtigen
        num_entries = dist;
        splitnode.num_entries = n - dist;  // muss wegen Rundung so bleiben !!
    }

}
