package aRtree;

import java.io.*;
import java.util.Collections;

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
        System.out.println("new data node " + block + ": capacity " + capacity + " " + d.get_size() + "  " + (rt.file.get_blocklength() - header_size));

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
        System.out.println("insert into the data block " + block);
        int i, last_cand;
        float mbr[], center[];
        SortMbr sm[]; //used for REINSERT
        Data nd, new_data[];


        if (get_num() == capacity) {
            Constants.error("RTDataNode.insert: maximum capacity violation", true);
        }

        // insert data into the node
        System.out.println("there are " + this.get_num() + " entries before insert !!!!");
        data[get_num()] = d;
        num_entries++;
        System.out.println("there are " + this.get_num() + " entries after insert !!!!");


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

                last_cand = (int) ((float) num_entries * 0.4);
                System.out.println(last_cand);

//                System.out.format("0.30 * %d = last_cand to reinsertion list: %d \n", num_entries, last_cand);
//                System.out.format("%d entries is keep in this node\n", num_entries - last_cand);
                // copy the nearest 70% candidates to new array.
                // If the last_cand is equal to 0, it will keep all the data[] to the new array new_data[].
                // Then there will no re-insert appended.

//              Because this.data needs to be updated, we need to initialize the bounds array at first.
                init_attrs_bounds();

                for (i = 0; i < num_entries - last_cand; i++) {
                    new_data[i] = data[sm[i].index];

                    //update aggregate value
                    for (int j = 0; j < attr_lower.length; j++) {
                        if (new_data[i].attrs[j] < attr_lower[j]) {
                            attr_lower[j] = new_data[i].attrs[j];
                        }
                    }
                    for (int j = 0; j < attr_upper.length; j++) {
                        if (new_data[i].attrs[j] > attr_upper[j]) {
                            attr_upper[j] = new_data[i].attrs[j];
                        }
                    }
                }

                // insert last 30% candidates into reinsertion list
                for (; i < num_entries; i++) {
                    nd = new Data(dimension);
                    nd = data[sm[i].index];
                    my_tree.re_data_cands.insert(nd);

                    System.out.println("reinsert +++++ : " + nd);
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
                System.out.println("split the data node " + this.block);
                // reinsert was applied before
                // --> split the node
                System.out.println("    Create one new data node to store the split datas");
                sn[0] = new aRTDataNode(my_tree); //the new node
                sn[0].level = level;
                split((aRTDataNode) sn[0]);
                System.out.println(sn[0]);
//                for (int q = 0; q < ((RTDataNode) sn[0]).num_entries; q++) {
//                    Data d1 = ((RTDataNode) sn[0]).data[q];
//                    System.out.println(d1.getPlaceId());
//                }
                System.out.println("Split +++++ ");
                System.out.println(this);
                return Constants.SPLIT;
            }
        } else {
            updateBounds(d);
            return Constants.NONE;
        }
    }

    private void updateBounds(Data d) {
        System.out.println("number of entries" + num_entries);
        for (int i = 0; i < attr_lower.length; i++) {
            if (d.attrs[i] < attr_lower[i]) {
                attr_lower[i] = d.attrs[i];
            }
        }

        for (int i = 0; i < attr_upper.length; i++) {
            if (d.attrs[i] > attr_upper[i]) {
                attr_upper[i] = d.attrs[i];
            }
        }
    }

    private void updateBounds() {
        for (int i = 0; i < get_num(); i++) {
            //update aggregate value
            for (int j = 0; j < attr_lower.length; j++) {
                if (this.data[i].attrs[j] < attr_lower[j]) {
                    attr_lower[j] = this.data[i].attrs[j];
                }
            }
            for (int j = 0; j < attr_upper.length; j++) {
                if (this.data[i].attrs[j] > attr_upper[j]) {
                    attr_upper[j] = this.data[i].attrs[j];
                }
            }
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
    //Todo: update the aggregate value here, the new node and the old new both needs to update
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

        //update the aggregate value of the split node
        splitnode.updateBounds();


        //because this.data already be updated, we need to initial the bounds array at first.
        this.init_attrs_bounds();
        this.updateBounds();
    }

    public boolean is_data_node() // this is a data node
    {
        return true;
    }

    public Data get(int i) {
        Data d;

        if (i >= get_num()) // if there is no i-th object -. null
        {
            return null;
        }

        d = new Data(dimension);
        d = data[i];

        return d;
    }

    public float[] get_mbr() // float
    // calculates the mbr of all data in this node
    {
        int i, j, n;
        float mbr[], tm[];

        mbr = data[0].get_mbr();
//        System.out.println(mbr[0]+" "+mbr[1]+" "+mbr[2]+" "+mbr[3]+" ");
        n = get_num();
        for (j = 1; j < n; j++) {
            tm = data[j].get_mbr();
//            System.out.println(tm[0]+" "+tm[1]+" "+tm[2]+" "+tm[3]+" ");

            for (i = 0; i < 2 * dimension; i += 2) {
//                System.out.println(i+" "+mbr[i]+" "+tm[i]);
//                System.out.println(i+" "+mbr[i+1]+" "+tm[i+1]);
                mbr[i] = Constants.min(mbr[i], tm[i]);
                mbr[i + 1] = Constants.max(mbr[i + 1], tm[i + 1]);
            }
        }
//        System.out.println(mbr[0]+" "+mbr[1]+" "+mbr[2]+" "+mbr[3]+" ");

        return mbr;
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("data node " + this.block + " lower:[");
        for (float f : this.getAttr_lower()) {
            sb.append(f).append(",");
        }

        sb = new StringBuffer(sb.substring(0, sb.lastIndexOf(","))).append("]");
        sb.append(", upper:[");
        for (float f : this.getAttr_upper()) {
            sb.append(f).append(",");
        }
        sb = new StringBuffer(sb.substring(0, sb.lastIndexOf(","))).append("]").append(" ").append(this.get_num());
        return sb.toString();
    }

    public float[] getAttr_upper() {
        float[] uppers = new float[Constants.attrs_length];

        for (int i = 0; i < uppers.length; i++) {
            uppers[i] = Float.MIN_VALUE;
        }

        for (int i = 0; i < this.get_num(); i++) {
            Data d = this.data[i];

            for (int j = 0; j < uppers.length; j++) {
                if (d.attrs[j] > uppers[j]) {
                    uppers[j] = d.attrs[j];
                }
            }

        }
        return uppers;    }

    public float[] getAttr_lower() {
        float[] lowers = new float[Constants.attrs_length];

        for (int i = 0; i < lowers.length; i++) {
            lowers[i] = Float.MAX_VALUE;
        }

        for (int i = 0; i < this.get_num(); i++) {
            Data d = this.data[i];

            for (int j = 0; j < lowers.length; j++) {
                if (d.attrs[j] < lowers[j]) {
                    lowers[j] = d.attrs[j];
                }
            }

        }
        return lowers;
    }

    public void print(String prefix, int times) {
        System.out.println((String.join("", Collections.nCopies(times, prefix)))+this);
        for(int i = 0 ; i< get_num();i++)
        {
            System.out.println((String.join("", Collections.nCopies(times+1, prefix)))+this.data[i]);
        }
    }
}
