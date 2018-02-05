package aRtree;

import java.io.*;
import java.util.Collections;

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
        System.out.println("new entries node " + block + ": capacity " + capacity + " " + d.get_size() + "  " + (rt.file.get_blocklength() - header_size));

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


    public void split(aRTDirNode brother) // splittet den aktuellen Knoten so auf, dass m mbr's nach sn verschoben
    // werden
    {
        int i, dist, n;
        int distribution[][];                    // distribution[0] will hold the best split distribution order
        float mbr_array[][];                     // array of the mbrs of all entries
        DirEntry new_entries1[], new_entries2[]; // the new directory entries that will hold the split parts

        //#ifdef SHOWMBR
        //    split_000++;
        //#endif
        // wieviele sind denn nun belegt?
        n = get_num(); // n = number of directory entries
        distribution = new int[1][];

        // mbr_array holds the mbrs of all entries
        mbr_array = new float[n][dimension * 2];
        for (i = 0; i < n; i++) {
            mbr_array[i] = entries[i].bounces;
        }

        // call super.split() to initialize distribution[0], dist
        dist = super.split(mbr_array, distribution);

        // neues Datenarray erzeugen
        // -. siehe Konstruktor
        //RTDirNode__dimension = dimension;
        //RTDirNode__my_tree = my_tree;
        //initialize the new entries that will hold the split parts
        new_entries1 = new DirEntry[capacity];
        new_entries2 = new DirEntry[capacity];

        // fill the new entries with the split parts
        for (i = 0; i < dist; i++) {
            new_entries1[i] = entries[distribution[0][i]];
        }

        for (i = dist; i < n; i++) {
            new_entries2[i - dist] = entries[distribution[0][i]];
        }

        // Datenarrays freigeben
        // da die Nachfolgeknoten aber nicht geloescht werden sollen
        // werden vor dem Aufruf von delete noch alle Pointer auf null gesetzt
        //for (i = 0; i < n; i++)
        //{
        //       entries[i].son_ptr = null;
        //       brother.entries[i].son_ptr = null;
        //}
        // update this' and the brother's entries after the split
        entries = new_entries1;
        brother.entries = new_entries2;

        // Anzahl der Eintraege berichtigen
        num_entries = dist;
        brother.num_entries = n - dist;  // muss wegen Rundung so bleiben !!
    }

    public int insert(Data d, aRTNode sn[]) {
        System.out.println("insert into the entry block " + block);

        int follow;
        aRTNode succ = null;
        aRTNode new_succ[] = new aRTNode[1];
        DirEntry de;
        int ret;
        float mbr[], nmbr[];
        float lower[], upper[];

        // choose subtree to follow
        mbr = d.get_mbr();
        follow = choose_subtree(mbr);


        // get corresponding son
        succ = entries[follow].get_son();

        // insert d into son
        ret = ((Node) succ).insert(d, new_succ);
        if (ret != Constants.NONE) // if anything (SPLIT or REINSERT) happend -. update bounces of entry "follow"
        // because these actions change the entries in succ
        {
            mbr = ((Node) succ).get_mbr();
            System.arraycopy(mbr, 0, entries[follow].bounces, 0, 2 * dimension);


            lower = ((Node) succ).getAttr_lower();
            upper = ((Node) succ).getAttr_upper();
            System.arraycopy(lower, 0, entries[follow].attr_lower, 0, Constants.attrs_length);
            System.arraycopy(upper, 0, entries[follow].attr_upper, 0, Constants.attrs_length);
        }

        // recalculate # of succeeders in the tree
        entries[follow].num_of_data = ((Node) succ).get_num_of_data();

        if (ret == Constants.SPLIT) // succ was split into succ and new_succ[0]
        {
            // some error checking
            if (get_num() == capacity) {
                System.out.println(this.block);
                Constants.error("RTDirNode.insert: maximum capacity violation", true);
            }

            // create a new entry to hold the new_succ[0] node
            //Todo: update aggregate value
            de = new DirEntry(dimension, son_is_data, my_tree);
            nmbr = ((Node) new_succ[0]).get_mbr();
            System.arraycopy(nmbr, 0, de.bounces, 0, 2 * dimension);
            de.son = new_succ[0].block;
            de.son_ptr = new_succ[0];
            de.son_is_data = son_is_data;
            de.num_of_data = ((Node) new_succ[0]).get_num_of_data();

            lower = ((Node) new_succ[0]).getAttr_lower();
            upper = ((Node) new_succ[0]).getAttr_upper();
            System.arraycopy(lower, 0, de.attr_lower, 0, Constants.attrs_length);
            System.arraycopy(upper, 0, de.attr_upper, 0, Constants.attrs_length);


//            Constants.print(de.attr_lower);

            // insert de to this
            enter(de);
            System.out.println("  inserted " + de.son_ptr.getClass().getName() + " " + de.son + " into " + this.block + " - " + this.get_num() + " " + capacity);

            if (get_num() == (capacity - 1)) // directory node overflows -. Split
            // this happens already if the node is nearly filled (capacity - 1)
            // for the algorithms are more easy then
            {
                // initialize brother(split) node
                sn[0] = new aRTDirNode(my_tree);
                ((aRTDirNode) sn[0]).son_is_data = ((aRTDirNode) this).son_is_data;
                sn[0].level = level;
                // split this --> this and sn[0]
                split((aRTDirNode) sn[0]);
                System.out.println("    split the entry node ++++" + ((aRTDirNode) this).block + " the new entry node is " + ((aRTDirNode) sn[0]).block);

                ret = Constants.SPLIT;
            } else {
                ret = Constants.NONE;
            }
        }
        // must write page. set dirty bit
        dirty = true;

        return ret;
    }

    /**
     * chooses the best subtree under this node to insert a new mbr There are
     * three cases: Case 1: the new mbr is contained (inside) in only one
     * directory entry mbr. In this case follow this subtree. Case 2: the new
     * mbr is contained (inside) in more than one directory entry mbr. In this
     * case follow the entry whose mbr has the minimum area Case 3: the new mbr
     * is not contained (inside) in any directory entry mbr In this case the
     * criteria are the following: - If the son nodes are data nodes consider as
     * criterion first the minimum overlap increase if we follow one node with
     * its neighbors, then the minimum area enlargement and finally the minimum
     * area - In the son nodes are dir nodes consider as criterion first the
     * minimum area enlargement and finally the minimum area After we choose the
     * subtree, we enlarge the directory entry (if has to be enlarged) and
     * return its index
     */
    public int choose_subtree(float mbr[]) {
        int i, j, n, follow, minindex = 0, inside[], inside_count, over[];
        float bmbr[] = new float[2 * dimension];
        float old_o, o, omin, a, amin, f, fmin;

        n = get_num();

        // faellt d in einen bestehenden Eintrag ?
        inside_count = 0;   // this variable holds the number of entries whose mbr contains the new mbr to be inserted
        inside = new int[n]; // this array holds the indices of entries whose mbr contains the new mbr to be inserted

        // calculate inside[]
        for (i = 0; i < n; i++) {
            switch (entries[i].section(mbr)) {
                case Constants.INSIDE:
                    // mbr is inside entries[i] mbr
                    inside[inside_count++] = i;
                    break;
            }
        }

        if (inside_count == 1) // Case 1: There is exactly one dir_mbr that contains mbr
        {
            follow = inside[0];
        } else if (inside_count > 1) // Case 2: There are many dir_mbrs that contain mbr
        // choose the one for which insertion causes the minimun area enlargement
        {
            fmin = Constants.MAXREAL;
            //printf("Punkt in %d von %d MBRs \n",inside_count,n);

            for (i = 0; i < inside_count; i++) {
                f = Constants.area(dimension, entries[inside[i]].bounces);
                if (f < fmin) {
                    minindex = i;
                    fmin = f;
                }
            }

            follow = inside[minindex];
        } else // Case 3: There are no dir_mbrs that contain mbr
        // choose the one for which insertion causes the minimun overlap if son_is_data
        // else choose the one for which insertion causes the minimun area enlargement
        // Case 3: Rechteck faellt in keinen Eintrag -.
        // fuer Knoten, die auf interne Knoten zeigen:
        // nimm den Eintrag, der am geringsten vergroessert wird;
        // bei gleicher Vergroesserung:
        // nimm den Eintrag, der die geringste Flaeche hat
        //
        // fuer Knoten, die auf Datenknoten zeigen:
        // nimm den, der die geringste Ueberlappung verursacht
        // bei gleicher Ueberlappung:
        // nimm den Eintrag, der am geringsten vergroessert wird;
        // bei gleicher Vergroesserung:
        // nimm den Eintrag, der die geringste Flaeche hat
        {
            if (son_is_data) {
                omin = Constants.MAXREAL;
                fmin = Constants.MAXREAL;
                amin = Constants.MAXREAL;
                for (i = 0; i < n; i++) {
                    // compute the MBR of mbr and entries[i]

                    Constants.enlarge(dimension, bmbr, mbr, entries[i].bounces);

                    // calculate area and area enlargement
                    a = Constants.area(dimension, entries[i].bounces);
                    f = Constants.area(dimension, bmbr) - a;

                    // calculate overlap before enlarging entry_i
                    old_o = o = (float) 0.0;

                    for (j = 0; j < n; j++) {
                        if (j != i) {
                            old_o += Constants.overlap(dimension,
                                    entries[i].bounces,
                                    entries[j].bounces);
                            o += Constants.overlap(dimension,
                                    bmbr,
                                    entries[j].bounces);
                        }
                    }
                    o -= old_o;

                    // is this entry better than the former optimum ?
                    if ((o < omin)
                            || (o == omin && f < fmin)
                            || (o == omin && f == fmin && a < amin)) {
                        minindex = i;
                        omin = o;
                        fmin = f;
                        amin = a;
                    }
                    //delete [] bmbr;
                }
            } else //son is not data
            {
                fmin = Constants.MAXREAL;
                amin = Constants.MAXREAL;
                for (i = 0; i < n; i++) {
                    // compute the MBR of mbr and entries[i]
                    Constants.enlarge(dimension, bmbr, mbr, entries[i].bounces);

                    // calculate area and area enlargement
                    a = Constants.area(dimension, entries[i].bounces);
                    f = Constants.area(dimension, bmbr) - a;

                    // is this entry better than the former optimum ?
                    if ((f < fmin) || (f == fmin && a < amin)) {
                        minindex = i;
                        fmin = f;
                        amin = a;
                    }
                    //delete [] bmbr;
                }
            }
            // enlarge the boundaries of the directoty entry we will follow
            Constants.enlarge(dimension, bmbr, mbr, entries[minindex].bounces);
            System.arraycopy(bmbr, 0, entries[minindex].bounces, 0, 2 * dimension);

            follow = minindex;

            // nod has changed; set the dirty bit
            dirty = true;
        }

        return follow;
    }

    public void enter(DirEntry de) {
        // ist ein Einfuegen ueberhaupt moeglich?
        if (get_num() > (capacity - 1)) {
            Constants.error("RTDirNode.enter: called, but node is full", true);
        }

        // Eintrag an erste freie Stelle kopieren
        entries[num_entries] = de;

        // jetzt gibts einen mehr
        num_entries++;
    }

    public Data get(int i) {
        int j, n, sum;
        aRTNode son;

        n = get_num();
        sum = 0;
        for (j = 0; j < n; j++) {
            sum += entries[j].num_of_data;

            if (sum > i) // i-th object is behind this node -. follow son
            {
                son = entries[j].get_son();
                return ((Node) son).get(i - (sum - entries[j].num_of_data));
            }
        }

        return null;
    }

    public void nodes(int nodes_a[]) {
        int i, n;
        aRTNode succ;

        // ein Knoten mehr besucht
        nodes_a[level]++;

        n = get_num();
        for (i = 0; i < n; i++) // teste alle Rechtecke auf Ueberschneidung
        {
            succ = entries[i].get_son();
            ((Node) succ).nodes(nodes_a);
        }
    }

    public int get_num_of_data() {
        int i, n, sum;

        n = get_num();
        sum = 0;
        for (i = 0; i < n; i++) {
            sum += entries[i].num_of_data;
        }
        return sum;
    }

    public float[] get_mbr() {
        int i, j, n;
        float mbr[];

        mbr = new float[2 * dimension];
        for (i = 0; i < 2 * dimension; i++) {
            mbr[i] = entries[0].bounces[i];
        }

        n = get_num();
        for (j = 1; j < n; j++) {
            for (i = 0; i < 2 * dimension; i += 2) {
                mbr[i] = Constants.min(mbr[i], entries[j].bounces[i]);
                mbr[i + 1] = Constants.max(mbr[i + 1], entries[j].bounces[i + 1]);
            }
        }
        return mbr;
    }

    public boolean is_data_node() {
        return false;
    }

    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("entry node " + this.block + " lower:[");
        for (float f : this.getAttr_lower()) {
            sb.append(f).append(",");
        }

        sb = new StringBuffer(sb.substring(0, sb.lastIndexOf(","))).append("]");
        sb.append(", upper:[");
        for (float f : this.getAttr_upper()) {
            sb.append(f).append(",");
        }
        sb = new StringBuffer(sb.substring(0, sb.lastIndexOf(","))).append("] ");
        sb.append("[");
        for(float f:get_mbr())
        {
            sb.append(f).append(",");
        }
        sb = new StringBuffer(sb.substring(0, sb.lastIndexOf(","))).append("] ");
        sb.append(get_num());
        return sb.toString();
    }

    public float[] getAttr_upper() {
        float[] uppers = new float[Constants.attrs_length];
        for (int i = 0; i < uppers.length; i++) {
            uppers[i] = Float.MIN_VALUE;
        }
        for (int i = 0; i < this.get_num(); i++) {
            DirEntry e = entries[i];

            for (int j = 0; j < uppers.length; j++) {
                if (e.attr_upper[j] > uppers[j]) {
                    uppers[j] = e.attr_upper[j];
                }
            }

        }
        return uppers;
    }

    public float[] getAttr_lower() {
        float[] lowers = new float[Constants.attrs_length];

        for (int i = 0; i < lowers.length; i++) {
            lowers[i] = Float.MAX_VALUE;
        }

        for (int i = 0; i < this.get_num(); i++) {
            DirEntry e = entries[i];

            for (int j = 0; j < lowers.length; j++) {
                if (e.attr_lower[j] < lowers[j]) {
                    lowers[j] = e.attr_lower[j];
                }
            }

        }
        return lowers;
    }

    public void print(String prefix,int times) {
        System.out.println((String.join("", Collections.nCopies(times, prefix)))+this);
        int i, n;
        aRTNode succ;

        n = get_num();
        //System.out.println("I'm here aaaaaa");

        for (i = 0; i < n; i++) // teste alle Rechtecke auf Ueberschneidung
        {
                succ = entries[i].get_son();
                ((Node) succ).print(prefix,times+1);

        }

    }


}