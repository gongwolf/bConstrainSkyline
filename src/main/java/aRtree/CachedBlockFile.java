package aRtree;

import java.io.IOException;

public class CachedBlockFile extends BlockFile {

    private static final int FREE = 0;      //...
    private static final int USED = 1;        //
    private static final int FIXED = 2;        //

    private int ptr; //current position in cache
    private int cachesize;
    private int[] cache_cont; // array of the indices of blocks that are in cache
    private int[] fuf_cont; //indicator array that shows whether one cache block is free, used or fixed
    byte[][] cache;

    public CachedBlockFile(String name, int blength, int csize) throws IOException {
        super(name, blength);
        ptr = 0;
        if (csize >= 0) {
            cachesize = csize;
        } else {
            System.out.println("CachedBlockFile.CachedBlockFile: neg. Cachesize");
            System.exit(0);
        }
        cache_cont = new int[cachesize];
        fuf_cont = new int[cachesize];

        for (int i = 0; i < cachesize; i++) {
            cache_cont[i] = 0;
            fuf_cont[i] = FREE;
        }
        cache = new byte[cachesize][get_blocklength()]; //Changed
    }

    public boolean write_block(byte[] block, int index) throws IOException {
        int c_ind;

        index++;
        if (index <= get_num_of_blocks() && index > 0) {
            c_ind = in_cache(index);
//            System.out.println("index__c_ind:  "+index+"  "+c_ind);
            if (c_ind >= 0) {
                int blclth = get_blocklength();
                for (int i = 0; i < blclth; i++) {
                    cache[c_ind][i] = block[i];
                }
            } else {
                c_ind = next();
                if (c_ind >= 0) {
                    int blclth = get_blocklength();
                    for (int i = 0; i < blclth; i++) {
                        cache[c_ind][i] = block[i];
                    }
                    cache_cont[c_ind] = index;
                    fuf_cont[c_ind] = USED;
                } else {
                    super.write_block(block, index - 1);
                }
            }
            return true;
        } else {
            return false;
        }
    }

    public boolean read_block(byte[] block, int index) throws IOException {
        int c_ind;

        //because the first block stored the information of this R tree,
        //The actual data stored from index 1 to ...
        index++;
        if (index <= get_num_of_blocks() && index > 0) {
            if ((c_ind = in_cache(index)) >= 0) // the block is in cache
            {
                int blclth = get_blocklength();
                for (int i = 0; i < blclth; i++) {
                    block[i] = cache[c_ind][i];
                }
            } else // the block is not in cache
            {
                // find next free cache block or free one used
                c_ind = next();
//                System.out.println("c_ind:" + c_ind+" ##  index:"+index);
                if (c_ind >= 0) {
                    super.read_block(cache[c_ind], index - 1);
                    cache_cont[c_ind] = index;
                    fuf_cont[c_ind] = USED;
                    int blclth = get_blocklength();
                    for (int i = 0; i < blclth; i++) {
                        block[i] = cache[c_ind][i];
                    }
                } else {
                    super.read_block(block, index - 1);
                }
            }
            return true;
        } else {
            return false;
        }
    }

    private int in_cache(int index) {
        int i;

        for (i = 0; i < cachesize; i++) {
            if (cache_cont[i] == index && fuf_cont[i] != FREE) {
                return i;
            }
        }
        return -1;
    }


    // returns the index of the next available block in cache
    private int next() throws IOException {
        int ret_val, tmp;

        if (cachesize == 0) {
            return -1;
        } else if (fuf_cont[ptr] == FREE) // if the next position is unused
        {
            //ret_val = ptr and then ptr=ptr++
            ret_val = ptr++;
            if (ptr == cachesize) {
                ptr = 0;
            }
            return ret_val;
        } else {
            tmp = (ptr + 1) % cachesize;
            //if(tmp==cachesize)
            //    tmp=0;
            while (tmp != ptr && fuf_cont[tmp] != FREE) //find the first free block
            {
                if (++tmp == cachesize) {
                    tmp = 0;
                }
            }
            if (ptr == tmp) //failed to find a free block
            {
                if (fuf_cont[ptr] == FIXED) //if ptr is fixed block find the first unfixed(used)
                {
                    tmp = (ptr + 1) % cachesize;
                    //if(tmp==cachesize)
                    //    tmp=0;
                    while (tmp != ptr && fuf_cont[tmp] != USED) {
                        if (++tmp == cachesize) {
                            tmp = 0;
                        }
                    }
                    if (ptr == tmp) // all blocks are fixed. failure :(
                    {
                        return -1;
                    } else {
                        ptr = tmp; //save what we found to ptr
                    }
                }
                // write block to be freed back to file
                super.write_block(cache[ptr], cache_cont[ptr] - 1);
                fuf_cont[ptr] = FREE;
                ret_val = ptr++;
                if (ptr == cachesize) {
                    ptr = 0;
                }
                return ret_val;
            } else {
                return tmp;
            }
        }
    }

    public void flush() throws IOException {
        int i;

        for (i = 0; i < cachesize; i++) {
            if (fuf_cont[i] != FREE) {
                super.write_block(cache[i], cache_cont[i] - 1);
            }
        }
    }
}
