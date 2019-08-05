package twoHop.Cached;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.HashMap;

public class CachedBlocks extends Blocks {
    LRUCache cache;
    int free_caches = 0;
    int[] mapping; // cache_id --> node_id
    int[] cacheType;//
    private int cachesize;
    private int pagesize;
    private int cache_num;
    public long cache_missed=0;
    public long cache_accessed=0;


    public CachedBlocks(int graphSize, int degree, int cachesize, int pagesize) {
        super(graphSize, degree, pagesize);
        cache_num = cachesize / pagesize;
        this.pagesize = pagesize * 1024;

        mapping = new int[cache_num];
        for (int i = 0; i < mapping.length; i++) {
            mapping[i] = -1;
        }

        cacheType = new int[cache_num];

        free_caches = cache_num;
        cache = new LRUCache(cache_num);
    }

    public static void main(String args[]) throws IOException {
        CachedBlocks cb_cache = new CachedBlocks(2000, 5, 2048, 2);
        HashMap<Integer, double[]> to_map = cb_cache.read_toIndex_blocks(0);
        System.out.println(to_map.get(2)[1]);
        System.out.println("to_map size : " + to_map.size());
        System.out.println("==========================");
        to_map = cb_cache.read_FromIndex_blocks(0);
        System.out.println("to_map size : " + to_map.size());
        System.out.println("==========================");
        HashMap<Integer, double[]> from_map = cb_cache.read_toIndex_blocks(1);
        System.out.println("to_map size : " + from_map.size());
        System.out.println("==========================");
//        cb_cache.read_toIndex_blocks(2);
//        System.out.println("==========================");
//        cb_cache.read_toIndex_blocks(100);
//        System.out.println("==========================");
//        cb_cache.read_toIndex_blocks(2);

        //Read from_index cases
//        cb_cache.read_FromIndex_blocks(0);
//        System.out.println("==========================");
//        cb_cache.read_FromIndex_blocks(1);
//        System.out.println("==========================");
//        cb_cache.read_FromIndex_blocks(2);
//        System.out.println("==========================");
//        cb_cache.read_FromIndex_blocks(100);
//        System.out.println("==========================");
//        cb_cache.read_FromIndex_blocks(2);
    }

    public HashMap<Integer, double[]> read_toIndex_blocks(int node_id) throws IOException {
        HashMap<Integer, double[]> to_map = new HashMap<>();
        int c_node_num = getNumInCache(node_id, toIndex_type);
        //if the node have not load the index into cache
        if (c_node_num == 0) {
            //number of page the node need
            int needPages_num = neededPage(node_id, toIndex_type);
            this.cache_missed += needPages_num;
            //if there is enough space to store the new blocks
            if (this.free_caches - needPages_num >= 0) {
                read_blocks(node_id, needPages_num, toIndex_type);
            } else {//if there is no enough space
                kickOutLastUsedPage(needPages_num);
                read_blocks(node_id, needPages_num, toIndex_type);
            }
//        } else {
//            System.out.println("already in cache");
//            System.out.println(cache);
        }

        this.cache_accessed += read_from_cache(to_map, node_id, toIndex_type);
        return to_map;

    }

    public HashMap<Integer, double[]> read_FromIndex_blocks(int node_id) throws IOException {
        HashMap<Integer, double[]> from_map = new HashMap<>();
        int c_node_num = getNumInCache(node_id, fromIndex_type);

        //if the node have not load the index into cache
        if (c_node_num == 0) {
            //number of page the node need
            int needPages_num = neededPage(node_id, fromIndex_type);
            this.cache_missed += needPages_num;

            //if there is enough space to store the new blocks
            if (this.free_caches - needPages_num >= 0) {
                read_blocks(node_id, needPages_num, fromIndex_type);
            } else {//if there is no enough space
                kickOutLastUsedPage(needPages_num);
                read_blocks(node_id, needPages_num, fromIndex_type);
            }
//        } else {
//            System.out.println("already in cache");
//            System.out.println(cache);
        }

        this.cache_accessed += read_from_cache(from_map, node_id, fromIndex_type);
//        System.out.println(from_map.size());
        return from_map;
    }

    private long read_from_cache(HashMap<Integer, double[]> index_map, int node_id, int index_type) throws IOException {
        int record_num = 0;
        long readPages = 0;
        for (int i = 0; i < this.mapping.length; i++) {
            if (mapping[i] == node_id && cacheType[i] == index_type) {
                readPages++;
//                this.cache.get(i);
                ByteArrayInputStream byte_in = new ByteArrayInputStream(this.cache.get(i));
                DataInputStream in = new DataInputStream(byte_in);
//                System.out.println(in.readInt() + "   " + in.readDouble() + "   " + in.readDouble() + "   " + in.readDouble());
                int index_node_id = in.readInt();

                int aaa = 1;

                while (index_node_id >= 0) {
                    double[] data = new double[3];
                    data[0] = in.readDouble();
                    data[1] = in.readDouble();
                    data[2] = in.readDouble();
                    index_map.put(index_node_id, data);
//                    System.out.println(aaa++ + " "+index_node_id + "   " + data[0] + "   " + data[1] + "   " + data[2]);
                    index_node_id = in.readInt();
                    record_num++;
//                    System.out.println(index_node_id);
                }
//                break;
            }
        }
//        System.out.println("there are " + record_num + " records of the type " + index_type + " of the node " + node_id + "read from cache ");
        return readPages;
    }

    private void kickOutLastUsedPage(int needPages_num) {
//        System.out.println("we need " + needPages_num + " pages, there are " + this.free_caches + " free pages");
        while (this.free_caches < needPages_num) {
            int removed = 0;
            int key_id = cache.getTheKeyOfLastUsedPage();
            int node_id = mapping[key_id];
            int lastType = cacheType[key_id];
//            System.out.println(key_id + " " + lastType);
            removed = setToFree(node_id, lastType);
//            System.out.println("removed " + removed + " blocks of the type " + lastType + " of the node " + node_id + "   --: now we have " + free_caches + " free blocks");
        }
    }

    private int setToFree(int node_id, int index_type) {
        int result = 0;
        for (int i = 0; i < mapping.length; i++) {
            if (mapping[i] == node_id && cacheType[i] == index_type) {
//            if (mapping[i] == node_id) {
                result++;
                cache.remove(i);
                free_caches++;
                mapping[i] = -1;
                cacheType[i] = 0;
            }
        }
        return result;
    }

    private void read_blocks(int node_id, int needPages_num, int Index_type) throws IOException {
        int startPage = getStartPage(node_id, Index_type);
//        System.out.println("node_id " + node_id + " " + startPage + " " + needPages_num + " " + free_caches);
        for (int i = 0; i < needPages_num; i++) {
            int free_id = findOneEmptyBlock();
//            System.out.println(free_id);

            this.mapping[free_id] = node_id;
            this.cacheType[free_id] = Index_type;
            //put to cache
            long seek_pos = (startPage + i) * this.pagesize;
            byte[] data = read_block_from_disk(seek_pos, Index_type);
//            System.out.println(data.length);

            //reduce the number of the free blocks
            this.free_caches--;

            this.cache.put(free_id, data);
        }
//        System.out.println(free_caches);
//        System.out.println(cache);
    }


    private int findOneEmptyBlock() {
        //mapping the empty cache blocks
        for (int i = 0; i < this.mapping.length; i++) {
            if (mapping[i] == -1) {
                return i;
            }
        }
        return -1;
    }


    private int getNumInCache(int node_id, int idx_type) {
        int count = 0;

        for (int i = 0; i < this.mapping.length; i++) {
            if (mapping[i] == node_id && cacheType[i] == idx_type) {
                count++;
            }
        }
        return count;
    }
}
