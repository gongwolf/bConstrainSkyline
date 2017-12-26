package twoHop.Cached;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

class LRUCache extends LinkedHashMap<Integer, byte[]> {
    private int maxSize;

    public LRUCache(int capacity) {
        super(capacity, 0.75f, true);
        this.maxSize = capacity;
    }

    //return -1 if miss
    public byte[] get(int key) {
        byte[] v = super.get(key);
        return v;
    }

    public void put(int key, byte[] value) {
        super.put(key, value);
    }

    public Integer getTheKeyOfLastUsedPage() {
        Iterator<Integer> k_iter = this.keySet().iterator();
        if (k_iter.hasNext()) {
            return k_iter.next();
        } else {
            return -1;
        }
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<Integer, byte[]> eldest) {
        return this.size() > maxSize; //must override it if used in a fixed cache
    }
}