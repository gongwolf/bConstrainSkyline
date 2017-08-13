package GraphPartition;

import java.io.File;
import java.io.FilenameFilter;

public class textFileFilter implements FilenameFilter {
    @Override
    public boolean accept(File dir, String name) {
        return name.endsWith(".txt") && name.startsWith("node_mapping");
    }
}