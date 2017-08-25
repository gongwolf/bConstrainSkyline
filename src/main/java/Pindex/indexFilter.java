package Pindex;

import java.io.File;
import java.io.FileFilter;
import java.io.FilenameFilter;

public class indexFilter implements FileFilter {
    @Override
    public boolean accept(File pathname) {
        return pathname.getName().endsWith("inner.idx");
    }
}
