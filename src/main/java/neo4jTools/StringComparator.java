package neo4jTools;

import java.util.Comparator;

public class StringComparator implements Comparator<String> {

    @Override
    public int compare(String o1, String o2){
        return Integer.parseInt(o1)-Integer.parseInt(o2);
    }
}