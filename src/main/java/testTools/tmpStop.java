package testTools;

public class tmpStop {
    String subName;
    long id;
    int order;

    public tmpStop(String subName, long id, int order) {
        this.subName = subName;
        this.id = id;
        this.order = order;
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer(subName);
        sb.append(",").append(order).append(",").append(id);
        return sb.toString();
    }
}
