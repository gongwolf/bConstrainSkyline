package testTools;

import org.apache.commons.lang3.builder.HashCodeBuilder;

public class SegObj {
    long sid, did;
    double d1, d2, d3;

    public SegObj(long sid, long did, double d1, double d2, double d3) {
        this.sid = sid;
        this.did = did;
        this.d1 = d1;
        this.d2 = d2;
        this.d3 = d3;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }

        if (!(obj instanceof SegObj)) {
            return false;
        }

        SegObj c = (SegObj) obj;

        if (c.sid == this.sid && c.did == this.did) {
            return true;
        }else
        {
            return false;
        }

    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(11, 33).append(sid).append(did)
                .toHashCode();
    }

    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append(sid).append(" ").append(did).append(" ").append(d1).append(" ").append(d2).append(" ").append(d3);
        return sb.toString();
    }
}
