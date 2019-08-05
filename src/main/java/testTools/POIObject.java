package testTools;

public class POIObject {
    int placeID;
    String g_p_id; //google map place id

    double[] locations;
    float[] data;

    public void cleanContents() {

        this.g_p_id = "";
        this.data = new float[]{-1, -1, -1};
        this.locations = new double[]{-1, -1};
    }


    @Override
    public String toString() {
        return this.placeID+" "+this.g_p_id+" "+this.data[0]+" "+this.data[1]+" "+this.data[2]+" "+this.locations[0]+" "+this.locations[1];
    }
}
