package twoHop;


import java.util.HashMap;

public class LabelsIndex {

    Labels[] labels;

    public LabelsIndex(int graphSize) {
        this.labels = new Labels[graphSize];
        for (int i = 0; i < this.labels.length; i++) {
            this.labels[i] = new Labels();
        }
        //System.out.println(this.labels.length);
    }

    //copy previous level index to the new one
    public LabelsIndex(LabelsIndex L) {
        this.labels = new Labels[L.labels.length];
        for (int i = 0; i < this.labels.length; i++) {
            this.labels[i] = new Labels(L.labels[i]);
        }
    }

    //Add the L2's to_index to this labels
    public void mergeToIndex(LabelsIndex L2) {
        for (int i = 0; i < this.labels.length; i++) {
            this.labels[i].toLabelSet.putAll(L2.labels[i].toLabelSet);
        }
    }
}

class Labels {

    public HashMap<Integer, Double[]> fromLabelSet;
    public HashMap<Integer, Double[]> toLabelSet;

    public Labels() {
        this.fromLabelSet = new HashMap<>();
        this.toLabelSet = new HashMap<>();
    }

    //copy labels from previous level
    public Labels(Labels label) {
        this.fromLabelSet = new HashMap<>(label.fromLabelSet);
        this.toLabelSet = new HashMap<>(label.toLabelSet);
    }
}
