package Pindex.vctest;

import org.apache.lucene.analysis.ar.ArabicAnalyzer;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.util.ArrayList;

public class DistanceEdge {
    Node startNode;
    Node endNode;

    ArrayList<myPath> paths = new ArrayList<>();


    public DistanceEdge(Node ns,Relationship rel1, Node nextNode,Relationship rel2, Node tarNode) {
        this.startNode=ns;
        this.endNode=tarNode;
        myPath p = new myPath( ns, rel1,  nextNode, rel2,  tarNode);
        addToSkylineResult(p);
    }

    public DistanceEdge(Node src, Node dest, Relationship rel) {
        this.startNode=src;
        this.endNode=dest;
        myPath p = new myPath( src,  dest,  rel);
        addToSkylineResult(p);
    }

    public DistanceEdge(DistanceEdge de, DistanceEdge next_de) {

        this.startNode = de.startNode;
        this.endNode=next_de.endNode;
        for(myPath p_de : de.paths)
        {
            for(myPath p_next : next_de.paths)
            {
                myPath p = new myPath(p_de,p_next);
                addToSkylineResult(p);
            }
        }

    }

    public DistanceEdge(DistanceEdge de) {
        this.startNode = de.startNode;
        this.endNode = de.endNode;
        this.paths = new ArrayList<>(de.paths);
    }

    @Override
    public boolean equals(Object o) {

        // If the object is compared with itself then return true
        if (o == this) {
            return true;
        }
        /* Check if o is an instance of Complex or not
          "null instanceof [type]" also returns false */
        if (!(o instanceof DistanceEdge)) {
            return false;
        }

        // typecast o to Complex so that we can compare data members
        DistanceEdge c = (DistanceEdge) o;

        // Compare the data members and return accordingly
        return this.endNode.getId()==c.endNode.getId()&&this.startNode.getId()==c.startNode.getId();
    }

    public void addToSkylineResult(myPath np) {
        int i = 0;
        if (paths.isEmpty()) {
            this.paths.add(np);
        } else {
            boolean alreadyinsert = false;
            for (; i < paths.size(); ) {
                if (checkDominated(paths.get(i).getCosts(), np.getCosts())) {
                    if (alreadyinsert && i != this.paths.size() - 1) {
                        this.paths.remove(this.paths.size() - 1);
                    }
                    break;
                } else {
                    if (checkDominated(np.getCosts(), paths.get(i).getCosts())) {
                        this.paths.remove(i);
                    } else {
                        i++;
                    }
                    if (!alreadyinsert) {
                        this.paths.add(np);
                        alreadyinsert = true;
                    }

                }
            }
        }
    }

    private boolean checkDominated(double[] costs, double[] estimatedCosts) {
        int numberOfLessThan = 0;
        for (int i = 0; i < costs.length; i++) {
//            double c = (double) Math.round(costs[i] * 1000000d) / 1000000d;
//            double e = (double) Math.round(estimatedCosts[i] * 1000000d) / 1000000d;
            double c = costs[i];
            double e = estimatedCosts[i];
            if (c > e) {
                return false;
            }
        }
        return true;
    }

}
