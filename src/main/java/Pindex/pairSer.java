package Pindex;

import java.io.Serializable;

import Pindex.path;

import java.util.ArrayList;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

public class pairSer implements Serializable{

    String startNode;
    String endNode;
    double[] lowerbond ;

    ArrayList<pathSer> pathinfos = new ArrayList<>();

    public pairSer(String sNode, String eNode, ArrayList<path> paths, double[] lowerb)
    {
        this.startNode = sNode;
        this.endNode = eNode;
        for(path p: paths)
        {
            pathinfos.add(new pathSer(p));
        }

        this.lowerbond = new double[lowerb.length];

        System.arraycopy(lowerb,0,this.lowerbond,0,lowerb.length);

    }
}



class pathSer implements Serializable{
    ArrayList<String> nodes = new ArrayList<>();
    ArrayList<String> rels= new ArrayList<>();
    double[] costs;

    public pathSer(path p)
    {
        for (Node n : p.Nodes)
        {
            this.nodes.add(String.valueOf(n.getId()));

        }

        for (Relationship r:p.relationships)
        {
            this.rels.add(String.valueOf(r.getId()));
        }

        costs = new double[p.NumberOfProperties];
        for(int i = 0 ; i< costs.length;i++)
        {
            costs[i]=p.getCosts()[i];
        }
    }
}
