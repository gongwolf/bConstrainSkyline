import matplotlib.pyplot as plt
import sys

graphsize = sys.argv[1]
degree = sys.argv[2]
graph_str = graphsize+"_"+degree

fname = "/home/gqxwolf/mydata/projectData/testGraph"+graph_str+"/data/SegInfo.txt"

node_degrees =  {}



with open(fname) as f:
    contents = f.readlines()
    for line in contents:
        edge = line.rstrip()
        start_node = edge.split(" ")[0]
        end_node= edge.split(" ")[1]
        #print "%s -> %s " % (start_node,end_node)
        if start_node in node_degrees:
            node_degrees[start_node][0]+=1
            node_degrees[start_node][1]+=1
        else:
            node_degrees[start_node] = [1,1,0]

        if end_node in node_degrees:
            node_degrees[end_node][0]+=1
            node_degrees[end_node][2]+=1
        else:
            node_degrees[end_node] = [1,0,1]

print "============================="

#for key,value in node_degrees.items():
#    print "the node %s : %s,%s,%s " % (key,value[0],value[1],value[2])

total_degree = {}
in_degree= {}
out_degree= {}


for key,value in node_degrees.items():
    if value[0] in total_degree:
        total_degree[value[0]] += 1
    else:
        total_degree[value[0]] = 1
    if value[1] in out_degree:
        out_degree[value[1]] += 1
    else:
        out_degree[value[1]] = 1
    if value[2] in in_degree:
        in_degree[value[2]] += 1
    else:
        in_degree[value[2]] = 1

print total_degree
print out_degree
print in_degree

plt.xlabel('# of degrees')
plt.ylabel("# of nodes")
plt.title(graph_str)

plt.plot(*zip(*sorted(total_degree.items())),label="total degree")
plt.plot(*zip(*sorted(in_degree.items())), label="in-coming degree")
plt.plot(*zip(*sorted(out_degree.items())),label="out-going degree")
plt.legend()


saved_file_name = graphsize+"_"+degree+"_degree.png"
plt.savefig(saved_file_name)
