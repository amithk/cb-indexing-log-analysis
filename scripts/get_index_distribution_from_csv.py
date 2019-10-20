"""
Get index distribution from csv.
Format of the csv is same as the list of indexes displayed in
the "indexes" tab in the support portal
"""

import sys
import os

def splitIndexName(name):
	if len(name) < len(" (replica 1)"):
		return name, 0

	if name[-1] != ")":
		return name, 0

	if name[-10:-3] != "replica":
		# More than 10 replicas?
		raise NotImplementedError

	return name[:-12], int(name[-2])


class Index:
	def __init__(self, name, node):
		self.name = name
		self.replicas = {}
		replicaid = self.getReplicaId(name)
		self.setReplica(replicaid, node)

	def getReplicaId(self, name):
		return splitIndexName(name)[1]

	def setReplica(self, replicaid, node):
		if self.replicas.get(replicaid):
			raise Exception("Same replica id found twice")

		self.replicas[replicaid] = node

	def addReplica(self, name, node):
		replicaid = self.getReplicaId(name)
		self.setReplica(replicaid, node)


def run(csvpath):
	f = open(csvpath, "r")
	indexes = {}
	nodes = {}
	while True:
		l = f.readline()
		if not l:
			break

		ll = l.split(",")
		bucket = ll[0]
		iname = ll[1]
		node = ll[2]

		name, _ = splitIndexName(iname)
		index = indexes.get(name)
		if index == None:
			indexes[name] = Index(iname, node)
		else:
			indexes[name].addReplica(iname, node)

		n = nodes.get(node)
		if n == None:
			nodes[node] = True

	max_num_replica = 0

	for name, index in indexes.items():
		if max_num_replica < len(index.replicas):
			max_num_replica = len(index.replicas)

	missing_replica = False
	for name, index in indexes.items():
		if len(index.replicas) < max_num_replica:
			missing_replica = True
			print "For index", name, "there is a missing replica. Available replicas are", index.replicas.keys()

	if missing_replica:
		print "There are missing replicas in the cluster"
	else:
		print "Cluster seems to be fine with num_replica =", max_num_replica, " and with num_nodes =", len(nodes)


if __name__ == "__main__":
	if len(sys.argv) != 2:
		print "Usage:\npython get)index_distribution_from_csv.py <csv-file-path>"
		os._exit(0)

	csvpath = sys.argv[1]
	run(csvpath)

