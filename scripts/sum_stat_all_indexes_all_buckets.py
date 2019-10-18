import json
import sys
import os

supported_stat_types = ["accumulative", "resetting"]

"""
def get_index_names_from_stats(stats):
	index_names = {}
	for k in stats:
		if k.startswith(bucket):
			iname = k.split(":")[-2]
			if iname not in index_names:
				index_names[iname] = True

	return index_names.keys()
"""

def run(filename, pstat, stype):
	f = open(filename, "r")
	i = 0
	sum = 0
	while True:
		l = f.readline()
		if not l:
			break

		if l.find("PeriodicStats") == -1:
			continue

		ll = l[53:]
		stats = json.loads(ll)

		for k in stats:
			if not k.endswith(pstat):
				continue

			if stype == "resetting":
				sum += stats[k]
			else:
				raise NotImplmentedError

	print "Sum =", sum

if __name__ == '__main__':
	if len(sys.argv) != 4:
		print "Usage:\npython sum_stat_all_indexes_all_bbuckets.py <log-file-path> <stat-name> <stat type accumulative or resetting>"
		os._exit(0)


	print "Expected file format: Any indexer.log file without restart"

	filename = sys.argv[1]
	pstat = sys.argv[2]
	stype = sys.argv[3]

	print "filename:", filename
	print "pstat:", pstat
	print "stat type:", stype

	if stype not in supported_stat_types:
		print "Usage:\npython sum_stat_all_indexes_all_bbuckets.py <log-file-path> <stat-name> <stat type \"accumulative\" or \"resetting\">"
		os._exit(0)

	run(filename, pstat, stype)

