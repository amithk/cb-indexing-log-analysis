import json
import sys
import os

def get_index_names_from_stats(stats):
	index_names = {}
	for k in stats:
		if k.startswith(bucket):
			iname = k.split(":")[-2]
			if iname not in index_names:
				index_names[iname] = True

	return index_names.keys()

def run(filename, bucket, pstat):
	f = open(filename, "r")
	changed = {}
	prev = {}
	i = 0
	while True:
		i += 1
		#print "i = ", i
		l = f.readline()
		if not l:
			break

		if l.find("PeriodicStats") == -1:
			continue

		ll = l[53:]
		d = json.loads(ll)

		index_names = get_index_names_from_stats(d)

		for iname in index_names:
			found_print = False
			ch = False
			pstat_key = bucket + ":" + iname + ":" + pstat
			pstat_val = d.get(pstat_key, -1)
			if pstat_val == -1:
				continue

			if prev.get(iname, -10000) == -10000:
				found_print = True
				prev[iname] = pstat_val
			else:
				if prev[iname] != pstat_val:
					ch = True

			if prev == pstat_val:
				continue

			if ch:
				changed[iname] = True

			if found_print or ch:
				print "For index", iname, pstat, "at", l[:16], "is", pstat_val

			prev[iname] = pstat_val

	print pstat, "changed for the indexes: ", changed.keys()
	if len(index_names) == len(changed.keys()):
		print pstat, "changed for all indexes"

if __name__ == '__main__':
	if len(sys.argv) != 4:
		print "Usage:\npython analyse_stat_change_over_time.py <log-file-path> <bucket-name> <stat-name>"
		os._exit(0)

	filename = sys.argv[1]
	bucket = sys.argv[2]
	pstat = sys.argv[3]

	print "filename:", filename
	print "bucket:", bucket
	print "pstat:", pstat
	run(filename, bucket, pstat)

