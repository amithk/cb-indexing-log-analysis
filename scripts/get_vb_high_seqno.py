import sys
import os

def process_path(d_act, d_rep, path, bucket):
	filepath = os.path.join(path, "stats.log")

	print "Processing file:", filepath

	if not os.path.exists(filepath):
		print "File does not exist:", filepath
		return

	ctx = 0

	last_vb = -1
	last_seqno = -1

	f = open(filepath)
	while True:
		l = f.readline()
		if not l:
			print "Done processing file:", filepath
			break

		# print "Line:", l

		if ctx == 0:
			# Find stats block
			if l.find("memcached stats checkpoint") == -1:
				continue

			l1 = f.readline()
			if not l1:
				break

			l2 = f.readline()
			if not l2:
				break

			l3 = f.readline()
			if not l3:
				break

			# stats block found
			print "Found stats block"
			ctx = 1

		elif ctx == 1:
 
			# Find bucket stats

			ll = l.strip()
			if ll != bucket:
				continue

			print "Found bucket"
			# Bucket stats found
			ctx = 2 

		elif ctx == 2:
			if l.find("=======================") != -1:
				# End of stats block has been reached.
				return
	
			if l.find("***********************") != -1:
				# TODO:
				# This needs testing

				# End of stats block or the bucket has been reached.
				return

			# Parse stats
			if l.find("persistence:cursor_seqno") != -1:
				ll = l.strip()
				ll1 = ll.split(" ")
				if len(ll1) <= 0:
					continue

				seqno = int(ll1[-1])

				ll2 = ll.split(":")
				if len(ll2) <= 0:
					continue

				ll3 = ll2[0].split("_")
				if len(ll3) <= 1:
					continue
					
				vb = int(ll3[1])
				last_vb = vb
				last_seqno = seqno

			if l.find("state:") != -1:
				ll = l.strip()
				ll1 = ll.split(" ")
				if len(ll1) <= 0:
					continue

				state = ll1[-1]
				if state == "active":
					d_act[last_vb] = last_seqno
					last_vb = -1
					last_seqno = -1
				elif state == "replica":	
					d_rep[last_vb] = last_seqno
					last_vb = -1
					last_seqno = -1

def run():
	d_act = {}
	d_rep = {}

	folder = sys.argv[1]
	bucket = sys.argv[2]

	children = os.listdir(folder)
	for child in children:
		fullpath = os.path.join(folder, child)

		print "Checking path:", fullpath

		if not os.path.isdir(fullpath):
			print "Path is not a directory:", fullpath
			continue

		
		print "Processing path:", fullpath
		process_path(d_act, d_rep, fullpath, bucket)

	print "\nResults:"

	print "Total number of vbs Active:", len(d_act)
	print "High Seqnos Active:", d_act

	sum_seqnos_act = 0
	for vb in d_act:
		sum_seqnos_act += d_act[vb]

	print "Sum of Seqnos Active:", sum_seqnos_act

	print "Total number of vbs Replica:", len(d_rep)
	print "High Seqnos Replica:", d_rep

	sum_seqnos_rep = 0
	for vb in d_rep:
		sum_seqnos_rep += d_rep[vb]

	print "Sum of Seqnos Replica:", sum_seqnos_rep


if __name__ == '__main__':
	run()

