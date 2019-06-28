import sys

class bfileInfo:
	def __init__(self, reqid, bfile):
		self.reqid = reqid
		self.bfile = bfile
		self.closed = False
		self.errored = False

	def Closed(self):
		self.closed = True

	def Errored(self):
		self.errored = True

def run(filepath):

	bfiles = {}
	onlyRemove = {}
	multipleReqids = {}

	f = open(filepath)
	while True:
		l = f.readline()
		if not l:
			break

		if l.find("new backfill file") != -1:
			ll = l.split(" ")
			bfile = ll[8].strip()
			reqid = ll[3].strip()
			binfo = bfiles.get(bfile)
			if not binfo:
				binfo = bfileInfo(reqid, bfile) 
				bfiles[bfile] = binfo
			else:	
				binfo = bfileInfo(reqid, bfile) 
				multipleBinfos[bfile] = binfo

		if l.find("removing backfill file") != -1:	
			ll = l.split(" ")
			bfile = ll[7].strip()
			reqid = ll[3].strip()[8:-1]
			binfo = bfiles.get(bfile, None)
			if not binfo:
				binfo = bfileInfo(reqid, bfile)
				onlyRemove[bfile] = binfo
			else:
				if binfo.reqid == reqid:
					binfo.Closed()
				else:
					binfo = bfileInfo(reqid, bfile)
					multipleBinfos[bfile] = binfo

		if l.find("remove backfill file") != -1 and l.find("unexpected failure") != -1:
			ll = l.split(" ")
			bfile = ll[6].strip()
			binfo = bfiles.get(bfile)
			if not binfo:
				binfo = onlyRemove.get(bfile)
				if not binfo:
					print "Ignoring line", l
				else:
					binfo.Errored()
			else:
				binfo.Errored()

	closed = len([bi for bf, bi in bfiles.items() if bi.closed == True])
	errored = len([bi for bf, bi in bfiles.items() if bi.errored == True])
	print "Total new backfill files created:", len(bfiles)
	print "Total new backfill files closed:", closed
	print "Total new backfill files close errors:", errored
	print "Total new backfill files only removed:", len(onlyRemove)
	print "Total new backfill files multiple Reqids:", len(multipleReqids)
	print "Multiple Reqids:", multipleReqids
	# print "OnlyRemove:", onlyRemove
	# print "Bfiles:", bfiles


if __name__ == "__main__":
	if len(sys.argv) != 2:
		print "Usage: python get_backfill_info.py <path-to-query.log-file>"

	filepath = sys.argv[1]
	run(filepath)

