import sys

class Indexer:
	def __init__(self):
		self.totalMemory = 0
		self.totalMemoryR = ""
		self.memory = 0
		self.memoryR = ""
		self.overhead = 0
		self.overheadR = ""
		self.data = 0
		self.dataR = ""
		self.indexes = []
		self.name = ""
		self.totalActual = 0
		self.totalAR = ""
		self.memoryActual = 0
		self.memoryAR = ""

	def printIndexer(self):
		print "total", self.totalMemoryR
		print "mem", self.memoryR
		print "overhead", self.overheadR
		print "data", self.dataR
		print "Actual total", self.totalAR
		print "Actual memory", self.memoryAR

		print "Total indexes", len(self.indexes)
		for index in self.indexes:
			if index.estimated:
				print ">>>>>>>>>>>", index.name, ">>>>>>>>>>>"
				index.printIndex()

class Index:
	def __init__(self):
		self.totalMemory = 0
		self.totalMemoryR = ""
		self.memory = 0
		self.memoryR = ""
		self.overhead = 0
		self.overheadR = ""
		self.data = 0
		self.dataR = ""
		self.estimated = False
		self.name = ""

	def printIndex(self):
		print "total", self.totalMemoryR
		print "mem", self.memoryR
		print "overhead", self.overheadR
		print "data", self.dataR

class Run:
	def __init__(self):
		self.indexers = []

	def addIndexer(self, indexer):
		self.indexers.append(indexer)

	def printRun(self):
		print "Total number of indexers:", len(self.indexers)
		for indexer in self.indexers:
			print "----------------------", indexer.name, "----------------------"
			indexer.printIndexer()
			# print "--------------------------------------------"

class Analyser:
	def __init__(self, logfile):
		self.logfile = logfile
		self.valid = True
		self.runs = []
		self.currIndexer = None
		self.currIndex = None

	def processIndex(self, l, ru):
		ll = l.split(" ")
		# print ll
		if l.find("Index name:") != -1:
			self.currIndex = Index()
			self.currIndex.name = ll[1][len("name:"):]

		# ['\t\tIndex', 'total', 'memory:0', '(0),', 'mem:0', '(0),', 'overhead:0', '(0),', 'data:16384', '(16K)', 'cpu:0.0000', 'io:0', '(0)', 'scan:0', 'drain:0\n']
		# ['\t\tIndex', 'resident:100%', 'build:0%', 'estimated:false', 'equivCheck:true', 'pendingCreate:false', 'pendingDelete:false\n']

		if l.find("Index total memory:") != -1:
			self.currIndex.totalMemory = int(ll[2][len("memory:"):])
			self.currIndex.totalMemoryR = ll[3]
			self.currIndex.memory = int(ll[4][len("mem:"):])
			self.currIndex.memoryR = ll[5]
			self.currIndex.overhead = int(ll[6][len("overhead:"):])
			self.currIndex.overheadR = ll[7]
			self.currIndex.data = int(ll[8][len("data:"):])
			self.currIndex.dataR = ll[9]

		if l.find("resident:") != -1:
			if ll[3] == "estimated:true":
				self.currIndex.estimated = True

			self.currIndexer.indexes.append(self.currIndex)

	def processIndexer(self, l, ru):
		if l.find("total memory") == -1:
			return

		ll = l.split(" ")
		# print "ll[1]", ll[1]
		self.currIndexer.totalMemory = int(ll[2][len("memory:"):])
		self.currIndexer.totalMemoryR = ll[3]
		self.currIndexer.memory = int(ll[4][len("mem:"):])
		self.currIndexer.memoryR = ll[5]
		self.currIndexer.overhead = int(ll[6][len("overhead:"):])
		self.currIndexer.overheadR = ll[7]
		self.currIndexer.data = int(ll[8][len("data:"):])
		self.currIndexer.dataR = ll[9]
		ru.addIndexer(self.currIndexer)	

	def processline(self, l, ru):
		if l.find("Indexer") != -1:
			if l.find("nodeId:") != -1:
				ll = l.split()
				self.currIndexer = Indexer()
				self.currIndexer.name = ll[2][len("nodeId:"):]
				return

			self.processIndexer(l, ru)
			return

		if l.find("Index name:") != -1 or l.find("Index total memory:") != -1 or l.find("Index resident:") != -1:
			self.processIndex(l, ru)
			return

	def ex(self):
		f = open(self.logfile)
		r = False
		ru = None
		while True:
			l = f.readline()
			if not l:
				break

			l = l[len("2020-05-18T22:15:47.546-07:00 [Info] "):]

			if l.find("************ Indexer Layout *************") != -1:
				# print "Starting"
				r = True
				ru = Run()

			if not r:
				# print "Continuing"
				continue

			if l.find("****************************************") != -1:
				# print "Ending"
				r = False
				if ru == None:
					# print "Ending Continue"
					continue

				self.runs.append(ru)
				ru = None

			self.processline(l, ru)

		self.calculateActuals()

		self.printRuns()

	def calculateActuals(self):
		for r in self.runs:
			for indexer in r.indexers:
				totalA = 0
				memA = 0
				for index in indexer.indexes:
					if index.estimated:
						totalA += index.totalMemory
						memA += index.memory

				indexer.totalActual = indexer.totalMemory - totalA
				indexer.totalAR = formatValue(indexer.totalActual)
				indexer.memoryActual = indexer.memory - memA
				indexer.memoryAR = formatValue(indexer.memoryActual)


	def printRuns(self):
		print "Total runs", len(self.runs)
		i = 0
		for r in self.runs:
			i += 1
			print "*****************************************************************"
			print "Run", i
			r.printRun()
			# print "*****************************************************************"

def formatValue(val):
	v = val / (1024.0 * 1024.0 * 1024.0 * 1024.0)

	if v > 1:
		r = "(%.3fT)" % (v,)
		return r

	v = v * 1024.0
	if v > 1:
		r = "(%.3fG)" % (v,)
		return r

	v = v * 1024.0
	if v > 1:
		r = "(%.3fM)" % (v,)
		return r

	v = v * 1024.0
	if v > 1:
		r = "(%.3fK)" % (v,)
		return r

	return "(%s)" % (val,)

if __name__ == "__main__":
	logfile = sys.argv[1]
	a = Analyser(logfile)
	a.ex()

	
