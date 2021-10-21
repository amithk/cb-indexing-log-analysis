import json
import sys

def run(completed):
	f = open(completed)
	l = f.read()
	d = json.loads(l)
	reqs = {}
	for r in d:
		req = r["completed_requests"]
		serviceTime = req["serviceTime"]
		if serviceTime.find("m") != -1 and serviceTime.find("ms") == -1:
			reqs[req["requestId"]] = req

	print "Requests running for more than a min are:"
	for req in reqs:
		print req

if __name__ == "__main__":
	if len(sys.argv) != 2:
		print "Prints all requests taking more than 1 min"
		print "Usage:"
		print "python get_long_running_completed_requests.py <path-to-completed-requests.json>"

	completed = sys.argv[1]
	run(completed)

