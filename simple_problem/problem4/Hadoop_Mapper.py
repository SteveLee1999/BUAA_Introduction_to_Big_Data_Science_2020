import sys
import json

for line in sys.stdin:
    line = line.strip()
    record = json.loads(line)
    print('%s\t%s' % (record[0], record[1]))
    print('%s\t%s' % (record[1], record[0]))
