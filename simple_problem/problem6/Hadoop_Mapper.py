import sys
import json

for line in sys.stdin:
    line = line.strip()
    record = json.loads(line)

    maxI = 10
    maxJ = 10
    if record[0] == 'a':
        i = record[1]
        for j in range(maxJ + 1):
            print('%s\t%s', ((i, j), record))
    elif record[0] == 'b':
        j = record[2]
        for i in range(maxI + 1):
            print('%s\t%s', ((i, j), record))
    else:
        pass
