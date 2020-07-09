import sys

friend2count = {}

for line in sys.stdin:
    line = line.strip()
    name, count = line.split('\t', 1)
    try:
        count = int(count)
        friend2count[name] = friend2count.get(name, 0) + count
    except ValueError:
        pass

for name in friend2count:
    print('[%s\t%s]' % (name, friend2count[name]))
