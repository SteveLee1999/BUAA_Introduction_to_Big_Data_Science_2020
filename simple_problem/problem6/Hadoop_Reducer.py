import sys

point2values = {}

for line in sys.stdin:
    line = line.strip()
    key, value = line.split('\t', 1)
    point2values.setdefault(key, [])
    point2values[key].append(value)

for key in point2values:
    values = point2values[key]
    a_rows = filter(lambda x: x[0] == 'a', values)
    b_rows = filter(lambda x: x[0] == 'b', values)

    result = 0
    for a in a_rows:
        for b in b_rows:
            if a[2] == b[1]:
                result += a[3] * b[3]

    if result != 0:
        print((key[0], key[1], result))
