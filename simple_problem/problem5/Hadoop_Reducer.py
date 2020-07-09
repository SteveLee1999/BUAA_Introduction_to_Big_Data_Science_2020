import sys

for line in sys.stdin:
    line = line.strip()
    trimmedNucleotide, seqId = line.split('\t', 1)
    print('%s' % (trimmedNucleotide))
