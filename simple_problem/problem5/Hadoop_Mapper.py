import sys
import json

for line in sys.stdin:
    line = line.strip()
    dna = json.loads(line)
    seqId = dna[0]
    nucleotide = dna[1]
    trimmedNucleotide = nucleotide[:-10]
    print('%s\t%s' % (trimmedNucleotide, seqId))
