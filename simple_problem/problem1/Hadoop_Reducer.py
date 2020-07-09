import sys

word2document = {}

for line in sys.stdin:
    line = line.strip()
    word, document = line.split('\t', 1)
    word2document.setdefault(word, [])
    if document not in word2document[word]:
        word2document[word].append(document)

for word in word2document:
    print([word, word2document[word]])
