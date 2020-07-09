import sys

person2friends = {}

for line in sys.stdin:
    line = line.strip()
    person1, person2 = line.split('\t', 1)
    person2friends.setdefault(person1, [])
    person2friends.setdefault(person2, [])
    person2friends[person1].append(person2)
    person2friends[person2].append(person1)

for person in person2friends:
    friendCount = {}
    for friend in person2friends.get(person):
        friendCount.setdefault(friend, 0)
        friendCount[friend] = friendCount[friend] + 1

    asymfriends = filter(lambda x: friendCount[x] == 1, friendCount.keys())

    for friend in asymfriends:
        print('%s\t%s' % (person, friend))
