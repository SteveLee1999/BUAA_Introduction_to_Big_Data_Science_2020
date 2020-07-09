import sys

orderid2item = {}
orderid2order = {}

for line in sys.stdin:
    line = line.strip()
    order_id, value = line.split('\t', 1)
    try:
        if value[0] == 'order':
            orderid2order[order_id] = value
        elif value[0] == 'line_item':
            orderid2item.setdefault(order_id, [])
            orderid2item[order_id].append(value)
    except ValueError:
        pass

for order_id in orderid2item:
    for item in orderid2item[order_id]:
        print((orderid2order[order_id] + item))
