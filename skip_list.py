import random


def random_level(max_level):
    k = 0
    while random.randint(1, 100) % 2 and k < max_level:
        k += 1
    return k


class Node:
    def __init__(self, key, value, level, nexts=None):
        self.key = key
        self.value = value
        self.nexts = [] if nexts is None else nexts
        self.level = level

    def find_lower_bound(self, key):
        if self.key >= key:
            return None

        for n in self.nexts:
            if n.key < key:
                return n.find_lower_bound(key)

        return self

    def __repr__(self):
        return f'<Node(key={self.key}, value={self.value}, level={self.level}, nexts={self.nexts})>'


class List:
    def __init__(self, max_level):
        self.head = Node(None, None, max_level)
        self.max_level = max_level

    def __repr__(self):
        r = []
        current = self.head
        while current.nexts:
            current = current.nexts[-1]
            r.append((current.key, current.value))
        return repr(r)

    def get(self, key):

        updates = self.get_updates(key)

        for update in updates:
            if update is None:
                continue
            for n in update.nexts:
                if n.key == key:
                    return n.value

        return None

    def insert(self, key, value):

        updates = self.get_updates(key)

        for update in updates:
            if update is None:
                continue
            for n in update.nexts:
                if n.key == key:
                    n.value = value
                    return

        level = random_level(self.max_level)

        new_node = Node(key, value, level)

        for update in list(reversed(updates))[:level + 1]:
            if update is None:
                continue

            idx = None
            for i, n in enumerate(update.nexts):
                if n.key < key:
                    idx = i
                    break

            if not update.nexts:
                update.nexts.append(new_node)
            else:
                if idx is None:
                    idx = len(update.nexts) - 1
                n = update.nexts[idx]
                update.nexts[idx] = new_node
                update.nexts.append(n)

    def get_updates(self, key):
        updates = [None] * (self.max_level + 1)

        current = self.head

        while current is not None:

            if not current.nexts:
                updates[self.max_level - current.level] = current
                break

            if current.nexts[-1].key >= key:
                updates[self.max_level - current.level] = current
                break

            for n in current.nexts:
                if n.key < key:
                    updates[self.max_level - current.level] = current
                    current = n
                    break

        return updates

    def merge_nexts(self, node, node0):
        node.level = node0.level
        if not node.nexts:
            node.nexts = node0.nexts
            return
        last = node.nexts.pop()
        node.nexts = []
        for n0 in node0.nexts:
            if n0.key != last.key:
                node.nexts.append(n0)
        node.nexts.append(last)

    def remove(self, key):

        updates = self.get_updates(key)

        for update in updates:

            if not update.nexts:
                continue

            idx = None

            for i, n in enumerate(update.nexts):
                if n.key == key:
                    idx = i
                    break
                if n.key < key:
                    break

            if idx is None:
                continue

            node = update.nexts.pop(idx)
            self.merge_nexts(update, node)


if __name__ == '__main__':
    l = List(10)
    kvs = [(random.randint(0, 1000), random.randint(0, 1000)) for _ in range(10000)]
    kvs = [(1,2), (3, 4), (0,3)]
    for k, v in kvs:
        l.insert(k, v)
    print(l.head)
    for k, v in kvs:
        if l.get(k) != v:
            print((k, v))
        assert v == l.get(k)
