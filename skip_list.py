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
        self.nexts = [None] * (level + 1) if nexts is None else nexts

    @property
    def level(self):
        return len(self.nexts) - 1

    def __repr__(self):
        return f'{self.key} = {self.nexts}'
        # return f'<Node(key={self.key}, value={self.value}, level={self.level}, nexts={self.nexts})>'


class List:
    def __init__(self, max_level):
        self.head = Node(None, None, max_level)
        self.max_level = max_level

    def __repr__(self):
        r = []
        current = self.head
        while current.nexts:
            current = current.nexts[-1]
            if current is None:
                break
            r.append((current.key, current.value))
        return repr(r)

    def get(self, key):

        updates = self.get_updates(key)

        for update in updates:
            if update is None:
                continue
            for n in update.nexts:
                if n is None:
                    continue
                if n.key == key:
                    return n.value

        return None

    def insert(self, key, value):

        updates = self.get_updates(key)

        for update in updates:
            if update is None:
                continue
            for n in update.nexts:
                if n is None:
                    continue
                if n.key == key:
                    n.value = value
                    return

        level = random_level(self.max_level)

        node = Node(key, value, level)

        for i in range(level + 1):
            current_idx = self.max_level - i
            update = updates[current_idx]
            if update is None:
                continue
            n = update.nexts[update.level - i]
            node.nexts[level - i] = n
            update.nexts[update.level - i] = node

    def get_updates(self, key):
        updates = [self.head] * (self.max_level + 1)

        current = self.head

        while True:

            flag = False
            for n in current.nexts:
                if n is None:
                    continue

                if n.key < key:
                    for i in range(current.level + 1):
                        updates[self.max_level - i] = current
                    current = n
                    flag = True
                    break

            if flag:
                continue

            for i in range(current.level + 1):
                updates[self.max_level - i] = current
            break

        return updates

    def remove(self, key):

        updates = self.get_updates(key)

        node = None

        for update in updates:

            if update is None:
                continue

            for i, n in enumerate(update.nexts):
                if n is None:
                    continue

                if n.key == key:
                    i0 = n.level - update.level + i
                    update.nexts[i] = n.nexts[i0]
                    if node is None:
                        node = n

                if n.key < key:
                    break

        if node is not None:
            return node.value


def test_random():
    l = List(10)
    seen = set()
    kvs = []
    for _ in range(10000):
        k = random.randint(0, 1000)
        v = random.randint(0, 1000)
        if k in seen:
            continue
        seen.add(k)
        kvs.append((k, v))
    for k, v in kvs:
        l.insert(k, v)
    for k, v in kvs:
        if l.get(k) != v:
            print((k, v))
        assert v == l.get(k)

    for k, v in kvs:
        assert v == l.remove(k)

    for k, _ in kvs:
        assert None == l.get(k)


if __name__ == '__main__':
    print('running...')
    test_random()
    print('done!')
