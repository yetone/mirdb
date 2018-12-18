import random


def random_level(max_level):
    k = 0
    while random.randint(1, 100) % 2 and k < max_level:
        k += 1
    return k


class Node:
    def __init__(self, key, value, level):
        self.key = key
        self.value = value
        self.nexts = []
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
        self.head = None
        self.max_level = max_level

    def __repr__(self):
        r = []
        if not self.head:
            return repr(r)
        node = self.head
        r.append(node.value)
        while node.nexts:
            # print("node:", node)
            node = node.nexts[-1]
            r.append(node.value)
        return repr(r)

    def get(self, key):
        print(f"get {key}")
        if self.head is None:
            return None

        if self.head.key > key:
            return None

        if self.head.key == key:
            return self.head.value

        node = self.head
        while node.nexts:
            # print("node:", node)
            # print("node.nexts[-1].key:", node.nexts[-1].key)
            if node.nexts[-1].key >= key:
                break
            for n in node.nexts:
                if n.key < key:
                    node = n
                    break

        if node.key == key:
            return node.value

        for n in node.nexts:
            if n.key == key:
                return n.value

        return None

    def insert(self, key, value):
        print(f"insert {key} {value}")
        if self.head is None:
            self.head = Node(key, value, 0)
            return

        if self.head.key == key:
            self.head.value = value
            return

        if self.head.key > key:
            new_node = Node(key, value, 0)
            new_node.nexts.append(self.head)
            self.head = new_node
            return

        l = len(self.head.nexts)

        if l == 0:
            new_node = Node(key, value, 0)
            if self.head.key > key:
                new_node.nexts.append(self.head)
                self.head = new_node
                return
            self.head.nexts.append(new_node)
            return

        level = random_level(self.max_level)

        updates = self.get_updates(key, level)

        if level > self.head.level:
            updates.append(self.head)
            self.head.level += 1

        print("updates:", updates)

        new_node = Node(key, value, level)

        for update in updates:
            if not update.nexts:
                update.nexts.append(new_node)
                continue
            idx = len(update.nexts) - 1
            for i, n in enumerate(update.nexts):
                if n.key == key:
                    n.value = value
                    return
                if n.key < key:
                    idx = i
                    break
            n = update.nexts[i]
            if n.level != level:
                update.nexts.insert(i, new_node)

    def get_updates(self, key, level):
        updates = []

        if not self.head or self.head.key >= key:
            return updates

        node = self.head

        while True:
            if not node.nexts:
                updates.append(node)
                break

            if node.nexts[-1].key >= key:
                updates.append(node)
                break

            l = False
            for n in node.nexts:
                if n.key < key:
                    if l and n.level <= level:
                        updates.append(node)
                    node = n
                    break
                l = True

        return updates

    def merge_nexts(self, node, node0):
        node.level = node0.level
        if not node.nexts:
            node.nexts = node0.nexts
            return
        for n0 in node0.nexts:
            idx = len(node.nexts) - 1
            for i, n in enumerate(node.nexts):
                if n0.key == n.key:
                    break
                if n0.key < n.key:
                    idx = i
                    break
            n = node.nexts[idx]
            if n.key == n0.key:
                continue
            node.nexts.insert(idx, n0)

    def remove(self, key):
        print("remove:", key)

        if self.head is None:
            return None

        if self.head.key > key:
            return None

        if self.head.key == key:
            if not self.head.nexts:
                self.head = None
                return
            head = self.head.nexts.pop()
            if not head.nexts:
                head.nexts = self.head.nexts
                head.level = self.head.level - 1
                self.head = head
                return
            self.merge_nexts(head, self.head)
            self.head = head

        updates = self.get_updates(key, self.head.level + 1)
        print("updates:", updates)

        for update in updates:
            if not update.nexts:
                return

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
            if update.level > 0:
                update.level -= 1


if __name__ == '__main__':
    l = List(3)
    print("head:", l.head)
    l.insert(3, 2)
    print("head:", l.head)
    print("l:", len(l.head.nexts))
    l.insert(1, 3)
    print("head:", l.head)
    print("l:", len(l.head.nexts))
    l.insert(4, 1)
    print("head:", l.head)
    print("l:", len(l.head.nexts))
    l.insert(7, 4)
    print("head:", l.head)
    print("l:", len(l.head.nexts))
    assert l.get(3) == 2
    assert l.get(0) == None
    print("l:", len(l.head.nexts))
    print(l)
    l.insert(7, 7)
    print("head:", l.head)
    print("l:", len(l.head.nexts))
    assert l.get(3) == 2
    assert l.get(0) == None
    print("l:", len(l.head.nexts))
    print(l)
    l.insert(3, 6)
    print(l)
    l.insert(1, 1)
    print("head:", l.head)
    print(l)
    l.remove(1)
    print("head:", l.head)
    print(l)
    l.remove(7)
    print("head:", l.head)
    print(l)
    l.remove(3)
    print("head:", l.head)
    print(l)
