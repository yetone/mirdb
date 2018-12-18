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
            # if node.nexts[-1].key >= key:
            #     break
            for n in node.nexts:
                if n.key < key:
                    node = n
                    break
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

        if not self.head.nexts:
            new_node = Node(key, value, 0)
            if self.head.key > key:
                new_node.nexts.append(self.head)
                self.head = new_node
                return
            self.head.nexts.append(new_node)
            return

        level = random_level(self.max_level)

        if level > self.head.level:
            level = self.head.level + 1

        updates = self.get_updates(key, level)

        if level > self.head.level:
            updates.append(self.head)
            self.head.level += 1

        print("updates:", updates)
        new_node = Node(key, value, level)

        for update in updates:
            idx = None
            for i, n in enumerate(update.nexts):
                if n.key == key:
                    n.value = value
                    return
                if n.key < key:
                    idx = i
                    break

            if not update.nexts:
                update.nexts.append(new_node)
            else:
                if idx is None:
                    idx = len(update.nexts) - 1
                n = update.nexts[idx]
                if n.level != level:
                    update.nexts.insert(idx, new_node)

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
        last = node.nexts.pop()
        node.nexts = []
        for n0 in node0.nexts:
            if n0.key != last.key:
                node.nexts.append(n0)
        node.nexts.append(last)

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
            # if not head.nexts:
            #     head.nexts = self.head.nexts
            #     head.level = self.head.level - 1
            #     self.head = head
            #     return
            self.merge_nexts(head, self.head)
            self.head = head

        updates = self.get_updates(key, self.head.level + 1)
        print("updates:", updates)

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
            if update.level > 0:
                update.level -= 1


if __name__ == '__main__':
    head = Node(key=1, value=1, level=2, nexts=[Node(key=7, value=7, level=2, nexts=[]), Node(key=4, value=1, level=1, nexts=[Node(key=7, value=7, level=2, nexts=[])]), Node(key=3, value=6, level=0, nexts=[Node(key=4, value=1, level=1, nexts=[Node(key=7, value=7, level=2, nexts=[])])])])
    l = List(3)
    l.head = head
    print("head:", l.head)
    l.remove(1)
    print("head:", l.head)
    print(l)


if __name__ == '__main0__':
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
