"""
Test Suite for LSM-Tree Architecture Metaphor Validation (Scenario 3)

This module validates that the MirDB poem metaphorically represents the LSM-tree
architecture as specified in REQ-3 of the PRD.

Test Cases:
1. Validate layered/hierarchical data structure metaphors
2. Validate LSM component references (memtable, SSTable, compaction, WAL)
"""

import unittest
import re
import os


class PoemLoader:
    """Helper class to load the poem content."""

    POEM_PATH = os.path.join(os.path.dirname(__file__), '..', 'versecraft', 'mirdb_poem.txt')

    @classmethod
    def load_poem(cls) -> str:
        """Load and return the poem content."""
        with open(cls.POEM_PATH, 'r', encoding='utf-8') as f:
            return f.read()

    @classmethod
    def get_poem_lines(cls) -> list:
        """Return poem as list of lines."""
        return cls.load_poem().strip().split('\n')


class TestLSMTreeMetaphorLayeredStructure(unittest.TestCase):
    """
    Test Case 1: Validate layered/hierarchical data structure metaphors

    Expected: Contains metaphorical representation of layered/hierarchical data structure

    The LSM-tree architecture has multiple levels:
    - Memtable (active in-memory)
    - Immutable memtables queue
    - Level 0 SSTables (recently flushed)
    - Level 1+ SSTables (sorted, non-overlapping)
    """

    @classmethod
    def setUpClass(cls):
        cls.poem_content = PoemLoader.load_poem().lower()
        cls.poem_lines = PoemLoader.get_poem_lines()

    def test_poem_exists_and_readable(self):
        """Verify the poem file exists and can be read."""
        self.assertTrue(os.path.exists(PoemLoader.POEM_PATH),
                       "Poem file should exist at versecraft/mirdb_poem.txt")
        self.assertGreater(len(self.poem_content), 0,
                          "Poem should have content")

    def test_layered_structure_metaphors(self):
        """
        Verify the poem contains metaphors about layers, cascading,
        levels, or hierarchical organization.

        LSM-tree context: Has multiple levels from memtable through SSTable levels
        """
        layer_patterns = [
            r'\blayer(?:s|ed)?\b',
            r'\bcascad(?:e|es|ing)\b',
            r'\blevel(?:s)?\b',
            r'\bhierarch(?:y|ical)\b',
            r'\bdepth(?:s)?\b',
            r'\bdeep(?:er|est)?\b',
            r'\bhall(?:s)?\b',  # metaphorical halls/levels
            r'\bstream(?:s)?\b',  # data streams flowing down
            r'\bpool(?:s)?\b',    # memory pools
        ]

        matches_found = []
        for pattern in layer_patterns:
            matches = re.findall(pattern, self.poem_content)
            if matches:
                matches_found.extend(matches)

        self.assertGreater(len(matches_found), 0,
            "Poem should contain metaphors about layers, cascading, levels, "
            "or hierarchical organization (e.g., 'layers', 'levels', 'cascading', 'depths')")

    def test_data_flow_structure(self):
        """
        Verify the poem represents data flowing through stages.

        LSM-tree data flow: Write -> WAL -> Memtable -> Imm Memtable -> Level 0 -> Level N
        """
        flow_patterns = [
            r'\bflow(?:s|ing)?\b',
            r'\bcours(?:e|es)\b',
            r'\bstream(?:s|ing)?\b',
            r'\bfill(?:s|ing)?\b',
            r'\bfirst\b.*\bthen\b',  # sequential flow
            r'\bstart(?:s)?\b',
            r'\bcascad(?:e|es|ing)\b',
        ]

        matches_found = []
        for pattern in flow_patterns:
            matches = re.findall(pattern, self.poem_content, re.DOTALL)
            if matches:
                matches_found.extend(matches)

        self.assertGreater(len(matches_found), 0,
            "Poem should represent data flowing through stages "
            "(e.g., 'flows', 'cascade', 'streams', 'First...then...')")

    def test_poem_structure_suggests_levels(self):
        """
        Verify the poem's stanza structure metaphorically represents levels.

        A well-structured poem about LSM-tree should have multiple stanzas
        that can represent different levels of the data structure.
        """
        # Count non-empty stanzas (separated by blank lines)
        poem_text = PoemLoader.load_poem()
        stanzas = [s.strip() for s in poem_text.split('\n\n') if s.strip()]

        self.assertGreaterEqual(len(stanzas), 3,
            "Poem should have multiple stanzas representing different "
            "levels/stages of the LSM-tree architecture")


class TestLSMTreeComponentReferences(unittest.TestCase):
    """
    Test Case 2: Validate LSM component references

    Expected: At least one LSM component (memtable, SSTable, compaction, WAL)
    is metaphorically referenced.

    LSM-tree components:
    - WAL (Write-Ahead Log): Safety net, first witness, promise keeper
    - Memtable: Active memory layer, skip list, swift thoughts
    - SSTables: Sorted tablets, persistent storage, ancient tablets
    - Compaction: Refinement, distillation, merging, organizing
    """

    @classmethod
    def setUpClass(cls):
        cls.poem_content = PoemLoader.load_poem().lower()
        cls.poem_original = PoemLoader.load_poem()

    def test_wal_metaphor(self):
        """
        Verify the poem metaphorically references the Write-Ahead Log (WAL).

        WAL characteristics to represent:
        - First step in write path (safety net)
        - Durability guarantee (promise keeper)
        - Crash recovery (witness to data)
        """
        wal_patterns = [
            r'\blog\b',
            r'\bwitness\b',
            r'\bpromise(?:s|d)?\b',
            r'\bfirst\b',
            r'\bsafety\b',
            r'\bwrite(?:s)?\b.*\bfirst\b',
            r'\bfirst\b.*\bwrite(?:s)?\b',
            r'\bink\b',  # writing metaphor
            r'\brecord(?:s|ed)?\b',
        ]

        matches_found = []
        for pattern in wal_patterns:
            matches = re.findall(pattern, self.poem_content, re.DOTALL)
            if matches:
                matches_found.extend(matches)

        self.assertGreater(len(matches_found), 0,
            "Poem should metaphorically reference the WAL "
            "(e.g., 'log', 'witness', 'promise', 'first writes', 'ink')")

    def test_memtable_metaphor(self):
        """
        Verify the poem metaphorically references the Memtable.

        Memtable characteristics to represent:
        - In-memory storage (swift, active)
        - Skip list data structure (interlaced, elegant leaps)
        - Temporary holding before flush (fleeting thoughts)
        """
        memtable_patterns = [
            r'\bmemory\b',
            r'\bmind\b',
            r'\bswift\b',
            r'\bactive\b',
            r'\bskip\b',
            r'\binterlace(?:d|s)?\b',
            r'\bfleeting\b',
            r'\bthought(?:s)?\b',
            r'\bhold(?:s|ing)?\b',
            r'\bpool(?:s)?\b',
        ]

        matches_found = []
        for pattern in memtable_patterns:
            matches = re.findall(pattern, self.poem_content)
            if matches:
                matches_found.extend(matches)

        self.assertGreater(len(matches_found), 0,
            "Poem should metaphorically reference the Memtable "
            "(e.g., 'memory', 'mind', 'swift', 'skip', 'fleeting thoughts')")

    def test_sstable_metaphor(self):
        """
        Verify the poem metaphorically references SSTables.

        SSTable characteristics to represent:
        - Sorted String Tables (sorted, tablets)
        - Persistent disk storage (eternal, permanent)
        - Organized blocks (pages, blocks)
        """
        sstable_patterns = [
            r'\btablet(?:s)?\b',
            r'\bsorted\b',
            r'\bpersistent\b',
            r'\beternal\b',
            r'\bpermanent\b',
            r'\blasting\b',
            r'\bpage(?:s)?\b',
            r'\bblock(?:s)?\b',
            r'\bstore(?:s|d)?\b',
            r'\bdream(?:s)?\b',  # stored/preserved things
            r'\bscroll(?:s)?\b',
        ]

        matches_found = []
        for pattern in sstable_patterns:
            matches = re.findall(pattern, self.poem_content)
            if matches:
                matches_found.extend(matches)

        self.assertGreater(len(matches_found), 0,
            "Poem should metaphorically reference SSTables "
            "(e.g., 'tablets', 'sorted', 'eternal', 'pages', 'stored')")

    def test_compaction_metaphor(self):
        """
        Verify the poem metaphorically references Compaction.

        Compaction characteristics to represent:
        - Merging process (merge, combine)
        - Refinement/optimization (distill, refine)
        - Organization (order from chaos)
        """
        compaction_patterns = [
            r'\bcompaction\b',
            r'\bmerg(?:e|es|ing|ed)\b',
            r'\bdistill(?:ing|ed|s)?\b',
            r'\brefin(?:e|es|ing|ed|ement)\b',
            r'\border\b',
            r'\bchaos\b',
            r'\bcombine(?:s|d)?\b',
            r'\borganiz(?:e|es|ing|ed)\b',
            r'\btireless\b',  # background compaction threads
            r'\bpatient\b',
        ]

        matches_found = []
        for pattern in compaction_patterns:
            matches = re.findall(pattern, self.poem_content)
            if matches:
                matches_found.extend(matches)

        self.assertGreater(len(matches_found), 0,
            "Poem should metaphorically reference Compaction "
            "(e.g., 'merge', 'distilling', 'order from chaos', 'tireless')")

    def test_at_least_one_component_referenced(self):
        """
        Meta-test: Verify at least one LSM component is metaphorically referenced.

        This is the primary acceptance criterion for Test Case 2.
        """
        component_tests = [
            ('WAL', [r'\blog\b', r'\bwitness\b', r'\bpromise\b', r'\bink\b']),
            ('Memtable', [r'\bmemory\b', r'\bmind\b', r'\bswift\b', r'\bskip\b', r'\bfleeting\b']),
            ('SSTable', [r'\btablet(?:s)?\b', r'\bsorted\b', r'\beternal\b', r'\blasting\b']),
            ('Compaction', [r'\bmerg(?:e|ing)\b', r'\bdistill\b', r'\border\b', r'\bchaos\b']),
        ]

        components_found = []
        for component_name, patterns in component_tests:
            for pattern in patterns:
                if re.search(pattern, self.poem_content):
                    components_found.append(component_name)
                    break

        self.assertGreater(len(components_found), 0,
            f"Poem should reference at least one LSM component. "
            f"None found among: WAL, Memtable, SSTable, Compaction")

        # Store for reporting
        self.components_referenced = list(set(components_found))


class TestLSMTreeDataFlowSteps(unittest.TestCase):
    """
    Additional validation: Check the poem follows the LSM-tree data flow pattern.

    Data flow: Write -> WAL -> Memtable -> Imm Memtable -> Level 0 -> Level N SSTables
    """

    @classmethod
    def setUpClass(cls):
        cls.poem_content = PoemLoader.load_poem().lower()
        cls.poem_lines = PoemLoader.get_poem_lines()

    def test_write_path_sequence(self):
        """
        Verify the poem suggests a sequence from memory to disk.

        The LSM-tree write path goes:
        1. First to WAL (durability)
        2. Then to memtable (memory)
        3. Eventually to SSTable (disk)
        """
        # Check for transition indicators
        transitions = [
            r'first.*then',
            r'when.*fill',
            r'cascade.*start',
            r'memory.*disk',
            r'mind.*store',
            r'fleeting.*lasting',
            r'active.*immutable',
        ]

        transition_found = False
        for pattern in transitions:
            if re.search(pattern, self.poem_content, re.DOTALL):
                transition_found = True
                break

        self.assertTrue(transition_found,
            "Poem should suggest a sequence/transition from memory to persistent storage")

    def test_persistence_theme(self):
        """
        Verify the poem conveys the persistence/durability theme.

        MirDB's key differentiator from memcached is persistence.
        """
        persistence_patterns = [
            r'\blast(?:s|ing)?\b',
            r'\beternal\b',
            r'\bpermanent\b',
            r'\bguard(?:s|ian)?\b',
            r'\bkeep(?:s|er)?\b',
            r'\bsav(?:e|es|ed|ing)\b',
            r'\bpreserv(?:e|es|ed|ing)\b',
            r'\brest\b',  # rest your data
            r'\bnever\b.*\bfade\b',
            r'\bnever\b.*\bsleep\b',
        ]

        persistence_found = []
        for pattern in persistence_patterns:
            matches = re.findall(pattern, self.poem_content, re.DOTALL)
            if matches:
                persistence_found.extend(matches)

        self.assertGreater(len(persistence_found), 0,
            "Poem should convey the persistence/durability theme "
            "(e.g., 'lasting', 'eternal', 'guardian', 'keeper', 'never fades')")


if __name__ == '__main__':
    # Run tests with verbosity
    unittest.main(verbosity=2)
