"""
Tests for validating that the MirDB poem references key-value store concepts.

This test module validates Scenario 1 (REQ-1): Key-Value Store Reference Validation
- Test Case 1: Poem contains at least one reference to key-value storage concept
- Test Case 2: References are technically accurate and do not misrepresent functionality
"""

import unittest
import os
import re


class PoemKeyValueReferenceTest(unittest.TestCase):
    """Test suite for validating key-value store references in the MirDB poem."""

    POEM_PATH = os.path.join(os.path.dirname(__file__), '..', 'POEM.md')

    # Explicit key-value terminology patterns
    EXPLICIT_KV_PATTERNS = [
        r'\bkey-value\b',
        r'\bkey\s+value\b',
        r'\bkeys?\s+and\s+values?\b',
        r'\bkeys?\b.*\bvalues?\b',
        r'\bvalues?\b.*\bkeys?\b',
    ]

    # Metaphorical references to key-value concepts as per PRD Appendix A
    METAPHORICAL_KV_PATTERNS = [
        r'\bkeeper\s+of\s+pairs\b',
        r'\bguardian\s+of\s+secrets\b',
        r'\bpairs?\b',
        r'\bstore\b',
        r'\bstorage\b',
        r'\bunlocks?\b',
        r'\btreasured?\b',
    ]

    # Technical accuracy patterns - things that SHOULD be in the poem
    ACCURATE_CLAIMS = [
        r'\bpersist',  # persistence is a core feature
        r'\bdisk\b',   # data goes to disk
        r'\bstore\b',  # it's a store
        r'\bmemory\b', # memtable is in memory
    ]

    # Technical inaccuracy patterns - things that should NOT be in the poem
    INACCURATE_CLAIMS = [
        r'\brelational\b',      # MirDB is NOT relational
        r'\bSQL\b',             # MirDB doesn't support SQL
        r'\btables?\b(?!\s+pressed)',  # No tables (except metaphorical "tablets pressed")
        r'\bjoins?\b',          # No joins
        r'\bschema\b',          # No schema
    ]

    @classmethod
    def setUpClass(cls):
        """Load the poem content once for all tests."""
        with open(cls.POEM_PATH, 'r') as f:
            cls.poem_content = f.read()
        cls.poem_content_lower = cls.poem_content.lower()

    def test_poem_file_exists(self):
        """Verify the poem file exists at the expected location."""
        self.assertTrue(
            os.path.exists(self.POEM_PATH),
            f"Poem file not found at {self.POEM_PATH}"
        )

    def test_poem_not_empty(self):
        """Verify the poem has content."""
        self.assertGreater(
            len(self.poem_content.strip()),
            0,
            "Poem file is empty"
        )

    def test_contains_explicit_key_value_reference(self):
        """
        Test Case 1: Verify poem contains explicit key-value terminology.

        The poem must contain at least one explicit reference to key-value
        concepts like 'key', 'value', 'key-value store', etc.
        """
        found_patterns = []
        for pattern in self.EXPLICIT_KV_PATTERNS:
            if re.search(pattern, self.poem_content_lower):
                found_patterns.append(pattern)

        self.assertGreater(
            len(found_patterns),
            0,
            f"Poem does not contain any explicit key-value references. "
            f"Searched patterns: {self.EXPLICIT_KV_PATTERNS}"
        )

    def test_contains_metaphorical_key_value_reference(self):
        """
        Test Case 1 (supplementary): Verify poem contains metaphorical references.

        According to the PRD, references can be metaphorical like
        'keeper of pairs', 'guardian of secrets', etc.
        """
        found_patterns = []
        for pattern in self.METAPHORICAL_KV_PATTERNS:
            if re.search(pattern, self.poem_content_lower):
                found_patterns.append(pattern)

        self.assertGreater(
            len(found_patterns),
            0,
            f"Poem does not contain any metaphorical key-value references. "
            f"Searched patterns: {self.METAPHORICAL_KV_PATTERNS}"
        )

    def test_key_value_references_count(self):
        """
        Test Case 1 (coverage): Verify poem has sufficient key-value coverage.

        The poem should have at least one reference to key-value concepts
        (explicit or metaphorical).
        """
        all_patterns = self.EXPLICIT_KV_PATTERNS + self.METAPHORICAL_KV_PATTERNS
        found_count = 0

        for pattern in all_patterns:
            matches = re.findall(pattern, self.poem_content_lower)
            found_count += len(matches)

        self.assertGreaterEqual(
            found_count,
            1,
            f"Poem should contain at least 1 key-value reference. Found: {found_count}"
        )

    def test_technical_accuracy_positive(self):
        """
        Test Case 2: Verify poem contains accurate technical claims.

        The poem should reference MirDB's actual features:
        - Persistence (data survives restarts)
        - Disk storage (SSTables)
        - In-memory operations (memtable)
        """
        found_accurate = []
        for pattern in self.ACCURATE_CLAIMS:
            if re.search(pattern, self.poem_content_lower):
                found_accurate.append(pattern)

        self.assertGreater(
            len(found_accurate),
            0,
            f"Poem should contain at least one accurate technical claim. "
            f"Expected patterns: {self.ACCURATE_CLAIMS}"
        )

    def test_no_inaccurate_claims(self):
        """
        Test Case 2: Verify poem does not misrepresent MirDB's functionality.

        MirDB is a key-value store, NOT a relational database.
        The poem should not claim SQL support, tables, joins, or schemas.
        """
        found_inaccurate = []
        for pattern in self.INACCURATE_CLAIMS:
            match = re.search(pattern, self.poem_content_lower)
            if match:
                found_inaccurate.append((pattern, match.group()))

        self.assertEqual(
            len(found_inaccurate),
            0,
            f"Poem contains inaccurate claims about MirDB: {found_inaccurate}. "
            f"MirDB is a key-value store, not a relational database."
        )

    def test_key_value_store_phrase_present(self):
        """
        Test Case 1 & 2: Verify the phrase 'key-value store' appears.

        This is the most direct and accurate way to describe MirDB.
        """
        pattern = r'key-value\s+store'
        match = re.search(pattern, self.poem_content_lower)
        self.assertIsNotNone(
            match,
            "Poem should contain the phrase 'key-value store' to clearly "
            "identify MirDB's nature"
        )

    def test_persistence_accurately_represented(self):
        """
        Test Case 2: Verify persistence is accurately represented.

        MirDB persists data to disk, unlike memcached which is volatile.
        The poem should convey this durability concept.
        """
        persistence_patterns = [
            r'\bpersist',
            r'\bforevermore\b',
            r'\bnever\s+part\b',
            r'\bendless\s+time\b',
            r'\bnever\s+(fades?|dies?|lies?)\b',
        ]

        found_persistence = []
        for pattern in persistence_patterns:
            if re.search(pattern, self.poem_content_lower):
                found_persistence.append(pattern)

        self.assertGreater(
            len(found_persistence),
            0,
            f"Poem should convey data persistence. Searched: {persistence_patterns}"
        )


class PoemStructureTest(unittest.TestCase):
    """Supplementary tests for poem structure and quality."""

    POEM_PATH = os.path.join(os.path.dirname(__file__), '..', 'POEM.md')

    @classmethod
    def setUpClass(cls):
        """Load the poem content once for all tests."""
        with open(cls.POEM_PATH, 'r') as f:
            cls.poem_content = f.read()

    def test_poem_line_count_in_range(self):
        """Verify poem is within the recommended 12-24 lines (REQ-7)."""
        # Remove header and empty lines for counting
        lines = [
            line for line in self.poem_content.split('\n')
            if line.strip() and not line.startswith('#')
        ]
        line_count = len(lines)

        self.assertGreaterEqual(
            line_count,
            12,
            f"Poem should have at least 12 lines. Found: {line_count}"
        )
        self.assertLessEqual(
            line_count,
            24,
            f"Poem should have at most 24 lines. Found: {line_count}"
        )


if __name__ == '__main__':
    unittest.main()
