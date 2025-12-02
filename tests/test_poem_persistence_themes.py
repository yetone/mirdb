"""
Test Suite: Data Persistence Theme Validation (Scenario 2)

This test suite validates that the MirDB poem properly conveys the concept
of data persistence and durability (REQ-2), as specified in the PRD.

Test Cases:
1. Verify poem contains themes of permanence, durability, or eternal storage
2. Verify poem conveys that data survives beyond temporary memory (disk persistence)
"""

import unittest
import os
import re


class TestDataPersistenceThemes(unittest.TestCase):
    """Test case for validating data persistence themes in the MirDB poem."""

    @classmethod
    def setUpClass(cls):
        """Load the poem content once for all tests."""
        poem_path = os.path.join(os.path.dirname(__file__), '..', 'poem.txt')
        with open(poem_path, 'r') as f:
            cls.poem_content = f.read()
        cls.poem_lower = cls.poem_content.lower()

    def test_persistence_theme_contains_permanence_keywords(self):
        """
        Test Case 1: Contains themes of permanence, durability, or eternal storage

        Input: Poem text content
        Expected: Contains themes of permanence, durability, or eternal storage
        Type: manual (automated)

        This test verifies that the poem includes language conveying durability,
        permanence, or data survival as per step 1 of the scenario.
        """
        # Keywords that convey permanence, durability, or eternal storage
        permanence_keywords = [
            'persist', 'persistent', 'persistence',
            'eternal', 'eternity', 'forever',
            'permanent', 'permanently',
            'endure', 'endures', 'enduring',
            'durable', 'durability',
            'survive', 'survives', 'survival',
            'preserve', 'preserved',
            'keep', 'keeper', 'keeps',
            'remember', 'remembers',
            'lasting', 'everlasting',
            'never fade', 'never fades',
            'remain', 'remains'
        ]

        found_keywords = []
        for keyword in permanence_keywords:
            if keyword in self.poem_lower:
                found_keywords.append(keyword)

        # Require at least 3 different permanence-related keywords
        self.assertGreaterEqual(
            len(found_keywords), 3,
            f"Poem should contain at least 3 permanence/durability keywords. "
            f"Found: {found_keywords}"
        )

    def test_persistence_theme_contains_storage_concepts(self):
        """
        Test Case 1 (continued): Verify eternal storage concepts

        Check for references to storage mechanisms that convey permanence.
        """
        storage_concepts = [
            'disk', 'sstable', 'record', 'records',
            'storage', 'store', 'stored',
            'etched', 'written', 'log'
        ]

        found_concepts = []
        for concept in storage_concepts:
            if concept in self.poem_lower:
                found_concepts.append(concept)

        self.assertGreaterEqual(
            len(found_concepts), 2,
            f"Poem should reference at least 2 storage concepts. Found: {found_concepts}"
        )

    def test_persistence_distinguishes_from_volatile_memory(self):
        """
        Test Case 2: Conveys that data survives beyond temporary memory (disk persistence)

        Input: Persistence language in poem
        Expected: Conveys that data survives beyond temporary memory (disk persistence)
        Type: manual (automated)

        This test verifies step 2 of the scenario: the poem distinguishes
        persistent storage from volatile caching (MirDB differs from memcached).
        """
        # Volatility/transient concepts that should be contrasted
        volatility_keywords = [
            'volatile', 'fleeting', 'transient', 'temporary',
            'ephemeral', 'fade', 'fades', 'fading',
            'vanish', 'lost', 'forget', 'forgets'
        ]

        # Memory/cache references that represent the volatile state
        volatile_memory_refs = ['memory', 'memcached', 'cache']

        found_volatility = []
        found_memory_refs = []

        for keyword in volatility_keywords:
            if keyword in self.poem_lower:
                found_volatility.append(keyword)

        for ref in volatile_memory_refs:
            if ref in self.poem_lower:
                found_memory_refs.append(ref)

        # Poem should acknowledge volatile nature of memory/cache
        self.assertTrue(
            len(found_volatility) >= 2,
            f"Poem should contrast with volatility concepts (at least 2). "
            f"Found: {found_volatility}"
        )

        # Poem should reference memory or memcached to show the contrast
        self.assertTrue(
            len(found_memory_refs) >= 1,
            f"Poem should reference volatile memory/cache systems to contrast. "
            f"Found: {found_memory_refs}"
        )

    def test_persistence_contrasts_memcached(self):
        """
        Test Case 2 (continued): Verify explicit contrast with memcached

        MirDB differs from memcached by offering persistence.
        The poem should highlight this distinction.
        """
        # Check for explicit memcached reference
        has_memcached = 'memcached' in self.poem_lower

        # Check for patterns that contrast volatile vs persistent
        contrast_patterns = [
            r'not\s+like\s+memcached',
            r'unlike\s+memcached',
            r'where\s+others?\s+(?:vanish|fade|lost)',
            r'memory.*(?:fleeting|volatile|fade)',
            r'(?:fleeting|volatile).*memory'
        ]

        has_contrast = any(
            re.search(pattern, self.poem_lower) for pattern in contrast_patterns
        )

        self.assertTrue(
            has_memcached or has_contrast,
            "Poem should contrast persistent storage with volatile caching "
            "(explicit memcached mention or contrast pattern)"
        )

    def test_persistence_disk_storage_reference(self):
        """
        Test Case 2 (continued): Verify disk persistence is mentioned

        The poem should convey that data is written to disk, not just memory.
        """
        disk_references = ['disk', 'sstable', 'file', 'written', 'etched']

        found_disk_refs = []
        for ref in disk_references:
            if ref in self.poem_lower:
                found_disk_refs.append(ref)

        self.assertTrue(
            len(found_disk_refs) >= 1,
            f"Poem should reference disk storage. Found: {found_disk_refs}"
        )

    def test_persistence_data_survival_language(self):
        """
        Additional validation: Data survives crashes/failures

        A key aspect of persistence is surviving system failures.
        """
        survival_patterns = [
            r'survive', r'crash', r'endure', r'remain',
            r'never.*(?:lost|fade|forget)',
            r'forever\s+keep',
            r'(?:power|server).*(?:fail|sleep)'
        ]

        found_patterns = []
        for pattern in survival_patterns:
            if re.search(pattern, self.poem_lower):
                found_patterns.append(pattern)

        self.assertTrue(
            len(found_patterns) >= 1,
            f"Poem should convey data survival through failures. Found patterns: {found_patterns}"
        )

    def test_poem_structure_readability(self):
        """
        Validate poem meets structure requirements for professional use.

        REQ-7 from PRD: Poem should be between 12-24 lines for optimal readability.
        """
        lines = [line for line in self.poem_content.strip().split('\n') if line.strip()]
        line_count = len(lines)

        self.assertGreaterEqual(
            line_count, 12,
            f"Poem should have at least 12 lines. Has: {line_count}"
        )
        self.assertLessEqual(
            line_count, 24,
            f"Poem should have at most 24 lines. Has: {line_count}"
        )


if __name__ == '__main__':
    unittest.main()
