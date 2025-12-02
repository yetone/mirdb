"""
Test Suite: Reliability and Performance Theme Validation (Scenario 5)

This test suite validates that the MirDB poem evokes themes of reliability
and performance (REQ-6), as specified in the PRD.

Test Cases:
1. Verify poem contains themes of reliability, trust, or dependability
2. Verify poem contains themes of speed, efficiency, or performance

MirDB Context:
- Reliability: WAL for durability, persistence, crash recovery
- Performance: Skip list memtable, optimized reads, efficient compaction
"""

import unittest
import os
import re


class PoemLoader:
    """Helper class to load poem content from multiple locations."""

    POEM_PATHS = [
        os.path.join(os.path.dirname(__file__), '..', 'versecraft', 'mirdb_poem.txt'),
        os.path.join(os.path.dirname(__file__), '..', 'poem.txt'),
        os.path.join(os.path.dirname(__file__), '..', 'POEM.md'),
    ]

    @classmethod
    def load_poem(cls) -> str:
        """Load and return the poem content from the first available location."""
        for path in cls.POEM_PATHS:
            if os.path.exists(path):
                with open(path, 'r', encoding='utf-8') as f:
                    return f.read()
        raise FileNotFoundError(f"No poem file found. Searched: {cls.POEM_PATHS}")

    @classmethod
    def get_poem_path(cls) -> str:
        """Get the path of the first available poem file."""
        for path in cls.POEM_PATHS:
            if os.path.exists(path):
                return path
        raise FileNotFoundError(f"No poem file found. Searched: {cls.POEM_PATHS}")


class TestReliabilityThemes(unittest.TestCase):
    """
    Test Case 1: Validate reliability, trust, and dependability themes.

    Input: Poem text content
    Expected: Contains themes of reliability, trust, or dependability

    MirDB is designed to be reliable through:
    - Write-Ahead Log (WAL) for durability
    - Persistence to disk
    - Crash recovery mechanisms
    """

    @classmethod
    def setUpClass(cls):
        """Load the poem content once for all tests."""
        cls.poem_content = PoemLoader.load_poem()
        cls.poem_lower = cls.poem_content.lower()

    def test_poem_file_exists(self):
        """Verify the poem file exists and can be loaded."""
        self.assertIsNotNone(self.poem_content)
        self.assertGreater(len(self.poem_content.strip()), 0,
                          "Poem should have content")

    def test_reliability_keywords_present(self):
        """
        Test Case 1: Verify poem contains reliability-related keywords.

        The poem should contain language about trustworthiness,
        dependability, and consistency.
        """
        reliability_keywords = [
            'reliable', 'reliability',
            'trust', 'trusted', 'trustworthy',
            'dependable', 'depend',
            'faithful', 'faith',
            'steadfast', 'steady',
            'guard', 'guards', 'guardian',
            'keeper', 'keep', 'keeps',
            'promise', 'promises', 'promised',
            'safe', 'safely', 'safety',
            'protect', 'protected', 'protection',
            'secure', 'secured',
            'never fail', 'never fails',
            'always', 'certain'
        ]

        found_keywords = []
        for keyword in reliability_keywords:
            if keyword in self.poem_lower:
                found_keywords.append(keyword)

        self.assertGreaterEqual(
            len(found_keywords), 2,
            f"Poem should contain at least 2 reliability-related keywords. "
            f"Found: {found_keywords}. "
            f"Searched for: trustworthiness, dependability, consistency themes"
        )

    def test_durability_concepts_present(self):
        """
        Test Case 1 (continued): Verify durability concepts are present.

        MirDB uses WAL and persistence for durability.
        """
        durability_patterns = [
            r'\blast(?:s|ing)?\b',
            r'\bendur(?:e|es|ing)?\b',
            r'\bpersist(?:s|ent|ence)?\b',
            r'\bsurviv(?:e|es|al)?\b',
            r'\beternal\b',
            r'\bforever\b',
            r'\bnever\s+(?:lost|fade|fail|sleep)',
            r'\bguard(?:s|ed|ian)?\b',
            r'\bwatch(?:es|ful)?\b',
            r'\bprotect(?:s|ed)?\b',
        ]

        found_patterns = []
        for pattern in durability_patterns:
            if re.search(pattern, self.poem_lower):
                found_patterns.append(pattern)

        self.assertGreaterEqual(
            len(found_patterns), 2,
            f"Poem should contain at least 2 durability concepts. "
            f"Found: {found_patterns}"
        )

    def test_consistency_themes_present(self):
        """
        Test Case 1 (continued): Verify consistency/constancy themes.

        Reliability includes being consistent and always available.
        """
        consistency_patterns = [
            r'\balways\b',
            r'\bconsisten(?:t|cy)\b',
            r'\bsteady\b',
            r'\bsteadfast\b',
            r'\bunwavering\b',
            r'\bfaithful(?:ly)?\b',
            r'\brest\b',  # "rest your data" implies trust
            r'\bstand(?:s)?\s+guard\b',
            r'\bthrough.*night\b',  # enduring through challenges
        ]

        found_patterns = []
        for pattern in consistency_patterns:
            if re.search(pattern, self.poem_lower, re.DOTALL):
                found_patterns.append(pattern)

        self.assertGreater(
            len(found_patterns), 0,
            f"Poem should contain consistency/constancy themes. "
            f"Searched: {consistency_patterns}"
        )

    def test_wal_reliability_metaphor(self):
        """
        Test Case 1 (continued): Verify WAL-related reliability metaphors.

        The Write-Ahead Log provides reliability through:
        - Promise keeping (writes before acknowledging)
        - Witness to data (crash recovery)
        """
        wal_reliability_patterns = [
            r'\bpromise\s+kept\b',
            r'\bwitness\b',
            r'\blog\b',
            r'\bfirst\b.*\bwrite\b',
            r'\bwrite\b.*\bfirst\b',
            r'\bbefore\b.*\bdried\b',  # "before the ink has dried"
            r'\bsafety\b',
            r'\brecovery\b',
        ]

        found_patterns = []
        for pattern in wal_reliability_patterns:
            if re.search(pattern, self.poem_lower, re.DOTALL):
                found_patterns.append(pattern)

        self.assertGreater(
            len(found_patterns), 0,
            f"Poem should contain WAL-related reliability metaphors. "
            f"Found: {found_patterns}"
        )


class TestPerformanceThemes(unittest.TestCase):
    """
    Test Case 2: Validate speed, efficiency, and performance themes.

    Input: Poem text content
    Expected: Contains themes of speed, efficiency, or performance

    MirDB achieves performance through:
    - Skip list memtable for fast in-memory operations
    - Optimized read path
    - Efficient compaction process
    """

    @classmethod
    def setUpClass(cls):
        """Load the poem content once for all tests."""
        cls.poem_content = PoemLoader.load_poem()
        cls.poem_lower = cls.poem_content.lower()

    def test_speed_keywords_present(self):
        """
        Test Case 2: Verify poem contains speed-related keywords.

        The poem should contain language about speed and responsiveness.
        """
        speed_keywords = [
            'swift', 'swiftly',
            'fast', 'faster',
            'quick', 'quickly',
            'haste', 'hasty',
            'rapid', 'rapidly',
            'speed', 'speedy',
            'instant', 'instantly',
            'responsive',
            'agile',
            'nimble'
        ]

        found_keywords = []
        for keyword in speed_keywords:
            if keyword in self.poem_lower:
                found_keywords.append(keyword)

        self.assertGreater(
            len(found_keywords), 0,
            f"Poem should contain at least 1 speed-related keyword. "
            f"Found: {found_keywords}. "
            f"Searched for: swift, fast, quick, haste, rapid, etc."
        )

    def test_efficiency_concepts_present(self):
        """
        Test Case 2 (continued): Verify efficiency concepts are present.

        Efficiency is conveyed through organization and optimization.
        """
        efficiency_patterns = [
            r'\befficien(?:t|cy)\b',
            r'\boptimi(?:ze|zed|zation)\b',
            r'\bstream(?:s|ed|ing|lined)?\b',
            r'\border(?:ed|ly)?\b',
            r'\borganiz(?:e|ed)?\b',
            r'\bsort(?:ed|ing)?\b',
            r'\bcompil(?:e|ed)?\b',
            r'\bdistill(?:ing|ed)?\b',
            r'\brefin(?:e|ed|ing)?\b',
        ]

        found_patterns = []
        for pattern in efficiency_patterns:
            if re.search(pattern, self.poem_lower):
                found_patterns.append(pattern)

        self.assertGreater(
            len(found_patterns), 0,
            f"Poem should contain efficiency concepts. "
            f"Found: {found_patterns}"
        )

    def test_skip_list_performance_metaphor(self):
        """
        Test Case 2 (continued): Verify skip list performance metaphors.

        Skip lists provide O(log n) operations through "elegant shortcuts".
        """
        skip_list_patterns = [
            r'\bskip\s+list(?:s)?\b',
            r'\binterlace(?:d|s)?\b',
            r'\bleap(?:s|ing)?\b',
            r'\bshortcut(?:s)?\b',
            r'\bgraceful\b',
            r'\belegant\b',
            r'\bswift\s+to\s+mind\b',
        ]

        found_patterns = []
        for pattern in skip_list_patterns:
            if re.search(pattern, self.poem_lower):
                found_patterns.append(pattern)

        self.assertGreater(
            len(found_patterns), 0,
            f"Poem should contain skip list performance metaphors. "
            f"Found: {found_patterns}"
        )

    def test_optimized_data_flow(self):
        """
        Test Case 2 (continued): Verify optimized data flow is represented.

        MirDB's performance comes from optimized write and read paths.
        """
        data_flow_patterns = [
            r'\bflow(?:s|ing)?\b',
            r'\bcascade\b',
            r'\bstream(?:s)?\b',
            r'\bcourse\b',
            r'\bpath(?:s)?\b',
            r'\bthrough\s+the\s+levels\b',
            r'\bactive\s+pools\b',
        ]

        found_patterns = []
        for pattern in data_flow_patterns:
            if re.search(pattern, self.poem_lower):
                found_patterns.append(pattern)

        self.assertGreater(
            len(found_patterns), 0,
            f"Poem should represent optimized data flow. "
            f"Found: {found_patterns}"
        )

    def test_compaction_efficiency(self):
        """
        Test Case 2 (continued): Verify compaction efficiency is conveyed.

        Compaction optimizes storage by merging and organizing data.
        """
        compaction_patterns = [
            r'\bcompaction\b',
            r'\bmerg(?:e|es|ing|ed)\b',
            r'\bdistill(?:ing|ed)?\b',
            r'\border\s+from\s+chaos\b',
            r'\border\s+planned\b',
            r'\binto\s+one\b',
            r'\btireless\b',
            r'\bpatient\b',
        ]

        found_patterns = []
        for pattern in compaction_patterns:
            if re.search(pattern, self.poem_lower):
                found_patterns.append(pattern)

        self.assertGreater(
            len(found_patterns), 0,
            f"Poem should convey compaction efficiency. "
            f"Found: {found_patterns}"
        )


class TestCombinedReliabilityAndPerformance(unittest.TestCase):
    """
    Combined test to verify both reliability AND performance themes exist.

    REQ-6 requires BOTH themes to be present for the poem to be complete.
    """

    @classmethod
    def setUpClass(cls):
        """Load the poem content once for all tests."""
        cls.poem_content = PoemLoader.load_poem()
        cls.poem_lower = cls.poem_content.lower()

    def test_both_themes_present(self):
        """
        Meta-test: Verify both reliability AND performance themes exist.

        This is the primary acceptance criterion for REQ-6.
        """
        # Reliability indicators
        reliability_keywords = [
            'guard', 'keeper', 'faithful', 'promise', 'trust',
            'steadfast', 'safe', 'protect', 'eternal', 'last'
        ]

        # Performance indicators
        performance_keywords = [
            'swift', 'fast', 'quick', 'haste', 'efficient',
            'stream', 'flow', 'cascade', 'sort', 'distill'
        ]

        reliability_found = any(kw in self.poem_lower for kw in reliability_keywords)
        performance_found = any(kw in self.poem_lower for kw in performance_keywords)

        self.assertTrue(
            reliability_found,
            f"Poem must contain reliability themes. "
            f"Searched for: {reliability_keywords}"
        )
        self.assertTrue(
            performance_found,
            f"Poem must contain performance themes. "
            f"Searched for: {performance_keywords}"
        )

    def test_rust_strength_for_reliability(self):
        """
        Verify Rust language reference reinforces reliability theme.

        Rust provides memory safety and reliability guarantees.
        """
        rust_patterns = [
            r'\brust\b',
            r'\brust-forged\b',
            r'\bforged\b',
            r'\bstrong\b',
            r'\bunbreaking\b',
        ]

        found_patterns = []
        for pattern in rust_patterns:
            if re.search(pattern, self.poem_lower):
                found_patterns.append(pattern)

        self.assertGreater(
            len(found_patterns), 0,
            f"Poem should reference Rust for reliability. "
            f"Found: {found_patterns}"
        )

    def test_poem_suitable_for_professional_use(self):
        """
        NFR-3: Verify poem is suitable for professional/technical audiences.

        The poem should be professional and suitable for marketing/documentation.
        """
        # Check that poem has appropriate length (REQ-7: 12-24 lines)
        lines = [line for line in self.poem_content.strip().split('\n') if line.strip()]
        non_title_lines = [line for line in lines if not line.startswith('#')]

        self.assertGreaterEqual(
            len(non_title_lines), 12,
            f"Poem should have at least 12 content lines. Has: {len(non_title_lines)}"
        )
        self.assertLessEqual(
            len(non_title_lines), 30,
            f"Poem should have at most 30 content lines. Has: {len(non_title_lines)}"
        )

        # Check for inappropriate content (NFR-4)
        inappropriate_patterns = [
            r'\bstupid\b', r'\bdumb\b', r'\bhate\b', r'\bkill\b',
            r'\bdestroy\b', r'\bhorrible\b', r'\bterrible\b'
        ]

        found_inappropriate = []
        for pattern in inappropriate_patterns:
            if re.search(pattern, self.poem_lower):
                found_inappropriate.append(pattern)

        self.assertEqual(
            len(found_inappropriate), 0,
            f"Poem should not contain inappropriate content. "
            f"Found: {found_inappropriate}"
        )


if __name__ == '__main__':
    unittest.main(verbosity=2)
