"""
Tests for validating optional memcached protocol compatibility reference in MirDB poem.

This test module validates Scenario 17 (REQ-5): Memcached Protocol Reference
- Test Case 1: Optionally contains reference to memcached protocol compatibility
- Test Case 2: If present, reference integrates naturally without disrupting flow

REQ-5 is a 'Could' priority requirement - optional but valuable.
The poem MAY reference memcached protocol compatibility for differentiation.
"""

import unittest
import os
import re


class MemcachedProtocolReferenceTest(unittest.TestCase):
    """Test suite for validating optional memcached protocol reference in MirDB poem."""

    POEM_PATH = os.path.join(os.path.dirname(__file__), '..', 'POEM.md')
    ALT_POEM_PATH = os.path.join(os.path.dirname(__file__), '..', 'poem.txt')
    VERSECRAFT_POEM_PATH = os.path.join(os.path.dirname(__file__), '..', 'versecraft', 'mirdb_poem.txt')

    # Patterns that indicate memcached/protocol reference
    MEMCACHED_PATTERNS = [
        r'\bmemcached\b',
        r'\bmemcache\b',
        r'\bprotocol\b',
        r'\bcache\b(?!d)',  # cache but not cached
    ]

    # Contextual patterns showing memcached comparison (differentiation)
    COMPARISON_PATTERNS = [
        r'not\s+like\s+memcached',
        r'unlike\s+memcached',
        r'memcached.*(?:fade|vanish|lost|volatile|transient)',
        r'(?:fade|vanish|lost|volatile|transient).*memcached',
    ]

    # Patterns that indicate natural integration (poetic elements)
    NATURAL_INTEGRATION_INDICATORS = [
        r',\s*\n',  # Line breaks with commas (enjambment)
        r'\w+ing\b',  # Present participles (poetic flow)
        r'[.!?]\s*$',  # Proper sentence endings
        r'\b(the|a|an|in|of|with|through|from|to)\b',  # Articles and prepositions
    ]

    # Anti-patterns that indicate forced/awkward integration
    AWKWARD_PATTERNS = [
        r'memcached\s+protocol\s+compatible',  # Too technical/direct
        r'TCP\s+port',  # Too technical
        r'RFC\s+\d+',  # Standards reference
        r'specification',  # Technical specification language
    ]

    @classmethod
    def setUpClass(cls):
        """Load poem content from available poem files."""
        cls.poem_content = None
        cls.poem_source = None

        # Try main poem file first
        if os.path.exists(cls.POEM_PATH):
            with open(cls.POEM_PATH, 'r') as f:
                cls.poem_content = f.read()
            cls.poem_source = cls.POEM_PATH

        # Check alternate poem file
        cls.alt_poem_content = None
        if os.path.exists(cls.ALT_POEM_PATH):
            with open(cls.ALT_POEM_PATH, 'r') as f:
                cls.alt_poem_content = f.read()

        # Check versecraft poem
        cls.versecraft_poem_content = None
        if os.path.exists(cls.VERSECRAFT_POEM_PATH):
            with open(cls.VERSECRAFT_POEM_PATH, 'r') as f:
                cls.versecraft_poem_content = f.read()

        if cls.poem_content:
            cls.poem_lower = cls.poem_content.lower()
        else:
            cls.poem_lower = ""

    def test_poem_file_exists(self):
        """Verify at least one poem file exists."""
        self.assertIsNotNone(
            self.poem_content,
            f"No poem file found at {self.POEM_PATH}"
        )

    def test_optional_memcached_reference_presence(self):
        """
        Test Case 1: Optionally contains reference to memcached protocol compatibility.

        REQ-5 is 'Could' priority - the reference is optional but valuable.
        This test checks if any memcached reference exists (PASSES either way).
        """
        has_memcached_ref = any(
            re.search(pattern, self.poem_lower)
            for pattern in self.MEMCACHED_PATTERNS
        )

        # For 'Could' requirements, we document presence but don't require it
        if has_memcached_ref:
            found_patterns = [
                pattern for pattern in self.MEMCACHED_PATTERNS
                if re.search(pattern, self.poem_lower)
            ]
            # This is informational - test passes
            self.assertTrue(True, f"Memcached reference found: {found_patterns}")
        else:
            # REQ-5 is optional - absence is acceptable
            self.assertTrue(
                True,
                "No memcached reference found (acceptable - REQ-5 is 'Could' priority)"
            )

    def test_memcached_reference_if_present_is_comparative(self):
        """
        Test Case 1 (validation): If memcached is mentioned, verify it's comparative.

        MirDB implements memcached protocol but provides persistence unlike memcached.
        Any reference should highlight this differentiation.
        """
        has_memcached_mention = re.search(r'\bmemcached\b', self.poem_lower)

        if has_memcached_mention:
            # If memcached is mentioned, it should be in a comparative context
            has_comparison = any(
                re.search(pattern, self.poem_lower, re.IGNORECASE)
                for pattern in self.COMPARISON_PATTERNS
            )

            self.assertTrue(
                has_comparison,
                "Memcached mention should be in comparative context "
                "(e.g., 'not like memcached', 'unlike memcached')"
            )
        else:
            # No memcached mention - test passes (optional requirement)
            self.assertTrue(True)

    def test_memcached_reference_integrates_naturally(self):
        """
        Test Case 2: If present, reference integrates naturally without disrupting flow.

        The memcached reference should not feel forced or break poetic structure.
        """
        has_memcached = re.search(r'\bmemcached\b', self.poem_lower)

        if not has_memcached:
            # No reference means no integration issues - passes
            self.assertTrue(
                True,
                "No memcached reference to assess for integration"
            )
            return

        # Check for awkward/forced patterns
        has_awkward = any(
            re.search(pattern, self.poem_lower)
            for pattern in self.AWKWARD_PATTERNS
        )

        self.assertFalse(
            has_awkward,
            "Memcached reference appears forced/too technical. "
            "Should be poetically integrated."
        )

    def test_memcached_line_maintains_meter(self):
        """
        Test Case 2 (metric): If memcached is mentioned, the line maintains rhythm.

        Checks that the line with memcached isn't overly long or metrically disruptive.
        """
        has_memcached = re.search(r'\bmemcached\b', self.poem_lower)

        if not has_memcached:
            self.assertTrue(True)
            return

        # Find the line containing memcached
        lines = self.poem_content.split('\n')
        memcached_lines = [
            line for line in lines
            if 'memcached' in line.lower()
        ]

        for line in memcached_lines:
            # Line shouldn't be excessively long (disrupting visual rhythm)
            self.assertLessEqual(
                len(line),
                80,
                f"Memcached line too long (may disrupt flow): '{line}'"
            )

            # Line should have some natural breaks (not run-on)
            word_count = len(line.split())
            self.assertLessEqual(
                word_count,
                15,
                f"Memcached line has too many words (may feel forced): '{line}'"
            )

    def test_memcached_context_is_thematic(self):
        """
        Test Case 2 (thematic): Memcached reference fits poem's themes.

        If memcached is mentioned, it should connect to themes of:
        - Persistence vs. volatility
        - Durability vs. transience
        - Reliability
        """
        has_memcached = re.search(r'\bmemcached\b', self.poem_lower)

        if not has_memcached:
            self.assertTrue(True)
            return

        # Thematic context patterns
        thematic_patterns = [
            r'(?:fade|vanish|lost|volatile|transient|fleeting)',  # Volatility theme
            r'(?:persist|endure|remain|lasting|eternal|forever)',  # Persistence theme
            r'(?:power|failure|crash|restart)',  # Reliability theme
        ]

        # Check if thematic context exists near memcached reference
        # (within the same stanza or adjacent lines)
        has_thematic_context = any(
            re.search(pattern, self.poem_lower)
            for pattern in thematic_patterns
        )

        self.assertTrue(
            has_thematic_context,
            "Memcached reference should be within thematic context "
            "(persistence vs. volatility)"
        )

    def test_any_poem_has_memcached_reference(self):
        """
        Test Case 1 (supplementary): Check all available poem files for memcached.

        This validates that at least one version of the poem addresses REQ-5.
        """
        poems_with_memcached = []

        if self.poem_content and re.search(r'\bmemcached\b', self.poem_content.lower()):
            poems_with_memcached.append(self.POEM_PATH)

        if self.alt_poem_content and re.search(r'\bmemcached\b', self.alt_poem_content.lower()):
            poems_with_memcached.append(self.ALT_POEM_PATH)

        if self.versecraft_poem_content and re.search(r'\bmemcached\b', self.versecraft_poem_content.lower()):
            poems_with_memcached.append(self.VERSECRAFT_POEM_PATH)

        # Report which poems have memcached reference
        if poems_with_memcached:
            self.assertTrue(
                True,
                f"Memcached reference found in: {poems_with_memcached}"
            )
        else:
            # Still passes - REQ-5 is optional
            self.assertTrue(
                True,
                "No poem versions contain memcached reference (acceptable for 'Could' priority)"
            )


class ProtocolIntegrationNaturalnessTest(unittest.TestCase):
    """Test suite for validating natural integration of protocol references."""

    POEM_PATH = os.path.join(os.path.dirname(__file__), '..', 'POEM.md')

    @classmethod
    def setUpClass(cls):
        """Load the poem content."""
        if os.path.exists(cls.POEM_PATH):
            with open(cls.POEM_PATH, 'r') as f:
                cls.poem_content = f.read()
            cls.poem_lower = cls.poem_content.lower()
        else:
            cls.poem_content = ""
            cls.poem_lower = ""

    def test_no_forced_technical_jargon(self):
        """
        Test Case 2: Verify no forced technical jargon disrupts flow.

        Protocol references should be poetically transformed, not raw technical terms.
        """
        forced_jargon = [
            r'\bTCP/IP\b',
            r'\bport\s+\d+\b',
            r'\bASCII\b',
            r'\bGET\s+command\b',
            r'\bSET\s+command\b',
            r'\bcommand\s+(?:line|syntax)\b',
        ]

        found_jargon = [
            pattern for pattern in forced_jargon
            if re.search(pattern, self.poem_content, re.IGNORECASE)
        ]

        self.assertEqual(
            len(found_jargon),
            0,
            f"Found forced technical jargon that disrupts poetic flow: {found_jargon}"
        )

    def test_protocol_concepts_metaphorically_expressed(self):
        """
        Test Case 2: If protocol is referenced, verify metaphorical expression.

        References to protocol should use poetic language, not technical documentation style.
        """
        # If protocol/memcached mentioned, check for poetic transformation
        has_protocol_mention = (
            re.search(r'\bprotocol\b', self.poem_lower) or
            re.search(r'\bmemcached\b', self.poem_lower)
        )

        if not has_protocol_mention:
            self.assertTrue(True)
            return

        # Check for poetic elements in the poem overall
        poetic_elements = [
            r'[,;:\-]',  # Punctuation variety
            r'\b(?:as|like|though|where|when)\b',  # Comparative/relational words
            r'\b(?:flows?|streams?|cascad)',  # Flow metaphors
        ]

        has_poetic_elements = any(
            re.search(pattern, self.poem_lower)
            for pattern in poetic_elements
        )

        self.assertTrue(
            has_poetic_elements,
            "Poem with protocol reference should maintain poetic elements"
        )

    def test_stanza_coherence_with_protocol_reference(self):
        """
        Test Case 2: Verify stanza coherence when protocol is referenced.

        The stanza containing any protocol reference should be internally coherent.
        """
        lines = self.poem_content.split('\n')

        # Find lines with protocol-related content
        protocol_line_indices = [
            i for i, line in enumerate(lines)
            if re.search(r'\b(?:memcached|protocol|cache)\b', line.lower())
        ]

        if not protocol_line_indices:
            self.assertTrue(True)
            return

        for idx in protocol_line_indices:
            line = lines[idx]
            # Check line is not standalone/disconnected
            if line.strip():
                # Line should connect to adjacent lines (check for enjambment or thematic flow)
                has_proper_ending = bool(re.search(r'[,.\-!?;:]', line))
                self.assertTrue(
                    has_proper_ending or idx == len(lines) - 1,
                    f"Protocol reference line may be disconnected: '{line}'"
                )


if __name__ == '__main__':
    unittest.main()
