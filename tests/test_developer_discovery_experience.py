"""
Test Suite: Developer Discovery Experience Validation (Scenario 14)

This test suite validates that the MirDB poem creates a positive developer
discovery experience, as specified in Story 1 of the PRD.

Test Cases:
1. Developer persona review - Poem creates positive emotional response for technical reader
2. Product identity clarity - Reader understands MirDB is a persistent key-value store after reading poem
3. Craftsmanship perception - Poem conveys that MirDB was built with attention to detail

MirDB Context:
- Target persona: Developer exploring MirDB for the first time
- Should feel inspired and remember MirDB as a product built with care
- Should understand it's a persistent key-value store
- Should perceive attention to detail and craftsmanship
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


class TestDeveloperPersonaEmotionalResponse(unittest.TestCase):
    """
    Test Case 1: Developer persona review - Poem creates positive emotional response.

    Story 1 Acceptance Criteria:
    - Given I am reading MirDB documentation
    - When I encounter the VerseCraft poem
    - Then I understand MirDB is a persistent key-value store
    - And I feel the product was crafted with attention to detail

    This tests the first impression impact for developers encountering
    the poem in documentation, ensuring they feel inspired.
    """

    @classmethod
    def setUpClass(cls):
        """Load the poem content once for all tests."""
        cls.poem_content = PoemLoader.load_poem()
        cls.poem_lower = cls.poem_content.lower()
        cls.content_lines = [
            line for line in cls.poem_content.strip().split('\n')
            if line.strip() and not line.strip().startswith('#')
        ]

    def test_poem_file_exists_and_has_content(self):
        """Verify the poem file exists and has meaningful content."""
        self.assertIsNotNone(self.poem_content)
        self.assertGreater(len(self.poem_content.strip()), 100,
                          "Poem should have substantial content")

    def test_positive_emotional_language_present(self):
        """
        Test Case 1: Verify poem contains language that evokes positive emotions.

        Developers should feel inspired, not bored or confused.
        Positive emotional language includes words that evoke:
        - Trust and reliability
        - Elegance and grace
        - Strength and security
        - Care and protection
        """
        positive_emotional_words = [
            'guardian', 'keeper', 'treasure', 'grace', 'steadfast',
            'trust', 'true', 'safe', 'secure', 'protect',
            'care', 'eternal', 'endure', 'elegant', 'faithful',
            'noble', 'sentinel', 'dream', 'bright', 'rest',
            'wisdom', 'beauty', 'promise'
        ]

        found_positive = []
        for word in positive_emotional_words:
            if word in self.poem_lower:
                found_positive.append(word)

        self.assertGreaterEqual(
            len(found_positive), 4,
            f"Poem should contain at least 4 positive emotional words to inspire developers. "
            f"Found: {found_positive}"
        )

    def test_no_negative_or_discouraging_language(self):
        """
        Test Case 1: Verify poem avoids negative or discouraging language.

        First impressions matter - developers should not encounter
        pessimistic, negative, or discouraging language.
        """
        negative_words = [
            r'\bfailed?\b', r'\bbroke\b', r'\bdestroy\b',
            r'\bwrong\b', r'\bbad\b', r'\bworst\b',
            r'\bhate\b', r'\bterrible\b', r'\bawful\b',
            r'\bdead\b', r'\bdeath\b', r'\bkill\b',
            r'\bpoor\b', r'\bweak\b', r'\bbroken\b',
            r'\bunstable\b', r'\bunsafe\b', r'\bunreliable\b'
        ]

        found_negative = []
        for pattern in negative_words:
            if re.search(pattern, self.poem_lower):
                found_negative.append(pattern)

        self.assertEqual(
            len(found_negative), 0,
            f"Poem should not contain negative/discouraging language. "
            f"Found: {found_negative}"
        )

    def test_inspiring_imagery_present(self):
        """
        Test Case 1: Verify poem uses inspiring imagery.

        According to PRD Appendix B, the poem should use metaphors relating to:
        - Nature (trees, layers, flow)
        - Craftsmanship and building
        - Reliability and trust
        """
        inspiring_imagery = [
            r'\bguardian\b', r'\bsentinel\b', r'\bkeeper\b',
            r'\bforged\b', r'\bcraft\b', r'\bbuild\b',
            r'\btree\b', r'\blayer\b', r'\bflow\b', r'\bstream\b',
            r'\bgrace\b', r'\belegant\b', r'\bartist\b',
            r'\bwisdom\b', r'\btreasure\b',
            r'\bcircuit\b', r'\bdream\b', r'\blight\b'
        ]

        found_imagery = []
        for pattern in inspiring_imagery:
            if re.search(pattern, self.poem_lower):
                found_imagery.append(pattern)

        self.assertGreaterEqual(
            len(found_imagery), 3,
            f"Poem should use inspiring imagery (at least 3 examples). "
            f"Found: {found_imagery}"
        )

    def test_poem_has_memorable_structure(self):
        """
        Test Case 1: Verify poem has structure that aids memorability.

        NFR-5: Poem should be easily memorizable (use of rhyme, repetition).
        A memorable poem creates a more positive and lasting impression.
        """
        # Check for reasonable stanza structure (groups of lines)
        total_lines = len(self.content_lines)

        # Poem should have sufficient lines for impact but not be too long
        self.assertGreaterEqual(
            total_lines, 12,
            f"Poem should have at least 12 lines for memorable impact. Has: {total_lines}"
        )
        self.assertLessEqual(
            total_lines, 30,
            f"Poem should not be too long (max 30 lines). Has: {total_lines}"
        )

        # Check for rhyming potential (similar endings or rhyme sounds)
        line_endings = []
        for line in self.content_lines:
            words = line.strip().rstrip('.,!?;:').split()
            if words:
                # Get last 3 characters of last word for rhyme detection
                last_word = words[-1].lower()
                line_endings.append(last_word[-3:] if len(last_word) >= 3 else last_word)

        # Some line endings should have similar sounds (rhymes)
        # Check if any ending patterns repeat
        unique_endings = len(set(line_endings))
        # Either some endings repeat OR poem has good structure (stanzas)
        has_rhyme_patterns = unique_endings < len(line_endings)
        has_stanza_structure = total_lines >= 12 and any(
            not line.strip() for line in self.poem_content.split('\n')
        )

        self.assertTrue(
            has_rhyme_patterns or has_stanza_structure,
            "Poem should have rhyming patterns or stanza structure for memorability"
        )

    def test_professional_yet_engaging_tone(self):
        """
        Test Case 1: Verify poem balances professionalism with engagement.

        The poem should feel professional (suitable for documentation)
        while still being emotionally engaging.
        """
        # Professional indicators
        professional_words = [
            'guardian', 'sentinel', 'steadfast', 'persistent',
            'reliable', 'trust', 'secure', 'safe', 'data',
            'store', 'memory', 'disk', 'persist'
        ]

        # Engaging/poetic indicators
        engaging_words = [
            'dream', 'grace', 'treasure', 'eternal', 'embrace',
            'guardian', 'sentinel', 'keeper', 'forged',
            'ode', 'verse', 'song', 'heart', 'soul'
        ]

        prof_count = sum(1 for w in professional_words if w in self.poem_lower)
        engage_count = sum(1 for w in engaging_words if w in self.poem_lower)

        self.assertGreaterEqual(
            prof_count, 3,
            f"Poem should have professional vocabulary. Found: {prof_count}"
        )
        self.assertGreaterEqual(
            engage_count, 2,
            f"Poem should have engaging/poetic vocabulary. Found: {engage_count}"
        )


class TestProductIdentityClarity(unittest.TestCase):
    """
    Test Case 2: Product identity clarity.

    Input: Product identity clarity check
    Expected: Reader understands MirDB is a persistent key-value store after reading poem

    This validates that the poem helps developers understand what MirDB is:
    - A persistent key-value store
    - Different from volatile caching solutions like memcached
    - Data is stored durably on disk
    """

    @classmethod
    def setUpClass(cls):
        """Load the poem content once for all tests."""
        cls.poem_content = PoemLoader.load_poem()
        cls.poem_lower = cls.poem_content.lower()

    def test_key_value_store_identity_clear(self):
        """
        Test Case 2: Verify poem clearly identifies MirDB as a key-value store.

        REQ-1: Poem must reference MirDB's key-value store nature.
        """
        key_value_patterns = [
            r'\bkey-value\b',
            r'\bkey\s+value\b',
            r'\bkeys?\b.*\bvalues?\b',
            r'\bvalues?\b.*\bkeys?\b',
            r'\bkeys?\s+and\s+values?\b',
            r'\beach\s+key\b',
            r'\beach\s+value\b'
        ]

        found_kv = []
        for pattern in key_value_patterns:
            if re.search(pattern, self.poem_lower):
                found_kv.append(pattern)

        self.assertGreater(
            len(found_kv), 0,
            f"Poem must reference key-value store nature. "
            f"Searched patterns: {key_value_patterns}"
        )

    def test_persistence_identity_clear(self):
        """
        Test Case 2: Verify poem clearly conveys persistence.

        REQ-2: Poem must convey the concept of data persistence/durability.
        MirDB differs from memcached by offering persistence - this should be clear.
        """
        persistence_indicators = [
            'persist', 'persistence', 'persistent',
            'eternal', 'forever', 'forevermore',
            'endure', 'enduring', 'endures',
            'never part', 'never fade', 'never lost',
            'permanent', 'durable', 'durability',
            'survive', 'survives'
        ]

        found_persistence = []
        for indicator in persistence_indicators:
            if indicator in self.poem_lower:
                found_persistence.append(indicator)

        self.assertGreaterEqual(
            len(found_persistence), 2,
            f"Poem should clearly convey persistence (at least 2 indicators). "
            f"Found: {found_persistence}"
        )

    def test_store_identity_explicit(self):
        """
        Test Case 2: Verify poem explicitly identifies MirDB as a 'store'.

        The word 'store' or similar should appear to make product identity clear.
        """
        store_patterns = [
            r'\bstore\b', r'\bstorage\b', r'\bstored\b',
            r'\bkeeper\b', r'\bguardian\b', r'\btreasure\b'
        ]

        found_store = []
        for pattern in store_patterns:
            if re.search(pattern, self.poem_lower):
                found_store.append(pattern)

        self.assertGreater(
            len(found_store), 0,
            f"Poem should identify MirDB as a store. Found: {found_store}"
        )

    def test_distinguishes_from_cache(self):
        """
        Test Case 2: Verify poem distinguishes MirDB from volatile caches.

        MirDB is NOT just a cache - it persists data. The poem should
        make this distinction clear to developers evaluating it.
        """
        # Check for language contrasting with volatility
        contrast_patterns = [
            r'\bnot\s+like\b', r'\bunlike\b',
            r'\bno\s+cache\b', r'\bnot\s+.*cache\b',
            r'\bfade\b', r'\bfleeting\b', r'\bvolatile\b',
            r'\bephemeral\b', r'\btemporary\b', r'\btransient\b',
            r'\bwhere\s+others\b', r'\bwhen\s+others\b'
        ]

        found_contrast = []
        for pattern in contrast_patterns:
            if re.search(pattern, self.poem_lower):
                found_contrast.append(pattern)

        self.assertGreater(
            len(found_contrast), 0,
            f"Poem should distinguish MirDB from volatile caches. "
            f"Found contrast patterns: {found_contrast}"
        )

    def test_mirdb_name_mentioned(self):
        """
        Test Case 2: Verify MirDB is explicitly named in the poem.

        For clear product identity, the name 'MirDB' should appear.
        """
        self.assertIn(
            'mirdb',
            self.poem_lower,
            "Poem should mention 'MirDB' by name for clear product identity"
        )


class TestCraftsmanshipPerception(unittest.TestCase):
    """
    Test Case 3: Craftsmanship perception.

    Input: Craftsmanship perception assessment
    Expected: Poem conveys that MirDB was built with attention to detail

    Story 1 Acceptance Criteria: Developer should "feel the product was
    crafted with attention to detail"

    This validates that the poem conveys:
    - Quality engineering and thoughtful design
    - Care and attention to detail
    - Technical sophistication (LSM trees, etc.)
    - Professional craftsmanship
    """

    @classmethod
    def setUpClass(cls):
        """Load the poem content once for all tests."""
        cls.poem_content = PoemLoader.load_poem()
        cls.poem_lower = cls.poem_content.lower()
        cls.content_lines = [
            line for line in cls.poem_content.strip().split('\n')
            if line.strip() and not line.strip().startswith('#')
        ]

    def test_craftsmanship_vocabulary_present(self):
        """
        Test Case 3: Verify poem uses craftsmanship vocabulary.

        Words and phrases that convey care, quality, and attention to detail.
        """
        craftsmanship_words = [
            'forged', 'craft', 'crafted', 'build', 'built',
            'design', 'designed', 'architected', 'engineered',
            'elegant', 'elegance', 'grace', 'graceful',
            'artistry', 'art', 'create', 'created',
            'care', 'careful', 'attention', 'detail',
            'sorted', 'ordered', 'organized', 'refined'
        ]

        found_craftsmanship = []
        for word in craftsmanship_words:
            if word in self.poem_lower:
                found_craftsmanship.append(word)

        self.assertGreaterEqual(
            len(found_craftsmanship), 3,
            f"Poem should use craftsmanship vocabulary (at least 3 words). "
            f"Found: {found_craftsmanship}"
        )

    def test_technical_sophistication_evident(self):
        """
        Test Case 3: Verify poem conveys technical sophistication.

        The poem should reference technical concepts that demonstrate
        thoughtful engineering (LSM trees, WAL, SSTables, etc.).
        """
        technical_concepts = [
            'lsm', 'memtable', 'sstable', 'sorted string',
            'log', 'wal', 'write-ahead', 'compaction',
            'tree', 'layer', 'levels', 'skip list',
            'rust', 'tokio', 'async'
        ]

        found_technical = []
        for concept in technical_concepts:
            if concept in self.poem_lower:
                found_technical.append(concept)

        self.assertGreaterEqual(
            len(found_technical), 2,
            f"Poem should demonstrate technical sophistication (at least 2 concepts). "
            f"Found: {found_technical}"
        )

    def test_quality_metaphors_used(self):
        """
        Test Case 3: Verify poem uses quality-evoking metaphors.

        Per PRD Appendix B, metaphors should relate to craftsmanship and building.
        """
        quality_metaphors = [
            r'\bforged\b', r'\bbuilt\b', r'\bcrafted\b',
            r'\bguardian\b', r'\bsentinel\b', r'\bkeeper\b',
            r'\bsorted\s+strings\b', r'\btablets\b', r'\bscrolls\b',
            r'\btreasure\b', r'\bsafe\b', r'\bprotect\b'
        ]

        found_metaphors = []
        for pattern in quality_metaphors:
            if re.search(pattern, self.poem_lower):
                found_metaphors.append(pattern)

        self.assertGreaterEqual(
            len(found_metaphors), 2,
            f"Poem should use quality-evoking metaphors (at least 2). "
            f"Found: {found_metaphors}"
        )

    def test_rust_implementation_highlighted(self):
        """
        Test Case 3: Verify poem highlights Rust implementation.

        REQ-4: Poem should highlight the Rust implementation (strength, safety).
        Using Rust conveys a commitment to quality, safety, and performance.
        """
        rust_patterns = [
            r'\brust\b',
            r'\bforged\b',  # "forged in Rust" metaphor
            r'\bunbreaking\b',  # Rust-like safety
            r'\bsafe\b', r'\bsafety\b'
        ]

        found_rust = []
        for pattern in rust_patterns:
            if re.search(pattern, self.poem_lower):
                found_rust.append(pattern)

        self.assertGreater(
            len(found_rust), 0,
            f"Poem should highlight Rust implementation or its qualities. "
            f"Found: {found_rust}"
        )

    def test_poem_itself_shows_craftsmanship(self):
        """
        Test Case 3: Verify the poem itself demonstrates craftsmanship.

        The poem's own quality (structure, rhyme, meter) should reflect
        the attention to detail that went into MirDB.
        """
        # Check for consistent line lengths (shows attention to form)
        line_lengths = [len(line) for line in self.content_lines if line.strip()]

        if len(line_lengths) > 4:
            avg_length = sum(line_lengths) / len(line_lengths)
            variance = sum((l - avg_length) ** 2 for l in line_lengths) / len(line_lengths)
            std_dev = variance ** 0.5

            # Reasonable consistency in line lengths (not too varied)
            self.assertLess(
                std_dev, avg_length,  # std dev should be less than average
                f"Poem should show consistency in structure. "
                f"Avg line length: {avg_length:.1f}, Std dev: {std_dev:.1f}"
            )

    def test_attention_to_detail_in_language(self):
        """
        Test Case 3: Verify poem uses precise, thoughtful language.

        Attention to detail is shown through:
        - Specific technical terms used correctly
        - Careful word choices that match MirDB's actual features
        """
        # Accurate technical references
        accurate_refs = [
            ('persist', 'persistence feature'),
            ('key', 'key-value store'),
            ('value', 'key-value store'),
            ('disk', 'disk persistence'),
            ('memory', 'memtable concept'),
            ('sorted', 'SSTable sorting'),
            ('log', 'WAL concept'),
        ]

        found_accurate = []
        for term, feature in accurate_refs:
            if term in self.poem_lower:
                found_accurate.append((term, feature))

        self.assertGreaterEqual(
            len(found_accurate), 4,
            f"Poem should use accurate technical references (at least 4). "
            f"Found: {[f[0] for f in found_accurate]}"
        )


class TestOverallDeveloperDiscoveryExperience(unittest.TestCase):
    """
    Combined validation of the overall developer discovery experience.

    This meta-test ensures all three aspects work together:
    1. Positive emotional response
    2. Clear product identity
    3. Perceived craftsmanship
    """

    @classmethod
    def setUpClass(cls):
        """Load the poem content once for all tests."""
        cls.poem_content = PoemLoader.load_poem()
        cls.poem_lower = cls.poem_content.lower()

    def test_first_impression_is_positive(self):
        """
        Meta-test: The overall first impression should be positive.

        Combines elements of emotional response, clarity, and craftsmanship.
        """
        # Positive indicators
        positive_indicators = [
            'guardian', 'keeper', 'treasure', 'safe', 'trust',
            'grace', 'steadfast', 'eternal', 'forged', 'true'
        ]

        # Clear identity indicators
        identity_indicators = [
            'key', 'value', 'store', 'persist', 'mirdb', 'data'
        ]

        # Quality indicators
        quality_indicators = [
            'rust', 'lsm', 'sorted', 'tree', 'disk', 'memory'
        ]

        positive_count = sum(1 for w in positive_indicators if w in self.poem_lower)
        identity_count = sum(1 for w in identity_indicators if w in self.poem_lower)
        quality_count = sum(1 for w in quality_indicators if w in self.poem_lower)

        # All three aspects should be represented
        self.assertGreaterEqual(positive_count, 3,
            f"First impression: needs positive language. Found: {positive_count}")
        self.assertGreaterEqual(identity_count, 3,
            f"First impression: needs clear identity. Found: {identity_count}")
        self.assertGreaterEqual(quality_count, 2,
            f"First impression: needs quality indicators. Found: {quality_count}")

    def test_poem_suitable_for_documentation(self):
        """
        Verify poem is suitable for embedding in MirDB documentation.

        Per PRD Integration Points: Poem can be embedded in README.md documentation.
        """
        # Check for reasonable length for documentation
        lines = [l for l in self.poem_content.split('\n') if l.strip() and not l.startswith('#')]

        self.assertGreaterEqual(len(lines), 12, "Poem should have substance for documentation")
        self.assertLessEqual(len(lines), 30, "Poem should not be too long for documentation")

        # Check for no inappropriate content
        inappropriate = ['http://', 'https://', '@', '#!', '```']
        found_inappropriate = [i for i in inappropriate if i in self.poem_content]

        self.assertEqual(
            len(found_inappropriate), 0,
            f"Poem should not contain non-poetic elements: {found_inappropriate}"
        )

    def test_developer_would_remember_mirdb(self):
        """
        Verify poem helps developers remember MirDB.

        NFR-5: Poem should be easily memorizable.
        A memorable poem ensures positive lasting impression.
        """
        # MirDB should be mentioned
        self.assertIn('mirdb', self.poem_lower, "MirDB should be named for memorability")

        # Key differentiator should be memorable
        differentiators = ['persist', 'key', 'value', 'store', 'rust']
        found = sum(1 for d in differentiators if d in self.poem_lower)

        self.assertGreaterEqual(
            found, 3,
            f"Key differentiators should be present for memorability. Found: {found}"
        )


if __name__ == '__main__':
    unittest.main(verbosity=2)
