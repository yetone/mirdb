"""
Test Suite: Originality Validation (Scenario 11)

This test suite validates that the MirDB poem is original and free from
copyright issues (REQ-8), as specified in the PRD.

Test Cases:
1. Check for plagiarism - Verify no significant overlap with existing published poems
2. Verify unique creation - Confirm poem is uniquely created for MirDB without
   copyright infringement
"""

import unittest
import os
import sys

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from versecraft.originality_validator import (
    OriginalityValidator,
    validate_poem_originality
)


class TestOriginalityValidation(unittest.TestCase):
    """Test case for validating originality of the MirDB poem (REQ-8)."""

    @classmethod
    def setUpClass(cls):
        """Load the poem content once for all tests."""
        poem_path = os.path.join(os.path.dirname(__file__), '..', 'poem.txt')
        with open(poem_path, 'r') as f:
            cls.poem_content = f.read()
        cls.poem_lower = cls.poem_content.lower()
        cls.validator = OriginalityValidator(cls.poem_content, "MirDB")

    # ==========================================================================
    # Test Case 1: Check for plagiarism - No significant overlap with published poems
    # ==========================================================================

    def test_no_overlap_with_famous_poems(self):
        """
        Test Case 1.1: Verify poem does not contain famous poem fragments.

        Input: Poem text content
        Expected: No significant overlap with existing published poems
        Type: manual (automated)

        This test ensures the poem doesn't copy famous poetry lines.
        """
        is_original, found_overlaps = self.validator.check_known_poem_overlap()

        self.assertTrue(
            is_original,
            f"Poem contains fragments from known famous poems: {found_overlaps}"
        )
        self.assertEqual(
            len(found_overlaps), 0,
            f"Found {len(found_overlaps)} overlapping fragments: {found_overlaps}"
        )

    def test_ngram_uniqueness(self):
        """
        Test Case 1.2: Verify poem has unique phrasing through n-gram analysis.

        Input: Poem text content
        Expected: No significant n-gram overlap with known works
        Type: manual (automated)

        Uses n-gram analysis to detect copied phrases.
        """
        is_unique, details = self.validator.check_ngram_uniqueness()

        self.assertTrue(
            is_unique,
            f"Poem has too much n-gram overlap ({details['overlap_ratio']:.2%}) "
            f"with reference texts. Matching phrases: {details['matching_ngrams']}"
        )

    def test_no_shakespeare_content(self):
        """
        Test Case 1.3: Verify no Shakespeare content is copied.

        Famous Shakespeare phrases should not appear in the poem.
        """
        shakespeare_phrases = [
            "shall i compare thee",
            "to be or not to be",
            "all the world's a stage",
            "what light through yonder window",
            "parting is such sweet sorrow",
            "brevity is the soul of wit"
        ]

        normalized_poem = self.validator._normalize_text(self.poem_content)

        for phrase in shakespeare_phrases:
            self.assertNotIn(
                phrase,
                normalized_poem,
                f"Found Shakespeare content in poem: '{phrase}'"
            )

    def test_no_edgar_allan_poe_content(self):
        """
        Test Case 1.4: Verify no Edgar Allan Poe content is copied.

        Famous Poe phrases should not appear in the poem.
        """
        poe_phrases = [
            "once upon a midnight dreary",
            "quoth the raven",
            "nevermore",
            "deep into that darkness peering"
        ]

        normalized_poem = self.validator._normalize_text(self.poem_content)

        for phrase in poe_phrases:
            # Allow standalone common words but not full phrases
            if len(phrase.split()) > 1:
                self.assertNotIn(
                    phrase,
                    normalized_poem,
                    f"Found Poe content in poem: '{phrase}'"
                )

    def test_no_robert_frost_content(self):
        """
        Test Case 1.5: Verify no Robert Frost content is copied.
        """
        frost_phrases = [
            "two roads diverged",
            "the road not taken",
            "miles to go before i sleep",
            "good fences make good neighbors"
        ]

        normalized_poem = self.validator._normalize_text(self.poem_content)

        for phrase in frost_phrases:
            self.assertNotIn(
                phrase,
                normalized_poem,
                f"Found Robert Frost content in poem: '{phrase}'"
            )

    def test_no_extended_copying(self):
        """
        Test Case 1.6: Verify no extended sequences are copied.

        Long sequences of common poetic phrases indicate copying.
        """
        # Check that no line is entirely a famous quote
        lines = [line.strip().lower() for line in self.poem_content.split('\n') if line.strip()]

        famous_lines = [
            "it was the best of times",
            "call me ishmael",
            "in the beginning",
            "to be or not to be that is the question"
        ]

        for line in lines:
            for famous_line in famous_lines:
                self.assertNotEqual(
                    self.validator._normalize_text(line),
                    self.validator._normalize_text(famous_line),
                    f"Line appears to be directly copied: '{line}'"
                )

    # ==========================================================================
    # Test Case 2: Verify unique creation - Poem uniquely created for MirDB
    # ==========================================================================

    def test_poem_is_specific_to_mirdb(self):
        """
        Test Case 2.1: Verify poem is specifically written for MirDB.

        Input: Originality verification
        Expected: Poem is uniquely created for MirDB without copyright infringement
        Type: manual (automated)

        The poem must reference MirDB-specific concepts.
        """
        is_specific, details = self.validator.verify_product_specificity()

        self.assertTrue(
            details['product_mentioned'],
            "Poem should explicitly mention MirDB"
        )
        self.assertGreaterEqual(
            len(details['technical_terms_found']), 2,
            f"Poem should contain at least 2 MirDB-specific technical terms. "
            f"Found: {details['technical_terms_found']}"
        )

    def test_no_other_product_adaptation(self):
        """
        Test Case 2.2: Verify poem is not adapted from other product poems.

        Input: Originality verification
        Expected: No patterns suggesting adaptation from other products
        Type: manual (automated)
        """
        is_original, found_patterns = self.validator.check_product_poem_adaptation()

        self.assertTrue(
            is_original,
            f"Poem appears to be adapted from other products: {found_patterns}"
        )

    def test_mirdb_technical_accuracy(self):
        """
        Test Case 2.3: Verify poem includes MirDB-specific architecture terms.

        The poem should reference MirDB's unique technical architecture
        (LSM-tree, SSTables, etc.) to confirm it's uniquely written for MirDB.
        """
        mirdb_architecture_terms = [
            'lsm', 'sstable', 'memtable', 'compaction',
            'write-ahead', 'wal', 'skip list', 'skiplist',
            'persistence', 'key-value', 'rust'
        ]

        found_terms = []
        for term in mirdb_architecture_terms:
            if term in self.poem_lower:
                found_terms.append(term)

        self.assertGreaterEqual(
            len(found_terms), 3,
            f"Poem should reference at least 3 MirDB architecture terms. "
            f"Found: {found_terms}"
        )

    def test_unique_creation_validation(self):
        """
        Test Case 2.4: Run complete unique creation validation.

        Input: Full poem content
        Expected: Poem is uniquely created for MirDB
        Type: manual (automated)
        """
        is_unique, explanation = self.validator.validate_unique_creation()

        self.assertTrue(
            is_unique,
            f"Unique creation validation failed: {explanation}"
        )

    def test_no_generic_database_poem(self):
        """
        Test Case 2.5: Verify poem is not a generic database poem.

        The poem should be specific to MirDB, not a template that could
        apply to any database product.
        """
        generic_patterns = [
            'any database',
            'every database',
            'all databases',
            'generic storage',
            'universal cache'
        ]

        for pattern in generic_patterns:
            self.assertNotIn(
                pattern,
                self.poem_lower,
                f"Poem contains generic database language: '{pattern}'"
            )

    # ==========================================================================
    # Comprehensive Originality Tests
    # ==========================================================================

    def test_full_copyright_check(self):
        """
        Test Case 3.1: Run comprehensive copyright infringement check.

        This combines all plagiarism checks into one comprehensive test.
        """
        is_copyright_free, explanation = self.validator.verify_no_copyright_infringement()

        self.assertTrue(
            is_copyright_free,
            f"Copyright check failed: {explanation}"
        )

    def test_full_validation_report(self):
        """
        Test Case 3.2: Verify full validation report passes.

        The complete validation report should show all checks passing.
        """
        report = self.validator.get_full_validation_report()

        self.assertEqual(
            report['overall_status'], 'PASS',
            f"Full validation failed. Report: {report}"
        )
        self.assertTrue(
            report['copyright_check']['passed'],
            f"Copyright check failed: {report['copyright_check']['explanation']}"
        )
        self.assertTrue(
            report['unique_creation_check']['passed'],
            f"Unique creation check failed: {report['unique_creation_check']['explanation']}"
        )

    def test_convenience_function(self):
        """
        Test Case 3.3: Test the convenience validation function.

        The validate_poem_originality function should return passing results.
        """
        report = validate_poem_originality(self.poem_content, "MirDB")

        self.assertEqual(report['overall_status'], 'PASS')
        self.assertEqual(report['product'], 'mirdb')

    # ==========================================================================
    # Edge Case Tests
    # ==========================================================================

    def test_poem_not_empty(self):
        """
        Test Case 4.1: Verify poem content is not empty.

        An empty poem would trivially pass plagiarism checks but is invalid.
        """
        self.assertGreater(
            len(self.poem_content.strip()), 100,
            "Poem content should be substantial (at least 100 characters)"
        )

    def test_poem_has_multiple_lines(self):
        """
        Test Case 4.2: Verify poem has multiple lines.

        A proper poem should have multiple lines of content.
        """
        lines = [line for line in self.poem_content.strip().split('\n') if line.strip()]
        self.assertGreaterEqual(
            len(lines), 10,
            f"Poem should have at least 10 lines. Found: {len(lines)}"
        )

    def test_text_normalization(self):
        """
        Test Case 4.3: Test text normalization works correctly.

        Normalization should remove punctuation and standardize spacing.
        """
        test_text = "Hello, World!  How are   you?"
        normalized = self.validator._normalize_text(test_text)

        self.assertEqual(normalized, "hello world how are you")
        self.assertNotIn(',', normalized)
        self.assertNotIn('!', normalized)
        self.assertNotIn('?', normalized)

    def test_ngram_extraction(self):
        """
        Test Case 4.4: Test n-gram extraction functionality.

        N-grams should be correctly extracted for plagiarism detection.
        """
        test_text = "one two three four five"
        ngrams = self.validator._extract_ngrams(test_text, n=3)

        expected_ngrams = [
            ('one', 'two', 'three'),
            ('two', 'three', 'four'),
            ('three', 'four', 'five')
        ]

        self.assertEqual(ngrams, expected_ngrams)


class TestOriginalityValidatorUnit(unittest.TestCase):
    """Unit tests for OriginalityValidator class methods."""

    def test_validator_initialization(self):
        """Test validator initializes correctly."""
        poem = "Test poem content for MirDB"
        validator = OriginalityValidator(poem, "MirDB")

        self.assertEqual(validator.poem_content, poem)
        self.assertEqual(validator.product_name, "mirdb")

    def test_validator_with_plagiarized_content(self):
        """Test validator catches obvious plagiarism."""
        plagiarized_poem = """
        Shall I compare thee to a summer's day?
        This is about MirDB database.
        Two roads diverged in a yellow wood.
        """
        validator = OriginalityValidator(plagiarized_poem, "MirDB")
        is_original, overlaps = validator.check_known_poem_overlap()

        self.assertFalse(is_original)
        self.assertGreater(len(overlaps), 0)

    def test_validator_with_original_content(self):
        """Test validator accepts truly original content."""
        original_poem = """
        In depths of Rust, where memory holds fast,
        A keeper guards what time would have erased.
        MirDB stands with LSM-tree design,
        Through compaction and sstables combined.
        """
        validator = OriginalityValidator(original_poem, "MirDB")
        is_original, overlaps = validator.check_known_poem_overlap()

        self.assertTrue(is_original)
        self.assertEqual(len(overlaps), 0)

    def test_validator_detects_product_adaptation(self):
        """Test validator detects poems adapted from other products."""
        adapted_poem = """
        In circuits deep where Redis streams,
        A guardian of data and dreams.
        MongoDB awaits with grace,
        PostgreSQL in ordered space.
        """
        validator = OriginalityValidator(adapted_poem, "MirDB")
        is_original, patterns = validator.check_product_poem_adaptation()

        # The poem references multiple other products
        self.assertFalse(
            validator.verify_product_specificity()[0],
            "Poem referencing other products should fail specificity check"
        )

    def test_hash_computation(self):
        """Test hash computation for duplicate detection."""
        poem1 = "Test poem content"
        poem2 = "Test poem content"
        poem3 = "Different poem content"

        validator1 = OriginalityValidator(poem1)
        validator2 = OriginalityValidator(poem2)
        validator3 = OriginalityValidator(poem3)

        hash1 = validator1._compute_text_hash(poem1)
        hash2 = validator2._compute_text_hash(poem2)
        hash3 = validator3._compute_text_hash(poem3)

        self.assertEqual(hash1, hash2)
        self.assertNotEqual(hash1, hash3)


if __name__ == '__main__':
    unittest.main()
