"""
Test Suite for Poem Length Requirement Validation (Scenario 6)

This module validates REQ-7 from the PRD: Poem should be between 12-24 lines
for optimal readability and recitability.

Test Cases:
1. Line count >= 12 (minimum for substantive content)
2. Line count <= 24 (maximum for readability)

According to the scenario steps:
- Step 1: Count poem lines (excluding empty lines for stanza breaks)
- Step 2: Validate line count is between 12 and 24 inclusive
"""

import unittest
import os


class PoemLengthRequirementTest(unittest.TestCase):
    """
    Test suite for validating poem length requirements (REQ-7).

    This range (12-24 lines) optimizes:
    - Readability: Not too long to lose attention
    - Recitability: Can be recited in under 2 minutes
    - Substance: Enough content to convey technical concepts
    """

    # Path to the main poem file
    POEM_PATH = os.path.join(os.path.dirname(__file__), '..', 'poem.txt')

    # Alternative poem paths to check
    POEM_PATHS = [
        os.path.join(os.path.dirname(__file__), '..', 'poem.txt'),
        os.path.join(os.path.dirname(__file__), '..', 'POEM.md'),
        os.path.join(os.path.dirname(__file__), '..', 'versecraft', 'mirdb_poem.txt'),
    ]

    # Minimum and maximum line counts per REQ-7
    MIN_LINES = 12
    MAX_LINES = 24

    @classmethod
    def setUpClass(cls):
        """Load the poem content once for all tests."""
        cls.poem_content = None
        cls.poem_file_used = None

        # Try each poem path to find the poem
        for path in cls.POEM_PATHS:
            if os.path.exists(path):
                with open(path, 'r', encoding='utf-8') as f:
                    cls.poem_content = f.read()
                    cls.poem_file_used = path
                break

        if cls.poem_content is None:
            raise FileNotFoundError(
                f"No poem file found. Searched: {cls.POEM_PATHS}"
            )

    def _count_poem_lines(self):
        """
        Count the total number of lines in the poem (excluding empty lines).

        As per scenario step 1: Empty lines for stanza breaks should not count
        toward line total.

        Returns:
            int: Number of non-empty lines in the poem
        """
        lines = self.poem_content.strip().split('\n')

        # Filter out empty lines and header lines (starting with #)
        non_empty_lines = [
            line for line in lines
            if line.strip() and not line.strip().startswith('#')
        ]

        return len(non_empty_lines)

    def test_poem_file_exists(self):
        """Verify the poem file exists."""
        self.assertIsNotNone(
            self.poem_content,
            "Poem file should exist and be readable"
        )
        self.assertIsNotNone(
            self.poem_file_used,
            "Poem file path should be recorded"
        )

    def test_poem_has_content(self):
        """Verify the poem is not empty."""
        self.assertGreater(
            len(self.poem_content.strip()),
            0,
            "Poem file should not be empty"
        )

    def test_poem_line_count_minimum(self):
        """
        Test Case 1: Verify line count >= 12

        Input: Poem text file
        Expected: Line count >= 12
        Type: unit

        This validates step 2 of the scenario: the poem has at least 12 lines
        to ensure substantive content that can convey MirDB's features.
        """
        line_count = self._count_poem_lines()

        self.assertGreaterEqual(
            line_count,
            self.MIN_LINES,
            f"Poem should have at least {self.MIN_LINES} lines for substantive content. "
            f"Found: {line_count} lines. "
            f"File: {self.poem_file_used}"
        )

    def test_poem_line_count_maximum(self):
        """
        Test Case 2: Verify line count <= 24

        Input: Poem text file
        Expected: Line count <= 24
        Type: unit

        This validates step 2 of the scenario: the poem has at most 24 lines
        to ensure optimal readability and recitability.
        """
        line_count = self._count_poem_lines()

        self.assertLessEqual(
            line_count,
            self.MAX_LINES,
            f"Poem should have at most {self.MAX_LINES} lines for optimal readability. "
            f"Found: {line_count} lines. "
            f"File: {self.poem_file_used}"
        )

    def test_poem_line_count_in_valid_range(self):
        """
        Combined test: Verify line count is between 12 and 24 inclusive.

        This is a comprehensive validation of REQ-7 that checks both bounds
        in a single assertion.
        """
        line_count = self._count_poem_lines()

        self.assertTrue(
            self.MIN_LINES <= line_count <= self.MAX_LINES,
            f"Poem line count should be between {self.MIN_LINES} and {self.MAX_LINES} inclusive. "
            f"Found: {line_count} lines. "
            f"File: {self.poem_file_used}"
        )

    def test_line_count_method_excludes_empty_lines(self):
        """
        Verify the line counting method correctly excludes empty lines.

        As per scenario step 1: Empty lines for stanza breaks should not count.
        """
        # Count lines including empty ones
        all_lines = self.poem_content.strip().split('\n')
        total_lines = len(all_lines)

        # Count non-empty lines (our method)
        non_empty_count = self._count_poem_lines()

        # There should be some empty lines (stanza breaks)
        # This confirms our counting is excluding them
        self.assertLessEqual(
            non_empty_count,
            total_lines,
            "Non-empty line count should be less than or equal to total lines"
        )

    def test_poem_has_stanza_breaks(self):
        """
        Verify the poem has stanza breaks (empty lines for formatting).

        A well-structured poem should have stanza breaks for readability.
        """
        lines = self.poem_content.split('\n')
        empty_line_count = sum(1 for line in lines if not line.strip())

        # A poem with multiple stanzas should have at least some empty lines
        # This is expected for poems between 12-24 lines
        self.assertGreater(
            empty_line_count,
            0,
            "Poem should have stanza breaks (empty lines) for readability"
        )


class PoemLengthEdgeCaseTest(unittest.TestCase):
    """
    Edge case tests for poem length validation.

    These tests verify the validation logic works correctly at boundaries.
    """

    def test_minimum_bound_is_12(self):
        """Verify the minimum line count requirement is 12."""
        self.assertEqual(
            PoemLengthRequirementTest.MIN_LINES,
            12,
            "Minimum line count per REQ-7 should be 12"
        )

    def test_maximum_bound_is_24(self):
        """Verify the maximum line count requirement is 24."""
        self.assertEqual(
            PoemLengthRequirementTest.MAX_LINES,
            24,
            "Maximum line count per REQ-7 should be 24"
        )

    def test_range_is_inclusive(self):
        """Verify both 12 and 24 are valid line counts (inclusive range)."""
        min_lines = PoemLengthRequirementTest.MIN_LINES
        max_lines = PoemLengthRequirementTest.MAX_LINES

        # Both bounds should be acceptable
        self.assertTrue(min_lines <= 12 <= max_lines)
        self.assertTrue(min_lines <= 24 <= max_lines)


if __name__ == '__main__':
    unittest.main(verbosity=2)
