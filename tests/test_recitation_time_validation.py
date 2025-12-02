"""
Test Suite for Recitation Time Validation (Scenario 13)

This module validates the Success Criteria from the PRD: Poem can be recited in under 2 minutes.

Test Cases:
1. Word count allows for recitation under 2 minutes (roughly < 300 words)
2. Actual recitation time estimation is under 120 seconds

According to the scenario steps:
- Step 1: Calculate estimated read time based on word count and complexity
  - Average recitation pace is ~120-150 words per minute with pauses
- Step 2: Perform actual recitation test (manual test case)
  - Include appropriate dramatic pauses for stanza breaks
"""

import unittest
import os
import re


class RecitationTimeValidator:
    """
    Utility class for validating poem recitation time.

    Uses word count and complexity metrics to estimate recitation time.
    Average recitation pace is ~120-150 words per minute with pauses.
    """

    # Recitation pace constants
    SLOW_PACE_WPM = 120  # Words per minute for slow, dramatic recitation
    FAST_PACE_WPM = 150  # Words per minute for faster recitation
    AVERAGE_PACE_WPM = 130  # Average pace with natural pauses

    # Maximum allowed recitation time in seconds
    MAX_RECITATION_TIME_SECONDS = 120  # 2 minutes

    # Maximum word count for 2-minute recitation at slowest pace
    MAX_WORDS_FOR_2_MINUTES = SLOW_PACE_WPM * 2  # 240 words at 120 wpm
    # With some buffer for dramatic pauses, we use ~300 as upper limit
    MAX_WORDS_WITH_BUFFER = 300

    # Stanza pause duration in seconds
    STANZA_PAUSE_SECONDS = 2  # Average pause between stanzas

    def __init__(self, poem_content):
        """
        Initialize the validator with poem content.

        Args:
            poem_content: The full text content of the poem
        """
        self.poem_content = poem_content
        self._word_count = None
        self._stanza_count = None

    def get_word_count(self):
        """
        Count the total number of words in the poem.

        Excludes:
        - Title/header lines (starting with #)
        - Empty lines

        Returns:
            int: Total word count
        """
        if self._word_count is not None:
            return self._word_count

        lines = self.poem_content.strip().split('\n')
        word_count = 0

        for line in lines:
            stripped = line.strip()
            # Skip empty lines and markdown headers
            if not stripped or stripped.startswith('#'):
                continue
            # Count words in the line
            words = stripped.split()
            word_count += len(words)

        self._word_count = word_count
        return self._word_count

    def get_stanza_count(self):
        """
        Count the number of stanzas in the poem.

        Stanzas are separated by empty lines.

        Returns:
            int: Number of stanzas
        """
        if self._stanza_count is not None:
            return self._stanza_count

        lines = self.poem_content.strip().split('\n')
        stanza_count = 0
        in_stanza = False

        for line in lines:
            stripped = line.strip()
            # Skip headers
            if stripped.startswith('#'):
                continue

            if stripped:
                if not in_stanza:
                    stanza_count += 1
                    in_stanza = True
            else:
                in_stanza = False

        self._stanza_count = max(stanza_count, 1)
        return self._stanza_count

    def estimate_recitation_time_seconds(self, pace_wpm=None):
        """
        Estimate the recitation time in seconds.

        Takes into account:
        - Word count at specified pace
        - Pause time between stanzas

        Args:
            pace_wpm: Words per minute pace. Defaults to AVERAGE_PACE_WPM.

        Returns:
            float: Estimated recitation time in seconds
        """
        if pace_wpm is None:
            pace_wpm = self.AVERAGE_PACE_WPM

        word_count = self.get_word_count()
        stanza_count = self.get_stanza_count()

        # Calculate base reading time
        reading_time_minutes = word_count / pace_wpm
        reading_time_seconds = reading_time_minutes * 60

        # Add stanza pauses (n-1 pauses for n stanzas)
        pause_time_seconds = (stanza_count - 1) * self.STANZA_PAUSE_SECONDS

        total_time_seconds = reading_time_seconds + pause_time_seconds

        return total_time_seconds

    def is_recitable_in_two_minutes(self):
        """
        Check if the poem can be recited in under 2 minutes.

        Uses the slow pace (120 wpm) to be conservative.

        Returns:
            bool: True if recitation time is under 120 seconds
        """
        # Use slow pace to be conservative
        estimated_time = self.estimate_recitation_time_seconds(self.SLOW_PACE_WPM)
        return estimated_time < self.MAX_RECITATION_TIME_SECONDS

    def word_count_allows_two_minute_recitation(self):
        """
        Check if word count alone allows for 2-minute recitation.

        Returns:
            bool: True if word count is under ~300 words
        """
        return self.get_word_count() < self.MAX_WORDS_WITH_BUFFER


class RecitationTimeValidationTest(unittest.TestCase):
    """
    Test suite for validating poem recitation time requirements.

    This validates Story 4 from the PRD: As a conference presenter,
    I want a memorable poem I can recite or display,
    So that my audience remembers MirDB after the presentation.

    Acceptance Criteria:
    - And the poem takes less than 2 minutes to recite
    """

    # Paths to poem files
    POEM_PATHS = [
        os.path.join(os.path.dirname(__file__), '..', 'poem.txt'),
        os.path.join(os.path.dirname(__file__), '..', 'POEM.md'),
        os.path.join(os.path.dirname(__file__), '..', 'versecraft', 'mirdb_poem.txt'),
    ]

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

        cls.validator = RecitationTimeValidator(cls.poem_content)

    def test_poem_file_exists(self):
        """Verify a poem file exists and was loaded."""
        self.assertIsNotNone(
            self.poem_content,
            "Poem file should exist and be readable"
        )
        self.assertIsNotNone(
            self.poem_file_used,
            "Poem file path should be recorded"
        )

    def test_word_count_under_300(self):
        """
        Test Case 1: Verify word count allows for recitation under 2 minutes.

        Input: Poem word count
        Expected: Word count allows for recitation under 2 minutes (roughly < 300 words)
        Type: unit

        At a slow recitation pace of 120 words per minute, 240 words would take
        exactly 2 minutes. With a buffer for dramatic pauses, we allow up to 300 words.
        """
        word_count = self.validator.get_word_count()
        max_words = RecitationTimeValidator.MAX_WORDS_WITH_BUFFER

        self.assertLess(
            word_count,
            max_words,
            f"Poem word count should be under {max_words} words for 2-minute recitation. "
            f"Found: {word_count} words. "
            f"File: {self.poem_file_used}"
        )

    def test_word_count_allows_two_minute_recitation(self):
        """
        Verify the word count allows for recitation under 2 minutes.

        This uses the validator's method to check the constraint.
        """
        result = self.validator.word_count_allows_two_minute_recitation()
        word_count = self.validator.get_word_count()

        self.assertTrue(
            result,
            f"Word count ({word_count}) should allow for 2-minute recitation"
        )

    def test_estimated_recitation_time_under_120_seconds(self):
        """
        Test Case 2 (Unit Estimation): Verify estimated recitation time is under 120 seconds.

        Input: Timed recitation (estimated)
        Expected: Estimated recitation time is under 120 seconds
        Type: unit

        Note: The actual timed recitation is a manual test case as it requires
        a human speaker. This test validates the estimation algorithm.
        """
        # Use slow pace for conservative estimate
        estimated_time = self.validator.estimate_recitation_time_seconds(
            RecitationTimeValidator.SLOW_PACE_WPM
        )
        max_time = RecitationTimeValidator.MAX_RECITATION_TIME_SECONDS

        self.assertLess(
            estimated_time,
            max_time,
            f"Estimated recitation time should be under {max_time} seconds. "
            f"Estimated: {estimated_time:.1f} seconds at {RecitationTimeValidator.SLOW_PACE_WPM} wpm. "
            f"File: {self.poem_file_used}"
        )

    def test_is_recitable_in_two_minutes(self):
        """
        Verify the poem passes the two-minute recitation check.

        This is the main validation method that checks all factors.
        """
        result = self.validator.is_recitable_in_two_minutes()
        estimated_time = self.validator.estimate_recitation_time_seconds(
            RecitationTimeValidator.SLOW_PACE_WPM
        )

        self.assertTrue(
            result,
            f"Poem should be recitable in under 2 minutes. "
            f"Estimated time: {estimated_time:.1f} seconds"
        )

    def test_stanza_count_reasonable(self):
        """
        Verify the poem has a reasonable number of stanzas.

        Too many stanzas would add too much pause time.
        """
        stanza_count = self.validator.get_stanza_count()

        # With 2-second pauses between stanzas, more than 30 stanzas
        # would add a full minute of pause time alone
        self.assertLess(
            stanza_count,
            30,
            f"Poem should have fewer than 30 stanzas. Found: {stanza_count}"
        )

        # Poem should have at least 1 stanza
        self.assertGreaterEqual(
            stanza_count,
            1,
            "Poem should have at least 1 stanza"
        )


class RecitationTimeValidatorUnitTest(unittest.TestCase):
    """
    Unit tests for the RecitationTimeValidator class itself.

    These tests validate the validator's algorithms work correctly.
    """

    def test_word_count_simple_poem(self):
        """Test word counting with a simple poem."""
        simple_poem = """
        Line one has four words
        Line two also has words
        """
        validator = RecitationTimeValidator(simple_poem)
        # "Line one has four words" = 5 words
        # "Line two also has words" = 5 words
        self.assertEqual(validator.get_word_count(), 10)

    def test_word_count_excludes_headers(self):
        """Test that markdown headers are excluded from word count."""
        poem_with_header = """# This Header Should Not Count

        These words should count though
        """
        validator = RecitationTimeValidator(poem_with_header)
        # Only "These words should count though" = 5 words
        self.assertEqual(validator.get_word_count(), 5)

    def test_stanza_count_single_stanza(self):
        """Test stanza counting with a single stanza."""
        single_stanza = """
        Line one
        Line two
        Line three
        """
        validator = RecitationTimeValidator(single_stanza)
        self.assertEqual(validator.get_stanza_count(), 1)

    def test_stanza_count_multiple_stanzas(self):
        """Test stanza counting with multiple stanzas."""
        multi_stanza = """
        Stanza one line one
        Stanza one line two

        Stanza two line one
        Stanza two line two

        Stanza three line one
        """
        validator = RecitationTimeValidator(multi_stanza)
        self.assertEqual(validator.get_stanza_count(), 3)

    def test_recitation_time_calculation(self):
        """Test recitation time estimation formula."""
        # Create a poem with exactly 120 words (1 minute at 120 wpm)
        words = ["word"] * 120
        poem = " ".join(words)

        validator = RecitationTimeValidator(poem)

        # At 120 wpm, 120 words should take 60 seconds
        time_at_120_wpm = validator.estimate_recitation_time_seconds(120)

        # Should be 60 seconds plus 0 stanza pauses (1 stanza = 0 pauses)
        self.assertEqual(time_at_120_wpm, 60.0)

    def test_recitation_time_includes_stanza_pauses(self):
        """Test that stanza pauses are included in time estimate."""
        # Create a poem with 2 stanzas
        stanza1 = " ".join(["word"] * 60)
        stanza2 = " ".join(["word"] * 60)
        poem = f"{stanza1}\n\n{stanza2}"

        validator = RecitationTimeValidator(poem)

        # At 120 wpm, 120 words = 60 seconds
        # Plus 1 stanza pause (2 seconds)
        # Total should be 62 seconds
        time = validator.estimate_recitation_time_seconds(120)
        self.assertEqual(time, 62.0)

    def test_constants_are_correct(self):
        """Verify the validator constants are set correctly."""
        self.assertEqual(RecitationTimeValidator.SLOW_PACE_WPM, 120)
        self.assertEqual(RecitationTimeValidator.FAST_PACE_WPM, 150)
        self.assertEqual(RecitationTimeValidator.MAX_RECITATION_TIME_SECONDS, 120)
        self.assertEqual(RecitationTimeValidator.MAX_WORDS_WITH_BUFFER, 300)


class ManualRecitationTestDocumentation(unittest.TestCase):
    """
    Documentation for the manual recitation test case.

    Test Case 2 (Manual): Timed recitation
    Input: Timed recitation
    Expected: Actual recitation time is under 120 seconds
    Type: manual

    This test case requires a human to:
    1. Read the poem aloud at a natural pace
    2. Include appropriate dramatic pauses for stanza breaks
    3. Time the complete recitation
    4. Verify it takes under 120 seconds (2 minutes)

    The automated tests provide estimation based on word count
    and industry-standard speaking rates.
    """

    POEM_PATHS = [
        os.path.join(os.path.dirname(__file__), '..', 'poem.txt'),
        os.path.join(os.path.dirname(__file__), '..', 'POEM.md'),
        os.path.join(os.path.dirname(__file__), '..', 'versecraft', 'mirdb_poem.txt'),
    ]

    @classmethod
    def setUpClass(cls):
        """Load the poem content for reference."""
        cls.poem_content = None

        for path in cls.POEM_PATHS:
            if os.path.exists(path):
                with open(path, 'r', encoding='utf-8') as f:
                    cls.poem_content = f.read()
                break

        if cls.poem_content:
            cls.validator = RecitationTimeValidator(cls.poem_content)

    def test_manual_recitation_instructions(self):
        """
        Provide instructions for manual recitation test.

        This test passes automatically but documents what a human tester
        should verify for the manual test case.
        """
        if self.poem_content is None:
            self.skipTest("No poem file found for manual test instructions")

        word_count = self.validator.get_word_count()
        stanza_count = self.validator.get_stanza_count()
        estimated_time = self.validator.estimate_recitation_time_seconds()

        instructions = f"""
        MANUAL RECITATION TEST INSTRUCTIONS
        ====================================

        Poem Statistics:
        - Word count: {word_count}
        - Stanza count: {stanza_count}
        - Estimated time: {estimated_time:.1f} seconds

        Test Procedure:
        1. Find a quiet space and prepare a timer
        2. Read the poem aloud at a natural, dramatic pace
        3. Include 2-second pauses between stanzas
        4. Start the timer when you begin, stop when you finish
        5. Record the total time

        Pass Criteria:
        - Total recitation time < 120 seconds (2 minutes)

        Based on the word count of {word_count} words and {stanza_count} stanzas,
        the estimated recitation time is {estimated_time:.1f} seconds,
        which is {"UNDER" if estimated_time < 120 else "OVER"} the 2-minute limit.
        """

        # This test always passes - it's documentation
        self.assertTrue(True, instructions)


if __name__ == '__main__':
    unittest.main(verbosity=2)
