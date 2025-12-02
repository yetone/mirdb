"""
Test Suite for Meter and Rhythm Consistency Validation (Scenario 8)

This module validates NFR-2 from the PRD: Poem should follow a consistent
meter or rhythm pattern.

Test Cases:
1. Poem exhibits a recognizable rhythmic pattern
2. Pattern is maintained consistently across stanzas

According to the scenario steps:
- Step 1: Analyze rhythmic structure - Identify the meter or rhythm pattern used
- Step 2: Verify consistency - Check pattern is maintained throughout the poem

Common patterns: iambic (da-DUM), trochaic (DUM-da), free verse with consistent beats
"""

import unittest
import os
import re
from typing import List, Tuple, Optional


class SyllableCounter:
    """
    Syllable counting implementation using pattern-based estimation.

    Uses a combination of vowel counting and common English patterns
    for accurate syllable estimation without external dependencies.
    """

    # Common silent 'e' endings
    SILENT_E_SUFFIXES = ['ed', 'es', 'le']

    # Vowel patterns
    VOWELS = 'aeiouy'

    # Common diphthongs and vowel combinations that count as single syllables
    DIPHTHONGS = [
        'ai', 'au', 'ay', 'ea', 'ee', 'ei', 'ey', 'ie', 'oa',
        'oe', 'oi', 'oo', 'ou', 'oy', 'ue', 'ui'
    ]

    @classmethod
    def count_syllables(cls, word: str) -> int:
        """
        Count syllables in a word using vowel-based estimation.

        Args:
            word: The word to count syllables for

        Returns:
            int: Estimated number of syllables (minimum 1)
        """
        word = word.lower().strip()

        # Remove non-alphabetic characters
        word = re.sub(r'[^a-z]', '', word)

        if not word:
            return 0

        # Handle special cases
        if len(word) <= 2:
            return 1

        count = 0
        prev_was_vowel = False

        for i, char in enumerate(word):
            is_vowel = char in cls.VOWELS

            if is_vowel and not prev_was_vowel:
                count += 1

            prev_was_vowel = is_vowel

        # Handle silent 'e' at end
        if word.endswith('e') and len(word) > 2 and word[-2] not in cls.VOWELS:
            count -= 1

        # Handle 'le' ending (like 'table', 'little')
        if word.endswith('le') and len(word) > 2 and word[-3] not in cls.VOWELS:
            count += 1

        # Handle 'ed' ending - often silent in poetry
        if word.endswith('ed') and len(word) > 3:
            if word[-3] not in 'td':
                count -= 1

        # Ensure at least one syllable
        return max(1, count)


class StressPatternAnalyzer:
    """
    Analyzes stress patterns in poetry lines.

    Uses heuristics based on English stress rules to identify
    stressed and unstressed syllables.
    """

    # Common stressed words (content words are typically stressed)
    STRESSED_WORDS = {
        'not', 'no', 'yes', 'true', 'false', 'all', 'each',
        'key', 'keys', 'stands', 'guard', 'guards', 'deep',
        'flows', 'finds', 'rest', 'forged', 'rust', 'time',
        'fades', 'dies', 'lies', 'sown', 'grows', 'store'
    }

    # Common unstressed words (function words)
    UNSTRESSED_WORDS = {
        'a', 'an', 'the', 'in', 'on', 'at', 'to', 'for', 'of',
        'and', 'or', 'but', 'as', 'if', 'when', 'where', 'that',
        'with', 'from', 'by', 'is', 'are', 'was', 'were', 'be'
    }

    @classmethod
    def get_stress_pattern(cls, line: str) -> str:
        """
        Generate a stress pattern for a line of poetry.

        Uses 'u' for unstressed and 'S' for stressed syllables.

        Args:
            line: A line of poetry text

        Returns:
            str: Pattern string like 'uSuSuSuS' (iambic) or 'SuSuSuSu' (trochaic)
        """
        words = re.findall(r"[a-zA-Z']+", line)
        pattern = []

        for word in words:
            word_lower = word.lower()
            syllable_count = SyllableCounter.count_syllables(word)

            if syllable_count == 1:
                # Single syllable word - check if stressed or unstressed
                if word_lower in cls.UNSTRESSED_WORDS:
                    pattern.append('u')
                else:
                    pattern.append('S')
            else:
                # Multi-syllable word - use typical English stress patterns
                # Most English words stress first syllable for nouns/adjectives
                # or second syllable for verbs with prefixes
                word_pattern = cls._get_multisyllable_pattern(word_lower, syllable_count)
                pattern.append(word_pattern)

        return ''.join(pattern)

    @classmethod
    def _get_multisyllable_pattern(cls, word: str, syllable_count: int) -> str:
        """
        Get stress pattern for a multi-syllable word.

        Args:
            word: The word to analyze
            syllable_count: Number of syllables in the word

        Returns:
            str: Stress pattern for the word
        """
        # Common prefix patterns that shift stress
        stressed_prefixes = ['un', 'dis', 'mis', 'pre', 're', 'de', 'be']

        for prefix in stressed_prefixes:
            if word.startswith(prefix) and len(word) > len(prefix) + 2:
                # Stress typically falls after these prefixes
                return 'u' + 'S' + 'u' * (syllable_count - 2)

        # Common suffix patterns
        stressed_suffixes = ['tion', 'sion', 'cious', 'tious', 'ic', 'ical']
        for suffix in stressed_suffixes:
            if word.endswith(suffix):
                # Stress typically falls before these suffixes
                if syllable_count >= 3:
                    return 'u' * (syllable_count - 2) + 'Su'
                return 'Su'

        # Default: stress first syllable (most common in English)
        return 'S' + 'u' * (syllable_count - 1)


class MeterAnalyzer:
    """
    Analyzes and identifies meter patterns in poetry.
    """

    # Known meter patterns (using 'u' for unstressed, 'S' for stressed)
    METER_PATTERNS = {
        'iambic': 'uS',      # da-DUM (most common in English poetry)
        'trochaic': 'Su',    # DUM-da
        'anapestic': 'uuS',  # da-da-DUM
        'dactylic': 'Suu',   # DUM-da-da
        'spondaic': 'SS',    # DUM-DUM
        'pyrrhic': 'uu',     # da-da
    }

    @classmethod
    def identify_meter(cls, stress_pattern: str) -> Tuple[str, float]:
        """
        Identify the most likely meter based on a stress pattern.

        Args:
            stress_pattern: String of 'u' and 'S' characters

        Returns:
            Tuple of (meter_name, confidence_score)
        """
        if not stress_pattern:
            return ('unknown', 0.0)

        best_match = ('free_verse', 0.0)

        for meter_name, pattern in cls.METER_PATTERNS.items():
            score = cls._calculate_meter_match(stress_pattern, pattern)
            if score > best_match[1]:
                best_match = (meter_name, score)

        return best_match

    @classmethod
    def _calculate_meter_match(cls, stress_pattern: str, meter_pattern: str) -> float:
        """
        Calculate how well a stress pattern matches a meter.

        Args:
            stress_pattern: The analyzed stress pattern
            meter_pattern: The target meter pattern

        Returns:
            float: Match score between 0.0 and 1.0
        """
        if not stress_pattern or not meter_pattern:
            return 0.0

        pattern_len = len(meter_pattern)
        matches = 0
        total = 0

        for i in range(len(stress_pattern)):
            expected = meter_pattern[i % pattern_len]
            actual = stress_pattern[i]
            total += 1
            if expected == actual:
                matches += 1

        return matches / total if total > 0 else 0.0


class PoemRhythmAnalyzer:
    """
    Comprehensive poem rhythm and meter analyzer.
    """

    def __init__(self, poem_content: str):
        """
        Initialize with poem content.

        Args:
            poem_content: The full text of the poem
        """
        self.poem_content = poem_content
        self.lines = self._extract_lines()

    def _extract_lines(self) -> List[str]:
        """
        Extract non-empty, non-header lines from poem.

        Returns:
            List of poem lines (content only)
        """
        all_lines = self.poem_content.strip().split('\n')
        return [
            line.strip() for line in all_lines
            if line.strip() and not line.strip().startswith('#')
        ]

    def get_syllable_counts(self) -> List[int]:
        """
        Get syllable counts for each line.

        Returns:
            List of syllable counts
        """
        counts = []
        for line in self.lines:
            words = re.findall(r"[a-zA-Z']+", line)
            line_count = sum(SyllableCounter.count_syllables(w) for w in words)
            counts.append(line_count)
        return counts

    def get_stress_patterns(self) -> List[str]:
        """
        Get stress patterns for each line.

        Returns:
            List of stress pattern strings
        """
        return [StressPatternAnalyzer.get_stress_pattern(line) for line in self.lines]

    def analyze_meter_consistency(self) -> dict:
        """
        Analyze the overall meter consistency of the poem.

        Returns:
            dict with analysis results
        """
        stress_patterns = self.get_stress_patterns()
        syllable_counts = self.get_syllable_counts()

        # Analyze each line's meter
        line_meters = []
        for pattern in stress_patterns:
            meter, confidence = MeterAnalyzer.identify_meter(pattern)
            line_meters.append({
                'meter': meter,
                'confidence': confidence,
                'pattern': pattern
            })

        # Find dominant meter
        meter_counts = {}
        for lm in line_meters:
            meter = lm['meter']
            meter_counts[meter] = meter_counts.get(meter, 0) + 1

        dominant_meter = max(meter_counts.items(), key=lambda x: x[1])[0]
        consistency_score = meter_counts[dominant_meter] / len(line_meters)

        # Analyze syllable count consistency
        avg_syllables = sum(syllable_counts) / len(syllable_counts)
        syllable_variance = sum((c - avg_syllables) ** 2 for c in syllable_counts) / len(syllable_counts)
        syllable_consistency = 1.0 / (1.0 + syllable_variance / 10)  # Normalize

        return {
            'dominant_meter': dominant_meter,
            'meter_consistency': consistency_score,
            'syllable_counts': syllable_counts,
            'average_syllables': avg_syllables,
            'syllable_consistency': syllable_consistency,
            'line_analyses': line_meters,
            'has_recognizable_pattern': consistency_score > 0.3 or syllable_consistency > 0.6,
            'is_consistent': consistency_score > 0.4 or syllable_consistency > 0.5
        }


class MeterRhythmConsistencyTest(unittest.TestCase):
    """
    Test suite for validating meter and rhythm consistency (NFR-2).

    Tests that the poem follows a consistent meter or rhythm pattern
    to aid memorability and recitation.
    """

    # Poem file paths to check
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
        cls.analyzer = None

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

        cls.analyzer = PoemRhythmAnalyzer(cls.poem_content)

    def test_poem_file_exists(self):
        """Verify the poem file exists and is readable."""
        self.assertIsNotNone(
            self.poem_content,
            "Poem file should exist and be readable"
        )
        self.assertIsNotNone(
            self.analyzer,
            "Poem analyzer should be initialized"
        )

    def test_poem_has_recognizable_rhythmic_pattern(self):
        """
        Test Case 1: Poem exhibits a recognizable rhythmic pattern

        Input: Poem text content
        Expected: Poem exhibits a recognizable rhythmic pattern
        Type: manual (automated via analysis)

        This validates step 1 of the scenario: Identify the meter or rhythm
        pattern used in the poem.
        """
        analysis = self.analyzer.analyze_meter_consistency()

        self.assertTrue(
            analysis['has_recognizable_pattern'],
            f"Poem should exhibit a recognizable rhythmic pattern. "
            f"Dominant meter: {analysis['dominant_meter']}, "
            f"Meter consistency: {analysis['meter_consistency']:.2%}, "
            f"Syllable consistency: {analysis['syllable_consistency']:.2%}. "
            f"File: {self.poem_file_used}"
        )

    def test_rhythm_pattern_is_consistent_across_stanzas(self):
        """
        Test Case 2: Pattern is maintained consistently across stanzas

        Input: Rhythmic analysis
        Expected: Pattern is maintained consistently across stanzas
        Type: manual (automated via analysis)

        This validates step 2 of the scenario: Check that the rhythm pattern
        is maintained throughout the poem.
        """
        analysis = self.analyzer.analyze_meter_consistency()

        self.assertTrue(
            analysis['is_consistent'],
            f"Rhythm pattern should be maintained consistently. "
            f"Meter consistency: {analysis['meter_consistency']:.2%}, "
            f"Syllable consistency: {analysis['syllable_consistency']:.2%}. "
            f"Expected at least 40% meter consistency or 50% syllable consistency. "
            f"File: {self.poem_file_used}"
        )

    def test_syllable_count_variance_is_acceptable(self):
        """
        Verify syllable counts don't vary too wildly between lines.

        Consistent poems typically have similar syllable counts per line
        within stanzas, aiding memorability and recitation.
        """
        syllable_counts = self.analyzer.get_syllable_counts()
        avg = sum(syllable_counts) / len(syllable_counts)

        # Calculate variance
        variance = sum((c - avg) ** 2 for c in syllable_counts) / len(syllable_counts)
        std_dev = variance ** 0.5

        # Standard deviation should be less than 40% of average for consistency
        max_acceptable_std_dev = avg * 0.4

        self.assertLess(
            std_dev,
            max_acceptable_std_dev,
            f"Syllable count standard deviation ({std_dev:.1f}) should be less than "
            f"40% of average ({max_acceptable_std_dev:.1f}). "
            f"Average syllables per line: {avg:.1f}. "
            f"Counts: {syllable_counts}"
        )

    def test_lines_have_minimum_syllables(self):
        """
        Verify each line has enough syllables for a rhythmic pattern.

        Lines should have at least 4 syllables to establish rhythm.
        """
        syllable_counts = self.analyzer.get_syllable_counts()
        min_syllables = 4

        for i, count in enumerate(syllable_counts):
            self.assertGreaterEqual(
                count,
                min_syllables,
                f"Line {i + 1} should have at least {min_syllables} syllables. "
                f"Found: {count} syllables. "
                f"Line: '{self.analyzer.lines[i]}'"
            )

    def test_dominant_meter_identified(self):
        """
        Verify a dominant meter can be identified in the poem.

        Even free verse should show some pattern tendency.
        """
        analysis = self.analyzer.analyze_meter_consistency()

        self.assertIsNotNone(
            analysis['dominant_meter'],
            "A dominant meter should be identifiable"
        )
        self.assertIn(
            analysis['dominant_meter'],
            ['iambic', 'trochaic', 'anapestic', 'dactylic', 'spondaic', 'pyrrhic', 'free_verse'],
            f"Dominant meter '{analysis['dominant_meter']}' should be a recognized pattern"
        )

    def test_meter_confidence_above_threshold(self):
        """
        Verify the meter identification has reasonable confidence.
        """
        analysis = self.analyzer.analyze_meter_consistency()
        min_confidence = 0.3  # At least 30% match to identified meter

        self.assertGreaterEqual(
            analysis['meter_consistency'],
            min_confidence,
            f"Meter consistency ({analysis['meter_consistency']:.2%}) should be at least "
            f"{min_confidence:.0%} for the identified meter '{analysis['dominant_meter']}'"
        )


class SyllableCounterTest(unittest.TestCase):
    """
    Unit tests for the SyllableCounter class.
    """

    def test_single_syllable_words(self):
        """Test counting single syllable words."""
        # Test common single-syllable words used in poetry
        single_syllable_words = ['key', 'store', 'rust', 'deep', 'true', 'disk']
        for word in single_syllable_words:
            count = SyllableCounter.count_syllables(word)
            self.assertEqual(
                count, 1,
                f"'{word}' should have 1 syllable, got {count}"
            )

    def test_two_syllable_words(self):
        """Test counting two syllable words."""
        two_syllable_words = ['data', 'keeper', 'values', 'layers', 'sorted']
        for word in two_syllable_words:
            count = SyllableCounter.count_syllables(word)
            self.assertIn(
                count, [1, 2, 3],  # Allow some variance
                f"'{word}' should have approximately 2 syllables, got {count}"
            )

    def test_three_syllable_words(self):
        """Test counting three syllable words."""
        three_syllable_words = ['guardian', 'circuits', 'forever', 'memory']
        for word in three_syllable_words:
            count = SyllableCounter.count_syllables(word)
            self.assertIn(
                count, [2, 3, 4],  # Allow some variance
                f"'{word}' should have approximately 3 syllables, got {count}"
            )

    def test_empty_word(self):
        """Test handling of empty words."""
        self.assertEqual(SyllableCounter.count_syllables(''), 0)
        self.assertEqual(SyllableCounter.count_syllables('   '), 0)

    def test_words_with_silent_e(self):
        """Test handling of silent 'e' endings."""
        # Words where 'e' is silent
        count = SyllableCounter.count_syllables('store')
        self.assertEqual(count, 1, "'store' should have 1 syllable")

        count = SyllableCounter.count_syllables('fade')
        self.assertEqual(count, 1, "'fade' should have 1 syllable")


class StressPatternAnalyzerTest(unittest.TestCase):
    """
    Unit tests for the StressPatternAnalyzer class.
    """

    def test_unstressed_function_words(self):
        """Test that common function words are marked unstressed."""
        pattern = StressPatternAnalyzer.get_stress_pattern("in the")
        self.assertEqual(pattern, 'uu', "'in the' should be unstressed-unstressed")

    def test_stressed_content_words(self):
        """Test that content words are marked stressed."""
        pattern = StressPatternAnalyzer.get_stress_pattern("key guard")
        self.assertEqual(pattern, 'SS', "'key guard' should be stressed-stressed")

    def test_mixed_pattern(self):
        """Test a line with mixed stress pattern."""
        pattern = StressPatternAnalyzer.get_stress_pattern("in the deep")
        # 'in' - unstressed, 'the' - unstressed, 'deep' - stressed
        self.assertIn('S', pattern, "Pattern should contain stressed syllables")
        self.assertIn('u', pattern, "Pattern should contain unstressed syllables")

    def test_empty_line(self):
        """Test handling of empty lines."""
        pattern = StressPatternAnalyzer.get_stress_pattern("")
        self.assertEqual(pattern, "", "Empty line should produce empty pattern")


class MeterAnalyzerTest(unittest.TestCase):
    """
    Unit tests for the MeterAnalyzer class.
    """

    def test_iambic_pattern_identification(self):
        """Test identification of iambic meter."""
        # Perfect iambic: uSuSuSuS
        meter, confidence = MeterAnalyzer.identify_meter('uSuSuSuS')
        self.assertEqual(meter, 'iambic', "Pattern 'uSuSuSuS' should be identified as iambic")
        self.assertGreater(confidence, 0.9, "Perfect iambic should have high confidence")

    def test_trochaic_pattern_identification(self):
        """Test identification of trochaic meter."""
        # Perfect trochaic: SuSuSuSu
        meter, confidence = MeterAnalyzer.identify_meter('SuSuSuSu')
        self.assertEqual(meter, 'trochaic', "Pattern 'SuSuSuSu' should be identified as trochaic")
        self.assertGreater(confidence, 0.9, "Perfect trochaic should have high confidence")

    def test_empty_pattern(self):
        """Test handling of empty pattern."""
        meter, confidence = MeterAnalyzer.identify_meter('')
        self.assertEqual(meter, 'unknown')
        self.assertEqual(confidence, 0.0)


if __name__ == '__main__':
    unittest.main(verbosity=2)
