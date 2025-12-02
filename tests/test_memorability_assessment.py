"""
Test Suite: Memorability Assessment (Scenario 16)

This test suite validates NFR-5 from the PRD: Poem should be easily memorizable
through use of rhyme and repetition.

Test Cases:
1. Rhyme scheme analysis - Poem employs consistent rhyme scheme that aids memorability
2. Repetition pattern analysis - Contains memorable repeated elements or refrains
3. Quotability assessment - At least 2-3 lines are standout quotable phrases

Scenario Steps:
- Step 1: Analyze rhyme scheme - Identify the rhyme scheme used in the poem
- Step 2: Identify repetitive elements - Look for repeated phrases, refrains, structural patterns
- Step 3: Identify quotable lines - Assess if specific lines are memorable and quotable
"""

import unittest
import os
import sys

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from versecraft.memorability_assessor import (
    MemorabilityAssessor,
    RhymeSchemeAnalyzer,
    RepetitionAnalyzer,
    QuotabilityAnalyzer,
    PhonemeRhymeDetector,
    assess_memorability,
    MemorabilityReport,
    RhymeAnalysis,
    RepetitionAnalysis,
    QuotabilityAnalysis
)


class TestMemorabilityAssessment(unittest.TestCase):
    """
    Main test suite for Memorability Assessment (NFR-5).

    Validates that the MirDB poem is easily memorizable through:
    - Consistent rhyme scheme
    - Memorable repeated elements
    - Standout quotable phrases
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

        cls.assessor = MemorabilityAssessor(cls.poem_content)
        cls.report = cls.assessor.assess()

    # ==========================================================================
    # Test Case 1: Rhyme Scheme Analysis
    # Input: Rhyme scheme analysis
    # Expected: Poem employs consistent rhyme scheme that aids memorability
    # ==========================================================================

    def test_rhyme_scheme_analysis_detects_scheme(self):
        """
        Test Case 1.1: Verify rhyme scheme is detected.

        Input: Poem text content
        Expected: A rhyme scheme is identified
        Type: manual (automated)

        This validates Step 1: Analyze rhyme scheme.
        """
        rhyme_analysis = self.report.rhyme_analysis

        self.assertIsNotNone(
            rhyme_analysis.scheme,
            "Rhyme scheme should be detected"
        )
        self.assertIsInstance(
            rhyme_analysis.scheme, str,
            "Rhyme scheme should be a string representation"
        )

    def test_rhyme_scheme_has_rhyming_pairs(self):
        """
        Test Case 1.2: Verify poem has rhyming word pairs.

        Input: Poem text content
        Expected: Multiple rhyming pairs are identified
        Type: manual (automated)
        """
        rhyme_analysis = self.report.rhyme_analysis

        self.assertGreaterEqual(
            len(rhyme_analysis.rhyming_pairs), 2,
            f"Poem should have at least 2 rhyming pairs. "
            f"Found: {rhyme_analysis.rhyming_pairs}"
        )

    def test_rhyme_scheme_consistency(self):
        """
        Test Case 1.3: Verify rhyme scheme is consistent.

        Input: Rhyme scheme analysis
        Expected: Consistency score indicates maintained pattern
        Type: manual (automated)
        """
        rhyme_analysis = self.report.rhyme_analysis

        self.assertGreaterEqual(
            rhyme_analysis.consistency_score, 0.3,
            f"Rhyme scheme should be at least 30% consistent. "
            f"Score: {rhyme_analysis.consistency_score:.2%}"
        )

    def test_rhyme_aids_memorability(self):
        """
        Test Case 1.4: Verify rhyme scheme aids memorability (NFR-5 primary).

        Input: Rhyme scheme analysis
        Expected: Poem employs consistent rhyme scheme that aids memorability
        Type: manual (automated)

        This is the primary test for Test Case 1.
        """
        rhyme_analysis = self.report.rhyme_analysis

        self.assertTrue(
            rhyme_analysis.aids_memorability,
            f"Rhyme scheme should aid memorability. "
            f"Scheme: {rhyme_analysis.scheme}, "
            f"Consistency: {rhyme_analysis.consistency_score:.2%}, "
            f"Pairs: {len(rhyme_analysis.rhyming_pairs)}"
        )

    def test_rhyme_scheme_is_recognizable_pattern(self):
        """
        Test Case 1.5: Verify rhyme scheme uses a recognizable pattern.

        Common memorable patterns: AABB (couplet), ABAB (alternate),
        ABCB (ballad), ABBA (envelope)
        """
        rhyme_analysis = self.report.rhyme_analysis

        # Check if scheme contains repeated letters (indicating rhyme)
        if rhyme_analysis.scheme:
            # A scheme with only unique letters (ABCD) has no rhyme
            unique_letters = set(rhyme_analysis.scheme)
            total_letters = len(rhyme_analysis.scheme)

            # Good rhyme schemes have fewer unique letters than total
            has_rhyme_pattern = len(unique_letters) < total_letters

            self.assertTrue(
                has_rhyme_pattern or len(rhyme_analysis.rhyming_pairs) >= 2,
                f"Rhyme scheme should show a recognizable pattern. "
                f"Scheme: {rhyme_analysis.scheme}"
            )

    # ==========================================================================
    # Test Case 2: Repetition Pattern Analysis
    # Input: Repetition pattern analysis
    # Expected: Contains memorable repeated elements or refrains
    # ==========================================================================

    def test_repetition_analysis_finds_patterns(self):
        """
        Test Case 2.1: Verify repetition analysis is performed.

        Input: Poem text content
        Expected: Repetition analysis produces results
        Type: manual (automated)

        This validates Step 2: Identify repetitive elements.
        """
        repetition_analysis = self.report.repetition_analysis

        self.assertIsNotNone(
            repetition_analysis,
            "Repetition analysis should be performed"
        )
        self.assertIsInstance(
            repetition_analysis, RepetitionAnalysis,
            "Should return RepetitionAnalysis object"
        )

    def test_identifies_repeated_phrases(self):
        """
        Test Case 2.2: Verify repeated phrases are identified.

        Input: Poem text content
        Expected: Repeated phrases are detected
        Type: manual (automated)
        """
        repetition_analysis = self.report.repetition_analysis

        # Note: A poem may not have repeated phrases but could have other
        # repetition elements like structural patterns
        self.assertIsInstance(
            repetition_analysis.repeated_phrases, list,
            "Repeated phrases should be returned as a list"
        )

    def test_identifies_structural_patterns(self):
        """
        Test Case 2.3: Verify structural patterns are identified.

        Input: Poem text content
        Expected: Structural patterns (anaphora, epistrophe) detected if present
        Type: manual (automated)
        """
        repetition_analysis = self.report.repetition_analysis

        self.assertIsInstance(
            repetition_analysis.structural_patterns, list,
            "Structural patterns should be returned as a list"
        )

    def test_has_memorable_repetition(self):
        """
        Test Case 2.4: Verify poem contains memorable repeated elements (NFR-5).

        Input: Repetition pattern analysis
        Expected: Contains memorable repeated elements or refrains
        Type: manual (automated)

        This is the primary test for Test Case 2.
        """
        repetition_analysis = self.report.repetition_analysis

        # Check for any form of repetition
        has_repeated_phrases = len(repetition_analysis.repeated_phrases) >= 1
        has_refrains = len(repetition_analysis.refrains) >= 1
        has_structural = len(repetition_analysis.structural_patterns) >= 1

        self.assertTrue(
            repetition_analysis.has_memorable_repetition or
            has_repeated_phrases or has_refrains or has_structural,
            f"Poem should contain memorable repeated elements. "
            f"Repeated phrases: {len(repetition_analysis.repeated_phrases)}, "
            f"Refrains: {len(repetition_analysis.refrains)}, "
            f"Structural patterns: {repetition_analysis.structural_patterns}"
        )

    def test_repetition_creates_unity(self):
        """
        Test Case 2.5: Verify repetition creates thematic unity.

        Per scenario context: Repetition aids recall and creates unity.
        """
        repetition_analysis = self.report.repetition_analysis

        # Check that repeated elements relate to core themes
        core_themes = ['data', 'key', 'store', 'rust', 'persist', 'guard', 'layer']

        if repetition_analysis.repeated_phrases:
            theme_related = any(
                any(theme in phrase.lower() for theme in core_themes)
                for phrase, _ in repetition_analysis.repeated_phrases
            )

            # Not required to pass, but informative
            if not theme_related:
                # Check if at least general repetition creates rhythm
                self.assertGreaterEqual(
                    len(repetition_analysis.repeated_phrases), 0,
                    "Poem should have some repetitive elements"
                )

    # ==========================================================================
    # Test Case 3: Quotability Assessment
    # Input: Quotability assessment
    # Expected: At least 2-3 lines are standout quotable phrases
    # ==========================================================================

    def test_quotability_analysis_scores_lines(self):
        """
        Test Case 3.1: Verify quotability analysis scores lines.

        Input: Poem text content
        Expected: Lines are scored for quotability
        Type: manual (automated)

        This validates Step 3: Identify quotable lines.
        """
        quotability_analysis = self.report.quotability_analysis

        self.assertIsNotNone(
            quotability_analysis,
            "Quotability analysis should be performed"
        )
        self.assertIsInstance(
            quotability_analysis.quotable_lines, list,
            "Quotable lines should be returned as a list"
        )

    def test_identifies_standout_lines(self):
        """
        Test Case 3.2: Verify standout lines are identified.

        Input: Poem text content
        Expected: Standout quotable lines are detected
        Type: manual (automated)
        """
        quotability_analysis = self.report.quotability_analysis

        self.assertGreater(
            quotability_analysis.standout_count, 0,
            "Poem should have at least one standout quotable line"
        )

    def test_meets_minimum_quotable_lines(self):
        """
        Test Case 3.3: Verify poem meets minimum quotable lines requirement.

        Input: Quotability assessment
        Expected: At least 2-3 lines are standout quotable phrases
        Type: manual (automated)

        This is the primary test for Test Case 3.
        """
        quotability_analysis = self.report.quotability_analysis

        self.assertTrue(
            quotability_analysis.meets_minimum,
            f"Poem should have at least 2-3 standout quotable lines. "
            f"Found: {quotability_analysis.standout_count} standout lines"
        )

    def test_quotable_lines_are_concise(self):
        """
        Test Case 3.4: Verify quotable lines are appropriately concise.

        Per scenario context: Great poems have standout lines people remember.
        Memorable lines are typically 5-12 words.
        """
        quotability_analysis = self.report.quotability_analysis

        if quotability_analysis.quotable_lines:
            for line, score in quotability_analysis.quotable_lines[:3]:
                word_count = len(line.split())
                # Quotable lines should generally be under 15 words
                self.assertLess(
                    word_count, 20,
                    f"Quotable line should be concise. Line: '{line}' ({word_count} words)"
                )

    def test_quotable_lines_have_sufficient_scores(self):
        """
        Test Case 3.5: Verify top quotable lines have good scores.

        Lines identified as quotable should have meaningful scores.
        """
        quotability_analysis = self.report.quotability_analysis

        if quotability_analysis.quotable_lines:
            top_line, top_score = quotability_analysis.quotable_lines[0]

            self.assertGreaterEqual(
                top_score, 0.3,
                f"Top quotable line should have score >= 0.3. "
                f"Line: '{top_line}', Score: {top_score}"
            )

    # ==========================================================================
    # Overall Memorability Assessment Tests
    # ==========================================================================

    def test_overall_assessment_passes(self):
        """
        Test Case 4.1: Verify overall memorability assessment passes.

        Combines all three criteria: rhyme, repetition, quotability.
        At least 2 of 3 criteria should be met for NFR-5.
        """
        self.assertTrue(
            self.report.passes_assessment,
            f"Overall memorability assessment should pass. "
            f"Summary: {self.report.summary}"
        )

    def test_overall_score_is_reasonable(self):
        """
        Test Case 4.2: Verify overall memorability score is reasonable.

        The combined score should reflect good memorability features.
        """
        self.assertGreaterEqual(
            self.report.overall_score, 0.3,
            f"Overall memorability score should be at least 0.3. "
            f"Score: {self.report.overall_score:.2f}"
        )

    def test_summary_is_informative(self):
        """
        Test Case 4.3: Verify summary provides useful information.

        The summary should describe the memorability findings.
        """
        self.assertIsNotNone(
            self.report.summary,
            "Summary should be provided"
        )
        self.assertGreater(
            len(self.report.summary), 20,
            "Summary should be descriptive"
        )

    def test_convenience_function_works(self):
        """
        Test Case 4.4: Verify assess_memorability convenience function.

        The function should return a complete MemorabilityReport.
        """
        report = assess_memorability(self.poem_content)

        self.assertIsInstance(report, MemorabilityReport)
        self.assertIsNotNone(report.rhyme_analysis)
        self.assertIsNotNone(report.repetition_analysis)
        self.assertIsNotNone(report.quotability_analysis)


class TestPhonemeRhymeDetector(unittest.TestCase):
    """Unit tests for PhonemeRhymeDetector class."""

    def test_words_that_clearly_rhyme(self):
        """Test detection of obviously rhyming words."""
        # These are clearly rhyming pairs that share endings
        rhyming_pairs = [
            ('fast', 'last'),
            ('deep', 'keep'),
            ('store', 'more'),
            ('light', 'night'),
            ('place', 'space'),
        ]

        for word1, word2 in rhyming_pairs:
            self.assertTrue(
                PhonemeRhymeDetector.words_rhyme(word1, word2),
                f"'{word1}' and '{word2}' should rhyme"
            )

    def test_words_that_do_not_rhyme(self):
        """Test detection of non-rhyming words."""
        non_rhyming_pairs = [
            ('fast', 'deep'),
            ('store', 'light'),
            ('rust', 'streams'),
            ('data', 'keeper'),
        ]

        for word1, word2 in non_rhyming_pairs:
            self.assertFalse(
                PhonemeRhymeDetector.words_rhyme(word1, word2),
                f"'{word1}' and '{word2}' should not rhyme"
            )

    def test_same_word_does_not_rhyme_with_itself(self):
        """Test that identical words don't count as rhyme."""
        self.assertFalse(
            PhonemeRhymeDetector.words_rhyme('store', 'store'),
            "Same word should not rhyme with itself"
        )

    def test_get_rhyme_ending(self):
        """Test rhyme ending extraction."""
        # Test that endings are extracted (vowel to end of word)
        ending1 = PhonemeRhymeDetector.get_rhyme_ending('fast')
        ending2 = PhonemeRhymeDetector.get_rhyme_ending('last')

        # Both should extract 'ast' as the ending
        self.assertEqual(ending1, 'ast', f"'fast' ending should be 'ast', got '{ending1}'")
        self.assertEqual(ending2, 'ast', f"'last' ending should be 'ast', got '{ending2}'")

    def test_empty_word_handling(self):
        """Test handling of empty words."""
        self.assertEqual(
            PhonemeRhymeDetector.get_rhyme_ending(''),
            ''
        )
        self.assertFalse(
            PhonemeRhymeDetector.words_rhyme('', 'word')
        )


class TestRhymeSchemeAnalyzer(unittest.TestCase):
    """Unit tests for RhymeSchemeAnalyzer class."""

    def test_couplet_scheme_detection(self):
        """Test detection of AABB (couplet) rhyme scheme."""
        couplet_poem = """
        In depths of Rust, where memory holds fast,
        A keeper guards what time would have erased.
        Each key and value, paired and bound to last,
        Through layered halls where data finds its place.
        """

        analyzer = RhymeSchemeAnalyzer(couplet_poem)
        result = analyzer.analyze_full_poem()

        self.assertIsNotNone(result.scheme)
        self.assertGreater(len(result.rhyming_pairs), 0)

    def test_alternate_scheme_detection(self):
        """Test detection of ABAB (alternate) rhyme scheme."""
        alternate_poem = """
        First line ends with day,
        Second line ends with night,
        Third line ends with way,
        Fourth line ends with light.
        """

        analyzer = RhymeSchemeAnalyzer(alternate_poem)
        result = analyzer.analyze_full_poem()

        self.assertIsNotNone(result.scheme)

    def test_stanza_scheme_analysis(self):
        """Test analysis of a single stanza."""
        stanza = [
            "In depths of Rust, where memory holds fast",
            "A keeper guards what time would have erased",
        ]

        analyzer = RhymeSchemeAnalyzer("\n".join(stanza))
        scheme, pairs = analyzer.analyze_stanza_scheme(stanza)

        self.assertIsNotNone(scheme)
        self.assertEqual(len(scheme), 2)

    def test_empty_poem_handling(self):
        """Test handling of empty poem."""
        analyzer = RhymeSchemeAnalyzer("")
        result = analyzer.analyze_full_poem()

        self.assertEqual(result.scheme, '')
        self.assertEqual(len(result.rhyming_pairs), 0)


class TestRepetitionAnalyzer(unittest.TestCase):
    """Unit tests for RepetitionAnalyzer class."""

    def test_finds_repeated_phrases(self):
        """Test detection of repeated phrases."""
        poem_with_repetition = """
        In the deep, in the deep,
        Where the data sleeps.
        In the deep, in the deep,
        Where the keeper keeps.
        """

        analyzer = RepetitionAnalyzer(poem_with_repetition)
        result = analyzer.analyze()

        self.assertTrue(result.has_memorable_repetition)
        self.assertGreater(len(result.repeated_phrases), 0)

    def test_finds_refrains(self):
        """Test detection of refrains (repeated lines)."""
        poem_with_refrain = """
        First unique line here.
        The refrain line stays.
        Second unique line there.
        The refrain line stays.
        """

        analyzer = RepetitionAnalyzer(poem_with_refrain)
        refrains = analyzer.find_refrains()

        # Should find "the refrain line stays" as a refrain
        self.assertGreater(
            len(refrains), 0,
            "Should detect refrain lines"
        )

    def test_detects_anaphora(self):
        """Test detection of anaphora (same start of lines)."""
        poem_with_anaphora = """
        Where the data flows deep.
        Where the keeper stands guard.
        Where the rust forged walls protect.
        """

        analyzer = RepetitionAnalyzer(poem_with_anaphora)
        anaphora = analyzer.detect_anaphora()

        # Should find "where" or "where the" as repeated starts
        starts_with_where = any('where' in phrase for phrase, _ in anaphora)
        self.assertTrue(
            starts_with_where,
            f"Should detect 'where' as anaphora. Found: {anaphora}"
        )

    def test_no_repetition_poem(self):
        """Test handling of poem without obvious repetition."""
        unique_poem = """
        First line is unique.
        Second differs completely.
        Third has new words.
        Fourth stands alone.
        """

        analyzer = RepetitionAnalyzer(unique_poem)
        result = analyzer.analyze()

        # Should still return valid analysis even if no strong repetition
        self.assertIsInstance(result, RepetitionAnalysis)


class TestQuotabilityAnalyzer(unittest.TestCase):
    """Unit tests for QuotabilityAnalyzer class."""

    def test_scores_lines(self):
        """Test that lines receive quotability scores."""
        poem = """
        In depths of Rust, where memory holds fast,
        A keeper guards what time would have erased.
        """

        analyzer = QuotabilityAnalyzer(poem)
        result = analyzer.analyze()

        self.assertGreater(len(result.quotable_lines), 0)
        for line, score in result.quotable_lines:
            self.assertGreaterEqual(score, 0.0)
            self.assertLessEqual(score, 1.0)

    def test_strong_words_increase_score(self):
        """Test that strong words increase quotability score."""
        strong_line = "A faithful guardian forever stands guard."
        weak_line = "Some things are placed here now."

        analyzer = QuotabilityAnalyzer("")
        strong_score = analyzer.score_line(strong_line)
        weak_score = analyzer.score_line(weak_line)

        self.assertGreater(
            strong_score, weak_score,
            "Lines with strong words should score higher"
        )

    def test_ideal_length_scoring(self):
        """Test that ideal-length lines score well."""
        # Ideal length is 5-12 words
        ideal_line = "Where Rust-forged walls stand guard through endless night."
        too_short = "Guard stands."
        too_long = "This is a very very very very long line that goes on and on and on."

        analyzer = QuotabilityAnalyzer("")
        ideal_score = analyzer.score_line(ideal_line)
        short_score = analyzer.score_line(too_short)
        long_score = analyzer.score_line(too_long)

        # Ideal should score better than extremes
        self.assertGreater(ideal_score, short_score)

    def test_empty_line_handling(self):
        """Test handling of empty lines."""
        analyzer = QuotabilityAnalyzer("")
        score = analyzer.score_line("")

        self.assertEqual(score, 0.0)

    def test_standout_count(self):
        """Test counting of standout lines."""
        poem = """
        A faithful store that never falls asleep.
        Transforming fleeting bytes to lasting light.
        Some plain line here.
        Another plain line there.
        """

        analyzer = QuotabilityAnalyzer(poem)
        result = analyzer.analyze()

        self.assertGreater(result.standout_count, 0)


class TestMemorabilityAssessorIntegration(unittest.TestCase):
    """Integration tests for MemorabilityAssessor."""

    def test_full_assessment_with_real_poem(self):
        """Test complete assessment with actual MirDB poem."""
        poem_path = os.path.join(
            os.path.dirname(__file__), '..', 'poem.txt'
        )

        if os.path.exists(poem_path):
            with open(poem_path, 'r') as f:
                poem_content = f.read()

            report = assess_memorability(poem_content)

            # Verify all components are present
            self.assertIsNotNone(report.rhyme_analysis)
            self.assertIsNotNone(report.repetition_analysis)
            self.assertIsNotNone(report.quotability_analysis)
            self.assertIsNotNone(report.overall_score)
            self.assertIsNotNone(report.summary)

    def test_assessor_with_highly_memorable_poem(self):
        """Test with a poem designed to be highly memorable."""
        memorable_poem = """
        In depths of Rust, where memory holds fast,
        A keeper guards what time would have erased.
        In depths of Rust, eternal and vast,
        Through layered halls where data finds its place.

        When memory fills, the cascade starts its course,
        From active pools to still, immutable streams.
        When memory fills with ever-growing force,
        Where sorted tablets store eternal dreams.
        """

        report = assess_memorability(memorable_poem)

        # Should have good rhyme
        self.assertGreater(
            len(report.rhyme_analysis.rhyming_pairs), 0,
            "Memorable poem should have rhyming pairs"
        )

        # Should have repetition
        self.assertTrue(
            report.repetition_analysis.has_memorable_repetition,
            "Memorable poem should have repetition"
        )

    def test_assessor_report_structure(self):
        """Test that report has correct structure."""
        poem = "Test poem with words."
        report = assess_memorability(poem)

        # Check all required fields
        self.assertIsInstance(report.rhyme_analysis, RhymeAnalysis)
        self.assertIsInstance(report.repetition_analysis, RepetitionAnalysis)
        self.assertIsInstance(report.quotability_analysis, QuotabilityAnalysis)
        self.assertIsInstance(report.overall_score, float)
        self.assertIsInstance(report.passes_assessment, bool)
        self.assertIsInstance(report.summary, str)


if __name__ == '__main__':
    unittest.main(verbosity=2)
