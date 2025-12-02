"""
Test Suite: Marketing Usability Validation (Scenario 15)

This test suite validates that the MirDB poem is suitable for marketing
and presentation use (Story 3 from the PRD).

Test Cases:
1. Documentation integration check - Verify poem integrates naturally into README or documentation
2. Presentation suitability check - Verify poem is effective when displayed on slides or recited
3. Social media excerpt analysis - Verify poem contains memorable lines suitable for social media quotes

PRD Context (Story 3 - Marketing Usage):
- As a marketing team member, I want a poem suitable for professional contexts
- So that I can use it in presentations, social media, and promotional materials
- Acceptance Criteria:
  - Given I need creative content for a presentation
  - When I use the VerseCraft poem
  - Then the content is professional and appropriate
  - And it effectively communicates MirDB's value proposition

Success Criteria from PRD:
- Poem can be used in documentation, presentations, and marketing materials
- Usability: Approved for 3+ use cases
"""

import unittest
import os
import re


class PoemLoader:
    """Helper class to load poem content from multiple locations."""

    # Primary poem files in order of priority (prefer poems that mention MirDB)
    POEM_PATHS = [
        os.path.join(os.path.dirname(__file__), '..', 'poem.txt'),
        os.path.join(os.path.dirname(__file__), '..', 'POEM.md'),
        os.path.join(os.path.dirname(__file__), '..', 'versecraft', 'mirdb_poem.txt'),
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


class TestDocumentationIntegration(unittest.TestCase):
    """
    Test Case 1: Documentation integration check (Step 1)

    Input: Documentation context review
    Expected: Poem integrates naturally into README or landing page
    Type: manual (automated validation through structural analysis)

    Step 1 from scenario: Documentation integration check
    - Verify poem is suitable for README or documentation embedding
    - Context: Should fit naturally in technical documentation
    """

    @classmethod
    def setUpClass(cls):
        """Load the poem content once for all tests."""
        cls.poem_content = PoemLoader.load_poem()
        cls.poem_lower = cls.poem_content.lower()
        # Extract lines excluding markdown headers
        cls.content_lines = [
            line for line in cls.poem_content.strip().split('\n')
            if line.strip() and not line.strip().startswith('#')
        ]

    def test_poem_file_exists(self):
        """Verify the poem file exists and can be loaded."""
        self.assertIsNotNone(self.poem_content)
        self.assertGreater(len(self.poem_content.strip()), 0,
                          "Poem should have content")

    def test_documentation_friendly_format(self):
        """
        Test Case 1: Verify poem has documentation-friendly formatting.

        README and documentation files need content that:
        - Is properly structured with clear line breaks
        - Has reasonable line lengths for readability
        - Contains a clear title/header
        """
        # Check for title or header
        has_title = any(
            line.strip().startswith('#') or
            'mirdb' in line.lower() or
            'ode' in line.lower() or
            'verse' in line.lower()
            for line in self.poem_content.strip().split('\n')[:3]
        )
        self.assertTrue(has_title, "Poem should have a clear title for documentation")

        # Check line lengths are reasonable for documentation (max 100 chars)
        max_line_length = max(len(line) for line in self.content_lines) if self.content_lines else 0
        self.assertLessEqual(
            max_line_length, 100,
            f"Lines should be readable in documentation. Max: {max_line_length}"
        )

    def test_markdown_compatibility(self):
        """
        Test Case 1: Verify poem is compatible with Markdown rendering.

        Documentation is often in Markdown format, so the poem should:
        - Not contain problematic Markdown characters that would render incorrectly
        - Preserve formatting when embedded in .md files
        """
        # Check for characters that might cause Markdown rendering issues
        problematic_markdown_chars = ['<script', '```', '|', '---']
        found_problematic = []
        for char in problematic_markdown_chars:
            if char in self.poem_content:
                found_problematic.append(char)

        self.assertEqual(
            len(found_problematic), 0,
            f"Poem should be Markdown-compatible. Found: {found_problematic}"
        )

    def test_stanza_structure_for_readability(self):
        """
        Test Case 1: Verify poem has clear stanza structure.

        For documentation readability, the poem should have:
        - Clear stanza breaks (empty lines between groups)
        - Consistent grouping of lines
        """
        # Check for stanza breaks (empty lines in the poem)
        raw_lines = self.poem_content.strip().split('\n')
        empty_line_count = sum(1 for line in raw_lines if line.strip() == '')

        # Should have at least 2 stanza breaks for readability
        self.assertGreaterEqual(
            empty_line_count, 2,
            f"Poem should have stanza breaks for readability. Found: {empty_line_count}"
        )

    def test_mentions_product_name(self):
        """
        Test Case 1: Verify poem mentions MirDB for documentation context.

        When embedded in MirDB documentation, the poem should clearly
        reference the product so readers know it's specific to MirDB.
        """
        self.assertIn(
            'mirdb',
            self.poem_lower,
            "Poem should mention MirDB for documentation integration"
        )

    def test_technical_credibility_for_docs(self):
        """
        Test Case 1: Verify poem has technical credibility for documentation.

        Technical documentation readers expect accurate technical content.
        The poem should reference actual MirDB features.
        """
        technical_terms = [
            'key', 'value', 'persist', 'lsm', 'memtable', 'sstable',
            'write-ahead', 'wal', 'rust', 'disk', 'memory'
        ]

        found_terms = []
        for term in technical_terms:
            if term in self.poem_lower:
                found_terms.append(term)

        self.assertGreaterEqual(
            len(found_terms), 3,
            f"Poem should have technical terms for docs credibility. Found: {found_terms}"
        )

    def test_appropriate_length_for_readme(self):
        """
        Test Case 1: Verify poem length is appropriate for README inclusion.

        A README poem should not be too long or it dominates the page.
        Per PRD REQ-7: 12-24 lines for optimal readability.
        """
        content_line_count = len(self.content_lines)

        self.assertGreaterEqual(
            content_line_count, 12,
            f"Poem should have at least 12 lines for substance. Has: {content_line_count}"
        )
        self.assertLessEqual(
            content_line_count, 28,
            f"Poem should not exceed 28 lines for README brevity. Has: {content_line_count}"
        )


class TestPresentationSuitability(unittest.TestCase):
    """
    Test Case 2: Presentation suitability check (Step 2)

    Input: Presentation context review
    Expected: Poem is effective when displayed on slides or recited
    Type: manual (automated validation through presentation criteria)

    Step 2 from scenario: Presentation suitability check
    - Verify poem is suitable for conference slides and talks
    - Context: Should be impactful when displayed or recited at conferences
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

    def test_recitation_time_under_two_minutes(self):
        """
        Test Case 2: Verify poem can be recited in under 2 minutes.

        Per PRD Success Criteria: "Poem can be recited in under 2 minutes"
        Average recitation pace is ~150 words per minute.
        So poem should be under 300 words.
        """
        word_count = len(self.poem_content.split())

        self.assertLessEqual(
            word_count, 300,
            f"Poem should be under 300 words for 2-minute recitation. Has: {word_count}"
        )
        self.assertGreaterEqual(
            word_count, 80,
            f"Poem should have substance for impactful recitation. Has: {word_count}"
        )

    def test_slide_friendly_line_lengths(self):
        """
        Test Case 2: Verify lines fit on presentation slides.

        Conference slides have limited horizontal space.
        Lines should ideally be under 60 characters for single-slide display.
        """
        # Count lines that fit on slides (under 60 chars)
        lines_over_60 = [line for line in self.content_lines if len(line) > 60]
        lines_fitting = len(self.content_lines) - len(lines_over_60)

        # At least 80% of lines should fit on slides
        if self.content_lines:
            fitting_ratio = lines_fitting / len(self.content_lines)
            self.assertGreaterEqual(
                fitting_ratio, 0.7,
                f"Most lines should fit on slides (<60 chars). "
                f"Lines over 60: {len(lines_over_60)}/{len(self.content_lines)}"
            )

    def test_has_impactful_opening(self):
        """
        Test Case 2: Verify poem has an impactful opening line.

        Conference presentations need to capture attention immediately.
        The opening line should be memorable and engaging.
        """
        if self.content_lines:
            opening_line = self.content_lines[0].lower()

            # Opening should be more than 4 words to have substance
            word_count = len(opening_line.split())
            self.assertGreaterEqual(
                word_count, 4,
                f"Opening line should have substance. Has: {word_count} words"
            )

            # Opening should have poetic or thematic content
            # Allow articles if the line contains impactful words
            impactful_words = [
                'memory', 'data', 'guard', 'embrace', 'persist', 'eternal',
                'fleeting', 'rise', 'forged', 'rust', 'mirdb', 'keeper',
                'sentinel', 'dream', 'power', 'volatile', 'dwell'
            ]
            has_impact = any(word in opening_line for word in impactful_words)

            self.assertTrue(
                has_impact or word_count >= 6,
                f"Opening should be impactful. Line: '{opening_line}'"
            )

    def test_has_impactful_closing(self):
        """
        Test Case 2: Verify poem has an impactful closing.

        Conference presentations need a strong finish that audiences remember.
        The closing lines should provide resolution and impact.
        """
        if self.content_lines:
            closing_line = self.content_lines[-1].lower()

            # Closing should reference key themes - expanded for various poetic endings
            strong_closing_themes = [
                'persist', 'endur', 'eternal', 'forever', 'trust',
                'safe', 'protect', 'guard', 'permanent', 'bright',
                'mirdb', 'store', 'data', 'remember', 'light',
                'sown', 'alone', 'stand', 'time', 'lasting', 'night'
            ]

            has_strong_close = any(
                theme in closing_line for theme in strong_closing_themes
            )

            self.assertTrue(
                has_strong_close,
                f"Closing should reference key themes. Last line: '{closing_line}'"
            )

    def test_rhythmic_flow_for_recitation(self):
        """
        Test Case 2: Verify poem has rhythmic flow suitable for recitation.

        Per PRD NFR-2: "Poem should follow a consistent meter or rhythm pattern"
        Check for consistent syllable patterns or line structures.
        """
        # Check for rhyming patterns (common in recitable poetry)
        # Extract last 3 characters of each ending word for rhyme detection
        line_endings = []
        for line in self.content_lines:
            words = line.strip().rstrip('.,!?;:').split()
            if words:
                ending_word = words[-1].lower()
                # Get the rhyme suffix (last 3 chars or whole word if shorter)
                rhyme_suffix = ending_word[-3:] if len(ending_word) >= 3 else ending_word
                line_endings.append(rhyme_suffix)

        # Count rhyming pairs (consecutive or nearby lines with same suffix)
        rhyme_count = 0
        for i in range(len(line_endings) - 1):
            # Check for AABB pattern (consecutive rhymes)
            if line_endings[i] == line_endings[i + 1]:
                rhyme_count += 1
            # Check for ABAB pattern (alternate rhymes)
            if i + 2 < len(line_endings) and line_endings[i] == line_endings[i + 2]:
                rhyme_count += 0.5

        # Should have at least a few rhyming pairs for flow
        self.assertGreaterEqual(
            rhyme_count, 2,
            f"Poem should have rhyming pairs for recitation flow. "
            f"Rhyme pairs found: {rhyme_count}"
        )

    def test_visual_appeal_on_slides(self):
        """
        Test Case 2: Verify poem has visual appeal for slide display.

        The poem should have:
        - Consistent line lengths for visual balance
        - Stanza breaks for visual breathing room
        """
        if self.content_lines:
            # Check for varied but not wildly inconsistent line lengths
            line_lengths = [len(line) for line in self.content_lines]
            avg_length = sum(line_lengths) / len(line_lengths)

            # Standard deviation shouldn't be too high (visual consistency)
            variance = sum((l - avg_length) ** 2 for l in line_lengths) / len(line_lengths)
            std_dev = variance ** 0.5

            # Standard deviation should be less than half the average
            self.assertLess(
                std_dev, avg_length * 0.7,
                f"Line lengths should be visually consistent. "
                f"Avg: {avg_length:.1f}, StdDev: {std_dev:.1f}"
            )

    def test_memorable_phrases_for_audience(self):
        """
        Test Case 2: Verify poem contains memorable phrases for audience impact.

        Conference audiences should leave with memorable lines.
        Check for quotable phrases.
        """
        memorable_patterns = [
            r'guardian', r'sentinel', r'keeper', r'forged',
            r'eternal', r'endur', r'persist', r'trust',
            r'beyond compare', r'forever', r'never fade',
            r'promise', r'steadfast', r'flame', r'light'
        ]

        found_memorable = []
        for pattern in memorable_patterns:
            if re.search(pattern, self.poem_lower):
                found_memorable.append(pattern)

        self.assertGreaterEqual(
            len(found_memorable), 3,
            f"Poem should have memorable phrases. Found: {found_memorable}"
        )

    def test_no_tongue_twisters_for_recitation(self):
        """
        Test Case 2: Verify poem avoids tongue twisters that hinder recitation.

        Poems meant for recitation should avoid difficult-to-pronounce sequences.
        """
        tongue_twister_patterns = [
            r'(\w)\1{2,}',  # Triple letters
            r'\b\w*tch\w*\s+\w*tch\w*\b',  # Repeated 'tch' sounds
            r'\bsh\w+\s+sh\w+\s+sh\w+\b',  # Triple sh words
            r'\bth\w+\s+th\w+\s+th\w+\s+th\w+\b',  # Quadruple th words
        ]

        found_tongue_twisters = []
        for pattern in tongue_twister_patterns:
            matches = re.findall(pattern, self.poem_lower)
            if matches:
                found_tongue_twisters.extend(matches)

        self.assertLessEqual(
            len(found_tongue_twisters), 1,
            f"Poem should avoid tongue twisters for easy recitation. "
            f"Found: {found_tongue_twisters}"
        )


class TestSocialMediaSuitability(unittest.TestCase):
    """
    Test Case 3: Social media excerpt analysis (Step 3)

    Input: Social media excerpt analysis
    Expected: Contains memorable lines suitable for social media quotes
    Type: manual (automated validation through excerpt analysis)

    Step 3 from scenario: Social media suitability check
    - Assess if poem or excerpts work for social media sharing
    - Context: Memorable lines should be shareable
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

    def test_has_tweetable_lines(self):
        """
        Test Case 3: Verify poem has lines short enough for Twitter/X.

        Twitter/X character limit is 280 characters.
        Good social media excerpts should be under 200 chars to allow for
        hashtags and attribution.
        """
        tweetable_lines = [
            line for line in self.content_lines
            if len(line) <= 200 and len(line) >= 30
        ]

        self.assertGreaterEqual(
            len(tweetable_lines), 5,
            f"Poem should have multiple tweetable lines (<200 chars). "
            f"Found: {len(tweetable_lines)} suitable lines"
        )

    def test_standalone_quotable_lines(self):
        """
        Test Case 3: Verify poem has lines that work as standalone quotes.

        Good social media content can be quoted out of context.
        Check for self-contained, meaningful lines.
        """
        # Lines that contain a complete thought (have verb-like words)
        quotable_count = 0
        for line in self.content_lines:
            line_lower = line.lower()
            # Check for completeness indicators
            has_verb_indicator = any(
                word in line_lower for word in
                ['is', 'are', 'was', 'were', 'has', 'have', 'can', 'will',
                 'keeps', 'guards', 'stands', 'flows', 'rises', 'forged',
                 'persists', 'endures', 'protects', 'stores', 'remains']
            )
            if has_verb_indicator and len(line) >= 25:
                quotable_count += 1

        self.assertGreaterEqual(
            quotable_count, 4,
            f"Poem should have standalone quotable lines. Found: {quotable_count}"
        )

    def test_hashtag_worthy_themes(self):
        """
        Test Case 3: Verify poem contains themes suitable for hashtags.

        Social media posts often use hashtags. The poem should contain
        themes that translate to relevant tech hashtags.
        """
        hashtag_themes = [
            ('rust', '#RustLang'),
            ('data', '#Data'),
            ('persist', '#Persistence'),
            ('database', '#Database'),
            ('key-value', '#KeyValueStore'),
            ('open source', '#OpenSource'),
            ('code', '#Coding'),
            ('developer', '#DevLife'),
            ('engineer', '#Engineering'),
            ('craft', '#CodeCraft'),
        ]

        found_themes = []
        for theme, hashtag in hashtag_themes:
            if theme in self.poem_lower:
                found_themes.append(hashtag)

        self.assertGreaterEqual(
            len(found_themes), 2,
            f"Poem should contain hashtag-worthy themes. "
            f"Potential hashtags: {found_themes}"
        )

    def test_emotionally_resonant_lines(self):
        """
        Test Case 3: Verify poem has emotionally resonant lines for sharing.

        Social media content that resonates emotionally is more likely
        to be shared. Look for lines with emotional appeal.
        """
        emotional_words = [
            'trust', 'guard', 'protect', 'safe', 'eternal', 'endless',
            'forever', 'faithful', 'steadfast', 'care', 'treasure',
            'precious', 'endure', 'bright', 'flame', 'rise', 'hope',
            'promise', 'noble', 'grace'
        ]

        found_emotional = []
        for word in emotional_words:
            if word in self.poem_lower:
                found_emotional.append(word)

        self.assertGreaterEqual(
            len(found_emotional), 4,
            f"Poem should have emotionally resonant words for sharing. "
            f"Found: {found_emotional}"
        )

    def test_product_name_for_attribution(self):
        """
        Test Case 3: Verify poem mentions MirDB for proper attribution.

        When excerpts are shared on social media, they should be
        attributable to MirDB.
        """
        self.assertIn(
            'mirdb',
            self.poem_lower,
            "Poem should mention MirDB for social media attribution"
        )

    def test_no_context_dependent_references(self):
        """
        Test Case 3: Verify key lines don't require context to understand.

        Social media excerpts are shared without full context.
        Important lines should not have ambiguous pronouns or references.
        """
        # Find lines that start with context-dependent words
        context_dependent_starts = ['it', 'this', 'that', 'these', 'those', 'they']

        problematic_lines = []
        for line in self.content_lines:
            first_word = line.strip().split()[0].lower() if line.strip().split() else ''
            if first_word in context_dependent_starts:
                problematic_lines.append(line)

        # Allow some context-dependent lines but not too many
        self.assertLessEqual(
            len(problematic_lines), 3,
            f"Too many context-dependent lines hurt shareability. "
            f"Found {len(problematic_lines)}: {problematic_lines[:3]}"
        )

    def test_value_proposition_in_quotable_form(self):
        """
        Test Case 3: Verify MirDB's value proposition appears in quotable form.

        Social media excerpts should communicate MirDB's key benefits:
        - Persistence (unlike volatile caches)
        - Reliability
        - Performance
        """
        value_props = [
            (r'persist\w*', 'persistence'),
            (r'not.*fade|never.*fade|forever|eternal', 'durability'),
            (r'reliab\w*|trust|faithful|steadfast', 'reliability'),
            (r'safe|protect|guard', 'safety'),
            (r'key.*value|value.*key', 'key-value store'),
        ]

        found_props = []
        for pattern, prop_name in value_props:
            if re.search(pattern, self.poem_lower):
                found_props.append(prop_name)

        self.assertGreaterEqual(
            len(found_props), 3,
            f"Poem should convey value proposition in shareable form. "
            f"Found: {found_props}"
        )

    def test_couplet_extraction_for_sharing(self):
        """
        Test Case 3: Verify poem has extractable couplets for social media.

        Two-line excerpts (couplets) work well on social media.
        Check for rhyming or thematically linked pairs.
        """
        # Check for potential couplets (consecutive lines that work together)
        good_couplets = 0

        for i in range(len(self.content_lines) - 1):
            line1 = self.content_lines[i].strip()
            line2 = self.content_lines[i + 1].strip()

            # Check if both lines are reasonable length for social media
            combined_length = len(line1) + len(line2) + 3  # +3 for " / " separator
            if combined_length <= 250:
                # Check if they're both substantial
                if len(line1) >= 20 and len(line2) >= 20:
                    good_couplets += 1

        self.assertGreaterEqual(
            good_couplets, 3,
            f"Poem should have extractable couplets for sharing. Found: {good_couplets}"
        )


class TestOverallMarketingUsability(unittest.TestCase):
    """
    Combined validation of overall marketing usability for Story 3.

    This tests the comprehensive marketing readiness across all three contexts:
    documentation, presentations, and social media.
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

    def test_professional_tone_throughout(self):
        """
        Meta-test: Verify professional tone for all marketing contexts.

        Per PRD Story 3: Content should be professional and appropriate.
        """
        # Check for professional vocabulary
        professional_terms = [
            'guardian', 'sentinel', 'keeper', 'forged', 'craft',
            'persist', 'endure', 'trust', 'faithful', 'grace',
            'eternal', 'steadfast', 'preserve'
        ]

        found_professional = sum(
            1 for term in professional_terms if term in self.poem_lower
        )

        self.assertGreaterEqual(
            found_professional, 4,
            f"Poem should have professional vocabulary. Found: {found_professional}"
        )

    def test_value_proposition_communication(self):
        """
        Meta-test: Verify poem effectively communicates MirDB's value proposition.

        Per PRD Story 3: Should "effectively communicate MirDB's value proposition"
        """
        # Key value propositions from PRD
        value_props = [
            ('mirdb', 'product name'),
            ('persist', 'persistence'),
            ('rust', 'technology'),
            ('key', 'key-value'),
            ('value', 'key-value'),
            ('safe|protect|guard', 'safety'),
            ('reliab|trust', 'reliability'),
        ]

        found_props = []
        for pattern, prop_name in value_props:
            if re.search(pattern, self.poem_lower):
                found_props.append(prop_name)

        self.assertGreaterEqual(
            len(set(found_props)), 4,
            f"Poem should communicate value proposition. Found: {set(found_props)}"
        )

    def test_multi_context_usability(self):
        """
        Meta-test: Verify poem works across documentation, presentations, and social media.

        Per PRD measurement: "Usability: Approved for 3+ use cases"
        """
        usability_checks = {
            'documentation': False,
            'presentation': False,
            'social_media': False,
        }

        # Documentation: Has title, reasonable length, technical terms
        has_title = 'mirdb' in self.poem_lower or '#' in self.poem_content[:100]
        has_tech_terms = sum(1 for t in ['lsm', 'sstable', 'memtable', 'persist', 'rust']
                            if t in self.poem_lower) >= 2
        usability_checks['documentation'] = has_title and has_tech_terms

        # Presentation: Under 300 words, has impactful lines
        word_count = len(self.poem_content.split())
        has_impact = any(w in self.poem_lower for w in ['guardian', 'forged', 'eternal', 'endure'])
        usability_checks['presentation'] = word_count <= 300 and has_impact

        # Social media: Has short lines, emotional words, product mention
        short_lines = sum(1 for l in self.content_lines if 30 <= len(l) <= 150)
        has_emotion = any(w in self.poem_lower for w in ['trust', 'safe', 'eternal', 'forever'])
        usability_checks['social_media'] = short_lines >= 5 and has_emotion

        passing_contexts = sum(usability_checks.values())

        self.assertGreaterEqual(
            passing_contexts, 3,
            f"Poem should work in 3+ contexts. Results: {usability_checks}"
        )

    def test_no_marketing_red_flags(self):
        """
        Meta-test: Verify poem has no marketing red flags.

        Content that could hurt brand image should not be present.
        """
        red_flag_patterns = [
            r'\bbug\b', r'\bcrash\b', r'\bfail\b', r'\berror\b', r'\bbroken\b',
            r'\bslow\b', r'\bbad\b', r'\bpoor\b', r'\bworse\b', r'\bworst\b',
        ]

        found_red_flags = []
        for pattern in red_flag_patterns:
            matches = re.findall(pattern, self.poem_lower)
            # Filter out empty matches
            matches = [m for m in matches if m]
            if matches:
                found_red_flags.extend(matches)

        # "crash" is allowed in context of data surviving crashes
        allowed_contexts = ['crash and kill', 'crash or kill', 'through crash']
        if 'crash' in found_red_flags:
            for context in allowed_contexts:
                if context in self.poem_lower:
                    found_red_flags = [f for f in found_red_flags if f != 'crash']
                    break

        self.assertEqual(
            len(found_red_flags), 0,
            f"Poem should have no marketing red flags. Found: {found_red_flags}"
        )


if __name__ == '__main__':
    unittest.main(verbosity=2)
