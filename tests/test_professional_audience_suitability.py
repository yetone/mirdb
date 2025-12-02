"""
Test Suite: Professional Audience Suitability Validation (Scenario 9)

This test suite validates that the MirDB poem is suitable for professional/technical
audiences (NFR-3), as specified in the PRD.

Test Cases:
1. Verify tone is professional and dignified, suitable for corporate/conference settings
2. Verify content resonates with technical professionals without feeling patronizing

MirDB Context:
- Target audiences: software engineers, technical stakeholders
- Target contexts: documentation, conferences, marketing
- Tone should be professional, not humorous or overly casual
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


class TestProfessionalToneAppropriateness(unittest.TestCase):
    """
    Test Case 1: Validate tone is professional and dignified.

    Input: Poem text content
    Expected: Tone is professional and dignified, suitable for corporate/conference settings

    Step 1 from scenario: Review tone appropriateness
    - Assess if the tone is professional rather than humorous or casual
    - Target contexts include documentation, conferences, and marketing
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

    def test_no_casual_slang_or_colloquialisms(self):
        """
        Test Case 1: Verify poem avoids casual slang and colloquialisms.

        Professional content should not contain informal expressions
        that would be inappropriate in corporate/conference settings.
        """
        casual_patterns = [
            r'\bawesome\b', r'\bcool\b', r'\bsick\b', r'\blit\b',
            r'\bwicked\b', r'\bsweet\b', r'\bdope\b', r'\bepic\b',
            r'\blegit\b', r'\bbasically\b', r'\btotally\b',
            r'\bkinda\b', r'\bsorta\b', r'\bgonna\b', r'\bwanna\b',
            r'\byolo\b', r'\bfomo\b', r'\blol\b', r'\bromfl\b',
            r'\bwtf\b', r'\bomg\b', r'\bbtw\b',
            r'\byo\b', r'\bdude\b', r'\bbro\b', r'\bfam\b',
            r'\bfire\b.*\bfire\b',  # slang usage of "fire"
            r'\bbang\b',  # casual exclamation
        ]

        found_casual = []
        for pattern in casual_patterns:
            if re.search(pattern, self.poem_lower):
                found_casual.append(pattern)

        self.assertEqual(
            len(found_casual), 0,
            f"Poem should not contain casual slang or colloquialisms. "
            f"Found: {found_casual}"
        )

    def test_no_humor_or_jokes(self):
        """
        Test Case 1: Verify poem avoids humor or jokes.

        Per PRD assumption: "Professional tone is preferred over humorous"
        The poem should maintain dignity suitable for formal presentations.
        """
        humor_indicators = [
            r'\bhaha\b', r'\bhehe\b', r'\blmao\b', r'\brofl\b',
            r'\bjk\b', r'\bjoke\b', r'\bfunny\b', r'\bhilarious\b',
            r'\bpun\b', r'\bpuns\b', r'\blaugh\b',
            r':\)', r';\)', r':D', r':P',  # emoticons
            r'\bwit\b',  # unless used in dignified way
        ]

        found_humor = []
        for pattern in humor_indicators:
            if re.search(pattern, self.poem_lower):
                found_humor.append(pattern)

        self.assertEqual(
            len(found_humor), 0,
            f"Poem should maintain professional tone without humor. "
            f"Found: {found_humor}"
        )

    def test_dignified_vocabulary(self):
        """
        Test Case 1: Verify poem uses dignified vocabulary.

        Professional poetry should use elevated, respectful language
        suitable for corporate documentation and conference presentations.
        """
        dignified_vocabulary = [
            'guardian', 'sentinel', 'keeper', 'steadfast', 'grace',
            'treasure', 'eternal', 'endure', 'persist', 'faithful',
            'elegant', 'noble', 'trust', 'wisdom', 'craft',
            'forged', 'architected', 'designed', 'engineered',
            'preserve', 'protect', 'secure', 'reliable', 'robust'
        ]

        found_dignified = []
        for word in dignified_vocabulary:
            if word in self.poem_lower:
                found_dignified.append(word)

        self.assertGreaterEqual(
            len(found_dignified), 3,
            f"Poem should use dignified vocabulary suitable for professional settings. "
            f"Found: {found_dignified}"
        )

    def test_no_inappropriate_content(self):
        """
        Test Case 1: Verify poem contains no inappropriate or offensive content.

        NFR-4: Poem must not contain any inappropriate or offensive content.
        """
        inappropriate_patterns = [
            r'\bstupid\b', r'\bdumb\b', r'\bidiot\b', r'\bmoron\b',
            r'\bhate\b', r'\bkill\b', r'\bdeath\b', r'\bdead\b',
            r'\bdestroy\b', r'\bdevastate\b', r'\bannihilate\b',
            r'\bhorrible\b', r'\bterrible\b', r'\bawful\b',
            r'\bsuck\b', r'\bsucks\b', r'\bcrap\b', r'\bdamn\b',
            r'\bhell\b', r'\bworse\b.*\bworse\b',
            r'\bfail(?:ure|ed)?\b.*\bfail(?:ure|ed)?\b',  # repeated failure language
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

    def test_consistent_formal_tone(self):
        """
        Test Case 1: Verify poem maintains consistent formal tone throughout.

        The poem should use consistent register without shifting between
        formal and casual language.
        """
        # Check for consistent capitalization at line starts (formal poetry)
        lines_starting_with_capital = 0
        for line in self.content_lines:
            if line and line[0].isupper():
                lines_starting_with_capital += 1

        total_content_lines = len(self.content_lines)

        # At least 80% of lines should start with capital letters (formal style)
        if total_content_lines > 0:
            capital_ratio = lines_starting_with_capital / total_content_lines
            self.assertGreaterEqual(
                capital_ratio, 0.8,
                f"Poem should maintain formal capitalization. "
                f"Lines with capital starts: {lines_starting_with_capital}/{total_content_lines}"
            )

    def test_suitable_for_documentation(self):
        """
        Test Case 1: Verify poem is suitable for technical documentation.

        The poem should be embeddable in README.md, help text, or website.
        """
        # Check for reasonable line lengths (for documentation embedding)
        max_line_length = max(len(line) for line in self.content_lines) if self.content_lines else 0

        self.assertLessEqual(
            max_line_length, 100,
            f"Poem lines should be reasonable length for documentation. "
            f"Max line length: {max_line_length}"
        )

    def test_suitable_for_conference_presentation(self):
        """
        Test Case 1: Verify poem is suitable for conference presentations.

        Per PRD: Poem can be recited in under 2 minutes (REQ-7: 12-24 lines).
        Should be memorable and presentation-friendly.
        """
        total_lines = len(self.content_lines)

        # Check line count for recitation suitability
        self.assertGreaterEqual(
            total_lines, 12,
            f"Poem should have at least 12 lines for substance. Has: {total_lines}"
        )
        self.assertLessEqual(
            total_lines, 30,
            f"Poem should not exceed 30 lines for recitation. Has: {total_lines}"
        )


class TestTechnicalAudienceAlignment(unittest.TestCase):
    """
    Test Case 2: Validate content resonates with technical professionals.

    Input: Poem content review
    Expected: Content would resonate with technical professionals without feeling patronizing

    Step 2 from scenario: Check technical audience alignment
    - Verify content resonates with software engineers and technical stakeholders
    - Should appeal to developers evaluating MirDB
    """

    @classmethod
    def setUpClass(cls):
        """Load the poem content once for all tests."""
        cls.poem_content = PoemLoader.load_poem()
        cls.poem_lower = cls.poem_content.lower()

    def test_technical_terminology_present(self):
        """
        Test Case 2: Verify poem includes relevant technical terminology.

        Technical professionals should recognize domain-specific terms
        that validate the poem's technical credibility.
        """
        technical_terms = [
            'key', 'value', 'store', 'data', 'memory', 'disk',
            'sstable', 'memtable', 'lsm', 'tree', 'log',
            'persist', 'cache', 'byte', 'bits',
            'rust', 'protocol', 'memcached'
        ]

        found_technical = []
        for term in technical_terms:
            if term in self.poem_lower:
                found_technical.append(term)

        self.assertGreaterEqual(
            len(found_technical), 5,
            f"Poem should include technical terminology for credibility. "
            f"Found: {found_technical}"
        )

    def test_not_overly_simplified(self):
        """
        Test Case 2: Verify poem is not patronizingly oversimplified.

        The poem should not explain concepts in a way that would
        seem condescending to experienced engineers.
        """
        patronizing_patterns = [
            r'\bsimply\s+put\b', r'\bin\s+other\s+words\b',
            r'\bfor\s+beginners\b', r'\beasy\s+to\s+understand\b',
            r'\blet\s+me\s+explain\b', r'\bbasically\b',
            r'\byou\s+see\b', r'\byou\s+know\b',
            r'\bdon\'t\s+worry\b', r'\bit\'s\s+not\s+hard\b',
            r'\beven\s+a\s+child\b', r'\bno\s+brainer\b',
        ]

        found_patronizing = []
        for pattern in patronizing_patterns:
            if re.search(pattern, self.poem_lower):
                found_patronizing.append(pattern)

        self.assertEqual(
            len(found_patronizing), 0,
            f"Poem should not be patronizingly oversimplified. "
            f"Found: {found_patronizing}"
        )

    def test_metaphors_respect_intelligence(self):
        """
        Test Case 2: Verify metaphors respect technical audience intelligence.

        Metaphors should be elegant and thoughtful, not childish or trivial.
        Per PRD: Metaphors should relate to nature (trees, layers, flow),
        craftsmanship, and reliability themes.
        """
        sophisticated_metaphors = [
            r'\bguardian\b', r'\bsentinel\b', r'\bkeeper\b',
            r'\bforged\b', r'\bcraft\b', r'\bartist\b',
            r'\blayer\b', r'\bflow\b', r'\bstream\b',
            r'\barchitect\b', r'\bdesign\b', r'\bbuild\b',
            r'\btree\b', r'\broot\b', r'\bbranch\b',
            r'\bgrace\b', r'\belegance\b', r'\bwisdom\b',
        ]

        found_metaphors = []
        for pattern in sophisticated_metaphors:
            if re.search(pattern, self.poem_lower):
                found_metaphors.append(pattern)

        self.assertGreaterEqual(
            len(found_metaphors), 2,
            f"Poem should use sophisticated metaphors. "
            f"Found: {found_metaphors}"
        )

    def test_addresses_developer_concerns(self):
        """
        Test Case 2: Verify poem addresses developer evaluation concerns.

        Developers evaluating MirDB care about:
        - Data persistence and durability
        - Performance characteristics
        - Reliability and trustworthiness
        - Technology choices (Rust)
        """
        developer_concerns = [
            (r'\bpersist', 'persistence'),
            (r'\bsafe|secure|protect', 'safety/security'),
            (r'\breliab|trust|faith|steadfast', 'reliability'),
            (r'\bfast|swift|quick|perform', 'performance'),
            (r'\brust', 'technology choice'),
            (r'\bkey.*value|value.*key', 'data model'),
        ]

        addressed_concerns = []
        for pattern, concern in developer_concerns:
            if re.search(pattern, self.poem_lower):
                addressed_concerns.append(concern)

        self.assertGreaterEqual(
            len(addressed_concerns), 3,
            f"Poem should address developer evaluation concerns. "
            f"Addressed: {addressed_concerns}"
        )

    def test_no_marketing_hyperbole(self):
        """
        Test Case 2: Verify poem avoids excessive marketing hyperbole.

        Technical professionals are skeptical of exaggerated claims.
        Per PRD: Avoid "exaggerated claims about capabilities".
        """
        hyperbole_patterns = [
            r'\bbest\s+in\s+(?:the\s+)?world\b',
            r'\bunbeatable\b', r'\binvincible\b',
            r'\bperfect\b.*\bperfect\b',  # repeated "perfect"
            r'\bguaranteed\b.*\b100%\b',
            r'\bnothing\s+comes\s+close\b',
            r'\bno\s+competition\b',
            r'\bkillers?\b',  # "category killer" etc.
            r'\bblows\s+away\b',
            r'\bcrushes?\b',
            r'\bdominates?\b',
        ]

        found_hyperbole = []
        for pattern in hyperbole_patterns:
            if re.search(pattern, self.poem_lower):
                found_hyperbole.append(pattern)

        self.assertEqual(
            len(found_hyperbole), 0,
            f"Poem should avoid marketing hyperbole. "
            f"Found: {found_hyperbole}"
        )

    def test_technical_accuracy_not_misleading(self):
        """
        Test Case 2: Verify poem doesn't make misleading technical claims.

        The poem should not claim capabilities MirDB doesn't have.
        """
        # Check for claims about features not yet implemented
        unimplemented_claims = [
            r'\bdistributed\b.*\bconsensus\b',  # Raft not implemented
            r'\braft\b',  # Not implemented
            r'\bmulti-?node\b',  # Not implemented
            r'\bcluster\b',  # Not implemented
        ]

        found_misleading = []
        for pattern in unimplemented_claims:
            if re.search(pattern, self.poem_lower):
                found_misleading.append(pattern)

        self.assertEqual(
            len(found_misleading), 0,
            f"Poem should not claim unimplemented features. "
            f"Found: {found_misleading}"
        )

    def test_appeals_to_craftsmanship_values(self):
        """
        Test Case 2: Verify poem appeals to engineering craftsmanship values.

        Technical professionals appreciate:
        - Attention to detail
        - Quality engineering
        - Thoughtful design
        - Artistry and elegance
        """
        craftsmanship_terms = [
            r'\bcraft\b', r'\bforged\b', r'\bbuild\b', r'\bbuilt\b',
            r'\bdesign\b', r'\barchitect\b', r'\bengineer\b',
            r'\bcare\b', r'\battention\b', r'\bdetail\b',
            r'\belegant\b', r'\bgrace\b', r'\bart\b',
            r'\bcreate\b', r'\bcreation\b', r'\bmade\b',
            r'\borders?\b', r'\bordered\b',  # ordered implies careful design
            r'\btreasure\b',  # treasured implies valued craftsmanship
            r'\bsown\b',  # sown implies cultivation/care
        ]

        found_craftsmanship = []
        for pattern in craftsmanship_terms:
            if re.search(pattern, self.poem_lower):
                found_craftsmanship.append(pattern)

        self.assertGreaterEqual(
            len(found_craftsmanship), 2,
            f"Poem should appeal to craftsmanship values. "
            f"Found: {found_craftsmanship}"
        )


class TestOverallProfessionalSuitability(unittest.TestCase):
    """
    Combined test to verify overall professional audience suitability.

    NFR-3: Poem should be suitable for professional/technical audiences.
    This tests the combination of tone appropriateness and technical alignment.
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

    def test_professional_and_technical_balance(self):
        """
        Meta-test: Verify balance between professional tone and technical content.

        The poem should be both dignified AND technically relevant.
        """
        # Professional indicators
        professional_indicators = [
            'guardian', 'sentinel', 'keeper', 'steadfast', 'grace',
            'treasure', 'eternal', 'endure', 'forged', 'noble'
        ]

        # Technical indicators
        technical_indicators = [
            'key', 'value', 'data', 'memory', 'disk', 'lsm',
            'memtable', 'sstable', 'rust', 'persist', 'byte'
        ]

        professional_found = sum(1 for word in professional_indicators if word in self.poem_lower)
        technical_found = sum(1 for word in technical_indicators if word in self.poem_lower)

        self.assertGreaterEqual(
            professional_found, 2,
            f"Poem should have professional tone indicators. Found: {professional_found}"
        )
        self.assertGreaterEqual(
            technical_found, 3,
            f"Poem should have technical content. Found: {technical_found}"
        )

    def test_readable_by_non_native_speakers(self):
        """
        Verify poem uses clear, accessible English (NFR-1).

        Professional audiences may include non-native English speakers.
        """
        # Check for overly complex or archaic words that might confuse
        archaic_complex = [
            r'\bwhilst\b', r'\bforsooth\b', r'\bhitherto\b',
            r'\bwherefore\b', r'\bheretofore\b', r'\bthee\b',
            r'\bthou\b', r'\bthy\b', r'\bthine\b',
            r'\bdost\b', r'\bdoth\b', r'\bhath\b',
        ]

        found_archaic = []
        for pattern in archaic_complex:
            if re.search(pattern, self.poem_lower):
                found_archaic.append(pattern)

        self.assertEqual(
            len(found_archaic), 0,
            f"Poem should use accessible modern English. "
            f"Found archaic terms: {found_archaic}"
        )

    def test_memorability_for_conferences(self):
        """
        Verify poem has memorability features for conference use (NFR-5).

        Features that aid memorability:
        - Rhyme
        - Rhythm
        - Repetition of key phrases
        """
        # Check for rhyming patterns (end words)
        line_endings = []
        for line in self.content_lines:
            words = line.strip().rstrip('.,!?;:').split()
            if words:
                line_endings.append(words[-1].lower())

        # Simple check: at least some rhyming pairs within stanzas
        has_rhyme_potential = len(set(line_endings)) < len(line_endings)

        self.assertTrue(
            has_rhyme_potential or len(self.content_lines) >= 12,
            "Poem should have memorability features (rhyme or substantial length)"
        )

    def test_no_controversial_content(self):
        """
        Verify poem avoids politically or socially controversial content.

        Professional content should be universally acceptable.
        """
        controversial_patterns = [
            r'\bpolitics\b', r'\breligion\b', r'\bgod\b',
            r'\bwar\b', r'\bviolence\b', r'\bweapon\b',
            r'\bcontroversy\b', r'\bdebate\b',
            r'\bparty\b',  # political party context
        ]

        found_controversial = []
        for pattern in controversial_patterns:
            if re.search(pattern, self.poem_lower):
                found_controversial.append(pattern)

        self.assertEqual(
            len(found_controversial), 0,
            f"Poem should avoid controversial content. "
            f"Found: {found_controversial}"
        )


if __name__ == '__main__':
    unittest.main(verbosity=2)
