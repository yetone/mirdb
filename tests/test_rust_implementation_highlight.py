"""
Test Suite: Rust Implementation Highlight Validation (Scenario 4)

This module validates that the MirDB poem highlights the Rust implementation
emphasizing strength and safety as specified in REQ-4 of the PRD.

Test Cases:
1. Verify poem contains reference to Rust language or its qualities (strength, safety, performance)
2. Verify imagery correctly associates Rust with positive attributes (not corrosion/decay)

Steps from scenario:
- Step 1: Identify Rust references - Look for direct mention of Rust or metaphorical
  references to its qualities (e.g., 'forged in fire', 'rust-resistant', 'unbreakable')
- Step 2: Validate safety and performance themes - Check for themes of safety, reliability,
  and performance that align with Rust's benefits (memory safety, performance, reliability)
"""

import unittest
import os
import re


class PoemFiles:
    """Paths to all poem files in the repository."""
    POEM_TXT = os.path.join(os.path.dirname(__file__), '..', 'poem.txt')
    POEM_MD = os.path.join(os.path.dirname(__file__), '..', 'POEM.md')
    MIRDB_POEM = os.path.join(os.path.dirname(__file__), '..', 'versecraft', 'mirdb_poem.txt')


class TestRustLanguageReferences(unittest.TestCase):
    """
    Test Case 1: Verify poem contains reference to Rust language or its qualities

    Input: Poem text content
    Expected: Contains reference to Rust language or its qualities (strength, safety, performance)

    Step 1 from scenario: Identify Rust references - Look for direct mention of Rust
    or metaphorical references to its qualities.
    """

    # Direct Rust language references
    RUST_DIRECT_PATTERNS = [
        r'\brust\b',          # Direct "Rust" mention
        r'\brust-forged\b',   # Rust as forge material
        r'\brust\'s\b',       # Rust's (possessive)
    ]

    # Metaphorical references to Rust qualities (from PRD Appendix A)
    RUST_METAPHORICAL_PATTERNS = [
        r'\bforged\s+in\s+(?:fire|rust)\b',  # Forged in fire/Rust
        r'\bunbreak(?:able|ing)\b',           # Unbreakable
        r'\brust-resistant\b',                # Rust-resistant
        r'\bforged\b',                        # Forged (general)
        r'\bstrong\b',                        # Strong
        r'\bstrength\b',                      # Strength
        r'\biron\b',                          # Iron (metal metaphor)
        r'\bsteel\b',                         # Steel (metal metaphor)
        r'\bsolid\b',                         # Solid
        r'\bmight(?:y)?\b',                   # Mighty/might
    ]

    # Rust language qualities to look for
    RUST_QUALITY_PATTERNS = [
        # Safety-related
        r'\bsafe(?:ty)?\b',
        r'\bsecure\b',
        r'\bguard(?:s|ian|ed)?\b',
        r'\bprotect(?:s|ed|ion)?\b',
        r'\bshield(?:s|ed)?\b',

        # Performance-related
        r'\bfast\b',
        r'\bswift\b',
        r'\bspeed\b',
        r'\bperform(?:ance)?\b',
        r'\befficient\b',

        # Reliability-related
        r'\breliab(?:le|ility)\b',
        r'\btrust(?:worthy|ed)?\b',
        r'\bfaithful\b',
        r'\bstead(?:y|fast)\b',
        r'\bdependab(?:le|ility)\b',
    ]

    @classmethod
    def setUpClass(cls):
        """Load all poem files for testing."""
        cls.poems = {}

        for name, path in [('poem.txt', PoemFiles.POEM_TXT),
                           ('POEM.md', PoemFiles.POEM_MD),
                           ('mirdb_poem.txt', PoemFiles.MIRDB_POEM)]:
            if os.path.exists(path):
                with open(path, 'r', encoding='utf-8') as f:
                    cls.poems[name] = f.read()

    def test_poem_txt_exists(self):
        """Verify poem.txt exists and has content."""
        self.assertTrue(os.path.exists(PoemFiles.POEM_TXT),
                       "poem.txt should exist in repository root")
        self.assertIn('poem.txt', self.poems,
                     "poem.txt should be loadable")
        self.assertGreater(len(self.poems.get('poem.txt', '')), 0,
                          "poem.txt should have content")

    def test_poem_txt_contains_direct_rust_reference(self):
        """
        Step 1: Verify poem.txt contains a direct mention of 'Rust'.

        A direct reference to Rust establishes the technological context.
        """
        poem = self.poems.get('poem.txt', '').lower()

        found_references = []
        for pattern in self.RUST_DIRECT_PATTERNS:
            matches = re.findall(pattern, poem, re.IGNORECASE)
            if matches:
                found_references.extend(matches)

        self.assertGreater(len(found_references), 0,
            f"poem.txt should contain direct reference to Rust. "
            f"Searched patterns: {self.RUST_DIRECT_PATTERNS}")

    def test_poem_md_contains_direct_rust_reference(self):
        """
        Step 1: Verify POEM.md contains a direct mention of 'Rust'.
        """
        poem = self.poems.get('POEM.md', '').lower()

        found_references = []
        for pattern in self.RUST_DIRECT_PATTERNS:
            matches = re.findall(pattern, poem, re.IGNORECASE)
            if matches:
                found_references.extend(matches)

        self.assertGreater(len(found_references), 0,
            f"POEM.md should contain direct reference to Rust. "
            f"Searched patterns: {self.RUST_DIRECT_PATTERNS}")

    def test_mirdb_poem_contains_direct_rust_reference(self):
        """
        Step 1: Verify versecraft/mirdb_poem.txt contains a direct mention of 'Rust'.
        """
        poem = self.poems.get('mirdb_poem.txt', '').lower()

        found_references = []
        for pattern in self.RUST_DIRECT_PATTERNS:
            matches = re.findall(pattern, poem, re.IGNORECASE)
            if matches:
                found_references.extend(matches)

        self.assertGreater(len(found_references), 0,
            f"mirdb_poem.txt should contain direct reference to Rust. "
            f"Searched patterns: {self.RUST_DIRECT_PATTERNS}")

    def test_poem_txt_contains_rust_metaphors(self):
        """
        Step 1: Verify poem.txt contains metaphorical references to Rust qualities.

        PRD Appendix A suggests: 'forged in fire', 'rust-resistant', 'unbreakable'
        """
        poem = self.poems.get('poem.txt', '').lower()

        found_metaphors = []
        for pattern in self.RUST_METAPHORICAL_PATTERNS:
            matches = re.findall(pattern, poem, re.IGNORECASE)
            if matches:
                found_metaphors.extend(matches)

        self.assertGreater(len(found_metaphors), 0,
            f"poem.txt should contain metaphorical Rust references like "
            f"'forged', 'unbreakable', 'strong'. Found: {found_metaphors}")

    def test_any_poem_contains_rust_qualities(self):
        """
        Step 1: Verify at least one poem contains Rust quality themes.

        Rust is known for safety, performance, and reliability.
        """
        all_poems_content = ' '.join(self.poems.values()).lower()

        found_qualities = []
        for pattern in self.RUST_QUALITY_PATTERNS:
            matches = re.findall(pattern, all_poems_content, re.IGNORECASE)
            if matches:
                found_qualities.extend(matches)

        self.assertGreater(len(found_qualities), 0,
            f"Poems should contain Rust quality references (safety, performance, reliability). "
            f"Found: {found_qualities}")


class TestRustPositiveImagery(unittest.TestCase):
    """
    Test Case 2: Verify imagery correctly associates Rust with positive attributes

    Input: Rust-related imagery
    Expected: Imagery correctly associates Rust with positive attributes (not corrosion/decay)

    This is critical: "Rust" in the context of the Rust programming language should
    evoke strength and safety, NOT corrosion or decay.
    """

    # Negative rust imagery (corrosion/decay) - should NOT appear
    # Note: "failing" in "power's failing light" is acceptable as it contrasts MirDB's
    # persistence with volatile systems. We exclude patterns where failure describes
    # external systems, not MirDB/Rust itself.
    NEGATIVE_RUST_PATTERNS = [
        r'\bcorrod(?:e|es|ed|ing|sion)\b',  # Corrosion
        r'\bdecay(?:s|ed|ing)?\b',          # Decay
        r'\brot(?:s|ted|ting)?\b',          # Rot
        r'\brust(?:s|ed|ing|y)\b(?!\s*(?:language|programming|forged|\'s|\-))',  # Rusty/rusting (not Rust language)
        r'\bcrumbl(?:e|es|ed|ing)\b',       # Crumble
        r'\bdeteriorate?\b',                # Deteriorate
        r'\bweak(?:en|ened|ening|ness)?\b', # Weaken (in negative context)
        r'\bbrittle\b',                     # Brittle
        # Removed 'fail' as "power's failing light" is a contrast showing MirDB's strength
    ]

    # Patterns that are acceptable in contrast contexts
    # e.g., "others fail" or "power's failing light" show MirDB's advantage
    ACCEPTABLE_NEGATIVE_CONTEXTS = [
        r'power\'s\s+failing',     # Power failing (contrast)
        r'others?\s+(?:vanish|fail|fade)',  # Others fail (contrast)
        r'when\s+(?:servers?|power)\s+(?:fail|sleep)',  # External failure (contrast)
    ]

    # Positive Rust imagery - should appear
    POSITIVE_RUST_PATTERNS = [
        r'\bforged\b',                      # Forged (strength)
        r'\bunbreak(?:able|ing)\b',         # Unbreakable
        r'\bstrong\b',                      # Strong
        r'\bmight(?:y)?\b',                 # Mighty
        r'\bguard(?:s|ian|ed)?\b',          # Guardian
        r'\bstand(?:s)?\s+(?:guard|firm|true)\b',  # Standing guard/firm/true
        r'\bprotect(?:s|ed|ion)?\b',        # Protection
        r'\bsafe(?:ty)?\b',                 # Safety
        r'\bsolid\b',                       # Solid
        r'\bendur(?:e|es|ing|ance)\b',      # Endurance
        r'\bstead(?:y|fast)\b',             # Steadfast
        r'\btrue\b',                        # True
        r'\breliab(?:le|ility)\b',          # Reliable
        r'\bfaithful\b',                    # Faithful
    ]

    @classmethod
    def setUpClass(cls):
        """Load all poem files for testing."""
        cls.poems = {}

        for name, path in [('poem.txt', PoemFiles.POEM_TXT),
                           ('POEM.md', PoemFiles.POEM_MD),
                           ('mirdb_poem.txt', PoemFiles.MIRDB_POEM)]:
            if os.path.exists(path):
                with open(path, 'r', encoding='utf-8') as f:
                    cls.poems[name] = f.read()

    def test_poem_txt_no_negative_rust_imagery(self):
        """
        Test Case 2: Verify poem.txt does NOT associate Rust with corrosion/decay.

        The word 'Rust' should evoke the programming language, not metal oxidation.
        """
        poem = self.poems.get('poem.txt', '').lower()

        found_negative = []
        for pattern in self.NEGATIVE_RUST_PATTERNS:
            matches = re.findall(pattern, poem, re.IGNORECASE)
            if matches:
                found_negative.extend(matches)

        self.assertEqual(len(found_negative), 0,
            f"poem.txt should NOT contain negative Rust imagery (corrosion/decay). "
            f"Found: {found_negative}")

    def test_poem_md_no_negative_rust_imagery(self):
        """
        Test Case 2: Verify POEM.md does NOT associate Rust with corrosion/decay.
        """
        poem = self.poems.get('POEM.md', '').lower()

        found_negative = []
        for pattern in self.NEGATIVE_RUST_PATTERNS:
            matches = re.findall(pattern, poem, re.IGNORECASE)
            if matches:
                found_negative.extend(matches)

        self.assertEqual(len(found_negative), 0,
            f"POEM.md should NOT contain negative Rust imagery (corrosion/decay). "
            f"Found: {found_negative}")

    def test_mirdb_poem_no_negative_rust_imagery(self):
        """
        Test Case 2: Verify mirdb_poem.txt does NOT associate Rust with corrosion/decay.
        """
        poem = self.poems.get('mirdb_poem.txt', '').lower()

        found_negative = []
        for pattern in self.NEGATIVE_RUST_PATTERNS:
            matches = re.findall(pattern, poem, re.IGNORECASE)
            if matches:
                found_negative.extend(matches)

        self.assertEqual(len(found_negative), 0,
            f"mirdb_poem.txt should NOT contain negative Rust imagery (corrosion/decay). "
            f"Found: {found_negative}")

    def test_poem_txt_has_positive_rust_imagery(self):
        """
        Test Case 2: Verify poem.txt contains positive Rust imagery.

        Rust should be associated with strength, safety, reliability.
        """
        poem = self.poems.get('poem.txt', '').lower()

        found_positive = []
        for pattern in self.POSITIVE_RUST_PATTERNS:
            matches = re.findall(pattern, poem, re.IGNORECASE)
            if matches:
                found_positive.extend(matches)

        self.assertGreater(len(found_positive), 0,
            f"poem.txt should contain positive Rust imagery. "
            f"Found: {found_positive}")

    def test_poem_md_has_positive_rust_imagery(self):
        """
        Test Case 2: Verify POEM.md contains positive Rust imagery.
        """
        poem = self.poems.get('POEM.md', '').lower()

        found_positive = []
        for pattern in self.POSITIVE_RUST_PATTERNS:
            matches = re.findall(pattern, poem, re.IGNORECASE)
            if matches:
                found_positive.extend(matches)

        self.assertGreater(len(found_positive), 0,
            f"POEM.md should contain positive Rust imagery. "
            f"Found: {found_positive}")

    def test_mirdb_poem_has_positive_rust_imagery(self):
        """
        Test Case 2: Verify mirdb_poem.txt contains positive Rust imagery.
        """
        poem = self.poems.get('mirdb_poem.txt', '').lower()

        found_positive = []
        for pattern in self.POSITIVE_RUST_PATTERNS:
            matches = re.findall(pattern, poem, re.IGNORECASE)
            if matches:
                found_positive.extend(matches)

        self.assertGreater(len(found_positive), 0,
            f"mirdb_poem.txt should contain positive Rust imagery. "
            f"Found: {found_positive}")


class TestRustSafetyAndPerformanceThemes(unittest.TestCase):
    """
    Step 2 validation: Validate safety and performance themes.

    Check for themes of safety, reliability, and performance that align with Rust's benefits.
    Rust is known for: memory safety, performance, and reliability.
    """

    # Memory safety themes
    SAFETY_THEMES = [
        r'\bsafe(?:ty)?\b',
        r'\bguard(?:s|ian|ed)?\b',
        r'\bprotect(?:s|ed|ion)?\b',
        r'\bshield(?:s|ed)?\b',
        r'\bkeep(?:s|er|ing)?\b',
        r'\bwatch(?:es|ful)?\b',
        r'\bsentinel\b',
    ]

    # Performance themes
    PERFORMANCE_THEMES = [
        r'\bfast\b',
        r'\bswift\b',
        r'\bspeed(?:y)?\b',
        r'\bquick\b',
        r'\befficient\b',
        r'\bperform(?:ance)?\b',
    ]

    # Reliability themes
    RELIABILITY_THEMES = [
        r'\breliab(?:le|ility)\b',
        r'\btrust(?:worthy|ed|s)?\b',
        r'\bfaithful\b',
        r'\bstead(?:y|fast)\b',
        r'\bdependab(?:le|ility)\b',
        r'\bconsistent\b',
        r'\bunfail(?:ing)?\b',
        r'\bnever\s+(?:fail|sleep|fade|die)\b',
        r'\bendur(?:e|es|ing|ance)\b',
        r'\blast(?:s|ing)?\b',
        r'\beternal\b',
        r'\bforever\b',
    ]

    @classmethod
    def setUpClass(cls):
        """Load all poem files for testing."""
        cls.all_poems = ''

        for path in [PoemFiles.POEM_TXT, PoemFiles.POEM_MD, PoemFiles.MIRDB_POEM]:
            if os.path.exists(path):
                with open(path, 'r', encoding='utf-8') as f:
                    cls.all_poems += f.read() + '\n'

        cls.all_poems_lower = cls.all_poems.lower()

    def test_poems_contain_safety_themes(self):
        """
        Step 2: Verify poems contain safety themes aligned with Rust.

        Rust is known for memory safety without garbage collection.
        """
        found_safety = []
        for pattern in self.SAFETY_THEMES:
            matches = re.findall(pattern, self.all_poems_lower, re.IGNORECASE)
            if matches:
                found_safety.extend(matches)

        self.assertGreater(len(found_safety), 0,
            f"Poems should contain safety themes (guard, protect, safe). "
            f"Found: {found_safety}")

    def test_poems_contain_performance_or_speed_themes(self):
        """
        Step 2: Verify poems contain performance/speed themes.

        Rust is known for performance comparable to C/C++.
        """
        found_performance = []
        for pattern in self.PERFORMANCE_THEMES:
            matches = re.findall(pattern, self.all_poems_lower, re.IGNORECASE)
            if matches:
                found_performance.extend(matches)

        # Performance is a 'should' requirement, so we check but may be lenient
        self.assertGreater(len(found_performance), 0,
            f"Poems should contain performance themes (fast, swift, efficient). "
            f"Found: {found_performance}")

    def test_poems_contain_reliability_themes(self):
        """
        Step 2: Verify poems contain reliability themes.

        Rust is known for producing reliable software.
        """
        found_reliability = []
        for pattern in self.RELIABILITY_THEMES:
            matches = re.findall(pattern, self.all_poems_lower, re.IGNORECASE)
            if matches:
                found_reliability.extend(matches)

        self.assertGreater(len(found_reliability), 0,
            f"Poems should contain reliability themes (faithful, steadfast, enduring). "
            f"Found: {found_reliability}")

    def test_combined_rust_theme_coverage(self):
        """
        Step 2: Verify poems have adequate coverage of Rust's key benefits.

        Should have at least one reference from each theme category or
        multiple references across categories.
        """
        safety_count = sum(1 for p in self.SAFETY_THEMES
                          if re.search(p, self.all_poems_lower))
        performance_count = sum(1 for p in self.PERFORMANCE_THEMES
                               if re.search(p, self.all_poems_lower))
        reliability_count = sum(1 for p in self.RELIABILITY_THEMES
                               if re.search(p, self.all_poems_lower))

        total_coverage = safety_count + performance_count + reliability_count

        self.assertGreaterEqual(total_coverage, 3,
            f"Poems should have broad coverage of Rust themes. "
            f"Safety: {safety_count}, Performance: {performance_count}, "
            f"Reliability: {reliability_count}. Total: {total_coverage}")


class TestRustContextualAccuracy(unittest.TestCase):
    """
    Additional validation to ensure Rust is referenced in the correct context.

    The word 'Rust' should appear in contexts that make it clear it refers to
    the programming language, not metal oxidation.
    """

    @classmethod
    def setUpClass(cls):
        """Load all poem files."""
        cls.poems = {}

        for name, path in [('poem.txt', PoemFiles.POEM_TXT),
                           ('POEM.md', PoemFiles.POEM_MD),
                           ('mirdb_poem.txt', PoemFiles.MIRDB_POEM)]:
            if os.path.exists(path):
                with open(path, 'r', encoding='utf-8') as f:
                    cls.poems[name] = f.read()

    def test_rust_in_programming_context(self):
        """
        Verify 'Rust' appears in a programming/technical context.

        Good contexts: 'forged in Rust', 'Rust's might', 'depths of Rust'
        Bad contexts: 'rust on metal', 'rusty chains'
        """
        programming_context_patterns = [
            r'\brust(?:\'s)?\s+(?:might|power|strength|safety)\b',
            r'\bforged\s+in\s+rust\b',
            r'\bdepths?\s+of\s+rust\b',
            r'\brust-forged\b',
            r'\bin\s+rust\b',
            r'\bbuilt\s+(?:in|with)\s+rust\b',
            r'\bwritten\s+in\s+rust\b',
        ]

        all_poems = ' '.join(self.poems.values()).lower()

        found_contexts = []
        for pattern in programming_context_patterns:
            matches = re.findall(pattern, all_poems, re.IGNORECASE)
            if matches:
                found_contexts.extend(matches)

        self.assertGreater(len(found_contexts), 0,
            f"'Rust' should appear in programming-related contexts. "
            f"Found: {found_contexts}")

    def test_all_poems_reference_mirdb_or_data(self):
        """
        Verify poems are about MirDB (a data store), establishing context.

        This contextualizes 'Rust' as a programming language reference.
        """
        data_context_patterns = [
            r'\bdata\b',
            r'\bstore\b',
            r'\bkey(?:s)?\b',
            r'\bvalue(?:s)?\b',
            r'\bmirdb\b',
            r'\bmemory\b',
            r'\bdisk\b',
            r'\bpersist\b',
        ]

        all_poems = ' '.join(self.poems.values()).lower()

        found_data_refs = []
        for pattern in data_context_patterns:
            if re.search(pattern, all_poems):
                found_data_refs.append(pattern)

        self.assertGreaterEqual(len(found_data_refs), 3,
            f"Poems should establish data store context. "
            f"Found patterns: {found_data_refs}")


if __name__ == '__main__':
    unittest.main(verbosity=2)
