"""
Test Suite for Language and Accessibility Validation (Scenario 7)

This module validates NFR-1 from the PRD: Poem must be written in English with
clear, accessible language.

Test Cases:
1. Verify all text is in English with proper grammar and spelling
2. Verify vocabulary is accessible without requiring specialized literary knowledge

Steps from scenario:
- Step 1: Verify English language - Confirm the poem is written entirely in English
  (Target audience is English-speaking technical community)
- Step 2: Assess language accessibility - Verify vocabulary is accessible to general
  technical audience (Avoid overly complex or obscure references)
"""

import unittest
import os
import re
from collections import Counter


class PoemFiles:
    """Paths to all poem files in the repository."""
    POEM_TXT = os.path.join(os.path.dirname(__file__), '..', 'poem.txt')
    POEM_MD = os.path.join(os.path.dirname(__file__), '..', 'POEM.md')
    MIRDB_POEM = os.path.join(os.path.dirname(__file__), '..', 'versecraft', 'mirdb_poem.txt')


class TestEnglishLanguageValidation(unittest.TestCase):
    """
    Test Case 1: Verify all text is in English with proper grammar and spelling

    Input: Poem text content
    Expected: All text is in English with proper grammar and spelling
    Type: manual (automated validation of English characteristics)

    Step 1 from scenario: Verify English language - Confirm the poem is written
    entirely in English (Target audience is English-speaking technical community)
    """

    # Common English words that should appear in any English poem
    COMMON_ENGLISH_WORDS = [
        'the', 'a', 'an', 'in', 'to', 'of', 'and', 'is', 'that', 'for',
        'with', 'as', 'on', 'from', 'by', 'or', 'where', 'through', 'no',
        'but', 'when', 'each', 'your', 'its', 'so', 'here'
    ]

    # English articles and prepositions (strong indicators of English)
    ENGLISH_ARTICLES_PREPOSITIONS = [
        'the', 'a', 'an', 'in', 'on', 'at', 'to', 'for', 'of', 'with',
        'by', 'from', 'through', 'into', 'over', 'under', 'between'
    ]

    # Non-ASCII characters that would indicate non-English text
    # (except for common English punctuation)
    NON_ENGLISH_PATTERN = re.compile(r'[^\x00-\x7F]')

    # Patterns for common non-English words/phrases from other languages
    NON_ENGLISH_WORDS = [
        # Common Spanish
        r'\bel\b', r'\bla\b', r'\blos\b', r'\blas\b', r'\bque\b', r'\ben\b',
        r'\bcon\b', r'\bpara\b', r'\bpor\b', r'\buno\b', r'\buna\b',
        # Common French
        r'\ble\b', r'\bla\b', r'\bles\b', r'\bun\b', r'\bune\b', r'\bdes\b',
        r'\bdu\b', r'\bet\b', r'\best\b', r'\bque\b', r'\bqui\b', r'\bavec\b',
        r'\bpour\b', r'\bdans\b', r'\bsur\b',
        # Common German
        r'\bder\b', r'\bdie\b', r'\bdas\b', r'\bein\b', r'\beine\b',
        r'\bund\b', r'\bist\b', r'\bmit\b', r'\bauf\b', r'\bfur\b',
        # Common Italian
        r'\bil\b', r'\blo\b', r'\bgli\b', r'\bche\b', r'\bnon\b',
        r'\bcon\b', r'\bper\b', r'\buna\b', r'\bsono\b',
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

    def _extract_words(self, text):
        """Extract words from text, converting to lowercase."""
        # Remove markdown headers and special characters
        text = re.sub(r'^#.*$', '', text, flags=re.MULTILINE)
        words = re.findall(r'\b[a-zA-Z]+\b', text.lower())
        return words

    def test_poem_txt_contains_english_words(self):
        """
        Step 1: Verify poem.txt contains common English words.

        A valid English poem should contain common function words like
        'the', 'a', 'in', 'to', 'and', etc.
        """
        poem = self.poems.get('poem.txt', '')
        words = self._extract_words(poem)

        found_english = [w for w in words if w in self.COMMON_ENGLISH_WORDS]

        self.assertGreater(len(found_english), 5,
            f"poem.txt should contain multiple common English words. "
            f"Found: {Counter(found_english).most_common(10)}")

    def test_poem_md_contains_english_words(self):
        """
        Step 1: Verify POEM.md contains common English words.
        """
        poem = self.poems.get('POEM.md', '')
        words = self._extract_words(poem)

        found_english = [w for w in words if w in self.COMMON_ENGLISH_WORDS]

        self.assertGreater(len(found_english), 5,
            f"POEM.md should contain multiple common English words. "
            f"Found: {Counter(found_english).most_common(10)}")

    def test_mirdb_poem_contains_english_words(self):
        """
        Step 1: Verify mirdb_poem.txt contains common English words.
        """
        poem = self.poems.get('mirdb_poem.txt', '')
        words = self._extract_words(poem)

        found_english = [w for w in words if w in self.COMMON_ENGLISH_WORDS]

        self.assertGreater(len(found_english), 5,
            f"mirdb_poem.txt should contain multiple common English words. "
            f"Found: {Counter(found_english).most_common(10)}")

    def test_poem_txt_uses_english_articles(self):
        """
        Step 1: Verify poem.txt uses English articles and prepositions.

        English articles (a, an, the) and prepositions are strong indicators
        of English text and proper grammatical structure.
        """
        poem = self.poems.get('poem.txt', '')
        words = self._extract_words(poem)

        found_articles_preps = [w for w in words
                                if w in self.ENGLISH_ARTICLES_PREPOSITIONS]

        self.assertGreater(len(found_articles_preps), 3,
            f"poem.txt should use English articles/prepositions. "
            f"Found: {Counter(found_articles_preps).most_common()}")

    def test_poem_txt_uses_ascii_characters(self):
        """
        Step 1: Verify poem.txt uses standard ASCII characters.

        English text should primarily use ASCII characters. Non-ASCII might
        indicate non-English content or encoding issues.
        """
        poem = self.poems.get('poem.txt', '')

        # Find non-ASCII characters
        non_ascii = self.NON_ENGLISH_PATTERN.findall(poem)

        self.assertEqual(len(non_ascii), 0,
            f"poem.txt should use standard ASCII characters. "
            f"Found non-ASCII: {non_ascii}")

    def test_poem_md_uses_ascii_characters(self):
        """
        Step 1: Verify POEM.md uses standard ASCII characters.
        """
        poem = self.poems.get('POEM.md', '')

        # Find non-ASCII characters
        non_ascii = self.NON_ENGLISH_PATTERN.findall(poem)

        self.assertEqual(len(non_ascii), 0,
            f"POEM.md should use standard ASCII characters. "
            f"Found non-ASCII: {non_ascii}")

    def test_mirdb_poem_uses_ascii_characters(self):
        """
        Step 1: Verify mirdb_poem.txt uses standard ASCII characters.
        """
        poem = self.poems.get('mirdb_poem.txt', '')

        # Find non-ASCII characters
        non_ascii = self.NON_ENGLISH_PATTERN.findall(poem)

        self.assertEqual(len(non_ascii), 0,
            f"mirdb_poem.txt should use standard ASCII characters. "
            f"Found non-ASCII: {non_ascii}")

    def test_poems_do_not_contain_foreign_language_indicators(self):
        """
        Step 1: Verify poems don't contain patterns from other languages.

        Check that the poems don't have telltale patterns from Spanish,
        French, German, or Italian that would indicate non-English content.
        """
        all_poems = ' '.join(self.poems.values()).lower()
        words = self._extract_words(all_poems)

        # These are patterns that should NOT match (non-English language markers)
        # We need to be careful because some patterns overlap with English
        # (e.g., 'a' is both English and other languages)
        suspicious_patterns = [
            # Only patterns that are clearly non-English
            r'\bfur\b',      # German "for"
            r'\bdas\b',      # German "the"
            r'\bdans\b',     # French "in"
            r'\bsono\b',     # Italian "I am"
            r'\bavec\b',     # French "with"
            r'\bpour\b',     # French "for"
        ]

        found_non_english = []
        for pattern in suspicious_patterns:
            matches = re.findall(pattern, all_poems)
            if matches:
                found_non_english.extend(matches)

        self.assertEqual(len(found_non_english), 0,
            f"Poems should not contain non-English language patterns. "
            f"Found: {found_non_english}")

    def test_poem_has_english_sentence_structure(self):
        """
        Step 1: Verify poem.txt follows English sentence patterns.

        English typically follows Subject-Verb-Object order and uses
        specific articles before nouns.
        """
        poem = self.poems.get('poem.txt', '')

        # Check for English sentence structure indicators
        # Pattern: article + word (noun pattern in English)
        article_noun_pattern = r'\b(the|a|an)\s+\w+'
        matches = re.findall(article_noun_pattern, poem.lower())

        self.assertGreater(len(matches), 2,
            f"poem.txt should have English sentence structure (article + noun). "
            f"Found {len(matches)} patterns")


class TestVocabularyAccessibility(unittest.TestCase):
    """
    Test Case 2: Verify vocabulary is accessible without requiring specialized
    literary knowledge

    Input: Poem vocabulary complexity
    Expected: Language is accessible without requiring specialized literary knowledge
    Type: manual (automated accessibility analysis)

    Step 2 from scenario: Assess language accessibility - Verify vocabulary is
    accessible to general technical audience (Avoid overly complex or obscure references)
    """

    # Overly complex/archaic words that may not be accessible
    COMPLEX_ARCHAIC_WORDS = [
        r'\bthee\b', r'\bthou\b', r'\bthy\b', r'\bthine\b',
        r'\bhither\b', r'\bthither\b', r'\bwhence\b', r'\bwhither\b',
        r'\bforsooth\b', r'\bprithee\b', r'\bverily\b', r'\bmayhaps?\b',
        r'\bhark\b', r'\bbehold\b', r'\bwherefore\b', r'\balack\b',
        r'\balas\b', r'\bbetwixt\b', r'\bforth\b', r'\bheretofore\b',
        r'\bhereafter\b', r'\bneither\b.*\bnor\b',
        r'\bdoth\b', r'\bhath\b',
        # "thou art" is archaic, but "the art of" or "fleeting art" is fine
        r'\bthou\s+art\b',
        r'\bshallt\b', r'\bwilt\b', r'\bwouldst\b', r'\bcouldst\b',
        r"\b'tis\b", r"\b'twas\b", r"\b'twill\b",
        r'\blo\b', r'\bsooth\b', r'\byea\b', r'\bnay\b',
        r'\bere\b(?!\s)', r'\boft\b', r'\banon\b',
    ]

    # Obscure literary/mythological references
    OBSCURE_REFERENCES = [
        # Greek mythology (obscure)
        r'\bstyx\b', r'\bcharon\b', r'\bcerberus\b', r'\bmnemosyne\b',
        r'\blethe\b', r'\bhypnos\b', r'\bthanatos\b', r'\berato\b',
        r'\bthalia\b', r'\bmelpomene\b', r'\bterpsichore\b',
        # Norse mythology (obscure)
        r'\byggdrasil\b', r'\bnidhogg\b', r'\bfenrir\b', r'\bjormungandr\b',
        r'\bhuginn\b', r'\bmuninn\b', r'\bsleipnir\b', r'\bbifrost\b',
        # Literary obscurities
        r'\bozymandias\b', r'\bxanadu\b', r'\bkubla\b',
        r'\bcitizen\s+kane\b',
        # Obscure philosophical terms
        r'\bquiddity\b', r'\bhaecceity\b', r'\bqualia\b',
        r'\bnoumenon\b', r'\bdasein\b', r'\bzeitgeist\b',
    ]

    # Technical jargon that should be "poeticized" not used raw
    # (From PRD: Avoid jargon without poetic transformation)
    RAW_TECHNICAL_JARGON = [
        # Raw database terms without poetic context
        r'\btcp/ip\b', r'\bhttp\b', r'\bjson\b', r'\bxml\b',
        r'\bapi\s+endpoint\b', r'\bcrud\b', r'\borm\b',
        r'\bsql\b', r'\bnosql\b', r'\bacid\b',  # Unless metaphorical
        r'\bcache\s+miss\b', r'\bcache\s+hit\b',
        r'\bgarbage\s+collect\b', r'\bheap\b(?!\s+of)',
        r'\bstack\s+overflow\b', r'\bsegfault\b',
        r'\bnull\s+pointer\b', r'\brace\s+condition\b',
    ]

    # Acceptable technical terms (from PRD - poetically transformed)
    ACCEPTABLE_TECHNICAL_TERMS = [
        'key', 'value', 'store', 'memory', 'disk', 'data',
        'persist', 'cache', 'memtable', 'sstable', 'log',
        'layers', 'levels', 'sorted', 'merge', 'compact',
        'rust', 'forged', 'guardian', 'keeper', 'sentinel',
        'tablets', 'scrolls', 'records', 'bytes', 'bits'
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

        cls.all_poems = ' '.join(cls.poems.values())
        cls.all_poems_lower = cls.all_poems.lower()

    def test_poem_txt_avoids_archaic_language(self):
        """
        Step 2: Verify poem.txt avoids overly archaic language.

        Modern technical audience may not understand "thee", "thou", "forsooth".
        """
        poem = self.poems.get('poem.txt', '').lower()

        found_archaic = []
        for pattern in self.COMPLEX_ARCHAIC_WORDS:
            matches = re.findall(pattern, poem)
            if matches:
                found_archaic.extend(matches)

        self.assertEqual(len(found_archaic), 0,
            f"poem.txt should avoid archaic language for accessibility. "
            f"Found: {found_archaic}")

    def test_poem_md_avoids_archaic_language(self):
        """
        Step 2: Verify POEM.md avoids overly archaic language.
        """
        poem = self.poems.get('POEM.md', '').lower()

        found_archaic = []
        for pattern in self.COMPLEX_ARCHAIC_WORDS:
            matches = re.findall(pattern, poem)
            if matches:
                found_archaic.extend(matches)

        self.assertEqual(len(found_archaic), 0,
            f"POEM.md should avoid archaic language for accessibility. "
            f"Found: {found_archaic}")

    def test_mirdb_poem_avoids_archaic_language(self):
        """
        Step 2: Verify mirdb_poem.txt avoids overly archaic language.
        """
        poem = self.poems.get('mirdb_poem.txt', '').lower()

        found_archaic = []
        for pattern in self.COMPLEX_ARCHAIC_WORDS:
            matches = re.findall(pattern, poem)
            if matches:
                found_archaic.extend(matches)

        self.assertEqual(len(found_archaic), 0,
            f"mirdb_poem.txt should avoid archaic language for accessibility. "
            f"Found: {found_archaic}")

    def test_poems_avoid_obscure_references(self):
        """
        Step 2: Verify poems avoid obscure literary/mythological references.

        Technical audiences may not recognize obscure mythological or
        literary references that require specialized knowledge.
        """
        found_obscure = []
        for pattern in self.OBSCURE_REFERENCES:
            matches = re.findall(pattern, self.all_poems_lower)
            if matches:
                found_obscure.extend(matches)

        self.assertEqual(len(found_obscure), 0,
            f"Poems should avoid obscure literary references. "
            f"Found: {found_obscure}")

    def test_poems_avoid_raw_technical_jargon(self):
        """
        Step 2: Verify poems avoid raw technical jargon.

        PRD states: "Avoid jargon without poetic transformation"
        Raw technical terms like "TCP/IP" or "JSON" don't belong in poetry.
        """
        found_jargon = []
        for pattern in self.RAW_TECHNICAL_JARGON:
            matches = re.findall(pattern, self.all_poems_lower)
            if matches:
                found_jargon.extend(matches)

        self.assertEqual(len(found_jargon), 0,
            f"Poems should avoid raw technical jargon. "
            f"Found: {found_jargon}")

    def test_poems_use_accessible_technical_terms(self):
        """
        Step 2: Verify poems use accessible, poetically transformed technical terms.

        The PRD provides guidance on acceptable technical terms that have
        been given poetic interpretation.
        """
        words = re.findall(r'\b[a-zA-Z]+\b', self.all_poems_lower)

        found_accessible = [w for w in words
                          if w in self.ACCEPTABLE_TECHNICAL_TERMS]

        self.assertGreater(len(found_accessible), 3,
            f"Poems should use accessible technical vocabulary from PRD. "
            f"Found: {Counter(found_accessible).most_common()}")

    def test_average_word_length_is_accessible(self):
        """
        Step 2: Verify average word length suggests accessible vocabulary.

        Highly complex vocabulary tends to have longer average word lengths.
        English prose averages ~4-5 characters per word.
        """
        words = re.findall(r'\b[a-zA-Z]+\b', self.all_poems_lower)

        if len(words) > 0:
            avg_length = sum(len(w) for w in words) / len(words)

            # Average word length should be reasonable (4-7 chars)
            # Poetry can have slightly longer words due to imagery
            self.assertLess(avg_length, 8,
                f"Average word length should be accessible. "
                f"Found: {avg_length:.2f} characters")
            self.assertGreater(avg_length, 3,
                f"Average word length seems too short. "
                f"Found: {avg_length:.2f} characters")

    def test_poem_vocabulary_diversity(self):
        """
        Step 2: Verify poems have appropriate vocabulary diversity.

        Good accessible writing has a reasonable type-token ratio -
        not too repetitive, but not using too many unique obscure words.
        """
        words = re.findall(r'\b[a-zA-Z]+\b', self.all_poems_lower)

        if len(words) > 0:
            unique_words = set(words)
            type_token_ratio = len(unique_words) / len(words)

            # TTR between 0.3-0.7 suggests accessible but varied vocabulary
            self.assertGreater(type_token_ratio, 0.25,
                f"Vocabulary may be too repetitive. TTR: {type_token_ratio:.2f}")
            self.assertLess(type_token_ratio, 0.85,
                f"Vocabulary may be too complex/varied. TTR: {type_token_ratio:.2f}")


class TestGrammarAndSpelling(unittest.TestCase):
    """
    Additional tests for grammar and spelling validation.

    While full grammar checking requires NLP tools, we can check for
    common issues that indicate poor grammar or spelling.
    """

    # Common spelling errors or typos
    COMMON_MISSPELLINGS = [
        (r'\bteh\b', 'the'),
        (r'\brecieve\b', 'receive'),
        (r'\boccured\b', 'occurred'),
        (r'\bseperately?\b', 'separately'),
        (r'\bdefinate\b', 'definite'),
        (r'\buntill\b', 'until'),  # Only match "untill" (double l), not "until"
        (r'\bwierd\b', 'weird'),
        (r'\baccidentaly\b', 'accidentally'),
        (r'\boccassion\b', 'occasion'),
        (r'\brefering\b', 'referring'),
    ]

    # Grammar issues - double words
    DOUBLE_WORD_PATTERN = re.compile(r'\b(\w+)\s+\1\b', re.IGNORECASE)

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

        cls.all_poems = ' '.join(cls.poems.values())
        cls.all_poems_lower = cls.all_poems.lower()

    def test_no_common_misspellings(self):
        """
        Test Case 1: Verify poems don't contain common misspellings.
        """
        found_misspellings = []

        for pattern, correct in self.COMMON_MISSPELLINGS:
            matches = re.findall(pattern, self.all_poems_lower)
            if matches:
                found_misspellings.append((matches[0], f"should be '{correct}'"))

        self.assertEqual(len(found_misspellings), 0,
            f"Poems should not contain common misspellings. "
            f"Found: {found_misspellings}")

    def test_no_double_words(self):
        """
        Test Case 1: Verify poems don't contain accidental double words.

        e.g., "the the" or "data data" (unless intentional repetition)
        """
        # Find double words
        doubles = self.DOUBLE_WORD_PATTERN.findall(self.all_poems_lower)

        # Filter out intentional poetic repetition (rare in technical poetry)
        # but allow for some common intentional cases
        intentional = ['never', 'ever', 'more']  # "forevermore" patterns
        unintentional = [d for d in doubles if d not in intentional]

        self.assertEqual(len(unintentional), 0,
            f"Poems should not contain unintentional double words. "
            f"Found: {unintentional}")

    def test_proper_capitalization(self):
        """
        Test Case 1: Verify poems have proper capitalization.

        Lines should generally start with capital letters.
        """
        for name, poem in self.poems.items():
            lines = [l for l in poem.split('\n')
                    if l.strip() and not l.strip().startswith('#')]

            if lines:
                # Check that most lines start with capital or punctuation
                capital_lines = sum(1 for l in lines
                                   if l.strip() and l.strip()[0].isupper())
                ratio = capital_lines / len(lines)

                self.assertGreater(ratio, 0.8,
                    f"{name} should have proper line capitalization. "
                    f"Only {capital_lines}/{len(lines)} lines start with capitals")

    def test_balanced_punctuation(self):
        """
        Test Case 1: Verify poems have balanced punctuation.

        Parentheses, quotes, etc. should be balanced.
        """
        for name, poem in self.poems.items():
            # Check parentheses balance
            open_parens = poem.count('(')
            close_parens = poem.count(')')
            self.assertEqual(open_parens, close_parens,
                f"{name} should have balanced parentheses. "
                f"Found: {open_parens} '(' and {close_parens} ')'")

            # Check quotes balance
            double_quotes = poem.count('"')
            self.assertEqual(double_quotes % 2, 0,
                f"{name} should have balanced double quotes. "
                f"Found: {double_quotes} quotes")


class TestProfessionalAudienceSuitability(unittest.TestCase):
    """
    Test for NFR-3: Poem should be suitable for professional/technical audiences.

    Validates that language is appropriate for technical/professional contexts.
    """

    # Words/phrases inappropriate for professional contexts
    INAPPROPRIATE_CONTENT = [
        # Profanity (basic check)
        r'\bdamn\b', r'\bhell\b(?!\s+of)',
        # Overly casual/slang
        r'\bgonna\b', r'\bwanna\b', r'\bgotta\b',
        r'\bkinda\b', r'\bsorta\b', r'\bdunno\b',
        r'\byeah\b', r'\bnope\b', r'\byup\b',
        # Internet slang
        r'\blol\b', r'\bromfl?\b', r'\bimho\b',
        r'\bbtw\b', r'\bfyi\b', r'\bomg\b',
        r'\bwow\b', r'\buh\b', r'\bum\b',
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

        cls.all_poems_lower = ' '.join(cls.poems.values()).lower()

    def test_no_inappropriate_language(self):
        """
        Verify poems don't contain inappropriate language.
        """
        found_inappropriate = []
        for pattern in self.INAPPROPRIATE_CONTENT:
            matches = re.findall(pattern, self.all_poems_lower)
            if matches:
                found_inappropriate.extend(matches)

        self.assertEqual(len(found_inappropriate), 0,
            f"Poems should not contain inappropriate/casual language. "
            f"Found: {found_inappropriate}")

    def test_maintains_formal_tone(self):
        """
        Verify poems maintain a formal/professional tone.

        Check for presence of formal language indicators.
        """
        formal_indicators = [
            r'\bguard(?:s|ian)?\b',
            r'\bstead(?:fast|y)\b',
            r'\bfaithful\b',
            r'\beternal\b',
            r'\bendur(?:e|es|ing)\b',
            r'\bpersist(?:s|ent)?\b',
            r'\bpreserve[ds]?\b',
            r'\btrust\b',
        ]

        found_formal = []
        for pattern in formal_indicators:
            matches = re.findall(pattern, self.all_poems_lower)
            if matches:
                found_formal.extend(matches)

        self.assertGreater(len(found_formal), 2,
            f"Poems should maintain formal tone. "
            f"Found formal indicators: {found_formal}")


if __name__ == '__main__':
    unittest.main(verbosity=2)
