"""
Test Suite: Content Appropriateness Validation (Scenario 10)

This test suite validates NFR-4 from the PRD: Poem must not contain any
inappropriate or offensive content.

Test Cases:
1. Verify no offensive, inappropriate, or controversial content is present
2. Verify all metaphors and references are culturally appropriate and neutral

Steps from scenario:
- Step 1: Review for inappropriate content - Check for any offensive,
  controversial, or inappropriate language (Content will be used in public
  marketing and documentation)
- Step 2: Verify cultural sensitivity - Ensure no culturally insensitive
  references or metaphors (Global audience requires cultural neutrality)

MirDB Context:
- Poem is used for public marketing and documentation
- Target audience is global and includes diverse cultural backgrounds
- Content must be suitable for professional/corporate settings
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


class TestInappropriateContentReview(unittest.TestCase):
    """
    Test Case 1: Review for inappropriate content

    Input: Poem text content
    Expected: No offensive, inappropriate, or controversial content present
    Type: manual (automated validation through pattern matching)

    Step 1 from scenario: Review for inappropriate content
    - Check for any offensive, controversial, or inappropriate language
    - Context: Content will be used in public marketing and documentation
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

    def test_poem_file_exists(self):
        """Verify the poem file exists and can be loaded."""
        self.assertIsNotNone(self.poem_content)
        self.assertGreater(len(self.poem_content.strip()), 0,
                          "Poem should have content")

    def test_no_profanity(self):
        """
        Test Case 1: Verify poem contains no profanity or vulgar language.

        NFR-4 explicitly requires no inappropriate content. Profanity
        would be completely unacceptable in professional marketing materials.
        """
        profanity_patterns = [
            r'\bdamn\b', r'\bhell\b(?!\s+(of|or))', r'\bcrap\b',
            r'\bshit\b', r'\bfuck\b', r'\bass\b(?!(ess|et|ign))',
            r'\bbastard\b', r'\bbitch\b', r'\bpiss\b',
            r'\bsuck\b(?!(cess|cumb))', r'\bsucks\b',
        ]

        found_profanity = []
        for pattern in profanity_patterns:
            matches = re.findall(pattern, self.poem_lower)
            if matches:
                found_profanity.extend(matches)

        self.assertEqual(
            len(found_profanity), 0,
            f"Poem must not contain profanity (NFR-4). Found: {found_profanity}"
        )

    def test_no_slurs_or_hate_speech(self):
        """
        Test Case 1: Verify poem contains no slurs or hate speech.

        Content for global marketing must be free from any discriminatory,
        racist, sexist, or otherwise hateful language.
        """
        # This is a conservative list - real implementation would be more comprehensive
        hate_speech_patterns = [
            r'\bidiot\b', r'\bmoron\b', r'\bstupid\b', r'\bretard\b',
            r'\bimbecile\b', r'\bfool(?:ish)?\b',
            r'\bdumb\b(?!(bell|found))',
        ]

        found_hate = []
        for pattern in hate_speech_patterns:
            matches = re.findall(pattern, self.poem_lower)
            if matches:
                found_hate.extend(matches)

        self.assertEqual(
            len(found_hate), 0,
            f"Poem must not contain slurs or derogatory terms. Found: {found_hate}"
        )

    def test_no_violence_references(self):
        """
        Test Case 1: Verify poem contains no violent or aggressive content.

        Marketing content should avoid violent imagery that might be
        offensive or inappropriate for professional contexts.
        """
        violence_patterns = [
            r'\bkill(?:ing|ed|er)?\b',
            r'\bmurder\b', r'\bslaughter\b', r'\bmassacre\b',
            r'\bblood(?:y|shed)?\b',
            r'\bwar(?:fare)?\b(?!(rant|n|e|d))',
            r'\battack\b', r'\bdestroy\b', r'\bannihilate\b',
            r'\bweapon\b', r'\bgun\b', r'\bsword\b', r'\bbomb\b',
            r'\bexplode\b', r'\bviolent\b', r'\bviolence\b',
            r'\btorture\b', r'\bharm\b(?!(ony|less))',
        ]

        found_violence = []
        for pattern in violence_patterns:
            matches = re.findall(pattern, self.poem_lower)
            if matches:
                found_violence.extend(matches)

        # Allow exceptions for technical context (e.g., "crash" is OK for software)
        technical_exceptions = ['crash']  # "crash" is acceptable in database context
        found_violence = [v for v in found_violence if v not in technical_exceptions]

        self.assertEqual(
            len(found_violence), 0,
            f"Poem should avoid violent imagery for professional content. "
            f"Found: {found_violence}"
        )

    def test_no_death_references(self):
        """
        Test Case 1: Verify poem avoids death-related themes.

        While poetic, death references may be inappropriate for
        corporate marketing materials targeting global audiences.
        """
        death_patterns = [
            r'\bdead\b(?!(line|lock|ly))',
            r'\bdeath\b',
            r'\bdie(?:d|s)?\b(?!(sel))',
            r'\bdying\b',
            r'\bmortal(?:ity)?\b',
            r'\bgrave\b(?!(l|s|ly))',
            r'\btomb\b',
            r'\bcorpse\b',
            r'\bfuneral\b',
        ]

        found_death = []
        for pattern in death_patterns:
            matches = re.findall(pattern, self.poem_lower)
            if matches:
                found_death.extend(matches)

        self.assertEqual(
            len(found_death), 0,
            f"Poem should avoid death references for broader appeal. "
            f"Found: {found_death}"
        )

    def test_no_controversial_topics(self):
        """
        Test Case 1: Verify poem avoids politically or socially controversial topics.

        Marketing content should remain neutral and avoid topics that
        could alienate parts of the global audience.
        """
        controversial_patterns = [
            r'\bpolitics\b', r'\bpolitical\b', r'\bpolitician\b',
            r'\breligion\b', r'\breligious\b',
            r'\bgod\b(?!(zilla))', r'\bdeity\b', r'\bdevil\b',
            r'\bheaven\b', r'\bhell\b',
            r'\bsin(?:ful|ner)?\b(?!(ce|gle|g))',
            r'\bholy\b', r'\bsacred\b(?!ness)',
            r'\bprotest\b', r'\bactivis[mt]\b',
            r'\bterror(?:ist|ism)?\b',
            r'\brace\b(?!(d|r|s\s|condition))',  # Allow "raced" but not race as topic
            r'\bgender\b',
            r'\babortion\b', r'\bvaccine\b',
        ]

        found_controversial = []
        for pattern in controversial_patterns:
            matches = re.findall(pattern, self.poem_lower)
            if matches:
                found_controversial.extend(matches)

        self.assertEqual(
            len(found_controversial), 0,
            f"Poem should avoid controversial political/religious topics. "
            f"Found: {found_controversial}"
        )

    def test_no_sexual_content(self):
        """
        Test Case 1: Verify poem contains no sexual content or innuendo.

        Professional marketing content must be appropriate for all audiences
        and workplace environments.
        """
        sexual_patterns = [
            r'\bsex(?:y|ual)?\b', r'\berotic\b',
            r'\bseduct(?:ion|ive)?\b', r'\bpornograph\b',
            r'\bnaked\b', r'\bnude\b', r'\bstrip\b(?!(ped|ping)?$)',
            r'\bintimate\b(?!\s+knowledge)',
            r'\blust\b(?!(er|rous))',
        ]

        found_sexual = []
        for pattern in sexual_patterns:
            matches = re.findall(pattern, self.poem_lower)
            if matches:
                found_sexual.extend(matches)

        self.assertEqual(
            len(found_sexual), 0,
            f"Poem must not contain sexual content for workplace appropriateness. "
            f"Found: {found_sexual}"
        )

    def test_no_drug_alcohol_references(self):
        """
        Test Case 1: Verify poem avoids drug and alcohol references.

        Marketing materials should not include substance-related content
        that could be inappropriate in professional or certain cultural contexts.
        """
        substance_patterns = [
            r'\bdrug\b', r'\bcocaine\b', r'\bheroin\b', r'\bmarijuana\b',
            r'\bweed\b', r'\balcohol\b', r'\bdrunk\b', r'\bintoxicat\b',
            r'\bhigh\b(?!\s+(performance|speed|quality|reliability))',
            r'\baddiction\b', r'\bsmok(?:e|ing)\b',
        ]

        found_substance = []
        for pattern in substance_patterns:
            matches = re.findall(pattern, self.poem_lower)
            if matches:
                found_substance.extend(matches)

        self.assertEqual(
            len(found_substance), 0,
            f"Poem should avoid substance references for professional appeal. "
            f"Found: {found_substance}"
        )

    def test_no_disparaging_competitor_references(self):
        """
        Test Case 1: Verify poem doesn't disparage competitors.

        While the poem can highlight MirDB's advantages over memcached,
        it should not actively disparage or mock competitors.
        """
        disparaging_patterns = [
            r'\bworse\s+than\b', r'\binferior\b',
            r'\bpathetic\b', r'\bterrible\b', r'\bawful\b',
            r'\bjunk\b', r'\btrash\b', r'\bgarbage\b(?!\s+collect)',
            r'\bworthless\b', r'\buseless\b', r'\bbroken\b',
        ]

        found_disparaging = []
        for pattern in disparaging_patterns:
            matches = re.findall(pattern, self.poem_lower)
            if matches:
                found_disparaging.extend(matches)

        self.assertEqual(
            len(found_disparaging), 0,
            f"Poem should not disparage competitors. Found: {found_disparaging}"
        )


class TestCulturalSensitivity(unittest.TestCase):
    """
    Test Case 2: Verify cultural sensitivity

    Input: Cultural sensitivity review
    Expected: All metaphors and references are culturally appropriate and neutral
    Type: manual (automated validation through pattern matching)

    Step 2 from scenario: Verify cultural sensitivity
    - Ensure no culturally insensitive references or metaphors
    - Context: Global audience requires cultural neutrality
    """

    @classmethod
    def setUpClass(cls):
        """Load the poem content once for all tests."""
        cls.poem_content = PoemLoader.load_poem()
        cls.poem_lower = cls.poem_content.lower()

    def test_no_culturally_specific_religious_references(self):
        """
        Test Case 2: Verify poem avoids culturally-specific religious references.

        A global audience includes people of many faiths and none.
        Religious references could exclude or offend some audiences.
        """
        religious_patterns = [
            r'\bjesus\b', r'\bchrist\b', r'\bbible\b', r'\bchurch\b',
            r'\ballah\b', r'\bmuhammad\b', r'\bquran\b', r'\bmosque\b',
            r'\bbuddha\b', r'\bhindu\b', r'\bshiva\b', r'\bvishnu\b',
            r'\bjewish\b', r'\bsynagogue\b', r'\btorah\b',
            r'\bpray(?:er|ing)?\b', r'\bworship\b',
            r'\bprophet\b', r'\bapostle\b', r'\bsaint\b',
        ]

        found_religious = []
        for pattern in religious_patterns:
            matches = re.findall(pattern, self.poem_lower)
            if matches:
                found_religious.extend(matches)

        self.assertEqual(
            len(found_religious), 0,
            f"Poem should avoid culturally-specific religious references for global neutrality. "
            f"Found: {found_religious}"
        )

    def test_no_culturally_specific_holidays(self):
        """
        Test Case 2: Verify poem avoids culturally-specific holiday references.

        Holidays are often tied to specific cultures/religions and
        may not resonate with or could exclude global audiences.
        """
        holiday_patterns = [
            r'\bchristmas\b', r'\beaster\b', r'\bthanksgiving\b',
            r'\bhanukkah\b', r'\bramadan\b', r'\bdiwali\b',
            r'\bholiday\b', r'\bcelebrat(?:e|ion)\b',
        ]

        found_holidays = []
        for pattern in holiday_patterns:
            matches = re.findall(pattern, self.poem_lower)
            if matches:
                found_holidays.extend(matches)

        self.assertEqual(
            len(found_holidays), 0,
            f"Poem should avoid culturally-specific holiday references. "
            f"Found: {found_holidays}"
        )

    def test_no_nationality_stereotypes(self):
        """
        Test Case 2: Verify poem avoids nationality-based stereotypes.

        Stereotyping nationalities or cultures is inappropriate for
        professional global marketing content.
        """
        stereotype_patterns = [
            r'\bamerican\s+dream\b',
            r'\bchinese\s+wall\b',  # Also a problematic term
            r'\bdutch\s+courage\b',
            r'\bfrench\s+leave\b',
            r'\birish\s+luck\b',
            r'\bspanish\s+inquisition\b',
            r'\bslave\b',  # Historical sensitivity
            r'\bcolonial\b',
            r'\bprimitive\b',
            r'\bsavage\b',
            r'\bbarbar(?:ian|ic|ous)\b',
        ]

        found_stereotypes = []
        for pattern in stereotype_patterns:
            matches = re.findall(pattern, self.poem_lower)
            if matches:
                found_stereotypes.extend(matches)

        self.assertEqual(
            len(found_stereotypes), 0,
            f"Poem should avoid nationality stereotypes. Found: {found_stereotypes}"
        )

    def test_universal_metaphors(self):
        """
        Test Case 2: Verify poem uses universal, culturally-neutral metaphors.

        Per PRD guidance, metaphors should relate to nature (trees, layers, flow),
        craftsmanship, and reliability - themes that are universally understood.
        """
        universal_metaphors = [
            r'\btree\b', r'\broot\b', r'\blayer\b', r'\blevel\b',
            r'\bflow\b', r'\bstream\b', r'\bwater\b',
            r'\bguard(?:ian)?\b', r'\bkeeper\b', r'\bsentinel\b',
            r'\bforge[d]?\b', r'\bcraft\b', r'\bbuild\b', r'\bmade\b',
            r'\blight\b', r'\bnight\b', r'\bday\b',
            r'\breturn\b', r'\brise\b', r'\bfall\b',
            r'\bsafe(?:ty)?\b', r'\bprotect\b', r'\bsecure\b',
            r'\btrust\b', r'\bfaithful\b', r'\breliab(?:le|ility)\b',
        ]

        found_universal = []
        for pattern in universal_metaphors:
            matches = re.findall(pattern, self.poem_lower)
            if matches:
                found_universal.extend(matches)

        self.assertGreater(
            len(found_universal), 3,
            f"Poem should use universally understood metaphors. "
            f"Found: {found_universal}"
        )

    def test_no_problematic_historical_references(self):
        """
        Test Case 2: Verify poem avoids problematic historical references.

        References to wars, conflicts, or controversial historical events
        may be sensitive in certain cultural contexts.
        """
        historical_patterns = [
            r'\bwar(?:\s+(?:i|ii|one|two))?\b',
            r'\bholocaust\b', r'\bgenocide\b',
            r'\bnazi\b', r'\bfascis[mt]\b',
            r'\bcoloniz(?:e|ation)\b',
            r'\bempire\b(?!(d|s\s))',  # "empire" alone could be problematic
            r'\bconquest\b', r'\boccup(?:y|ation)\b',
        ]

        found_historical = []
        for pattern in historical_patterns:
            matches = re.findall(pattern, self.poem_lower)
            if matches:
                found_historical.extend(matches)

        self.assertEqual(
            len(found_historical), 0,
            f"Poem should avoid sensitive historical references. "
            f"Found: {found_historical}"
        )

    def test_no_western_centric_idioms(self):
        """
        Test Case 2: Verify poem avoids Western-centric idioms.

        Idioms specific to Western cultures may not translate well
        or may be confusing to global audiences.
        """
        western_idioms = [
            r'\bamerican\s+pie\b',
            r'\bball\s+in\s+your\s+court\b',
            r'\bbite\s+the\s+bullet\b',
            r'\bbreak\s+a\s+leg\b',
            r'\bcost\s+an\s+arm\s+and\s+a\s+leg\b',
            r'\bhit\s+the\s+nail\b',
            r'\bkick\s+the\s+bucket\b',
            r'\bpiece\s+of\s+cake\b',
            r'\braining\s+cats\s+and\s+dogs\b',
            r'\bspill\s+the\s+beans\b',
            r'\bonce\s+upon\s+a\s+time\b',  # Western fairy tale trope
        ]

        found_idioms = []
        for pattern in western_idioms:
            matches = re.findall(pattern, self.poem_lower)
            if matches:
                found_idioms.extend(matches)

        self.assertEqual(
            len(found_idioms), 0,
            f"Poem should avoid Western-centric idioms. Found: {found_idioms}"
        )

    def test_no_gender_biased_language(self):
        """
        Test Case 2: Verify poem uses gender-neutral language where appropriate.

        Gender-inclusive language ensures the content is welcoming
        to all members of the global technical community.
        """
        gender_patterns = [
            r'\bmankind\b',  # Prefer "humanity" or "humankind"
            r'\bman-made\b',  # Prefer "artificial" or "human-made"
            r'\bmastermind\b',
            r'\bman\s+hours\b',
            r'\bforefathers\b',
            r'\bbrotherhood\b',
        ]

        found_gendered = []
        for pattern in gender_patterns:
            matches = re.findall(pattern, self.poem_lower)
            if matches:
                found_gendered.extend(matches)

        self.assertEqual(
            len(found_gendered), 0,
            f"Poem should use gender-neutral language. Found: {found_gendered}"
        )

    def test_no_ableist_language(self):
        """
        Test Case 2: Verify poem avoids ableist language.

        Language that disparages disabilities is inappropriate
        for inclusive professional content.
        """
        ableist_patterns = [
            r'\blame\b(?!d)',  # Allow "blamed" but check context
            r'\bcrippl(?:e|ed|ing)\b',
            r'\bblind(?!ed|ing|s)\b.*\bto\b',  # "blind to" is problematic
            r'\bdeaf\s+to\b',
            r'\bdumb\b(?!(?:bell|found))',
            r'\binsane\b', r'\bcrazy\b', r'\blunatic\b', r'\bmental\b',
            r'\bpsycho\b', r'\bschizo\b',
        ]

        found_ableist = []
        for pattern in ableist_patterns:
            matches = re.findall(pattern, self.poem_lower)
            if matches:
                found_ableist.extend(matches)

        self.assertEqual(
            len(found_ableist), 0,
            f"Poem should avoid ableist language. Found: {found_ableist}"
        )


class TestOverallContentAppropriateness(unittest.TestCase):
    """
    Combined validation of overall content appropriateness for NFR-4.

    Ensures the poem meets the highest standards for public marketing
    and documentation use across global audiences.
    """

    @classmethod
    def setUpClass(cls):
        """Load the poem content once for all tests."""
        cls.poem_content = PoemLoader.load_poem()
        cls.poem_lower = cls.poem_content.lower()

    def test_suitable_for_public_marketing(self):
        """
        Meta-test: Verify poem is suitable for public marketing use.

        The poem should be something a company would be proud to
        display on their website, in documentation, or at conferences.
        """
        # Check for presence of professional, positive language
        # These terms align with MirDB's marketing themes from the PRD
        positive_professional_terms = [
            r'\breliab(?:le|ility)\b', r'\btrust\b', r'\bsafe\b',
            r'\bpersist(?:s|ent)?\b', r'\bdurabl(?:e|ility)\b', r'\bpermanent\b',
            r'\bguardian\b', r'\bprotect\b', r'\bsecur(?:e|ity)\b',
            r'\bcraft\b', r'\belegant?\b', r'\bgrace\b',
            r'\bsentinel\b', r'\bkeeper\b', r'\bendur(?:e|es)\b',
            r'\beternal\b', r'\bforged\b', r'\bpreserved?\b',
        ]

        found_positive = []
        for pattern in positive_professional_terms:
            matches = re.findall(pattern, self.poem_lower)
            if matches:
                found_positive.extend(matches)

        self.assertGreater(
            len(found_positive), 3,
            f"Poem should use positive, professional language for marketing. "
            f"Found: {found_positive}"
        )

    def test_appropriate_for_all_ages(self):
        """
        Meta-test: Verify poem is appropriate for all ages.

        Content should be suitable for display at conferences and
        events where attendees of all ages might be present.
        """
        # This is a comprehensive check combining multiple categories
        inappropriate_for_all_ages = [
            # Violence
            r'\bkill\b', r'\bmurder\b', r'\bblood\b',
            # Sex
            r'\bsex\b', r'\berotic\b', r'\bnaked\b',
            # Profanity
            r'\bdamn\b', r'\bhell\b', r'\bcrap\b',
            # Drugs
            r'\bdrug\b', r'\balcohol\b', r'\bdrunk\b',
            # Fear-inducing
            r'\bterror\b', r'\bhorror\b', r'\bnightmare\b(?!\s+scenario)',
        ]

        found_inappropriate = []
        for pattern in inappropriate_for_all_ages:
            matches = re.findall(pattern, self.poem_lower)
            if matches:
                found_inappropriate.extend(matches)

        self.assertEqual(
            len(found_inappropriate), 0,
            f"Poem must be appropriate for all ages. Found: {found_inappropriate}"
        )

    def test_tone_is_respectful_and_dignified(self):
        """
        Meta-test: Verify poem maintains respectful and dignified tone.

        The poem should elevate MirDB without mocking competitors
        or using disrespectful language toward anyone or anything.
        """
        # Check for elevated vocabulary (per PRD tone guidelines)
        elevated_vocabulary = [
            'guardian', 'sentinel', 'keeper', 'steadfast',
            'grace', 'eternal', 'endure', 'forge', 'craft',
            'preserve', 'trust', 'faithful', 'elegant'
        ]

        found_elevated = []
        for word in elevated_vocabulary:
            if word in self.poem_lower:
                found_elevated.append(word)

        self.assertGreater(
            len(found_elevated), 2,
            f"Poem should use elevated, respectful vocabulary. Found: {found_elevated}"
        )


if __name__ == '__main__':
    unittest.main(verbosity=2)
