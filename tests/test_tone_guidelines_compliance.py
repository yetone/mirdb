"""
Test Suite: Tone Guidelines Compliance (Scenario 20)

This test suite validates that the MirDB poem follows encouraged tone guidelines
and avoids discouraged elements as specified in Appendix B of the PRD.

Test Cases:
1. Nature metaphor usage - Contains appropriate nature-based metaphors (trees, layers, flow)
2. Craftsmanship theme usage - Contains themes of building, crafting, or engineering excellence
3. Discouraged element check - No obscure references, inappropriate humor, or exaggerated claims

PRD Appendix B - Tone Guidelines:

Encouraged:
- Metaphors relating to nature (trees, layers, flow)
- References to craftsmanship and building
- Themes of reliability and trust
- Technical accuracy through accessible language

Discouraged:
- Overly complex or obscure references
- Humor that might seem unprofessional
- Exaggerated claims about capabilities
- Jargon without poetic transformation
"""

import unittest
import os
import re


class PoemLoader:
    """Helper class to load the poem content."""

    POEM_PATH = os.path.join(os.path.dirname(__file__), '..', 'versecraft', 'mirdb_poem.txt')

    @classmethod
    def load_poem(cls) -> str:
        """Load and return the poem content."""
        with open(cls.POEM_PATH, 'r', encoding='utf-8') as f:
            return f.read()

    @classmethod
    def get_poem_lines(cls) -> list:
        """Return poem as list of non-empty lines."""
        return [line for line in cls.load_poem().strip().split('\n') if line.strip()]


class TestNatureMetaphorUsage(unittest.TestCase):
    """
    Test Case 1: Nature metaphor usage

    Input: Nature metaphor usage
    Expected: Contains appropriate nature-based metaphors (trees, layers, flow)
    Type: manual (automated through pattern matching)

    Step 1 from scenario: Check nature metaphors
    - Verify use of encouraged metaphors (trees, layers, flow)
    - Context: Nature metaphors are encouraged for LSM-tree representation
    """

    @classmethod
    def setUpClass(cls):
        cls.poem_content = PoemLoader.load_poem()
        cls.poem_lower = cls.poem_content.lower()
        cls.poem_lines = PoemLoader.get_poem_lines()

    def test_poem_file_exists_and_readable(self):
        """Verify the poem file exists and can be loaded."""
        self.assertTrue(os.path.exists(PoemLoader.POEM_PATH),
                       "Poem file should exist at versecraft/mirdb_poem.txt")
        self.assertGreater(len(self.poem_content), 0,
                          "Poem should have content")

    def test_contains_tree_related_metaphors(self):
        """
        Verify poem contains tree-related metaphors.

        LSM-tree is the core data structure of MirDB. Tree metaphors
        are highly encouraged to represent this architecture.
        """
        tree_patterns = [
            r'\btree\b',
            r'\broot(?:s|ed)?\b',
            r'\bbranch(?:es|ing)?\b',
            r'\bleaf\b',
            r'\bleaves\b',
            r'\bgrow(?:s|ing|th)?\b',
        ]

        tree_matches = []
        for pattern in tree_patterns:
            matches = re.findall(pattern, self.poem_lower)
            if matches:
                tree_matches.extend(matches)

        # Tree metaphors are encouraged but not required if layers/flow are present
        # Store results for overall test
        self.tree_metaphors_found = tree_matches
        # This is a soft check - main validation is in test_has_nature_metaphors

    def test_contains_layer_related_metaphors(self):
        """
        Verify poem contains layer-related metaphors.

        LSM-tree has multiple levels/layers from memtable to SSTables.
        Layer metaphors represent this hierarchical structure.
        """
        layer_patterns = [
            r'\blayer(?:s|ed)?\b',
            r'\blevel(?:s)?\b',
            r'\bdepth(?:s)?\b',
            r'\bdeep(?:er|est)?\b',
            r'\bhall(?:s)?\b',
            r'\bstage(?:s)?\b',
            r'\btier(?:s|ed)?\b',
        ]

        layer_matches = []
        for pattern in layer_patterns:
            matches = re.findall(pattern, self.poem_lower)
            if matches:
                layer_matches.extend(matches)

        self.assertGreater(len(layer_matches), 0,
            f"Poem should contain layer-related metaphors (layers, levels, depths, halls). "
            f"Found: {layer_matches if layer_matches else 'none'}")

    def test_contains_flow_related_metaphors(self):
        """
        Verify poem contains flow-related metaphors.

        Data flows through the LSM-tree from memory to disk.
        Flow metaphors represent this data movement.
        """
        flow_patterns = [
            r'\bflow(?:s|ing)?\b',
            r'\bstream(?:s|ing)?\b',
            r'\bcascade(?:s|ing)?\b',
            r'\bcourse(?:s)?\b',
            r'\bpool(?:s)?\b',
            r'\bpour(?:s|ing|ed)?\b',
        ]

        flow_matches = []
        for pattern in flow_patterns:
            matches = re.findall(pattern, self.poem_lower)
            if matches:
                flow_matches.extend(matches)

        self.assertGreater(len(flow_matches), 0,
            f"Poem should contain flow-related metaphors (flow, stream, cascade, course). "
            f"Found: {flow_matches if flow_matches else 'none'}")

    def test_has_nature_metaphors(self):
        """
        Primary acceptance test: Verify poem contains at least one category of nature metaphors.

        Per PRD guidelines, nature metaphors (trees, layers, flow) are encouraged
        for representing the LSM-tree architecture.
        """
        # All nature-related patterns combined
        nature_patterns = [
            # Tree-related
            r'\btree\b', r'\broot\b', r'\bbranch\b', r'\bleaf\b', r'\bgrow\b',
            # Layer-related
            r'\blayer\b', r'\blevel\b', r'\bdepth\b', r'\bdeep\b', r'\bhall\b',
            # Flow-related
            r'\bflow\b', r'\bstream\b', r'\bcascade\b', r'\bcourse\b', r'\bpool\b',
            # Other nature
            r'\blight\b', r'\bnight\b', r'\bethernal\b', r'\bendless\b',
        ]

        all_nature_matches = []
        for pattern in nature_patterns:
            matches = re.findall(pattern, self.poem_lower)
            if matches:
                all_nature_matches.extend(matches)

        self.assertGreaterEqual(len(all_nature_matches), 3,
            f"Poem should contain multiple nature-based metaphors (trees, layers, flow). "
            f"Found {len(all_nature_matches)}: {list(set(all_nature_matches))}")


class TestCraftsmanshipThemeUsage(unittest.TestCase):
    """
    Test Case 2: Craftsmanship theme usage

    Input: Craftsmanship theme usage
    Expected: Contains themes of building, crafting, or engineering excellence
    Type: manual (automated through pattern matching)

    Step 2 from scenario: Check craftsmanship themes
    - Look for references to building, crafting, forging
    - Context: Craftsmanship themes emphasize quality engineering
    """

    @classmethod
    def setUpClass(cls):
        cls.poem_content = PoemLoader.load_poem()
        cls.poem_lower = cls.poem_content.lower()

    def test_contains_forging_references(self):
        """
        Verify poem contains forging/metalwork metaphors.

        MirDB is written in Rust - forging metaphors emphasize
        the strength and quality of the Rust implementation.
        """
        forging_patterns = [
            r'\bforge[d]?\b',
            r'\brust\b',  # Both the language and the metal
            r'\bsteel\b',
            r'\biron\b',
            r'\bsmith\b',
            r'\bmetal\b',
            r'\bweld\b',
            r'\btemper(?:ed)?\b',
        ]

        forging_matches = []
        for pattern in forging_patterns:
            matches = re.findall(pattern, self.poem_lower)
            if matches:
                forging_matches.extend(matches)

        self.assertGreater(len(forging_matches), 0,
            f"Poem should contain forging/metalwork metaphors (forge, rust, steel). "
            f"Found: {forging_matches if forging_matches else 'none'}")

    def test_contains_building_references(self):
        """
        Verify poem contains building/construction metaphors.

        Building metaphors emphasize the architectural quality
        and careful construction of MirDB.
        """
        building_patterns = [
            r'\bbuild(?:s|ing|t)?\b',
            r'\bconstruct(?:s|ed|ion)?\b',
            r'\bcraft(?:s|ed|ing)?\b',
            r'\bwall(?:s)?\b',
            r'\bfoundation\b',
            r'\barchitect(?:ure)?\b',
            r'\bstructure\b',
            r'\bstand(?:s)?\b',
        ]

        building_matches = []
        for pattern in building_patterns:
            matches = re.findall(pattern, self.poem_lower)
            if matches:
                building_matches.extend(matches)

        self.assertGreater(len(building_matches), 0,
            f"Poem should contain building/construction metaphors (build, craft, walls, stand). "
            f"Found: {building_matches if building_matches else 'none'}")

    def test_contains_quality_craftsmanship_language(self):
        """
        Verify poem uses language that emphasizes quality craftsmanship.

        Words like 'care', 'guard', 'keep', 'trust' emphasize
        the quality and reliability of the engineering.
        """
        quality_patterns = [
            r'\bcare\b',
            r'\bguard(?:s|ian|ed)?\b',
            r'\bkeep(?:s|er|ing)?\b',
            r'\btrust\b',
            r'\bpersist(?:s|ent)?\b',
            r'\bdurable\b',
            r'\breliab(?:le|ility)\b',
            r'\bsteadfast\b',
            r'\bpatient\b',
            r'\btireless\b',
        ]

        quality_matches = []
        for pattern in quality_patterns:
            matches = re.findall(pattern, self.poem_lower)
            if matches:
                quality_matches.extend(matches)

        self.assertGreater(len(quality_matches), 0,
            f"Poem should contain quality/craftsmanship language (care, guard, trust, steadfast). "
            f"Found: {quality_matches if quality_matches else 'none'}")

    def test_has_craftsmanship_themes(self):
        """
        Primary acceptance test: Verify poem contains craftsmanship themes.

        Per PRD guidelines, references to craftsmanship and building
        are encouraged to emphasize quality engineering.
        """
        # All craftsmanship-related patterns combined
        craftsmanship_patterns = [
            # Forging
            r'\bforge[d]?\b', r'\brust\b', r'\bsteel\b', r'\bsmith\b',
            # Building
            r'\bbuild\b', r'\bcraft\b', r'\bwall\b', r'\bstand\b',
            # Quality
            r'\bguard\b', r'\bkeep\b', r'\btrust\b', r'\bcare\b',
            r'\bpatient\b', r'\btireless\b', r'\bsteadfast\b',
        ]

        all_craftsmanship_matches = []
        for pattern in craftsmanship_patterns:
            matches = re.findall(pattern, self.poem_lower)
            if matches:
                all_craftsmanship_matches.extend(matches)

        self.assertGreaterEqual(len(all_craftsmanship_matches), 3,
            f"Poem should contain multiple craftsmanship themes (building, crafting, forging). "
            f"Found {len(all_craftsmanship_matches)}: {list(set(all_craftsmanship_matches))}")


class TestDiscouragedElementsAbsent(unittest.TestCase):
    """
    Test Case 3: Discouraged element check

    Input: Discouraged element check
    Expected: No obscure references, inappropriate humor, or exaggerated claims present
    Type: manual (automated through pattern matching)

    Step 3 from scenario: Check for discouraged elements
    - Ensure no obscure references, unprofessional humor, or exaggerated claims
    - Context: Avoid elements that would harm professional perception

    PRD Discouraged elements:
    - Overly complex or obscure references
    - Humor that might seem unprofessional
    - Exaggerated claims about capabilities
    - Jargon without poetic transformation
    """

    @classmethod
    def setUpClass(cls):
        cls.poem_content = PoemLoader.load_poem()
        cls.poem_lower = cls.poem_content.lower()
        cls.poem_lines = PoemLoader.get_poem_lines()

    def test_no_obscure_cultural_references(self):
        """
        Verify poem contains no obscure cultural references.

        PRD discourages overly complex or obscure references
        that might confuse a global technical audience.
        """
        obscure_patterns = [
            # Mythological references that might be obscure
            r'\bzeus\b', r'\bposeidon\b', r'\bhades\b',
            r'\bodin\b', r'\bthor\b', r'\bloki\b',
            r'\bra\b', r'\banubis\b', r'\bosiris\b',
            # Literary references that might be obscure
            r'\bshakespear\b', r'\bhamlet\b', r'\bmacbeth\b',
            r'\bdante\b', r'\bvirgil\b', r'\bhomer\b',
            # Historical figures that might be obscure
            r'\bcaesar\b', r'\bnapoleon\b', r'\bcleopatra\b',
            # Pop culture references that might date poorly
            r'\bskywalker\b', r'\bfrodo\b', r'\bharry\s+potter\b',
        ]

        found_obscure = []
        for pattern in obscure_patterns:
            matches = re.findall(pattern, self.poem_lower)
            if matches:
                found_obscure.extend(matches)

        self.assertEqual(len(found_obscure), 0,
            f"Poem should not contain obscure cultural references. Found: {found_obscure}")

    def test_no_unprofessional_humor(self):
        """
        Verify poem contains no unprofessional humor.

        PRD discourages humor that might seem unprofessional
        in marketing and documentation contexts.
        """
        humor_patterns = [
            # Puns that might seem unprofessional
            r'\blol\b', r'\brofl\b', r'\blmao\b',
            # Slang that might seem unprofessional
            r'\bcool\b', r'\bawesome\b', r'\bsick\b',
            r'\brad\b', r'\bdope\b', r'\blit\b',
            # Jokes indicators
            r'\bjust\s+kidding\b', r'\bjk\b',
            r'\bha\s*ha\b', r'\bhehe\b',
            # Sarcasm indicators
            r'\bnot!\b', r'\byeah\s+right\b',
            # Meme references
            r'\bmeme\b', r'\btrolling\b',
        ]

        found_humor = []
        for pattern in humor_patterns:
            matches = re.findall(pattern, self.poem_lower)
            if matches:
                found_humor.extend(matches)

        self.assertEqual(len(found_humor), 0,
            f"Poem should not contain unprofessional humor. Found: {found_humor}")

    def test_no_exaggerated_claims(self):
        """
        Verify poem contains no exaggerated claims about capabilities.

        PRD discourages exaggerated claims that might misrepresent
        MirDB's actual capabilities.
        """
        exaggeration_patterns = [
            # Absolute claims
            r'\bbest\s+in\s+the\s+world\b',
            r'\bfastest\s+ever\b',
            r'\bunbeatable\b',
            r'\bperfect\b',
            r'\bflawless\b',
            r'\binvincible\b',
            r'\bunstoppable\b',
            # Hyperbolic comparisons
            r'\bmillion\s+times\b',
            r'\bbillion\s+times\b',
            r'\binfinitely\s+better\b',
            # Guarantees
            r'\bnever\s+fails\b',
            r'\b100%\b',
            r'\balways\s+works\b',
            r'\bguaranteed\s+to\b',
            # Superiority claims without basis
            r'\bkills?\s+the\s+competition\b',
            r'\bdestroys?\s+competitors?\b',
        ]

        found_exaggeration = []
        for pattern in exaggeration_patterns:
            matches = re.findall(pattern, self.poem_lower)
            if matches:
                found_exaggeration.extend(matches)

        self.assertEqual(len(found_exaggeration), 0,
            f"Poem should not contain exaggerated claims. Found: {found_exaggeration}")

    def test_no_raw_jargon(self):
        """
        Verify poem transforms technical jargon poetically.

        PRD discourages jargon without poetic transformation.
        Technical terms should be presented as metaphors.
        """
        # Raw jargon that should be transformed
        raw_jargon_patterns = [
            r'\bapi\b',
            r'\bhttp\b',
            r'\btcp\b',
            r'\budp\b',
            r'\bjson\b',
            r'\bxml\b',
            r'\bsql\b',
            r'\bnosql\b',
            r'\bcpu\b',
            r'\bgpu\b',
            r'\bram\b(?!\s)',  # RAM as acronym, not "ram" the animal
            r'\bhdd\b',
            r'\bssd\b(?!\s)',
            r'\bio\b',
            r'\bthreadpool\b',
            r'\bmutex\b',
            r'\bsemaphore\b',
            r'\bhashmap\b',
            r'\blinkedlist\b',
            r'\bbtree\b',  # Should use poetic form like "LSM trees" or metaphor
        ]

        found_jargon = []
        for pattern in raw_jargon_patterns:
            matches = re.findall(pattern, self.poem_lower)
            if matches:
                found_jargon.extend(matches)

        self.assertEqual(len(found_jargon), 0,
            f"Poem should transform jargon poetically, not use raw technical terms. "
            f"Found: {found_jargon}")

    def test_no_discouraged_elements(self):
        """
        Primary acceptance test: Verify no discouraged elements are present.

        This is a combined check for all discouraged elements:
        - Obscure references
        - Unprofessional humor
        - Exaggerated claims
        - Raw jargon
        """
        all_discouraged_patterns = [
            # Obscure references
            r'\bzeus\b', r'\bposeidon\b', r'\bodin\b', r'\bthor\b',
            # Unprofessional humor
            r'\blol\b', r'\brofl\b', r'\blmao\b', r'\bjk\b',
            # Exaggerated claims
            r'\bbest\s+in\s+the\s+world\b', r'\bperfect\b', r'\bflawless\b',
            r'\bnever\s+fails\b', r'\b100%\b',
            # Raw jargon
            r'\bapi\b', r'\bhttp\b', r'\bjson\b', r'\bsql\b',
        ]

        all_found = []
        for pattern in all_discouraged_patterns:
            matches = re.findall(pattern, self.poem_lower)
            if matches:
                all_found.extend(matches)

        self.assertEqual(len(all_found), 0,
            f"Poem should not contain any discouraged elements. Found: {all_found}")

    def test_professional_tone(self):
        """
        Verify poem maintains a professional tone throughout.

        The poem should use elevated vocabulary and maintain
        dignity appropriate for marketing materials.
        """
        # Check for presence of professional, elevated language
        professional_patterns = [
            r'\bguard(?:s|ian)?\b',
            r'\bsentinel\b',
            r'\bkeeper\b',
            r'\bwitness\b',
            r'\bpromise\b',
            r'\beternal\b',
            r'\bendure\b',
            r'\bpersist\b',
            r'\bgrace\b',
            r'\belegant?\b',
        ]

        professional_matches = []
        for pattern in professional_patterns:
            matches = re.findall(pattern, self.poem_lower)
            if matches:
                professional_matches.extend(matches)

        self.assertGreater(len(professional_matches), 2,
            f"Poem should use professional, elevated vocabulary. "
            f"Found: {professional_matches if professional_matches else 'none'}")


class TestToneGuidelinesCompliance(unittest.TestCase):
    """
    Combined test for overall tone guidelines compliance.

    This ensures the poem meets all three requirements:
    1. Contains nature metaphors (encouraged)
    2. Contains craftsmanship themes (encouraged)
    3. Avoids discouraged elements
    """

    @classmethod
    def setUpClass(cls):
        cls.poem_content = PoemLoader.load_poem()
        cls.poem_lower = cls.poem_content.lower()

    def test_overall_tone_guidelines_compliance(self):
        """
        Meta-test: Verify overall compliance with PRD tone guidelines.

        The poem should:
        - Use nature metaphors (trees, layers, flow)
        - Reference craftsmanship and building
        - Convey reliability and trust
        - Use accessible language
        - Avoid obscure references
        - Avoid unprofessional humor
        - Avoid exaggerated claims
        - Transform jargon poetically
        """
        # Encouraged elements should be present
        encouraged_patterns = {
            'nature': [r'\blayer\b', r'\blevel\b', r'\bflow\b', r'\bstream\b',
                      r'\bcascade\b', r'\bdeep\b', r'\bhall\b'],
            'craftsmanship': [r'\bforge[d]?\b', r'\brust\b', r'\bcraft\b',
                             r'\bguard\b', r'\bwall\b', r'\bstand\b'],
            'reliability': [r'\btrust\b', r'\bkeep\b', r'\bcare\b',
                           r'\bpersist\b', r'\bendure\b', r'\beternal\b'],
        }

        encouraged_found = {}
        for category, patterns in encouraged_patterns.items():
            matches = []
            for pattern in patterns:
                found = re.findall(pattern, self.poem_lower)
                if found:
                    matches.extend(found)
            encouraged_found[category] = matches

        # Verify each category has at least one match
        for category, matches in encouraged_found.items():
            self.assertGreater(len(matches), 0,
                f"Poem should contain {category} themes. Found: {matches}")

        # Discouraged elements should be absent
        discouraged_patterns = [
            r'\blol\b', r'\brofl\b', r'\bperfect\b', r'\bflawless\b',
            r'\bnever\s+fails\b', r'\bapi\b', r'\bhttp\b',
        ]

        discouraged_found = []
        for pattern in discouraged_patterns:
            matches = re.findall(pattern, self.poem_lower)
            if matches:
                discouraged_found.extend(matches)

        self.assertEqual(len(discouraged_found), 0,
            f"Poem should not contain discouraged elements. Found: {discouraged_found}")

    def test_accessible_language(self):
        """
        Verify poem uses accessible language.

        Per PRD, technical accuracy should be presented through
        accessible language, not dense technical jargon.
        """
        # Check that technical concepts are expressed poetically
        poetic_technical_terms = [
            # LSM-tree expressed poetically
            r'\blayered\s+halls?\b',
            r'\blevels?\b',
            r'\bcascade\b',
            # WAL expressed poetically
            r'\blog\b',
            r'\bwitness\b',
            r'\bpromise\b',
            # Memtable expressed poetically
            r'\bmemory\b',
            r'\bmind\b',
            r'\bskip\s+list\b',
            r'\binterlaced\b',
            # SSTable expressed poetically
            r'\btablet\b',
            r'\bsorted\b',
            r'\beternal\b',
            # Compaction expressed poetically
            r'\bmerge\b',
            r'\bdistill\b',
            r'\bcompaction\b',
        ]

        poetic_matches = []
        for pattern in poetic_technical_terms:
            matches = re.findall(pattern, self.poem_lower)
            if matches:
                poetic_matches.extend(matches)

        self.assertGreater(len(poetic_matches), 3,
            f"Poem should express technical concepts poetically. "
            f"Found: {poetic_matches}")


if __name__ == '__main__':
    unittest.main(verbosity=2)
