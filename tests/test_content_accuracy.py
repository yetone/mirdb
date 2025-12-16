"""
Test suite for MirDB Homepage Content Accuracy.

This module tests the content accuracy requirements for the MirDB homepage,
ensuring all product descriptions, technical details, and marketing content
accurately represent MirDB and contain no placeholder text.
"""

import unittest
import os
import re


def load_file(filepath):
    """Load and return the contents of a file."""
    with open(filepath, 'r', encoding='utf-8') as f:
        return f.read()


class TestPersistentKeyValueStoreDescription(unittest.TestCase):
    """Test Case 1: Verify homepage accurately describes MirDB as a persistent key-value store."""

    @classmethod
    def setUpClass(cls):
        """Load the index.html file for testing."""
        cls.html_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'index.html')
        cls.html_content = load_file(cls.html_path)

    def test_persistent_mentioned(self):
        """Test that 'persistent' is mentioned in the homepage content."""
        self.assertIn(
            'persistent',
            self.html_content.lower(),
            "Homepage must mention 'persistent' to describe the storage capabilities"
        )

    def test_key_value_store_mentioned(self):
        """Test that 'key-value store' is mentioned in the homepage content."""
        self.assertIn(
            'key-value store',
            self.html_content.lower(),
            "Homepage must mention 'key-value store' to describe the database type"
        )

    def test_memcached_protocol_mentioned(self):
        """Test that Memcached protocol compatibility is mentioned."""
        self.assertIn(
            'memcached',
            self.html_content.lower(),
            "Homepage must mention Memcached protocol compatibility"
        )

    def test_product_description_in_tagline(self):
        """Test that the tagline accurately describes MirDB."""
        # Check for the tagline with key descriptors
        tagline_pattern = r'<p[^>]*class="tagline"[^>]*>([^<]+)</p>'
        match = re.search(tagline_pattern, self.html_content, re.IGNORECASE)
        self.assertIsNotNone(match, "Tagline must exist with class='tagline'")
        tagline_text = match.group(1).lower()
        self.assertIn('persistent', tagline_text, "Tagline must mention 'persistent'")
        self.assertIn('key-value', tagline_text, "Tagline must mention 'key-value'")


class TestRustLanguageMention(unittest.TestCase):
    """Test Case 2: Verify homepage mentions that MirDB is written in Rust."""

    @classmethod
    def setUpClass(cls):
        """Load the index.html file for testing."""
        cls.html_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'index.html')
        cls.html_content = load_file(cls.html_path)

    def test_rust_mentioned_in_content(self):
        """Test that Rust is mentioned in the homepage content."""
        self.assertIn(
            'rust',
            self.html_content.lower(),
            "Homepage must mention that MirDB is written in Rust"
        )

    def test_rust_in_subtitle_or_badge(self):
        """Test that Rust is prominently displayed (subtitle or badge)."""
        # Check for Rust in subtitle
        subtitle_pattern = r'<p[^>]*class="subtitle"[^>]*>([^<]+)</p>'
        subtitle_match = re.search(subtitle_pattern, self.html_content, re.IGNORECASE)

        # Check for Rust badge
        badge_pattern = r'<span[^>]*class="badge"[^>]*>([^<]*rust[^<]*)</span>'
        badge_match = re.search(badge_pattern, self.html_content, re.IGNORECASE)

        rust_found = False
        if subtitle_match and 'rust' in subtitle_match.group(1).lower():
            rust_found = True
        if badge_match:
            rust_found = True

        self.assertTrue(
            rust_found,
            "Rust must be prominently displayed in subtitle or as a badge"
        )

    def test_tokio_mentioned(self):
        """Test that Tokio (Rust async runtime) is mentioned."""
        self.assertIn(
            'tokio',
            self.html_content.lower(),
            "Homepage should mention Tokio as part of the technology stack"
        )


class TestNoPlaceholderText(unittest.TestCase):
    """Test Case 3: Verify no placeholder text remains on the page."""

    @classmethod
    def setUpClass(cls):
        """Load the index.html file for testing."""
        cls.html_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'index.html')
        cls.html_content = load_file(cls.html_path)

    def test_no_lorem_ipsum(self):
        """Test that no Lorem Ipsum placeholder text exists."""
        self.assertNotIn(
            'lorem',
            self.html_content.lower(),
            "Homepage must not contain Lorem Ipsum placeholder text"
        )

    def test_no_todo_comments(self):
        """Test that no TODO comments exist in the HTML."""
        # Check for various TODO patterns
        todo_patterns = [
            r'\bTODO\b',
            r'\bTODO:',
            r'<!--\s*TODO',
        ]
        for pattern in todo_patterns:
            match = re.search(pattern, self.html_content, re.IGNORECASE)
            self.assertIsNone(
                match,
                f"Homepage must not contain TODO placeholder: {pattern}"
            )

    def test_no_tbd_text(self):
        """Test that no TBD (To Be Determined) placeholder text exists."""
        tbd_pattern = r'\bTBD\b'
        match = re.search(tbd_pattern, self.html_content, re.IGNORECASE)
        self.assertIsNone(
            match,
            "Homepage must not contain TBD placeholder text"
        )

    def test_no_fixme_comments(self):
        """Test that no FIXME comments exist."""
        fixme_pattern = r'\bFIXME\b'
        match = re.search(fixme_pattern, self.html_content, re.IGNORECASE)
        self.assertIsNone(
            match,
            "Homepage must not contain FIXME placeholder comments"
        )

    def test_no_placeholder_text(self):
        """Test that no generic placeholder text exists."""
        placeholder_patterns = [
            r'placeholder',
            r'\[insert',
            r'xxx+',
        ]
        for pattern in placeholder_patterns:
            match = re.search(pattern, self.html_content, re.IGNORECASE)
            self.assertIsNone(
                match,
                f"Homepage must not contain placeholder text: {pattern}"
            )

    def test_all_content_is_meaningful(self):
        """Test that all text content appears meaningful and complete."""
        # Check that key sections have substantial content
        sections_to_check = ['features', 'getting-started', 'architecture']
        for section_id in sections_to_check:
            section_pattern = rf'id=["\']?{section_id}["\']?[^>]*>(.*?)(?=<section|</main)'
            match = re.search(section_pattern, self.html_content, re.IGNORECASE | re.DOTALL)
            self.assertIsNotNone(
                match,
                f"Section '{section_id}' must exist"
            )
            section_content = match.group(1)
            # Each section should have meaningful content (at least 100 characters)
            text_content = re.sub(r'<[^>]+>', '', section_content)
            self.assertGreater(
                len(text_content.strip()), 100,
                f"Section '{section_id}' must have substantial content"
            )


class TestCircleCIBadge(unittest.TestCase):
    """Test Case 4: Verify CircleCI badge links to correct build status page."""

    @classmethod
    def setUpClass(cls):
        """Load the index.html file for testing."""
        cls.html_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'index.html')
        cls.html_content = load_file(cls.html_path)

    def test_circleci_badge_exists(self):
        """Test that CircleCI badge image exists on the page."""
        circleci_img_pattern = r'<img[^>]*circleci[^>]*>'
        match = re.search(circleci_img_pattern, self.html_content, re.IGNORECASE)
        self.assertIsNotNone(
            match,
            "Homepage must have a CircleCI badge image"
        )

    def test_circleci_badge_has_alt_text(self):
        """Test that CircleCI badge has appropriate alt text."""
        # Match img tag with circleci in src or class, and extract alt text
        circleci_img_pattern = r'<img[^>]*circleci[^>]*alt="([^"]+)"[^>]*>'
        match = re.search(circleci_img_pattern, self.html_content, re.IGNORECASE)
        if not match:
            # Try alternate pattern where alt comes before circleci
            circleci_img_pattern = r'<img[^>]*alt="([^"]+)"[^>]*circleci[^>]*>'
            match = re.search(circleci_img_pattern, self.html_content, re.IGNORECASE)
        self.assertIsNotNone(
            match,
            "CircleCI badge must have alt text"
        )
        alt_text = match.group(1).lower()
        self.assertTrue(
            'build' in alt_text or 'status' in alt_text or 'circleci' in alt_text,
            "CircleCI badge alt text must describe build status"
        )

    def test_circleci_badge_links_to_build_page(self):
        """Test that CircleCI badge links to the correct build status page."""
        # Look for anchor tag containing CircleCI link with badge image
        circleci_link_pattern = r'<a[^>]*href="([^"]*circleci[^"]*)"[^>]*>'
        match = re.search(circleci_link_pattern, self.html_content, re.IGNORECASE)
        self.assertIsNotNone(
            match,
            "CircleCI badge must be wrapped in a link"
        )
        href = match.group(1)
        self.assertIn(
            'circleci.com',
            href.lower(),
            "CircleCI badge must link to circleci.com"
        )
        self.assertIn(
            'mirdb',
            href.lower(),
            "CircleCI badge must link to the mirdb project"
        )

    def test_circleci_badge_uses_correct_svg_url(self):
        """Test that CircleCI badge uses the correct SVG URL format."""
        svg_pattern = r'src="([^"]*circleci[^"]*\.svg[^"]*)"'
        match = re.search(svg_pattern, self.html_content, re.IGNORECASE)
        self.assertIsNotNone(
            match,
            "CircleCI badge must use SVG image"
        )
        svg_url = match.group(1)
        self.assertIn(
            'circleci.com',
            svg_url.lower(),
            "CircleCI SVG must be from circleci.com"
        )


class TestTaglineDisplay(unittest.TestCase):
    """Test Case 5: Verify tagline about Memcached protocol compatibility is visible in hero section."""

    @classmethod
    def setUpClass(cls):
        """Load the index.html file for testing."""
        cls.html_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'index.html')
        cls.html_content = load_file(cls.html_path)

    def test_hero_section_exists(self):
        """Test that hero section exists."""
        hero_pattern = r'<section[^>]*id=["\']?hero["\']?[^>]*class=["\'][^"\']*hero["\']?[^>]*>'
        match = re.search(hero_pattern, self.html_content, re.IGNORECASE)
        self.assertIsNotNone(
            match,
            "Hero section must exist with id='hero' and class='hero'"
        )

    def test_tagline_in_hero_section(self):
        """Test that tagline is located within the hero section."""
        # Extract hero section content
        hero_pattern = r'<section[^>]*id=["\']?hero["\']?[^>]*>(.*?)</section>'
        hero_match = re.search(hero_pattern, self.html_content, re.IGNORECASE | re.DOTALL)
        self.assertIsNotNone(hero_match, "Hero section must exist")

        hero_content = hero_match.group(1)

        # Check for tagline within hero
        tagline_pattern = r'<p[^>]*class="tagline"[^>]*>'
        tagline_match = re.search(tagline_pattern, hero_content, re.IGNORECASE)
        self.assertIsNotNone(
            tagline_match,
            "Tagline must be within the hero section"
        )

    def test_tagline_mentions_memcached(self):
        """Test that tagline mentions Memcached protocol."""
        tagline_pattern = r'<p[^>]*class="tagline"[^>]*>([^<]+)</p>'
        match = re.search(tagline_pattern, self.html_content, re.IGNORECASE)
        self.assertIsNotNone(match, "Tagline element must exist")

        tagline_text = match.group(1).lower()
        self.assertIn(
            'memcached',
            tagline_text,
            "Tagline must mention Memcached protocol compatibility"
        )

    def test_tagline_has_prominent_styling(self):
        """Test that tagline has styling for prominence (via CSS class)."""
        tagline_pattern = r'<p[^>]*class="tagline"[^>]*>'
        match = re.search(tagline_pattern, self.html_content, re.IGNORECASE)
        self.assertIsNotNone(
            match,
            "Tagline must have class='tagline' for prominent styling"
        )

    def test_tagline_matches_prd_requirement(self):
        """Test that tagline matches PRD REQ-1 requirements."""
        # PRD specifies: "A persistent key-value store with Memcached protocol compatibility"
        tagline_pattern = r'<p[^>]*class="tagline"[^>]*>([^<]+)</p>'
        match = re.search(tagline_pattern, self.html_content, re.IGNORECASE)
        self.assertIsNotNone(match, "Tagline element must exist")

        tagline_text = match.group(1).lower()
        # Check for key components from PRD
        self.assertIn('persistent', tagline_text, "Tagline must mention 'persistent'")
        self.assertIn('key-value', tagline_text, "Tagline must mention 'key-value'")
        self.assertIn('memcached', tagline_text, "Tagline must mention 'memcached'")


if __name__ == '__main__':
    unittest.main(verbosity=2)
