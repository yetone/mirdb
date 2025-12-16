"""
Test suite for MirDB Homepage Hero Section CTAs.

This module tests the hero section call-to-action buttons as specified in the PRD
Interface Requirements, ensuring proper CTAs, technology badges, product name,
and tagline are present.

Scenario: Hero Section CTAs
- Test Case 1: Hero section displays 'MirDB' prominently
- Test Case 2: Hero displays tagline about persistent key-value store with Memcached compatibility
- Test Case 3: Hero section contains 'Get Started' button or link
- Test Case 4: Hero section contains 'View on GitHub' button or link
- Test Case 5: Hero section displays Rust badge or indicator
- Test Case 6: Hero section displays Memcached Protocol badge or indicator
- Test Case 7: GitHub link opens in new tab, Get Started scrolls to section
"""

import unittest
import os
import re


def load_html_file(filepath):
    """Load and return the contents of an HTML file."""
    with open(filepath, 'r', encoding='utf-8') as f:
        return f.read()


def extract_hero_section(html_content):
    """Extract the hero section content from the HTML."""
    hero_pattern = r'<section[^>]*id=["\']?hero["\']?[^>]*>(.*?)</section>'
    match = re.search(hero_pattern, html_content, re.IGNORECASE | re.DOTALL)
    return match.group(1) if match else None


class TestHeroSectionProductName(unittest.TestCase):
    """Test Case 1: Hero section displays 'MirDB' prominently."""

    @classmethod
    def setUpClass(cls):
        """Load the index.html file for testing."""
        cls.html_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'index.html')
        cls.html_content = load_html_file(cls.html_path)
        cls.hero_content = extract_hero_section(cls.html_content)

    def test_hero_section_exists(self):
        """Test that the hero section exists in the page."""
        self.assertIsNotNone(
            self.hero_content,
            "Hero section must exist with id='hero'"
        )

    def test_mirdb_in_hero_h1(self):
        """Test that 'MirDB' appears in an h1 element within the hero section."""
        h1_pattern = r'<h1[^>]*>([^<]*)</h1>'
        match = re.search(h1_pattern, self.hero_content, re.IGNORECASE)
        self.assertIsNotNone(match, "Hero section must contain an <h1> element")
        h1_text = match.group(1)
        self.assertIn(
            'MirDB',
            h1_text,
            "Hero <h1> must contain 'MirDB' product name"
        )

    def test_mirdb_displayed_prominently(self):
        """Test that MirDB is displayed prominently (in heading or large text)."""
        # Check that MirDB appears in h1 specifically
        h1_pattern = r'<h1[^>]*>.*?MirDB.*?</h1>'
        match = re.search(h1_pattern, self.hero_content, re.IGNORECASE | re.DOTALL)
        self.assertIsNotNone(
            match,
            "MirDB must be displayed prominently in the hero section's h1"
        )


class TestHeroSectionTagline(unittest.TestCase):
    """Test Case 2: Hero displays tagline about persistent key-value store with Memcached compatibility."""

    @classmethod
    def setUpClass(cls):
        """Load the index.html file for testing."""
        cls.html_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'index.html')
        cls.html_content = load_html_file(cls.html_path)
        cls.hero_content = extract_hero_section(cls.html_content)

    def test_tagline_element_exists_in_hero(self):
        """Test that a tagline element exists within the hero section."""
        tagline_pattern = r'<p[^>]*class="tagline"[^>]*>'
        match = re.search(tagline_pattern, self.hero_content, re.IGNORECASE)
        self.assertIsNotNone(
            match,
            "Hero section must contain a tagline element with class='tagline'"
        )

    def test_tagline_mentions_persistent(self):
        """Test that the tagline mentions 'persistent'."""
        tagline_pattern = r'<p[^>]*class="tagline"[^>]*>([^<]+)</p>'
        match = re.search(tagline_pattern, self.hero_content, re.IGNORECASE)
        self.assertIsNotNone(match, "Tagline element must exist")
        tagline_text = match.group(1).lower()
        self.assertIn(
            'persistent',
            tagline_text,
            "Tagline must mention 'persistent' to describe storage type"
        )

    def test_tagline_mentions_key_value_store(self):
        """Test that the tagline mentions 'key-value store'."""
        tagline_pattern = r'<p[^>]*class="tagline"[^>]*>([^<]+)</p>'
        match = re.search(tagline_pattern, self.hero_content, re.IGNORECASE)
        self.assertIsNotNone(match, "Tagline element must exist")
        tagline_text = match.group(1).lower()
        self.assertIn(
            'key-value',
            tagline_text,
            "Tagline must mention 'key-value' store"
        )

    def test_tagline_mentions_memcached(self):
        """Test that the tagline mentions 'Memcached' compatibility."""
        tagline_pattern = r'<p[^>]*class="tagline"[^>]*>([^<]+)</p>'
        match = re.search(tagline_pattern, self.hero_content, re.IGNORECASE)
        self.assertIsNotNone(match, "Tagline element must exist")
        tagline_text = match.group(1).lower()
        self.assertIn(
            'memcached',
            tagline_text,
            "Tagline must mention Memcached protocol compatibility"
        )


class TestGetStartedCTA(unittest.TestCase):
    """Test Case 3: Hero section contains 'Get Started' button or link."""

    @classmethod
    def setUpClass(cls):
        """Load the index.html file for testing."""
        cls.html_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'index.html')
        cls.html_content = load_html_file(cls.html_path)
        cls.hero_content = extract_hero_section(cls.html_content)

    def test_get_started_button_exists(self):
        """Test that a 'Get Started' button or link exists in the hero section."""
        get_started_pattern = r'<a[^>]*>([^<]*Get Started[^<]*)</a>'
        match = re.search(get_started_pattern, self.hero_content, re.IGNORECASE)
        self.assertIsNotNone(
            match,
            "Hero section must contain a 'Get Started' button or link"
        )

    def test_get_started_is_styled_as_button(self):
        """Test that the Get Started link is styled as a button (has btn class)."""
        get_started_pattern = r'<a[^>]*class="[^"]*btn[^"]*"[^>]*>[^<]*Get Started[^<]*</a>'
        match = re.search(get_started_pattern, self.hero_content, re.IGNORECASE)
        self.assertIsNotNone(
            match,
            "Get Started CTA must be styled as a button with 'btn' class"
        )

    def test_get_started_links_to_getting_started_section(self):
        """Test that Get Started links to the getting-started section."""
        get_started_pattern = r'<a[^>]*href="([^"]*)"[^>]*>[^<]*Get Started[^<]*</a>'
        match = re.search(get_started_pattern, self.hero_content, re.IGNORECASE)
        self.assertIsNotNone(match, "Get Started link must have an href")
        href = match.group(1)
        self.assertIn(
            'getting-started',
            href.lower(),
            "Get Started must link to the getting-started section"
        )


class TestViewOnGitHubCTA(unittest.TestCase):
    """Test Case 4: Hero section contains 'View on GitHub' button or link."""

    @classmethod
    def setUpClass(cls):
        """Load the index.html file for testing."""
        cls.html_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'index.html')
        cls.html_content = load_html_file(cls.html_path)
        cls.hero_content = extract_hero_section(cls.html_content)

    def test_view_on_github_button_exists(self):
        """Test that a 'View on GitHub' button or link exists in the hero section."""
        github_pattern = r'<a[^>]*>[^<]*(?:View on GitHub|GitHub)[^<]*</a>'
        match = re.search(github_pattern, self.hero_content, re.IGNORECASE)
        self.assertIsNotNone(
            match,
            "Hero section must contain a 'View on GitHub' button or link"
        )

    def test_view_on_github_is_styled_as_button(self):
        """Test that the View on GitHub link is styled as a button."""
        github_pattern = r'<a[^>]*class="[^"]*btn[^"]*"[^>]*>[^<]*(?:View on GitHub|GitHub)[^<]*</a>'
        match = re.search(github_pattern, self.hero_content, re.IGNORECASE)
        self.assertIsNotNone(
            match,
            "View on GitHub CTA must be styled as a button with 'btn' class"
        )

    def test_github_link_points_to_github(self):
        """Test that the GitHub link points to github.com."""
        github_pattern = r'<a[^>]*href="([^"]*github[^"]*)"[^>]*>'
        match = re.search(github_pattern, self.hero_content, re.IGNORECASE)
        self.assertIsNotNone(match, "GitHub link must exist")
        href = match.group(1)
        self.assertIn(
            'github.com',
            href.lower(),
            "GitHub button must link to github.com"
        )


class TestRustBadge(unittest.TestCase):
    """Test Case 5: Hero section displays Rust badge or indicator."""

    @classmethod
    def setUpClass(cls):
        """Load the index.html file for testing."""
        cls.html_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'index.html')
        cls.html_content = load_html_file(cls.html_path)
        cls.hero_content = extract_hero_section(cls.html_content)

    def test_rust_badge_exists_in_hero(self):
        """Test that a Rust badge or indicator exists in the hero section."""
        # Check for badge element with 'Rust' text
        rust_badge_pattern = r'<span[^>]*class="[^"]*badge[^"]*"[^>]*>[^<]*Rust[^<]*</span>'
        match = re.search(rust_badge_pattern, self.hero_content, re.IGNORECASE)
        self.assertIsNotNone(
            match,
            "Hero section must display a Rust badge with class='badge'"
        )

    def test_badges_container_exists(self):
        """Test that a badges container exists in the hero section."""
        badges_container_pattern = r'<div[^>]*class="[^"]*badges[^"]*"[^>]*>'
        match = re.search(badges_container_pattern, self.hero_content, re.IGNORECASE)
        self.assertIsNotNone(
            match,
            "Hero section must have a badges container with class='badges'"
        )

    def test_rust_mentioned_in_hero(self):
        """Test that Rust is mentioned somewhere in the hero section."""
        self.assertIn(
            'rust',
            self.hero_content.lower(),
            "Hero section must mention Rust technology"
        )


class TestMemcachedProtocolBadge(unittest.TestCase):
    """Test Case 6: Hero section displays Memcached Protocol badge or indicator."""

    @classmethod
    def setUpClass(cls):
        """Load the index.html file for testing."""
        cls.html_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'index.html')
        cls.html_content = load_html_file(cls.html_path)
        cls.hero_content = extract_hero_section(cls.html_content)

    def test_memcached_protocol_badge_exists(self):
        """Test that a Memcached Protocol badge exists in the hero section."""
        # Check for badge element with 'Memcached' text
        memcached_badge_pattern = r'<span[^>]*class="[^"]*badge[^"]*"[^>]*>[^<]*Memcached[^<]*</span>'
        match = re.search(memcached_badge_pattern, self.hero_content, re.IGNORECASE)
        self.assertIsNotNone(
            match,
            "Hero section must display a Memcached Protocol badge"
        )

    def test_memcached_badge_contains_protocol(self):
        """Test that the Memcached badge mentions 'Protocol'."""
        memcached_badge_pattern = r'<span[^>]*class="[^"]*badge[^"]*"[^>]*>([^<]*Memcached[^<]*)</span>'
        match = re.search(memcached_badge_pattern, self.hero_content, re.IGNORECASE)
        self.assertIsNotNone(match, "Memcached badge must exist")
        badge_text = match.group(1)
        self.assertIn(
            'Protocol',
            badge_text,
            "Memcached badge should indicate 'Memcached Protocol'"
        )


class TestCTALinkBehavior(unittest.TestCase):
    """Test Case 7: GitHub link opens in new tab, Get Started scrolls to section."""

    @classmethod
    def setUpClass(cls):
        """Load the index.html file for testing."""
        cls.html_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'index.html')
        cls.html_content = load_html_file(cls.html_path)
        cls.hero_content = extract_hero_section(cls.html_content)

    def test_github_link_opens_in_new_tab(self):
        """Test that the GitHub link has target='_blank' to open in new tab."""
        github_link_pattern = r'<a[^>]*href="[^"]*github[^"]*"[^>]*target="([^"]*)"[^>]*>'
        match = re.search(github_link_pattern, self.hero_content, re.IGNORECASE)
        self.assertIsNotNone(
            match,
            "GitHub link must have a target attribute"
        )
        target = match.group(1)
        self.assertEqual(
            target, '_blank',
            "GitHub link must open in new tab (target='_blank')"
        )

    def test_github_link_has_noopener_noreferrer(self):
        """Test that the GitHub link has rel='noopener noreferrer' for security."""
        github_link_pattern = r'<a[^>]*href="[^"]*github[^"]*"[^>]*rel="([^"]*)"[^>]*>'
        match = re.search(github_link_pattern, self.hero_content, re.IGNORECASE)
        self.assertIsNotNone(
            match,
            "GitHub link must have a rel attribute"
        )
        rel = match.group(1).lower()
        self.assertIn('noopener', rel, "GitHub link must have 'noopener' for security")
        self.assertIn('noreferrer', rel, "GitHub link must have 'noreferrer' for security")

    def test_get_started_uses_anchor_link(self):
        """Test that Get Started uses an anchor link (#) for in-page scrolling."""
        get_started_pattern = r'<a[^>]*href="([^"]*)"[^>]*>[^<]*Get Started[^<]*</a>'
        match = re.search(get_started_pattern, self.hero_content, re.IGNORECASE)
        self.assertIsNotNone(match, "Get Started link must exist")
        href = match.group(1)
        self.assertTrue(
            href.startswith('#'),
            "Get Started must use anchor link (#) for in-page scrolling"
        )

    def test_get_started_does_not_open_new_tab(self):
        """Test that Get Started does not have target='_blank' (should scroll on same page)."""
        get_started_pattern = r'<a[^>]*href="#[^"]*"[^>]*>[^<]*Get Started[^<]*</a>'
        match = re.search(get_started_pattern, self.hero_content, re.IGNORECASE)
        self.assertIsNotNone(match, "Get Started link must exist")
        full_match = match.group(0)
        # Get Started should NOT have target="_blank"
        self.assertNotIn(
            'target="_blank"',
            full_match,
            "Get Started should not open in new tab - it should scroll to section"
        )


if __name__ == '__main__':
    unittest.main(verbosity=2)
