"""
Test suite for MirDB Homepage Structure and Layout.

This module tests the HTML structure and content requirements for the MirDB homepage,
ensuring proper HTML5 structure, semantic elements, and content presentation.
"""

import unittest
import os
import re
from html.parser import HTMLParser


class HTMLStructureParser(HTMLParser):
    """Custom HTML parser to extract structural information from HTML documents."""

    def __init__(self):
        super().__init__()
        self.doctype = None
        self.elements = []
        self.current_text = []
        self.in_element = []
        self.element_contents = {}
        self.element_attributes = {}

    def handle_decl(self, decl):
        if decl.lower().startswith('doctype'):
            self.doctype = decl

    def handle_starttag(self, tag, attrs):
        self.elements.append(tag)
        self.in_element.append(tag)
        attrs_dict = dict(attrs)
        if tag not in self.element_attributes:
            self.element_attributes[tag] = []
        self.element_attributes[tag].append(attrs_dict)

    def handle_endtag(self, tag):
        if self.in_element and self.in_element[-1] == tag:
            text = ''.join(self.current_text).strip()
            if tag not in self.element_contents:
                self.element_contents[tag] = []
            self.element_contents[tag].append(text)
            self.in_element.pop()
            self.current_text = []

    def handle_data(self, data):
        self.current_text.append(data)


def load_html_file(filepath):
    """Load and return the contents of an HTML file."""
    with open(filepath, 'r', encoding='utf-8') as f:
        return f.read()


def parse_html(html_content):
    """Parse HTML content and return the parser with extracted data."""
    parser = HTMLStructureParser()
    parser.feed(html_content)
    return parser


class TestDoctypeAndHTML5Structure(unittest.TestCase):
    """Test Case 1: DOCTYPE declaration and valid HTML5 structure."""

    @classmethod
    def setUpClass(cls):
        """Load the index.html file for testing."""
        cls.html_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'index.html')
        cls.html_content = load_html_file(cls.html_path)
        cls.parser = parse_html(cls.html_content)

    def test_doctype_declaration_exists(self):
        """Test that the page contains <!DOCTYPE html> declaration."""
        self.assertTrue(
            self.html_content.strip().lower().startswith('<!doctype html>'),
            "Page must start with <!DOCTYPE html> declaration"
        )

    def test_doctype_is_html5(self):
        """Test that the DOCTYPE is HTML5 (not XHTML or HTML4)."""
        doctype_match = re.match(r'<!DOCTYPE\s+html\s*>', self.html_content.strip(), re.IGNORECASE)
        self.assertIsNotNone(doctype_match, "DOCTYPE must be HTML5 format: <!DOCTYPE html>")

    def test_html_element_has_lang_attribute(self):
        """Test that the <html> element has a lang attribute."""
        html_attrs = self.parser.element_attributes.get('html', [])
        self.assertTrue(
            len(html_attrs) > 0 and 'lang' in html_attrs[0],
            "HTML element must have a lang attribute for accessibility"
        )

    def test_head_element_exists(self):
        """Test that a <head> element exists."""
        self.assertIn('head', self.parser.elements, "Page must have a <head> element")

    def test_meta_charset_exists(self):
        """Test that meta charset is defined."""
        meta_tags = self.parser.element_attributes.get('meta', [])
        has_charset = any('charset' in m for m in meta_tags)
        self.assertTrue(has_charset, "Page must have a meta charset declaration")

    def test_meta_viewport_exists(self):
        """Test that meta viewport is defined for responsive design."""
        meta_tags = self.parser.element_attributes.get('meta', [])
        has_viewport = any(m.get('name') == 'viewport' for m in meta_tags)
        self.assertTrue(has_viewport, "Page must have a meta viewport for responsive design")

    def test_title_element_exists(self):
        """Test that a <title> element exists."""
        self.assertIn('title', self.parser.elements, "Page must have a <title> element")


class TestSemanticElements(unittest.TestCase):
    """Test Case 2: Semantic HTML elements."""

    @classmethod
    def setUpClass(cls):
        """Load the index.html file for testing."""
        cls.html_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'index.html')
        cls.html_content = load_html_file(cls.html_path)
        cls.parser = parse_html(cls.html_content)

    def test_header_element_exists(self):
        """Test that a <header> element exists."""
        self.assertIn('header', self.parser.elements, "Page must have a <header> element")

    def test_main_element_exists(self):
        """Test that a <main> element exists."""
        self.assertIn('main', self.parser.elements, "Page must have a <main> element")

    def test_footer_element_exists(self):
        """Test that a <footer> element exists."""
        self.assertIn('footer', self.parser.elements, "Page must have a <footer> element")

    def test_section_elements_exist(self):
        """Test that <section> elements exist for content organization."""
        section_count = self.parser.elements.count('section')
        self.assertGreater(
            section_count, 0,
            "Page must have at least one <section> element"
        )

    def test_nav_element_exists(self):
        """Test that a <nav> element exists for navigation."""
        self.assertIn('nav', self.parser.elements, "Page must have a <nav> element")

    def test_heading_hierarchy(self):
        """Test that the page has proper heading hierarchy starting with h1."""
        self.assertIn('h1', self.parser.elements, "Page must have an <h1> element")
        self.assertIn('h2', self.parser.elements, "Page must have <h2> elements")


class TestHeroSection(unittest.TestCase):
    """Test Case 3: Hero section with product name."""

    @classmethod
    def setUpClass(cls):
        """Load the index.html file for testing."""
        cls.html_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'index.html')
        cls.html_content = load_html_file(cls.html_path)
        cls.parser = parse_html(cls.html_content)

    def test_hero_section_exists(self):
        """Test that a hero section exists."""
        self.assertIn('hero', self.html_content.lower(), "Page must have a hero section")

    def test_product_name_mirdb_displayed(self):
        """Test that 'MirDB' product name is displayed prominently."""
        h1_contents = self.parser.element_contents.get('h1', [])
        mirdb_in_h1 = any('mirdb' in content.lower() for content in h1_contents)
        self.assertTrue(mirdb_in_h1, "MirDB product name must be displayed in an <h1> element")

    def test_tagline_present(self):
        """Test that a tagline is present describing the product."""
        # Check for key phrases that should be in the tagline
        tagline_keywords = ['persistent', 'key-value', 'memcached']
        content_lower = self.html_content.lower()
        keywords_found = sum(1 for kw in tagline_keywords if kw in content_lower)
        self.assertGreaterEqual(
            keywords_found, 2,
            "Page must have a tagline with keywords about the product"
        )

    def test_cta_buttons_present(self):
        """Test that call-to-action buttons are present."""
        has_get_started = 'get started' in self.html_content.lower()
        has_github = 'github' in self.html_content.lower()
        self.assertTrue(
            has_get_started or has_github,
            "Page must have call-to-action buttons"
        )


class TestFeaturesSection(unittest.TestCase):
    """Test Case 4: Features section with key MirDB features."""

    @classmethod
    def setUpClass(cls):
        """Load the index.html file for testing."""
        cls.html_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'index.html')
        cls.html_content = load_html_file(cls.html_path)
        cls.parser = parse_html(cls.html_content)

    def test_features_section_exists(self):
        """Test that a features section exists."""
        self.assertIn('features', self.html_content.lower(), "Page must have a features section")

    def test_memcached_protocol_feature(self):
        """Test that Memcached protocol compatibility is mentioned."""
        self.assertIn(
            'memcached',
            self.html_content.lower(),
            "Features must mention Memcached protocol compatibility"
        )

    def test_skip_list_feature(self):
        """Test that Skip list is mentioned as a feature."""
        self.assertIn(
            'skip list',
            self.html_content.lower(),
            "Features must mention Skip list implementation"
        )

    def test_lsm_tree_feature(self):
        """Test that LSM-tree is mentioned as a feature."""
        self.assertIn(
            'lsm',
            self.html_content.lower(),
            "Features must mention LSM-tree storage"
        )

    def test_write_ahead_logging_feature(self):
        """Test that Write-ahead logging is mentioned."""
        wal_present = 'write-ahead' in self.html_content.lower() or 'wal' in self.html_content.lower()
        self.assertTrue(
            wal_present,
            "Features must mention Write-ahead logging for durability"
        )

    def test_compression_feature(self):
        """Test that compression (Snappy) is mentioned."""
        compression_present = 'snappy' in self.html_content.lower() or 'compression' in self.html_content.lower()
        self.assertTrue(
            compression_present,
            "Features must mention compression capability"
        )

    def test_cuckoo_filter_feature(self):
        """Test that Cuckoo filter or bloom filter functionality is mentioned."""
        filter_present = 'cuckoo' in self.html_content.lower() or 'filter' in self.html_content.lower()
        self.assertTrue(
            filter_present,
            "Features must mention Cuckoo filter for fast lookups"
        )

    def test_features_have_descriptions(self):
        """Test that features have descriptive content."""
        feature_section_match = re.search(
            r'id=["\']features["\'].*?(?=<section|</main)',
            self.html_content,
            re.IGNORECASE | re.DOTALL
        )
        self.assertIsNotNone(feature_section_match, "Features section must exist with id='features'")
        features_content = feature_section_match.group()
        # Count feature cards/items - should have multiple
        h3_count = features_content.lower().count('<h3')
        self.assertGreaterEqual(
            h3_count, 4,
            "Features section should have at least 4 feature items"
        )


if __name__ == '__main__':
    unittest.main(verbosity=2)
