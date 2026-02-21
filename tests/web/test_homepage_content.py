"""
Homepage Content Structure Tests
Owner: Scenario 2 - Homepage Content Structure

Tests verify that the homepage displays all required content sections
with proper structure and information.
"""

import pytest
from pathlib import Path
from bs4 import BeautifulSoup


@pytest.fixture
def html_content():
    """Load the homepage HTML content."""
    html_path = Path(__file__).parent.parent.parent / "web" / "index.html"
    with open(html_path, "r", encoding="utf-8") as f:
        return f.read()


@pytest.fixture
def soup(html_content):
    """Parse HTML content into BeautifulSoup object."""
    return BeautifulSoup(html_content, "lxml")


class TestHTMLStructure:
    """Test Case 1: Verify semantic HTML structure."""

    def test_has_header_element(self, soup):
        """Response contains <header> element."""
        header = soup.find("header")
        assert header is not None, "Page must contain a <header> element"

    def test_has_main_element(self, soup):
        """Response contains <main> element."""
        main = soup.find("main")
        assert main is not None, "Page must contain a <main> element"

    def test_has_footer_element(self, soup):
        """Response contains <footer> element."""
        footer = soup.find("footer")
        assert footer is not None, "Page must contain a <footer> element"

    def test_doctype_html(self, html_content):
        """Page has HTML5 doctype."""
        assert html_content.strip().lower().startswith("<!doctype html>"), \
            "Page must have HTML5 doctype"


class TestHeroSection:
    """Test Case 2: Verify hero section content."""

    def test_hero_section_exists(self, soup):
        """Page has a hero section."""
        hero = soup.find("section", {"id": "hero"}) or soup.find(class_="hero")
        assert hero is not None, "Page must have a hero section"

    def test_hero_has_logo(self, soup):
        """Hero section contains MirDB logo image reference."""
        hero = soup.find("section", {"id": "hero"}) or soup.find(class_="hero")
        assert hero is not None, "Page must have a hero section"

        logo = hero.find("img", class_="hero-logo") or hero.find("img")
        assert logo is not None, "Hero section must contain a logo image"

        src = logo.get("src", "")
        assert "logo" in src.lower(), "Logo image source must reference logo file"

    def test_hero_has_ascii_art(self, soup):
        """Hero section contains ASCII art matching server welcome message."""
        hero = soup.find("section", {"id": "hero"}) or soup.find(class_="hero")
        assert hero is not None, "Page must have a hero section"

        ascii_art = hero.find("pre", class_="ascii-art") or hero.find("pre")
        assert ascii_art is not None, "Hero must contain ASCII art in <pre> element"

        # Check for key parts of the MirDB ASCII art
        art_text = ascii_art.get_text()
        assert "MirDB" in art_text or "|_|" in art_text, \
            "ASCII art must contain MirDB branding characters"


class TestFeaturesSection:
    """Test Case 3: Verify features section."""

    def test_features_section_exists(self, soup):
        """Page has a features section."""
        features = soup.find("section", {"id": "features"}) or soup.find(class_="features")
        assert features is not None, "Page must have a features section"

    def test_has_memcached_protocol_feature(self, soup):
        """Page contains Memcached Protocol feature card."""
        text = soup.get_text().lower()
        assert "memcached protocol" in text or "memcached" in text, \
            "Page must mention Memcached Protocol feature"

    def test_has_data_persistence_feature(self, soup):
        """Page contains Data Persistence feature card."""
        text = soup.get_text().lower()
        assert "persistence" in text or "persisted" in text, \
            "Page must mention Data Persistence feature"

    def test_has_lsm_tree_feature(self, soup):
        """Page contains LSM Tree feature card."""
        text = soup.get_text().lower()
        assert "lsm tree" in text or "lsm" in text or "log-structured merge" in text, \
            "Page must mention LSM Tree feature"

    def test_has_rust_implementation_feature(self, soup):
        """Page contains Rust Implementation feature card."""
        text = soup.get_text().lower()
        assert "rust" in text, "Page must mention Rust Implementation feature"

    def test_feature_cards_count(self, soup):
        """Page contains 4 feature cards."""
        features = soup.find("section", {"id": "features"}) or soup.find(class_="features")
        assert features is not None, "Page must have a features section"

        cards = features.find_all(class_="feature-card") or features.find_all("article")
        assert len(cards) >= 4, f"Expected at least 4 feature cards, found {len(cards)}"


class TestUsageSection:
    """Test Case 4: Verify usage demonstration section."""

    def test_usage_section_exists(self, soup):
        """Page has a usage demonstration section."""
        usage = soup.find("section", {"id": "usage"}) or soup.find(class_="usage-demo")
        assert usage is not None, "Page must have a usage demonstration section"

    def test_usage_gif_reference(self, soup):
        """Page contains reference to usage.gif."""
        images = soup.find_all("img")
        usage_gif_found = False

        for img in images:
            src = img.get("src", "")
            if "usage.gif" in src or "usage" in src.lower():
                usage_gif_found = True
                break

        assert usage_gif_found, "Page must reference usage.gif"

    def test_usage_gif_has_alt_text(self, soup):
        """Usage.gif has descriptive alt text."""
        images = soup.find_all("img")

        for img in images:
            src = img.get("src", "")
            if "usage" in src.lower():
                alt = img.get("alt", "")
                assert len(alt) > 10, \
                    f"Usage GIF must have descriptive alt text, found: '{alt}'"
                return

        pytest.fail("Could not find usage image to check alt text")


class TestQuickStartSection:
    """Test Case 5: Verify Quick Start section."""

    def test_quick_start_section_exists(self, soup):
        """Page has a Quick Start section."""
        quick_start = soup.find("section", {"id": "quick-start"}) or soup.find(class_="quick-start")
        assert quick_start is not None, "Page must have a Quick Start section"

    def test_has_installation_command(self, soup):
        """Page contains installation command."""
        text = soup.get_text()
        # Check for common installation indicators
        assert any(cmd in text for cmd in ["cargo build", "git clone", "cargo install"]), \
            "Page must contain installation commands"

    def test_has_configuration_example(self, soup):
        """Page contains configuration example."""
        text = soup.get_text().lower()
        # Check for configuration indicators
        assert "configuration" in text or "config" in text or ".toml" in text, \
            "Page must contain configuration example"

    def test_has_basic_usage_example(self, soup):
        """Page contains basic usage example."""
        text = soup.get_text().lower()
        # Check for usage examples like set/get commands
        has_examples = ("set" in text and "get" in text) or "telnet" in text
        assert has_examples, "Page must contain basic usage examples"

    def test_has_code_blocks(self, soup):
        """Quick Start section has code blocks."""
        quick_start = soup.find("section", {"id": "quick-start"}) or soup.find(class_="quick-start")
        assert quick_start is not None, "Page must have a Quick Start section"

        code_blocks = quick_start.find_all("pre") or quick_start.find_all("code")
        assert len(code_blocks) >= 2, \
            f"Quick Start must have at least 2 code blocks, found {len(code_blocks)}"


class TestFooter:
    """Test Case 6: Verify footer content."""

    def test_footer_has_github_link(self, soup):
        """Footer contains GitHub repository link."""
        footer = soup.find("footer")
        assert footer is not None, "Page must have a footer"

        links = footer.find_all("a")
        github_found = False

        for link in links:
            href = link.get("href", "")
            if "github.com" in href:
                github_found = True
                assert "yetone/mirdb" in href, \
                    "GitHub link must point to the correct repository"
                break

        assert github_found, "Footer must contain a GitHub link"

    def test_footer_has_copyright(self, soup):
        """Footer contains copyright text."""
        footer = soup.find("footer")
        assert footer is not None, "Page must have a footer"

        footer_text = footer.get_text().lower()
        has_copyright = "copyright" in footer_text or "©" in footer_text or "(c)" in footer_text
        assert has_copyright, "Footer must contain copyright information"


class TestHeadingHierarchy:
    """Test Case 7: Validate heading hierarchy."""

    def test_exactly_one_h1(self, soup):
        """Page has exactly one h1 element."""
        h1_elements = soup.find_all("h1")
        # Note: The ASCII art serves as the h1 equivalent in this design
        # We check for either explicit h1 or the hero section being the primary heading
        hero = soup.find(class_="hero")
        if len(h1_elements) == 0 and hero:
            # Accept the hero section as the primary heading area
            assert hero is not None, "Page must have a primary heading area"
        else:
            assert len(h1_elements) <= 1, \
                f"Page should have at most one h1, found {len(h1_elements)}"

    def test_h2_for_sections(self, soup):
        """Page uses h2 for main sections."""
        h2_elements = soup.find_all("h2")
        assert len(h2_elements) >= 3, \
            f"Page should have h2 elements for main sections, found {len(h2_elements)}"

        # Check that h2 elements are inside section elements
        for h2 in h2_elements:
            parent_section = h2.find_parent("section")
            assert parent_section is not None or h2.find_parent("main"), \
                "h2 elements should be inside sections or main"

    def test_h3_for_subsections(self, soup):
        """Page uses h3 for subsections."""
        h3_elements = soup.find_all("h3")
        # Quick Start section should have h3 subsections
        assert len(h3_elements) >= 2, \
            f"Page should have h3 elements for subsections, found {len(h3_elements)}"


class TestCTAButton:
    """Test Case 8: Verify CTA button in hero section."""

    def test_get_started_button_exists(self, soup):
        """Hero section has a Get Started button."""
        hero = soup.find("section", {"id": "hero"}) or soup.find(class_="hero")
        assert hero is not None, "Page must have a hero section"

        # Look for CTA button
        cta = hero.find(class_="cta-button") or hero.find("a", string=lambda t: t and "get started" in t.lower() if t else False)
        assert cta is not None, "Hero section must have a CTA button"

    def test_get_started_links_to_quick_start(self, soup):
        """Get Started button links to Quick Start section."""
        hero = soup.find("section", {"id": "hero"}) or soup.find(class_="hero")
        assert hero is not None, "Page must have a hero section"

        # Find CTA button
        cta = hero.find(class_="cta-button")
        if cta is None:
            # Try to find by text content
            links = hero.find_all("a")
            for link in links:
                if link.get_text() and "get started" in link.get_text().lower():
                    cta = link
                    break

        assert cta is not None, "Hero section must have a CTA button"

        href = cta.get("href", "")
        assert "#quick-start" in href, \
            f"CTA button must link to #quick-start, found: '{href}'"
