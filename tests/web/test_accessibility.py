"""
Accessibility Compliance Tests
Owner: Scenario 5 - Accessibility Compliance

Tests verify that the homepage meets accessibility standards including:
- Semantic HTML structure
- Alt text for images
- Proper heading hierarchy
- Keyboard navigation support
- ARIA labels for icons
- Skip navigation link
- Language attribute
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


class TestLogoAltText:
    """Test Case 1: Check for alt attribute on logo image."""

    def test_logo_has_alt_attribute(self, soup):
        """Logo image must have alt attribute."""
        images = soup.find_all("img")
        logo_found = False

        for img in images:
            src = img.get("src", "")
            if "logo" in src.lower():
                logo_found = True
                alt = img.get("alt")
                assert alt is not None, "Logo image must have alt attribute"
                assert len(alt.strip()) > 0, "Logo alt text must not be empty"
                assert "mirdb" in alt.lower() or "logo" in alt.lower(), \
                    f"Logo alt text should describe the MirDB logo, found: '{alt}'"
                break

        assert logo_found, "Page must contain a logo image"


class TestUsageGifAltText:
    """Test Case 2: Check for alt attribute on usage.gif."""

    def test_usage_gif_has_alt_attribute(self, soup):
        """Usage GIF must have descriptive alt text."""
        images = soup.find_all("img")
        usage_found = False

        for img in images:
            src = img.get("src", "")
            if "usage" in src.lower():
                usage_found = True
                alt = img.get("alt")
                assert alt is not None, "Usage GIF must have alt attribute"
                assert len(alt.strip()) > 10, \
                    f"Usage GIF alt text must be descriptive (>10 chars), found: '{alt}'"
                # Should describe what the demonstration shows
                assert any(word in alt.lower() for word in ["demonstration", "demo", "usage", "operation", "example"]), \
                    f"Usage GIF alt text should describe the demonstration, found: '{alt}'"
                break

        assert usage_found, "Page must contain usage GIF image"


class TestH1Count:
    """Test Case 3: Count h1 elements in page."""

    def test_exactly_one_h1(self, soup):
        """Page must contain exactly one h1 element."""
        h1_elements = soup.find_all("h1")
        assert len(h1_elements) == 1, \
            f"Page must have exactly one h1 element, found {len(h1_elements)}"


class TestHeadingHierarchy:
    """Test Case 4: Check heading hierarchy."""

    def test_no_skipped_heading_levels(self, soup):
        """No heading levels should be skipped (h1 followed by h2, not h3)."""
        headings = soup.find_all(["h1", "h2", "h3", "h4", "h5", "h6"])

        if not headings:
            pytest.fail("Page must have headings")

        # Extract heading levels
        levels = []
        for h in headings:
            level = int(h.name[1])
            levels.append(level)

        # Check that no level is skipped
        # First heading should be h1
        assert levels[0] == 1, f"First heading must be h1, found h{levels[0]}"

        # Check for skipped levels
        for i in range(1, len(levels)):
            current = levels[i]
            prev_max = max(levels[:i])
            # Allow same level or going up or going down by one
            if current > prev_max + 1:
                pytest.fail(
                    f"Heading hierarchy is broken: found h{current} after h{prev_max}, "
                    f"which skips level h{prev_max + 1}"
                )


class TestLangAttribute:
    """Test Case 7: Check for lang attribute on html element."""

    def test_html_has_lang_attribute(self, soup):
        """HTML element must have lang='en' attribute."""
        html_tag = soup.find("html")
        assert html_tag is not None, "Page must have html element"

        lang = html_tag.get("lang")
        assert lang is not None, "HTML element must have lang attribute"
        assert lang.lower() == "en", \
            f"HTML element must have lang='en', found lang='{lang}'"


class TestSkipToContentLink:
    """Test Case 8: Check for skip-to-content link."""

    def test_skip_link_exists(self, soup):
        """Page must contain a skip navigation link as first focusable element."""
        body = soup.find("body")
        assert body is not None, "Page must have body element"

        # Find the first anchor element in body
        first_focusable = body.find(["a", "button", "input", "select", "textarea"])
        assert first_focusable is not None, "Page must have focusable elements"

        # Check if it's a skip link
        if first_focusable.name == "a":
            href = first_focusable.get("href", "")
            text = first_focusable.get_text().lower()

            # Skip link should point to main content and have appropriate text
            is_skip_link = (
                ("#main" in href or "#content" in href) and
                ("skip" in text or "main" in text or "content" in text)
            )
            assert is_skip_link, \
                f"First focusable element should be skip-to-content link, found: href='{href}', text='{text}'"
        else:
            pytest.fail(
                f"First focusable element should be a skip link (anchor), found: {first_focusable.name}"
            )


class TestSemanticStructure:
    """Test Case 9: Validate HTML structure with semantic elements."""

    def test_has_header_element(self, soup):
        """Page must use <header> element."""
        header = soup.find("header")
        assert header is not None, "Page must contain a <header> element"

    def test_has_main_element(self, soup):
        """Page must use <main> element."""
        main = soup.find("main")
        assert main is not None, "Page must contain a <main> element"

    def test_has_section_elements(self, soup):
        """Page must use <section> elements."""
        sections = soup.find_all("section")
        assert len(sections) >= 1, "Page must contain at least one <section> element"

    def test_has_footer_element(self, soup):
        """Page must use <footer> element."""
        footer = soup.find("footer")
        assert footer is not None, "Page must contain a <footer> element"

    def test_semantic_structure_complete(self, soup):
        """Page must use header, main, section, and footer elements."""
        header = soup.find("header")
        main = soup.find("main")
        sections = soup.find_all("section")
        footer = soup.find("footer")

        assert header is not None, "Page must contain <header>"
        assert main is not None, "Page must contain <main>"
        assert len(sections) >= 1, "Page must contain <section>"
        assert footer is not None, "Page must contain <footer>"


class TestAriaLabels:
    """Test Case 10: Check feature cards for proper ARIA labels if using icons."""

    def test_decorative_icons_have_aria_hidden(self, soup):
        """Decorative icons must have aria-hidden='true'."""
        feature_icons = soup.find_all(class_="feature-icon")

        if not feature_icons:
            # If no feature icons exist, check for any decorative elements
            decorative = soup.find_all(attrs={"role": "presentation"})
            if decorative:
                for elem in decorative:
                    aria_hidden = elem.get("aria-hidden")
                    assert aria_hidden == "true", \
                        "Decorative elements should have aria-hidden='true'"
            return

        for icon in feature_icons:
            # Feature icons using emojis are decorative
            aria_hidden = icon.get("aria-hidden")
            assert aria_hidden == "true", \
                f"Decorative feature icon must have aria-hidden='true', found: aria-hidden='{aria_hidden}'"

    def test_functional_icons_have_aria_label(self, soup):
        """Functional icons must have aria-label."""
        # Find icons that appear to be functional (inside buttons or links without text)
        buttons = soup.find_all("button")
        links = soup.find_all("a")

        for elem in buttons + links:
            # If element has no text content but has an icon
            text = elem.get_text(strip=True)
            if not text:
                # Element might be icon-only, should have aria-label
                aria_label = elem.get("aria-label")
                title = elem.get("title")
                # Either aria-label or title should be present for accessibility
                if elem.find(class_="icon") or elem.find("svg") or elem.find("i"):
                    assert aria_label or title, \
                        "Icon-only interactive elements must have aria-label or title"


class TestFocusIndicators:
    """Test Case 5 & 6: Check for keyboard navigation and focus indicators (CSS check)."""

    def test_focus_styles_defined(self, html_content):
        """Page must define focus styles for keyboard navigation."""
        # Check if focus styles are defined in inline styles or linked CSS
        has_focus_styles = (
            ":focus" in html_content or
            "focus-visible" in html_content or
            "outline" in html_content
        )
        assert has_focus_styles, \
            "Page must define focus styles for keyboard navigation"

    def test_skip_link_has_focus_style(self, soup):
        """Skip link must be visible when focused."""
        # Check for skip-link class and its focus behavior
        style_tags = soup.find_all("style")
        style_content = " ".join(str(tag) for tag in style_tags)

        # Should have skip-link:focus styles
        has_skip_focus = (
            "skip-link:focus" in style_content or
            ".skip-link:focus" in style_content
        )
        assert has_skip_focus, \
            "Skip link must have :focus styles to be visible when focused"


class TestMainContentId:
    """Additional test: Verify main content has proper id for skip link."""

    def test_main_has_id_for_skip_link(self, soup):
        """Main element must have an id that matches the skip link target."""
        skip_link = soup.find("a", class_="skip-link")
        if skip_link:
            href = skip_link.get("href", "")
            if href.startswith("#"):
                target_id = href[1:]
                target_element = soup.find(id=target_id)
                assert target_element is not None, \
                    f"Skip link target '#{target_id}' must exist in the page"
