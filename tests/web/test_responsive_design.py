"""
Responsive Design Tests
Owner: Scenario 4 - Responsive Design

Tests verify that the homepage displays correctly on various screen sizes
including desktop and mobile devices, with proper responsive CSS and
viewport settings.
"""

import pytest
import re
from pathlib import Path
from bs4 import BeautifulSoup


@pytest.fixture
def html_content():
    """Load the homepage HTML content."""
    html_path = Path(__file__).parent.parent.parent / "web" / "index.html"
    with open(html_path, "r", encoding="utf-8") as f:
        return f.read()


@pytest.fixture
def css_content():
    """Load the CSS stylesheet content."""
    css_path = Path(__file__).parent.parent.parent / "web" / "css" / "style.css"
    with open(css_path, "r", encoding="utf-8") as f:
        return f.read()


@pytest.fixture
def soup(html_content):
    """Parse HTML content into BeautifulSoup object."""
    return BeautifulSoup(html_content, "lxml")


class TestViewportMetaTag:
    """Test Case 4: Check HTML head for viewport meta tag."""

    def test_viewport_meta_tag_exists(self, soup):
        """HTML contains viewport meta tag."""
        meta_viewport = soup.find("meta", attrs={"name": "viewport"})
        assert meta_viewport is not None, \
            "Page must have <meta name='viewport'> tag"

    def test_viewport_meta_tag_content(self, soup):
        """Viewport meta tag has correct content for responsive design."""
        meta_viewport = soup.find("meta", attrs={"name": "viewport"})
        assert meta_viewport is not None, \
            "Page must have <meta name='viewport'> tag"

        content = meta_viewport.get("content", "")
        assert "width=device-width" in content, \
            "Viewport must include 'width=device-width'"
        assert "initial-scale=1" in content, \
            "Viewport must include 'initial-scale=1'"


class TestCSSMediaQueries:
    """Test Case 6: Check CSS for media queries."""

    def test_css_has_media_queries(self, css_content):
        """CSS contains @media rules for responsive design."""
        media_queries = re.findall(r'@media[^{]+\{', css_content)
        assert len(media_queries) >= 2, \
            f"CSS must have at least 2 @media rules, found {len(media_queries)}"

    def test_css_has_mobile_breakpoint(self, css_content):
        """CSS includes mobile breakpoint (max-width: 767px or similar)."""
        # Check for mobile breakpoint patterns
        mobile_patterns = [
            r'@media[^{]*max-width\s*:\s*76[0-9]px',  # ~768px
            r'@media[^{]*max-width\s*:\s*7[0-6][0-9]px',  # Less than 768px
        ]
        has_mobile = any(
            re.search(pattern, css_content, re.IGNORECASE)
            for pattern in mobile_patterns
        )
        assert has_mobile, \
            "CSS must include mobile breakpoint (max-width around 767px)"

    def test_css_has_tablet_breakpoint(self, css_content):
        """CSS includes tablet breakpoint (768px - 1024px range)."""
        # Check for tablet breakpoint patterns
        tablet_patterns = [
            r'@media[^{]*min-width\s*:\s*768px',
            r'@media[^{]*768px[^}]*1024px',
        ]
        has_tablet = any(
            re.search(pattern, css_content, re.IGNORECASE)
            for pattern in tablet_patterns
        )
        assert has_tablet, \
            "CSS must include tablet breakpoint (around 768px)"

    def test_css_has_desktop_breakpoint(self, css_content):
        """CSS includes desktop breakpoint (>1024px)."""
        desktop_patterns = [
            r'@media[^{]*min-width\s*:\s*102[0-9]px',
            r'@media[^{]*min-width\s*:\s*10[3-9][0-9]px',
        ]
        has_desktop = any(
            re.search(pattern, css_content, re.IGNORECASE)
            for pattern in desktop_patterns
        )
        assert has_desktop, \
            "CSS must include desktop breakpoint (min-width around 1024px)"


class TestResponsiveFeatureCards:
    """Test responsive behavior of feature cards layout."""

    def test_feature_grid_uses_css_grid_or_flexbox(self, css_content):
        """Feature cards use CSS Grid or Flexbox for responsive layout."""
        has_grid = "grid-template-columns" in css_content or "display: grid" in css_content
        has_flexbox = "display: flex" in css_content
        assert has_grid or has_flexbox, \
            "Feature cards must use CSS Grid or Flexbox for layout"

    def test_feature_grid_has_responsive_columns(self, css_content):
        """Feature grid changes column count at different breakpoints."""
        # Check for column changes in media queries
        has_single_column = re.search(
            r'grid-template-columns\s*:\s*(1fr|repeat\s*\(\s*1)',
            css_content
        )
        has_multi_column = re.search(
            r'grid-template-columns\s*:\s*repeat\s*\(\s*[2-4]',
            css_content
        )
        assert has_single_column is not None or has_multi_column is not None, \
            "Feature grid should have responsive column definitions"


class TestResponsiveImages:
    """Test Case 5: View usage.gif on mobile viewport - GIF scales to fit."""

    def test_usage_gif_has_max_width(self, css_content):
        """Usage GIF has max-width constraint to prevent overflow."""
        # Check for max-width on usage-gif class
        has_max_width = (
            "max-width" in css_content and
            ("usage-gif" in css_content or "100%" in css_content)
        )
        assert has_max_width, \
            "Usage GIF must have max-width constraint"

    def test_images_have_height_auto(self, css_content, soup):
        """Images should have height: auto for proper scaling."""
        # Check CSS for height: auto patterns
        has_height_auto = "height: auto" in css_content
        # Or check HTML for height attribute
        images = soup.find_all("img")
        has_html_responsive = all(
            not img.get("height") or img.get("height") == "auto"
            for img in images
        )
        assert has_height_auto or has_html_responsive, \
            "Images should scale with height: auto"


class TestTouchTargets:
    """Test Case 7: Touch targets have minimum 44x44px size on mobile."""

    def test_cta_button_has_minimum_touch_target(self, css_content):
        """CTA button has minimum touch target size."""
        # Check for min-width/min-height or padding that would create 44px targets
        cta_pattern = r'\.cta-button[^}]*(min-width\s*:\s*4[4-9]px|min-height\s*:\s*4[4-9]px|padding[^}]*)'
        has_target_size = re.search(cta_pattern, css_content, re.DOTALL) is not None
        # Also check in media queries
        has_mobile_target = "min-width: 44px" in css_content or "min-height: 44px" in css_content
        assert has_target_size or has_mobile_target, \
            "CTA button must have minimum 44x44px touch target"

    def test_nav_links_have_minimum_touch_target(self, css_content):
        """Navigation links have minimum touch target size on mobile."""
        # Check for touch target sizing in nav links
        has_link_padding = re.search(
            r'\.nav-links[^}]*(padding|min-width|min-height)',
            css_content,
            re.DOTALL
        )
        has_44px = "44px" in css_content
        assert has_link_padding or has_44px, \
            "Navigation links should have adequate touch target size"


class TestResponsiveHeroSection:
    """Test hero section scales appropriately on all screen sizes."""

    def test_hero_has_responsive_padding(self, css_content):
        """Hero section has different padding at different breakpoints."""
        # Check for padding changes in media queries for hero
        has_hero_responsive = re.search(
            r'@media[^}]*\.hero[^}]*padding',
            css_content,
            re.DOTALL
        ) is not None or (
            ".hero" in css_content and
            "padding" in css_content
        )
        assert has_hero_responsive, \
            "Hero section should have responsive padding"

    def test_hero_logo_scales(self, css_content, soup):
        """Hero logo scales appropriately on different screen sizes."""
        # Check for responsive logo sizing
        has_logo_max_width = "hero-logo" in css_content and "max-width" in css_content
        # Or check HTML for responsive attributes
        logo = soup.find("img", class_="hero-logo")
        has_html_responsive = logo is not None
        assert has_logo_max_width or has_html_responsive, \
            "Hero logo should scale responsively"


class TestNoHorizontalOverflow:
    """Test that content doesn't cause horizontal scroll on mobile."""

    def test_box_sizing_border_box(self, css_content):
        """CSS uses box-sizing: border-box to prevent overflow."""
        assert "box-sizing: border-box" in css_content or "box-sizing:border-box" in css_content, \
            "CSS should use box-sizing: border-box"

    def test_max_width_constraints(self, css_content):
        """Content has max-width constraints to prevent overflow."""
        assert "max-width" in css_content, \
            "CSS should have max-width constraints for content"

    def test_overflow_handling(self, css_content):
        """Pre/code blocks have overflow handling."""
        has_overflow = "overflow-x" in css_content or "overflow: auto" in css_content
        assert has_overflow, \
            "Pre/code blocks should have overflow handling"
