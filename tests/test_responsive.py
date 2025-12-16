"""
Test suite for MirDB Homepage Responsive Design.

This module tests the responsive design requirements for the MirDB homepage,
ensuring proper viewport meta tag, CSS media queries, and layout adaptations
at different screen sizes (desktop, tablet, mobile, and minimum width).
"""

import unittest
import os
import re


def load_file(filepath):
    """Load and return the contents of a file."""
    with open(filepath, 'r', encoding='utf-8') as f:
        return f.read()


class TestViewportMetaTag(unittest.TestCase):
    """Test Case 1: Check for viewport meta tag."""

    @classmethod
    def setUpClass(cls):
        """Load the index.html file for testing."""
        cls.html_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'index.html')
        cls.html_content = load_file(cls.html_path)

    def test_viewport_meta_tag_exists(self):
        """Test that the page contains <meta name="viewport"> tag for responsive design."""
        viewport_pattern = r'<meta\s+[^>]*name\s*=\s*["\']viewport["\'][^>]*>'
        match = re.search(viewport_pattern, self.html_content, re.IGNORECASE)
        self.assertIsNotNone(
            match,
            "Page must contain <meta name=\"viewport\"> tag for responsive design"
        )

    def test_viewport_has_width_device_width(self):
        """Test that viewport meta tag includes width=device-width."""
        viewport_pattern = r'<meta\s+[^>]*name\s*=\s*["\']viewport["\'][^>]*content\s*=\s*["\']([^"\']*)["\'][^>]*>'
        match = re.search(viewport_pattern, self.html_content, re.IGNORECASE)
        self.assertIsNotNone(match, "Viewport meta tag must exist")
        content = match.group(1)
        self.assertIn(
            'width=device-width',
            content,
            "Viewport must include width=device-width for responsive design"
        )

    def test_viewport_has_initial_scale(self):
        """Test that viewport meta tag includes initial-scale=1.0."""
        viewport_pattern = r'<meta\s+[^>]*name\s*=\s*["\']viewport["\'][^>]*content\s*=\s*["\']([^"\']*)["\'][^>]*>'
        match = re.search(viewport_pattern, self.html_content, re.IGNORECASE)
        self.assertIsNotNone(match, "Viewport meta tag must exist")
        content = match.group(1)
        has_initial_scale = 'initial-scale=1' in content or 'initial-scale=1.0' in content
        self.assertTrue(
            has_initial_scale,
            "Viewport must include initial-scale=1.0 for proper mobile rendering"
        )


class TestCSSMediaQueries(unittest.TestCase):
    """Test Case 5: Check for CSS media queries."""

    @classmethod
    def setUpClass(cls):
        """Load the styles.css file for testing."""
        cls.css_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'styles.css')
        cls.css_content = load_file(cls.css_path)

    def test_media_queries_exist(self):
        """Test that stylesheet contains media queries for responsive breakpoints."""
        media_query_pattern = r'@media\s*\([^)]+\)'
        matches = re.findall(media_query_pattern, self.css_content)
        self.assertGreater(
            len(matches), 0,
            "Stylesheet must contain media queries for responsive breakpoints"
        )

    def test_tablet_breakpoint_exists(self):
        """Test that a media query for tablet view (around 768px) exists."""
        tablet_pattern = r'@media\s*\([^)]*max-width\s*:\s*768px[^)]*\)'
        match = re.search(tablet_pattern, self.css_content)
        self.assertIsNotNone(
            match,
            "Stylesheet must contain media query for tablet breakpoint (768px)"
        )

    def test_mobile_breakpoint_exists(self):
        """Test that a media query for mobile view exists."""
        # Check for any breakpoint under 600px which would cover mobile
        mobile_pattern = r'@media\s*\([^)]*max-width\s*:\s*(\d+)px[^)]*\)'
        matches = re.findall(mobile_pattern, self.css_content)
        mobile_breakpoints = [int(m) for m in matches if int(m) <= 600]
        self.assertGreater(
            len(mobile_breakpoints), 0,
            "Stylesheet must contain media query for mobile breakpoint (<=600px)"
        )

    def test_minimum_width_breakpoint_exists(self):
        """Test that a media query for minimum width (320px) exists per NFR-1."""
        # Check for breakpoint that handles 320px minimum width
        # This can be a max-width: 320px query or a small width query
        min_width_pattern = r'@media\s*\([^)]*max-width\s*:\s*(\d+)px[^)]*\)'
        matches = re.findall(min_width_pattern, self.css_content)
        # Should have a breakpoint at or below 480px to handle 320px devices
        small_breakpoints = [int(m) for m in matches if int(m) <= 480]
        self.assertGreater(
            len(small_breakpoints), 0,
            "Stylesheet must contain media query to handle minimum supported width (320px per NFR-1)"
        )

    def test_responsive_navbar_styles(self):
        """Test that navbar has responsive styles."""
        # Check if navbar adjusts in media queries
        navbar_in_media = '.navbar' in self.css_content and '@media' in self.css_content
        self.assertTrue(
            navbar_in_media,
            "Navbar must have responsive styles for different screen sizes"
        )

    def test_responsive_grid_layout(self):
        """Test that grid layout is responsive (uses auto-fit/auto-fill or adjusts in media queries)."""
        has_responsive_grid = (
            'auto-fit' in self.css_content or
            'auto-fill' in self.css_content or
            ('grid' in self.css_content and '@media' in self.css_content)
        )
        self.assertTrue(
            has_responsive_grid,
            "Grid layout must be responsive (use auto-fit/auto-fill or adjust in media queries)"
        )


class TestDesktopLayout(unittest.TestCase):
    """Test Case 2: Layout at desktop viewport (1920px width)."""

    @classmethod
    def setUpClass(cls):
        """Load the CSS file for testing."""
        cls.css_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'styles.css')
        cls.css_content = load_file(cls.css_path)

    def test_max_width_container(self):
        """Test that containers have max-width for desktop to maintain readability."""
        max_width_pattern = r'max-width\s*:\s*\d+px'
        match = re.search(max_width_pattern, self.css_content)
        self.assertIsNotNone(
            match,
            "Layout must have max-width containers for proper desktop spacing"
        )

    def test_container_centered(self):
        """Test that main container is centered with margin: 0 auto."""
        centered_pattern = r'margin\s*:\s*0\s+auto'
        match = re.search(centered_pattern, self.css_content)
        self.assertIsNotNone(
            match,
            "Container must be centered with margin: 0 auto for desktop layout"
        )

    def test_grid_layout_for_features(self):
        """Test that features section uses grid layout for desktop."""
        grid_pattern = r'display\s*:\s*grid'
        match = re.search(grid_pattern, self.css_content)
        self.assertIsNotNone(
            match,
            "Features section must use grid layout for proper desktop display"
        )


class TestTabletLayout(unittest.TestCase):
    """Test Case 3: Layout at tablet viewport (768px width)."""

    @classmethod
    def setUpClass(cls):
        """Load the CSS file for testing."""
        cls.css_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'styles.css')
        cls.css_content = load_file(cls.css_path)
        # Extract 768px media query content
        tablet_pattern = r'@media\s*\([^)]*max-width\s*:\s*768px[^)]*\)\s*\{([^{}]*(?:\{[^{}]*\}[^{}]*)*)\}'
        match = re.search(tablet_pattern, cls.css_content, re.DOTALL)
        cls.tablet_css = match.group(1) if match else ""

    def test_tablet_media_query_has_navbar_styles(self):
        """Test that tablet media query adjusts navbar layout."""
        self.assertIn(
            '.navbar',
            self.tablet_css,
            "Tablet media query must adjust navbar layout"
        )

    def test_tablet_media_query_has_nav_links_styles(self):
        """Test that tablet media query adjusts navigation links."""
        self.assertIn(
            '.nav-links',
            self.tablet_css,
            "Tablet media query must adjust navigation links layout"
        )

    def test_tablet_hero_font_adjustment(self):
        """Test that hero section font sizes are adjusted for tablet."""
        hero_in_tablet = '.hero' in self.tablet_css or 'h1' in self.tablet_css
        self.assertTrue(
            hero_in_tablet,
            "Tablet media query should adjust hero section styling"
        )


class TestMobileLayout(unittest.TestCase):
    """Test Case 4: Layout at mobile viewport (375px width)."""

    @classmethod
    def setUpClass(cls):
        """Load the CSS file for testing."""
        cls.css_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'styles.css')
        cls.css_content = load_file(cls.css_path)
        # Extract mobile media query content (480px or smaller)
        mobile_pattern = r'@media\s*\([^)]*max-width\s*:\s*480px[^)]*\)\s*\{([^{}]*(?:\{[^{}]*\}[^{}]*)*)\}'
        match = re.search(mobile_pattern, cls.css_content, re.DOTALL)
        cls.mobile_css = match.group(1) if match else ""

    def test_mobile_media_query_exists(self):
        """Test that mobile-specific media query exists."""
        self.assertTrue(
            len(self.mobile_css) > 0,
            "Mobile media query (480px) must exist for mobile layout"
        )

    def test_mobile_hero_padding_adjustment(self):
        """Test that hero section padding is adjusted for mobile."""
        self.assertIn(
            '.hero',
            self.mobile_css,
            "Mobile media query must adjust hero section for smaller screens"
        )

    def test_mobile_section_padding_adjustment(self):
        """Test that section padding is adjusted for mobile."""
        self.assertIn(
            'section',
            self.mobile_css,
            "Mobile media query must adjust section padding for smaller screens"
        )


class TestMinimumWidthLayout(unittest.TestCase):
    """Test Case 6: Layout at minimum supported width (320px) per NFR-1."""

    @classmethod
    def setUpClass(cls):
        """Load files for testing."""
        cls.css_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'styles.css')
        cls.html_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'index.html')
        cls.css_content = load_file(cls.css_path)
        cls.html_content = load_file(cls.html_path)

    def test_box_sizing_border_box(self):
        """Test that box-sizing: border-box is set to prevent overflow at 320px."""
        self.assertIn(
            'box-sizing: border-box',
            self.css_content,
            "box-sizing: border-box must be set to prevent layout overflow at minimum width"
        )

    def test_no_fixed_large_widths(self):
        """Test that no fixed widths larger than 320px are set on main content."""
        # Check for problematic fixed widths (excluding max-width which is fine)
        # Looking for width: [number]px where number > 300
        fixed_width_pattern = r'(?<!max-)width\s*:\s*(\d+)px'
        matches = re.findall(fixed_width_pattern, self.css_content)
        large_widths = [int(m) for m in matches if int(m) > 300]
        self.assertEqual(
            len(large_widths), 0,
            f"No fixed widths larger than 300px should be set (found: {large_widths})"
        )

    def test_responsive_images(self):
        """Test that images have responsive styling."""
        # Check for max-width: 100% or width: 100% for images
        responsive_img = 'max-width: 100%' in self.css_content or 'width: 100%' in self.css_content
        self.assertTrue(
            responsive_img,
            "Images must have responsive styling (max-width: 100% or width: 100%)"
        )

    def test_overflow_handling(self):
        """Test that overflow is handled for tables and code blocks."""
        overflow_handled = 'overflow-x: auto' in self.css_content or 'overflow: auto' in self.css_content
        self.assertTrue(
            overflow_handled,
            "Overflow must be handled for content that might exceed 320px width"
        )

    def test_flexible_containers(self):
        """Test that containers use flexible units or percentages."""
        # Check for percentage-based or viewport-based widths
        flexible = (
            'padding: 0 20px' in self.css_content or
            'padding: 0 1rem' in self.css_content or
            '%' in self.css_content
        )
        self.assertTrue(
            flexible,
            "Containers must use flexible units for proper display at 320px"
        )

    def test_minmax_grid_usage(self):
        """Test that grid uses minmax for responsive sizing."""
        # Matches both minmax(300px, 1fr) and minmax(min(300px, 100%), 1fr)
        has_minmax = 'minmax(' in self.css_content and '1fr' in self.css_content
        self.assertTrue(
            has_minmax,
            "Grid should use minmax() for responsive column sizing at minimum width"
        )


class TestNoHorizontalScroll(unittest.TestCase):
    """Additional tests to ensure no horizontal scroll at various breakpoints."""

    @classmethod
    def setUpClass(cls):
        """Load CSS file for testing."""
        cls.css_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'styles.css')
        cls.css_content = load_file(cls.css_path)

    def test_table_overflow_handled(self):
        """Test that table container has overflow handling to prevent horizontal scroll."""
        table_overflow = 'commands-table' in self.css_content and 'overflow' in self.css_content
        self.assertTrue(
            table_overflow,
            "Table container must handle overflow to prevent horizontal scroll on small screens"
        )

    def test_code_block_overflow_handled(self):
        """Test that code blocks have overflow handling."""
        code_overflow = 'pre' in self.css_content and 'overflow' in self.css_content
        self.assertTrue(
            code_overflow,
            "Code blocks (pre) must have overflow handling for small screens"
        )

    def test_flexible_button_layout(self):
        """Test that CTA buttons use flexible layout."""
        flex_buttons = 'cta-buttons' in self.css_content and 'flex' in self.css_content
        self.assertTrue(
            flex_buttons,
            "CTA buttons must use flexible layout for responsive display"
        )


if __name__ == '__main__':
    unittest.main(verbosity=2)
