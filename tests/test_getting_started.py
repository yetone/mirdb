"""
Test suite for MirDB Homepage - Installation and Getting Started Section.

Scenario: Verify that the homepage provides clear instructions for getting started with MirDB

This module tests:
- Test Case 1: Homepage contains installation or getting started section
- Test Case 2: Code examples are wrapped in <pre> or <code> elements
- Test Case 3: Homepage shows 'cargo build' or similar Rust build command
- Test Case 4: Homepage demonstrates basic get/set operations or references usage.gif
- Test Case 5: Homepage shows example of connecting with standard Memcached client
"""

import unittest
import os
import re


def load_file(filepath):
    """Load and return the contents of a file."""
    with open(filepath, 'r', encoding='utf-8') as f:
        return f.read()


class TestGettingStartedSectionExists(unittest.TestCase):
    """Test Case 1: Homepage contains installation or getting started section."""

    @classmethod
    def setUpClass(cls):
        """Load the index.html file for testing."""
        cls.html_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'index.html')
        cls.html_content = load_file(cls.html_path)

    def test_getting_started_section_exists(self):
        """Test that a getting-started section exists with proper id."""
        pattern = r'<section[^>]*id=["\']?getting-started["\']?'
        match = re.search(pattern, self.html_content, re.IGNORECASE)
        self.assertIsNotNone(
            match,
            "Homepage must have a section with id='getting-started'"
        )

    def test_getting_started_heading_exists(self):
        """Test that Getting Started heading exists."""
        self.assertIn(
            'getting started',
            self.html_content.lower(),
            "Homepage must contain 'Getting Started' heading or text"
        )

    def test_installation_instructions_present(self):
        """Test that installation instructions are present."""
        # Check for installation-related keywords
        installation_keywords = ['install', 'cargo', 'build', 'run']
        content_lower = self.html_content.lower()
        keywords_found = sum(1 for kw in installation_keywords if kw in content_lower)
        self.assertGreaterEqual(
            keywords_found, 2,
            "Homepage must contain installation instructions with keywords like 'install', 'cargo', 'build', 'run'"
        )

    def test_getting_started_has_steps(self):
        """Test that getting started section has numbered steps."""
        # Extract getting started section
        section_pattern = r'<section[^>]*id=["\']?getting-started["\']?[^>]*>(.*?)</section>'
        match = re.search(section_pattern, self.html_content, re.IGNORECASE | re.DOTALL)
        self.assertIsNotNone(match, "Getting started section must exist")

        section_content = match.group(1)
        # Check for step indicators (numbered steps or step class)
        has_steps = 'step' in section_content.lower() or any(
            f'>{i}.' in section_content or f'>{i}<' in section_content
            for i in range(1, 5)
        )
        self.assertTrue(
            has_steps,
            "Getting started section must have numbered steps or step indicators"
        )

    def test_navigation_links_to_getting_started(self):
        """Test that navigation includes link to getting started section."""
        pattern = r'<a[^>]*href=["\']?#getting-started["\']?'
        match = re.search(pattern, self.html_content, re.IGNORECASE)
        self.assertIsNotNone(
            match,
            "Navigation must include a link to #getting-started section"
        )


class TestCodeBlocksFormatting(unittest.TestCase):
    """Test Case 2: Code examples are wrapped in <pre> or <code> elements."""

    @classmethod
    def setUpClass(cls):
        """Load the index.html file for testing."""
        cls.html_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'index.html')
        cls.html_content = load_file(cls.html_path)

    def test_pre_elements_exist(self):
        """Test that <pre> elements exist for code formatting."""
        self.assertIn(
            '<pre>',
            self.html_content,
            "Homepage must have <pre> elements for code blocks"
        )

    def test_code_elements_exist(self):
        """Test that <code> elements exist for code formatting."""
        self.assertIn(
            '<code>',
            self.html_content,
            "Homepage must have <code> elements for inline code"
        )

    def test_pre_contains_code_elements(self):
        """Test that <pre> elements contain <code> elements for proper semantics."""
        pattern = r'<pre>\s*<code>'
        match = re.search(pattern, self.html_content, re.IGNORECASE)
        self.assertIsNotNone(
            match,
            "Code blocks should use <pre><code> pattern for proper semantics"
        )

    def test_code_blocks_have_content(self):
        """Test that code blocks have actual content."""
        pattern = r'<pre>\s*<code>([^<]+)</code>\s*</pre>'
        matches = re.findall(pattern, self.html_content, re.IGNORECASE | re.DOTALL)
        self.assertGreater(
            len(matches), 0,
            "Code blocks must have content"
        )
        # Each code block should have meaningful content
        for match in matches:
            self.assertGreater(
                len(match.strip()), 5,
                "Code blocks must have meaningful content"
            )

    def test_multiple_code_examples_exist(self):
        """Test that multiple code examples exist for comprehensive instructions."""
        pre_count = self.html_content.lower().count('<pre>')
        self.assertGreaterEqual(
            pre_count, 3,
            "Homepage should have at least 3 code blocks for install, run, and connect steps"
        )


class TestCargoRustBuildCommand(unittest.TestCase):
    """Test Case 3: Homepage shows 'cargo build' or similar Rust build command."""

    @classmethod
    def setUpClass(cls):
        """Load the index.html file for testing."""
        cls.html_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'index.html')
        cls.html_content = load_file(cls.html_path)

    def test_cargo_command_present(self):
        """Test that cargo command is present in the homepage."""
        self.assertIn(
            'cargo',
            self.html_content.lower(),
            "Homepage must mention 'cargo' for Rust installation"
        )

    def test_cargo_install_command(self):
        """Test that cargo install command is shown."""
        # Check for cargo install pattern
        pattern = r'cargo\s+(install|build|run)'
        match = re.search(pattern, self.html_content, re.IGNORECASE)
        self.assertIsNotNone(
            match,
            "Homepage must show a cargo install/build/run command"
        )

    def test_cargo_command_in_code_block(self):
        """Test that cargo command is properly formatted in a code block."""
        # Extract code blocks
        code_pattern = r'<pre>\s*<code>([^<]*cargo[^<]*)</code>\s*</pre>'
        match = re.search(code_pattern, self.html_content, re.IGNORECASE | re.DOTALL)
        self.assertIsNotNone(
            match,
            "Cargo command must be inside a <pre><code> block"
        )

    def test_mirdb_server_mentioned(self):
        """Test that mirdb-server is mentioned for installation."""
        self.assertIn(
            'mirdb',
            self.html_content.lower(),
            "Homepage must mention mirdb for installation"
        )

    def test_config_or_run_instructions(self):
        """Test that configuration or run instructions are present."""
        # Check for config file or run command
        config_patterns = ['config', '--config', 'mirdb.toml', 'server']
        content_lower = self.html_content.lower()
        has_config = any(pattern in content_lower for pattern in config_patterns)
        self.assertTrue(
            has_config,
            "Homepage must include configuration or run instructions"
        )


class TestUsageExample(unittest.TestCase):
    """Test Case 4: Homepage demonstrates basic get/set operations or references usage.gif."""

    @classmethod
    def setUpClass(cls):
        """Load the index.html file for testing."""
        cls.html_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'index.html')
        cls.html_content = load_file(cls.html_path)

    def test_usage_gif_exists(self):
        """Test that usage.gif file exists in assets."""
        usage_gif_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            'assets',
            'usage.gif'
        )
        self.assertTrue(
            os.path.exists(usage_gif_path),
            "usage.gif must exist in assets directory"
        )

    def test_usage_gif_referenced_in_html(self):
        """Test that usage.gif is referenced in the homepage."""
        self.assertIn(
            'usage.gif',
            self.html_content,
            "Homepage must reference usage.gif for demonstration"
        )

    def test_get_command_example(self):
        """Test that 'get' command example is present."""
        self.assertIn(
            'get',
            self.html_content.lower(),
            "Homepage must demonstrate 'get' command"
        )

    def test_set_command_example(self):
        """Test that 'set' command example is present."""
        self.assertIn(
            'set',
            self.html_content.lower(),
            "Homepage must demonstrate 'set' command"
        )

    def test_usage_section_exists(self):
        """Test that a usage or demonstration section exists."""
        # Check for usage section or usage-related content
        usage_patterns = ['usage', 'see it in action', 'demonstration', 'example']
        content_lower = self.html_content.lower()
        has_usage = any(pattern in content_lower for pattern in usage_patterns)
        self.assertTrue(
            has_usage,
            "Homepage must have a usage/demonstration section"
        )

    def test_code_example_shows_operations(self):
        """Test that code examples show get/set operations."""
        # Extract code blocks and check for operations
        code_pattern = r'<pre>\s*<code>(.*?)</code>\s*</pre>'
        matches = re.findall(code_pattern, self.html_content, re.IGNORECASE | re.DOTALL)

        operations_found = False
        for code_block in matches:
            if 'get' in code_block.lower() or 'set' in code_block.lower():
                operations_found = True
                break

        self.assertTrue(
            operations_found,
            "Code examples must demonstrate get/set operations"
        )


class TestMemcachedClientExample(unittest.TestCase):
    """Test Case 5: Homepage shows example of connecting with standard Memcached client."""

    @classmethod
    def setUpClass(cls):
        """Load the index.html file for testing."""
        cls.html_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'index.html')
        cls.html_content = load_file(cls.html_path)

    def test_memcached_client_mentioned(self):
        """Test that Memcached client is mentioned."""
        self.assertIn(
            'memcached',
            self.html_content.lower(),
            "Homepage must mention Memcached client usage"
        )

    def test_memcached_client_connection_example(self):
        """Test that connection example is provided."""
        # Check for connection-related keywords
        connection_keywords = ['connect', 'telnet', 'localhost', '11211', 'client']
        content_lower = self.html_content.lower()
        keywords_found = sum(1 for kw in connection_keywords if kw in content_lower)
        self.assertGreaterEqual(
            keywords_found, 2,
            "Homepage must show how to connect with a Memcached client"
        )

    def test_default_port_mentioned(self):
        """Test that the default Memcached port (11211) is mentioned."""
        self.assertIn(
            '11211',
            self.html_content,
            "Homepage must mention the default port 11211 for Memcached protocol"
        )

    def test_connection_code_in_block(self):
        """Test that connection example is in a code block."""
        code_pattern = r'<pre>\s*<code>(.*?)</code>\s*</pre>'
        matches = re.findall(code_pattern, self.html_content, re.IGNORECASE | re.DOTALL)

        connection_in_code = False
        for code_block in matches:
            if 'localhost' in code_block.lower() or '11211' in code_block:
                connection_in_code = True
                break

        self.assertTrue(
            connection_in_code,
            "Connection example must be inside a code block"
        )

    def test_set_command_syntax(self):
        """Test that proper set command syntax is shown."""
        # Check for proper Memcached set command format
        set_pattern = r'set\s+\w+\s+\d+\s+\d+\s+\d+'
        match = re.search(set_pattern, self.html_content, re.IGNORECASE)
        self.assertIsNotNone(
            match,
            "Homepage must show proper Memcached set command syntax (set key flags exptime bytes)"
        )

    def test_stored_response_shown(self):
        """Test that STORED response is shown for successful set."""
        self.assertIn(
            'STORED',
            self.html_content,
            "Homepage must show 'STORED' response for successful set command"
        )

    def test_value_response_shown(self):
        """Test that VALUE response is shown for successful get."""
        self.assertIn(
            'VALUE',
            self.html_content,
            "Homepage must show 'VALUE' response for successful get command"
        )


if __name__ == '__main__':
    unittest.main(verbosity=2)
