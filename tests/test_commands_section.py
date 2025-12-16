"""
Test suite for MirDB Homepage Supported Commands Section.

This module tests the supported Memcached commands section on the homepage,
verifying that all supported commands are listed with appropriate descriptions
per REQ-5 requirements.
"""

import unittest
import os
import re


def load_html_file(filepath):
    """Load and return the contents of an HTML file."""
    with open(filepath, 'r', encoding='utf-8') as f:
        return f.read()


class TestCommandsSectionExists(unittest.TestCase):
    """Test Case 1: Homepage contains a section listing supported Memcached commands."""

    @classmethod
    def setUpClass(cls):
        """Load the index.html file for testing."""
        cls.html_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'index.html')
        cls.html_content = load_html_file(cls.html_path)

    def test_commands_section_exists(self):
        """Test that a commands section exists on the homepage."""
        # Check for section with id="commands"
        commands_pattern = r'<section[^>]*id=["\']?commands["\']?[^>]*>'
        match = re.search(commands_pattern, self.html_content, re.IGNORECASE)
        self.assertIsNotNone(
            match,
            "Homepage must have a section with id='commands' for listing supported Memcached commands"
        )

    def test_commands_section_has_heading(self):
        """Test that commands section has a proper heading."""
        # Extract commands section content
        commands_pattern = r'<section[^>]*id=["\']?commands["\']?[^>]*>(.*?)</section>'
        match = re.search(commands_pattern, self.html_content, re.IGNORECASE | re.DOTALL)
        self.assertIsNotNone(match, "Commands section must exist")

        commands_content = match.group(1)
        # Check for h2 heading
        has_heading = re.search(r'<h2[^>]*>.*?commands.*?</h2>', commands_content, re.IGNORECASE)
        self.assertIsNotNone(
            has_heading,
            "Commands section must have an h2 heading mentioning 'commands'"
        )

    def test_commands_section_has_table_or_list(self):
        """Test that commands are displayed in a table or list format."""
        # Extract commands section content
        commands_pattern = r'<section[^>]*id=["\']?commands["\']?[^>]*>(.*?)</section>'
        match = re.search(commands_pattern, self.html_content, re.IGNORECASE | re.DOTALL)
        self.assertIsNotNone(match, "Commands section must exist")

        commands_content = match.group(1)
        # Check for table or ul/ol list
        has_table = '<table' in commands_content.lower()
        has_list = '<ul' in commands_content.lower() or '<ol' in commands_content.lower()
        self.assertTrue(
            has_table or has_list,
            "Commands must be displayed in a table or list format"
        )


class TestGetCommandDocumentation(unittest.TestCase):
    """Test Case 2: Homepage lists 'get' command with description for retrieving values."""

    @classmethod
    def setUpClass(cls):
        """Load the index.html file for testing."""
        cls.html_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'index.html')
        cls.html_content = load_html_file(cls.html_path)

    def test_get_command_listed(self):
        """Test that 'get' command is listed."""
        # Extract commands section
        commands_pattern = r'<section[^>]*id=["\']?commands["\']?[^>]*>(.*?)</section>'
        match = re.search(commands_pattern, self.html_content, re.IGNORECASE | re.DOTALL)
        self.assertIsNotNone(match, "Commands section must exist")

        commands_content = match.group(1).lower()
        self.assertIn('get', commands_content, "Commands section must list 'get' command")

    def test_get_command_has_code_formatting(self):
        """Test that 'get' command is displayed with code formatting."""
        commands_pattern = r'<section[^>]*id=["\']?commands["\']?[^>]*>(.*?)</section>'
        match = re.search(commands_pattern, self.html_content, re.IGNORECASE | re.DOTALL)
        self.assertIsNotNone(match, "Commands section must exist")

        commands_content = match.group(1)
        # Check for <code>get</code> or similar
        get_code_pattern = r'<code[^>]*>\s*get\s*</code>'
        get_code_match = re.search(get_code_pattern, commands_content, re.IGNORECASE)
        self.assertIsNotNone(
            get_code_match,
            "get command must be displayed with <code> formatting"
        )

    def test_get_command_description_mentions_retrieve(self):
        """Test that 'get' command description mentions retrieving values."""
        commands_pattern = r'<section[^>]*id=["\']?commands["\']?[^>]*>(.*?)</section>'
        match = re.search(commands_pattern, self.html_content, re.IGNORECASE | re.DOTALL)
        self.assertIsNotNone(match, "Commands section must exist")

        commands_content = match.group(1).lower()
        # Check for retrieve/get value semantics
        has_retrieve = 'retrieve' in commands_content
        has_get_value = 'value' in commands_content and 'get' in commands_content
        self.assertTrue(
            has_retrieve or has_get_value,
            "get command description must mention retrieving values"
        )


class TestSetCommandDocumentation(unittest.TestCase):
    """Test Case 3: Homepage lists 'set' command with description for storing key-value pairs."""

    @classmethod
    def setUpClass(cls):
        """Load the index.html file for testing."""
        cls.html_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'index.html')
        cls.html_content = load_html_file(cls.html_path)

    def test_set_command_listed(self):
        """Test that 'set' command is listed."""
        commands_pattern = r'<section[^>]*id=["\']?commands["\']?[^>]*>(.*?)</section>'
        match = re.search(commands_pattern, self.html_content, re.IGNORECASE | re.DOTALL)
        self.assertIsNotNone(match, "Commands section must exist")

        commands_content = match.group(1).lower()
        self.assertIn('set', commands_content, "Commands section must list 'set' command")

    def test_set_command_has_code_formatting(self):
        """Test that 'set' command is displayed with code formatting."""
        commands_pattern = r'<section[^>]*id=["\']?commands["\']?[^>]*>(.*?)</section>'
        match = re.search(commands_pattern, self.html_content, re.IGNORECASE | re.DOTALL)
        self.assertIsNotNone(match, "Commands section must exist")

        commands_content = match.group(1)
        # Check for <code>set</code>
        set_code_pattern = r'<code[^>]*>\s*set\s*</code>'
        set_code_match = re.search(set_code_pattern, commands_content, re.IGNORECASE)
        self.assertIsNotNone(
            set_code_match,
            "set command must be displayed with <code> formatting"
        )

    def test_set_command_description_mentions_store(self):
        """Test that 'set' command description mentions storing key-value pairs."""
        commands_pattern = r'<section[^>]*id=["\']?commands["\']?[^>]*>(.*?)</section>'
        match = re.search(commands_pattern, self.html_content, re.IGNORECASE | re.DOTALL)
        self.assertIsNotNone(match, "Commands section must exist")

        commands_content = match.group(1).lower()
        # Check for store semantics
        has_store = 'store' in commands_content
        has_key_value = 'key-value' in commands_content or ('key' in commands_content and 'value' in commands_content)
        self.assertTrue(
            has_store or has_key_value,
            "set command description must mention storing key-value pairs"
        )


class TestDeleteCommandDocumentation(unittest.TestCase):
    """Test Case 4: Homepage lists 'delete' command with description for removing keys."""

    @classmethod
    def setUpClass(cls):
        """Load the index.html file for testing."""
        cls.html_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'index.html')
        cls.html_content = load_html_file(cls.html_path)

    def test_delete_command_listed(self):
        """Test that 'delete' command is listed."""
        commands_pattern = r'<section[^>]*id=["\']?commands["\']?[^>]*>(.*?)</section>'
        match = re.search(commands_pattern, self.html_content, re.IGNORECASE | re.DOTALL)
        self.assertIsNotNone(match, "Commands section must exist")

        commands_content = match.group(1).lower()
        self.assertIn('delete', commands_content, "Commands section must list 'delete' command")

    def test_delete_command_has_code_formatting(self):
        """Test that 'delete' command is displayed with code formatting."""
        commands_pattern = r'<section[^>]*id=["\']?commands["\']?[^>]*>(.*?)</section>'
        match = re.search(commands_pattern, self.html_content, re.IGNORECASE | re.DOTALL)
        self.assertIsNotNone(match, "Commands section must exist")

        commands_content = match.group(1)
        # Check for <code>delete</code>
        delete_code_pattern = r'<code[^>]*>\s*delete\s*</code>'
        delete_code_match = re.search(delete_code_pattern, commands_content, re.IGNORECASE)
        self.assertIsNotNone(
            delete_code_match,
            "delete command must be displayed with <code> formatting"
        )

    def test_delete_command_description_mentions_remove(self):
        """Test that 'delete' command description mentions removing keys."""
        commands_pattern = r'<section[^>]*id=["\']?commands["\']?[^>]*>(.*?)</section>'
        match = re.search(commands_pattern, self.html_content, re.IGNORECASE | re.DOTALL)
        self.assertIsNotNone(match, "Commands section must exist")

        commands_content = match.group(1).lower()
        # Check for remove semantics
        has_remove = 'remove' in commands_content
        has_delete_key = 'delete' in commands_content and ('key' in commands_content or 'pair' in commands_content)
        self.assertTrue(
            has_remove or has_delete_key,
            "delete command description must mention removing keys"
        )


class TestAdditionalSetterCommands(unittest.TestCase):
    """Test Case 5: Homepage lists add, replace, append, prepend commands."""

    @classmethod
    def setUpClass(cls):
        """Load the index.html file for testing."""
        cls.html_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'index.html')
        cls.html_content = load_html_file(cls.html_path)
        # Extract commands section
        commands_pattern = r'<section[^>]*id=["\']?commands["\']?[^>]*>(.*?)</section>'
        match = re.search(commands_pattern, cls.html_content, re.IGNORECASE | re.DOTALL)
        cls.commands_content = match.group(1).lower() if match else ""

    def test_add_command_listed(self):
        """Test that 'add' command is listed."""
        self.assertIn('add', self.commands_content, "Commands section must list 'add' command")

    def test_replace_command_listed(self):
        """Test that 'replace' command is listed."""
        self.assertIn('replace', self.commands_content, "Commands section must list 'replace' command")

    def test_append_command_listed(self):
        """Test that 'append' command is listed."""
        self.assertIn('append', self.commands_content, "Commands section must list 'append' command")

    def test_prepend_command_listed(self):
        """Test that 'prepend' command is listed."""
        self.assertIn('prepend', self.commands_content, "Commands section must list 'prepend' command")

    def test_all_setter_commands_have_code_formatting(self):
        """Test that all setter commands are displayed with code formatting."""
        commands_pattern = r'<section[^>]*id=["\']?commands["\']?[^>]*>(.*?)</section>'
        match = re.search(commands_pattern, self.html_content, re.IGNORECASE | re.DOTALL)
        commands_content = match.group(1) if match else ""

        setter_commands = ['add', 'replace', 'append', 'prepend']
        for cmd in setter_commands:
            code_pattern = rf'<code[^>]*>\s*{cmd}\s*</code>'
            code_match = re.search(code_pattern, commands_content, re.IGNORECASE)
            self.assertIsNotNone(
                code_match,
                f"{cmd} command must be displayed with <code> formatting"
            )


class TestGetsCommandDocumentation(unittest.TestCase):
    """Test Case 6: Homepage lists 'gets' command with CAS token description."""

    @classmethod
    def setUpClass(cls):
        """Load the index.html file for testing."""
        cls.html_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'index.html')
        cls.html_content = load_html_file(cls.html_path)

    def test_gets_command_listed(self):
        """Test that 'gets' command is listed."""
        commands_pattern = r'<section[^>]*id=["\']?commands["\']?[^>]*>(.*?)</section>'
        match = re.search(commands_pattern, self.html_content, re.IGNORECASE | re.DOTALL)
        self.assertIsNotNone(match, "Commands section must exist")

        commands_content = match.group(1).lower()
        self.assertIn('gets', commands_content, "Commands section must list 'gets' command")

    def test_gets_command_has_code_formatting(self):
        """Test that 'gets' command is displayed with code formatting."""
        commands_pattern = r'<section[^>]*id=["\']?commands["\']?[^>]*>(.*?)</section>'
        match = re.search(commands_pattern, self.html_content, re.IGNORECASE | re.DOTALL)
        self.assertIsNotNone(match, "Commands section must exist")

        commands_content = match.group(1)
        # Check for <code>gets</code>
        gets_code_pattern = r'<code[^>]*>\s*gets\s*</code>'
        gets_code_match = re.search(gets_code_pattern, commands_content, re.IGNORECASE)
        self.assertIsNotNone(
            gets_code_match,
            "gets command must be displayed with <code> formatting"
        )

    def test_gets_command_description_mentions_cas(self):
        """Test that 'gets' command description mentions CAS token."""
        commands_pattern = r'<section[^>]*id=["\']?commands["\']?[^>]*>(.*?)</section>'
        match = re.search(commands_pattern, self.html_content, re.IGNORECASE | re.DOTALL)
        self.assertIsNotNone(match, "Commands section must exist")

        commands_content = match.group(1).lower()
        # Check for CAS token mention
        has_cas = 'cas' in commands_content
        self.assertTrue(
            has_cas,
            "gets command description must mention CAS token"
        )


class TestCommandSyntaxExamples(unittest.TestCase):
    """Test Case 7: At least some commands include syntax examples."""

    @classmethod
    def setUpClass(cls):
        """Load the index.html file for testing."""
        cls.html_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'index.html')
        cls.html_content = load_html_file(cls.html_path)

    def test_page_has_command_syntax_examples(self):
        """Test that the page includes command syntax examples."""
        html_lower = self.html_content.lower()

        # Look for command syntax patterns that include arguments
        # Memcached commands with arguments follow patterns like:
        # set <key> <flags> <ttl> <bytes>
        # get <key>
        # Check for either formal syntax or usage examples

        # Check for formal syntax in commands section or getting-started
        has_set_syntax = re.search(r'set\s+\S+\s+\d+\s+\d+\s+\d+', self.html_content, re.IGNORECASE)
        has_get_syntax = re.search(r'get\s+\S+', self.html_content, re.IGNORECASE)

        # Also check for code blocks with command examples
        has_pre_code = '<pre' in self.html_content.lower() and '<code' in self.html_content.lower()

        # Verify we have at least some syntax examples
        syntax_found = has_set_syntax or has_get_syntax or has_pre_code
        self.assertTrue(
            syntax_found,
            "Page must include at least some command syntax examples"
        )

    def test_getting_started_has_command_examples(self):
        """Test that the getting started section has command usage examples."""
        # Extract getting-started section
        getting_started_pattern = r'<section[^>]*id=["\']?getting-started["\']?[^>]*>(.*?)</section>'
        match = re.search(getting_started_pattern, self.html_content, re.IGNORECASE | re.DOTALL)
        self.assertIsNotNone(match, "Getting started section must exist")

        getting_started_content = match.group(1).lower()

        # Check for set/get command examples
        has_set_example = 'set ' in getting_started_content
        has_get_example = 'get ' in getting_started_content

        self.assertTrue(
            has_set_example or has_get_example,
            "Getting started section must have command usage examples"
        )

    def test_commands_have_descriptions(self):
        """Test that all commands have descriptions (not just names)."""
        commands_pattern = r'<section[^>]*id=["\']?commands["\']?[^>]*>(.*?)</section>'
        match = re.search(commands_pattern, self.html_content, re.IGNORECASE | re.DOTALL)
        self.assertIsNotNone(match, "Commands section must exist")

        commands_content = match.group(1)

        # If using table format, check that we have both th/td for command and description
        if '<table' in commands_content.lower():
            # Count td elements - should be at least 2 per row (command + description)
            td_count = commands_content.lower().count('<td')
            tr_count = commands_content.lower().count('<tr')
            # Expect at least 8 commands (get, gets, set, add, replace, append, prepend, delete)
            self.assertGreaterEqual(
                tr_count, 8,
                "Commands table should have at least 8 rows for all supported commands"
            )
            # Each row should have 2 cells (command + description)
            self.assertGreaterEqual(
                td_count, 16,
                "Commands table should have command name and description for each command"
            )


if __name__ == '__main__':
    unittest.main(verbosity=2)
