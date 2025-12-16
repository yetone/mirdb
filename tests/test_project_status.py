"""
Test suite for MirDB Homepage Project Status Section.

This module tests the project status section requirements for the MirDB homepage,
verifying that completed features and planned features are properly displayed
per REQ-7 requirements.
"""

import unittest
import os
import re


def load_file(filepath):
    """Load and return the contents of a file."""
    with open(filepath, 'r', encoding='utf-8') as f:
        return f.read()


class TestProjectStatusSectionExists(unittest.TestCase):
    """Test Case 1: Verify homepage contains a section about project status."""

    @classmethod
    def setUpClass(cls):
        """Load the index.html file for testing."""
        cls.html_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'index.html')
        cls.html_content = load_file(cls.html_path)

    def test_status_section_exists(self):
        """Test that a project status or roadmap section exists."""
        # Check for a section with id 'status' or 'roadmap' or containing those keywords
        status_pattern = r'<section[^>]*id=["\']?(status|roadmap)["\']?[^>]*>'
        match = re.search(status_pattern, self.html_content, re.IGNORECASE)
        self.assertIsNotNone(
            match,
            "Homepage must have a section with id='status' or id='roadmap'"
        )

    def test_status_section_has_heading(self):
        """Test that the status section has an appropriate heading."""
        # Extract the status section content
        section_pattern = r'<section[^>]*id=["\']?status["\']?[^>]*>(.*?)</section>'
        section_match = re.search(section_pattern, self.html_content, re.IGNORECASE | re.DOTALL)
        self.assertIsNotNone(section_match, "Status section must exist")

        section_content = section_match.group(1)

        # Check for a heading containing 'status' or 'roadmap'
        heading_pattern = r'<h[1-6][^>]*>([^<]*(?:status|roadmap)[^<]*)</h[1-6]>'
        heading_match = re.search(heading_pattern, section_content, re.IGNORECASE)
        self.assertIsNotNone(
            heading_match,
            "Status section must have a heading mentioning 'Status' or 'Roadmap'"
        )


class TestCompletedFeaturesIndicator(unittest.TestCase):
    """Test Case 2: Verify completed features are clearly marked or listed."""

    @classmethod
    def setUpClass(cls):
        """Load the index.html file for testing."""
        cls.html_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'index.html')
        cls.html_content = load_file(cls.html_path)

    def test_completed_features_have_visual_indicator(self):
        """Test that completed features have a visual indicator (checkmark or similar)."""
        # Extract the status section content
        section_pattern = r'<section[^>]*id=["\']?status["\']?[^>]*>(.*?)</section>'
        section_match = re.search(section_pattern, self.html_content, re.IGNORECASE | re.DOTALL)
        self.assertIsNotNone(section_match, "Status section must exist")

        section_content = section_match.group(1)

        # Check for completed class or checkmark indicator
        completed_patterns = [
            r'class=["\'][^"\']*completed[^"\']*["\']',  # CSS class 'completed'
            r'&#10003;',  # HTML entity checkmark
            r'✓',  # Unicode checkmark
            r'✔',  # Heavy checkmark
            r'\[x\]',  # Markdown-style checkbox
        ]

        found = False
        for pattern in completed_patterns:
            if re.search(pattern, section_content, re.IGNORECASE):
                found = True
                break

        self.assertTrue(
            found,
            "Completed features must have a visual indicator (checkmark or 'completed' class)"
        )

    def test_multiple_completed_features_listed(self):
        """Test that multiple completed features are listed."""
        # Extract the status section content
        section_pattern = r'<section[^>]*id=["\']?status["\']?[^>]*>(.*?)</section>'
        section_match = re.search(section_pattern, self.html_content, re.IGNORECASE | re.DOTALL)
        self.assertIsNotNone(section_match, "Status section must exist")

        section_content = section_match.group(1)

        # Count completed items
        completed_count = len(re.findall(r'class=["\'][^"\']*completed[^"\']*["\']', section_content, re.IGNORECASE))

        self.assertGreater(
            completed_count, 0,
            "At least one completed feature must be listed"
        )


class TestPlannedFeaturesRaft(unittest.TestCase):
    """Test Case 3: Verify planned features like Raft consensus are mentioned."""

    @classmethod
    def setUpClass(cls):
        """Load the index.html file for testing."""
        cls.html_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'index.html')
        cls.html_content = load_file(cls.html_path)

    def test_raft_consensus_mentioned(self):
        """Test that Raft consensus is mentioned as a planned feature."""
        # Extract the status section content
        section_pattern = r'<section[^>]*id=["\']?status["\']?[^>]*>(.*?)</section>'
        section_match = re.search(section_pattern, self.html_content, re.IGNORECASE | re.DOTALL)
        self.assertIsNotNone(section_match, "Status section must exist")

        section_content = section_match.group(1)

        # Check for Raft mention
        self.assertIn(
            'raft',
            section_content.lower(),
            "Raft consensus must be mentioned in the status section"
        )

    def test_planned_features_indicated(self):
        """Test that planned features have a 'planned' indicator."""
        # Extract the status section content
        section_pattern = r'<section[^>]*id=["\']?status["\']?[^>]*>(.*?)</section>'
        section_match = re.search(section_pattern, self.html_content, re.IGNORECASE | re.DOTALL)
        self.assertIsNotNone(section_match, "Status section must exist")

        section_content = section_match.group(1)

        # Check for 'planned' text or class
        planned_patterns = [
            r'\bplanned\b',
            r'class=["\'][^"\']*planned[^"\']*["\']',
            r'upcoming',
            r'future',
        ]

        found = False
        for pattern in planned_patterns:
            if re.search(pattern, section_content, re.IGNORECASE):
                found = True
                break

        self.assertTrue(
            found,
            "Planned features must be indicated with 'planned', 'upcoming', or similar text"
        )


class TestMemtableCompletionStatus(unittest.TestCase):
    """Test Case 4: Verify Memtable is shown as a completed feature."""

    @classmethod
    def setUpClass(cls):
        """Load the index.html file for testing."""
        cls.html_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'index.html')
        cls.html_content = load_file(cls.html_path)

    def test_memtable_in_status_section(self):
        """Test that Memtable is mentioned in the status section."""
        # Extract the status section content
        section_pattern = r'<section[^>]*id=["\']?status["\']?[^>]*>(.*?)</section>'
        section_match = re.search(section_pattern, self.html_content, re.IGNORECASE | re.DOTALL)
        self.assertIsNotNone(section_match, "Status section must exist")

        section_content = section_match.group(1)

        self.assertIn(
            'memtable',
            section_content.lower(),
            "Memtable must be mentioned in the status section"
        )

    def test_memtable_marked_as_completed(self):
        """Test that Memtable is marked as a completed feature."""
        # Extract the status section content
        section_pattern = r'<section[^>]*id=["\']?status["\']?[^>]*>(.*?)</section>'
        section_match = re.search(section_pattern, self.html_content, re.IGNORECASE | re.DOTALL)
        self.assertIsNotNone(section_match, "Status section must exist")

        section_content = section_match.group(1)

        # Check that Memtable is in a completed item
        # Look for a completed status item containing 'memtable'
        completed_item_pattern = r'<div[^>]*class=["\'][^"\']*status-item[^"\']*completed[^"\']*["\'][^>]*>[^<]*<[^>]*>[^<]*</[^>]*>[^<]*<span[^>]*>([^<]*memtable[^<]*)</span>'
        match = re.search(completed_item_pattern, section_content, re.IGNORECASE | re.DOTALL)

        # Alternative: check for completed class near memtable text
        if not match:
            # Check if memtable appears in a completed context
            completed_memtable = re.search(
                r'class=["\'][^"\']*completed[^"\']*["\'][^>]*>.*?memtable',
                section_content,
                re.IGNORECASE | re.DOTALL
            )
            self.assertIsNotNone(
                completed_memtable,
                "Memtable must be marked as completed"
            )
        else:
            self.assertIsNotNone(match, "Memtable must be in a completed status item")


class TestCompactionCompletionStatus(unittest.TestCase):
    """Test Case 5: Verify Minor and major compaction are shown as completed."""

    @classmethod
    def setUpClass(cls):
        """Load the index.html file for testing."""
        cls.html_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'index.html')
        cls.html_content = load_file(cls.html_path)

    def test_minor_compaction_in_status_section(self):
        """Test that minor compaction is mentioned in the status section."""
        # Extract the status section content
        section_pattern = r'<section[^>]*id=["\']?status["\']?[^>]*>(.*?)</section>'
        section_match = re.search(section_pattern, self.html_content, re.IGNORECASE | re.DOTALL)
        self.assertIsNotNone(section_match, "Status section must exist")

        section_content = section_match.group(1)

        self.assertIn(
            'minor compaction',
            section_content.lower(),
            "Minor compaction must be mentioned in the status section"
        )

    def test_major_compaction_in_status_section(self):
        """Test that major compaction is mentioned in the status section."""
        # Extract the status section content
        section_pattern = r'<section[^>]*id=["\']?status["\']?[^>]*>(.*?)</section>'
        section_match = re.search(section_pattern, self.html_content, re.IGNORECASE | re.DOTALL)
        self.assertIsNotNone(section_match, "Status section must exist")

        section_content = section_match.group(1)

        self.assertIn(
            'major compaction',
            section_content.lower(),
            "Major compaction must be mentioned in the status section"
        )

    def test_compaction_marked_as_completed(self):
        """Test that compaction features are marked as completed."""
        # Extract the status section content
        section_pattern = r'<section[^>]*id=["\']?status["\']?[^>]*>(.*?)</section>'
        section_match = re.search(section_pattern, self.html_content, re.IGNORECASE | re.DOTALL)
        self.assertIsNotNone(section_match, "Status section must exist")

        section_content = section_match.group(1)

        # Check for completed class items containing compaction
        minor_completed = re.search(
            r'class=["\'][^"\']*completed[^"\']*["\'][^>]*>.*?minor.*?compaction',
            section_content,
            re.IGNORECASE | re.DOTALL
        )
        major_completed = re.search(
            r'class=["\'][^"\']*completed[^"\']*["\'][^>]*>.*?major.*?compaction',
            section_content,
            re.IGNORECASE | re.DOTALL
        )

        self.assertIsNotNone(
            minor_completed,
            "Minor compaction must be marked as completed"
        )
        self.assertIsNotNone(
            major_completed,
            "Major compaction must be marked as completed"
        )


class TestDistinctionCompletedPlanned(unittest.TestCase):
    """Test Case 6: Verify clear distinction between completed and planned features."""

    @classmethod
    def setUpClass(cls):
        """Load the index.html file for testing."""
        cls.html_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'index.html')
        cls.html_content = load_file(cls.html_path)

    def test_different_classes_for_completed_and_planned(self):
        """Test that completed and planned features use different CSS classes."""
        # Extract the status section content
        section_pattern = r'<section[^>]*id=["\']?status["\']?[^>]*>(.*?)</section>'
        section_match = re.search(section_pattern, self.html_content, re.IGNORECASE | re.DOTALL)
        self.assertIsNotNone(section_match, "Status section must exist")

        section_content = section_match.group(1)

        # Check for both completed and planned classes
        has_completed_class = re.search(
            r'class=["\'][^"\']*completed[^"\']*["\']',
            section_content,
            re.IGNORECASE
        )
        has_planned_class = re.search(
            r'class=["\'][^"\']*planned[^"\']*["\']',
            section_content,
            re.IGNORECASE
        )

        self.assertIsNotNone(
            has_completed_class,
            "Status section must have items with 'completed' class"
        )
        self.assertIsNotNone(
            has_planned_class,
            "Status section must have items with 'planned' class"
        )

    def test_different_icons_for_completed_and_planned(self):
        """Test that completed and planned features use different icons."""
        # Extract the status section content
        section_pattern = r'<section[^>]*id=["\']?status["\']?[^>]*>(.*?)</section>'
        section_match = re.search(section_pattern, self.html_content, re.IGNORECASE | re.DOTALL)
        self.assertIsNotNone(section_match, "Status section must exist")

        section_content = section_match.group(1)

        # Check for checkmark icon (completed)
        checkmark_patterns = [r'&#10003;', r'✓', r'✔']
        has_checkmark = False
        for pattern in checkmark_patterns:
            if re.search(pattern, section_content):
                has_checkmark = True
                break

        # Check for open circle icon (planned)
        open_circle_patterns = [r'&#9675;', r'○', r'◯']
        has_open_circle = False
        for pattern in open_circle_patterns:
            if re.search(pattern, section_content):
                has_open_circle = True
                break

        self.assertTrue(
            has_checkmark,
            "Completed features must have a checkmark icon"
        )
        self.assertTrue(
            has_open_circle,
            "Planned features must have an open circle icon"
        )

    def test_visual_distinction_in_styling(self):
        """Test that there is clear visual distinction between completed and planned."""
        # Extract the status section content
        section_pattern = r'<section[^>]*id=["\']?status["\']?[^>]*>(.*?)</section>'
        section_match = re.search(section_pattern, self.html_content, re.IGNORECASE | re.DOTALL)
        self.assertIsNotNone(section_match, "Status section must exist")

        section_content = section_match.group(1)

        # Count items with different classes
        completed_items = len(re.findall(
            r'class=["\'][^"\']*status-item[^"\']*completed[^"\']*["\']',
            section_content,
            re.IGNORECASE
        ))
        planned_items = len(re.findall(
            r'class=["\'][^"\']*status-item[^"\']*planned[^"\']*["\']',
            section_content,
            re.IGNORECASE
        ))

        self.assertGreater(
            completed_items, 0,
            "There must be at least one completed status item"
        )
        self.assertGreater(
            planned_items, 0,
            "There must be at least one planned status item"
        )


if __name__ == '__main__':
    unittest.main(verbosity=2)
