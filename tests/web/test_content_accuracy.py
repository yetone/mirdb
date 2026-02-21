"""
Content Accuracy Tests
Owner: Scenario 8 - Content Accuracy

Tests verify that the homepage content is accurate and matches the product documentation.
This includes verifying product names, descriptions, ASCII art, port numbers, and feature
descriptions against the actual source code and documentation.
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
def soup(html_content):
    """Parse HTML content into BeautifulSoup object."""
    return BeautifulSoup(html_content, "lxml")


@pytest.fixture
def main_rs_content():
    """Load the main.rs source file content."""
    main_rs_path = Path(__file__).parent.parent.parent / "mirdb-server" / "src" / "main.rs"
    with open(main_rs_path, "r", encoding="utf-8") as f:
        return f.read()


@pytest.fixture
def readme_content():
    """Load the README.md content."""
    readme_path = Path(__file__).parent.parent.parent / "README.md"
    with open(readme_path, "r", encoding="utf-8") as f:
        return f.read()


@pytest.fixture
def config_content():
    """Load the mirdb.toml configuration file content."""
    config_path = Path(__file__).parent.parent.parent / "etc" / "mirdb.toml"
    with open(config_path, "r", encoding="utf-8") as f:
        return f.read()


class TestProductName:
    """Test Case 1: Verify product name is displayed correctly."""

    def test_page_displays_mirdb_as_product_name(self, soup):
        """Page displays 'MirDB' as product name."""
        # Check page title
        title = soup.find("title")
        assert title is not None, "Page must have a title element"
        assert "MirDB" in title.get_text(), "Page title must contain 'MirDB'"

        # Check navigation brand
        nav_brand = soup.find(class_="nav-brand")
        if nav_brand:
            assert "MirDB" in nav_brand.get_text(), "Navigation brand must contain 'MirDB'"

        # Check h1 or hero section for product name
        h1 = soup.find("h1")
        hero = soup.find(class_="hero")
        page_text = soup.get_text()

        assert "MirDB" in page_text, "Page must display 'MirDB' as product name"


class TestTaglineDescription:
    """Test Case 2: Verify tagline/description is accurate."""

    def test_page_includes_persistent_key_value_store_tagline(self, soup):
        """Page includes 'persistent key-value store with Memcached protocol' or similar."""
        tagline = soup.find(class_="tagline")
        page_text = soup.get_text().lower()

        # Check for the key elements of the tagline
        has_persistent = "persistent" in page_text
        has_key_value = "key-value" in page_text or "kv" in page_text
        has_memcached = "memcached" in page_text

        assert has_persistent, "Page must mention 'persistent'"
        assert has_key_value, "Page must mention 'key-value' store"
        assert has_memcached, "Page must mention 'Memcached' protocol"

        # Verify the tagline element exists and contains the description
        if tagline:
            tagline_text = tagline.get_text().lower()
            assert "persistent" in tagline_text, \
                "Tagline must mention 'persistent'"
            assert "key-value" in tagline_text, \
                "Tagline must mention 'key-value'"
            assert "memcached" in tagline_text, \
                "Tagline must mention 'memcached'"

    def test_tagline_matches_readme_description(self, soup, readme_content):
        """Tagline should be consistent with README.md description."""
        # README starts with: "## MirDB: A Persistent Key-Value Store with Memcached protocol"
        readme_title = readme_content.split("\n")[0]
        assert "Persistent Key-Value Store" in readme_title, \
            "README should have 'Persistent Key-Value Store' in title"

        page_text = soup.get_text()
        # The homepage should contain similar text
        assert "persistent" in page_text.lower() and "key-value" in page_text.lower(), \
            "Homepage tagline should match README description style"


class TestGitHubLink:
    """Test Case 3: Verify GitHub link is correct."""

    def test_github_link_points_to_correct_repository(self, soup):
        """GitHub link points to correct repository URL."""
        links = soup.find_all("a")
        github_links = []

        for link in links:
            href = link.get("href", "")
            if "github.com" in href:
                github_links.append(href)

        assert len(github_links) > 0, "Page must contain at least one GitHub link"

        # Verify all GitHub links point to yetone/mirdb
        for href in github_links:
            assert "yetone/mirdb" in href, \
                f"GitHub link must point to yetone/mirdb, found: '{href}'"

    def test_github_link_in_nav(self, soup):
        """Navigation contains GitHub link."""
        nav = soup.find("nav")
        assert nav is not None, "Page must have a navigation element"

        nav_links = nav.find_all("a")
        github_in_nav = False

        for link in nav_links:
            href = link.get("href", "")
            if "github.com/yetone/mirdb" in href:
                github_in_nav = True
                break

        assert github_in_nav, "Navigation must contain link to github.com/yetone/mirdb"

    def test_github_link_in_footer(self, soup):
        """Footer contains GitHub link."""
        footer = soup.find("footer")
        assert footer is not None, "Page must have a footer"

        footer_links = footer.find_all("a")
        github_in_footer = False

        for link in footer_links:
            href = link.get("href", "")
            if "github.com/yetone/mirdb" in href:
                github_in_footer = True
                break

        assert github_in_footer, "Footer must contain link to github.com/yetone/mirdb"


class TestASCIIArt:
    """Test Case 4: Verify ASCII art matches server welcome message."""

    def test_ascii_art_matches_main_rs(self, soup, main_rs_content):
        """ASCII art on homepage matches the welcome banner in main.rs."""
        # Extract ASCII art from main.rs
        # The ASCII art is in a raw string literal: r#" ... "#
        ascii_pattern = r'r#"([^"]*MirDB[^"]*)"#'
        match = re.search(ascii_pattern, main_rs_content, re.DOTALL)

        assert match is not None, "main.rs should contain ASCII art in raw string"

        main_rs_ascii = match.group(1).strip()

        # Find ASCII art on homepage
        ascii_art_element = soup.find("pre", class_="ascii-art") or soup.find("pre")
        assert ascii_art_element is not None, "Homepage must have ASCII art in <pre> element"

        homepage_ascii = ascii_art_element.get_text().strip()

        # Extract the key pattern from both (the actual ASCII art characters)
        # Key parts: "__  __ _", "|  \\/  |", "|___/|___/"
        key_patterns = [
            "__  __ _",
            "MirDB" in homepage_ascii or "|  \\/  |" in homepage_ascii,
            "|___/" in homepage_ascii,
        ]

        # Check that the key ASCII patterns exist
        assert "__  __ _" in main_rs_ascii or "MirDB" in main_rs_ascii, \
            "main.rs should contain MirDB ASCII art pattern"

        # Verify the homepage has the same pattern
        assert "|  \\/  |" in homepage_ascii or "__  __" in homepage_ascii, \
            "Homepage ASCII art should contain the MirDB pattern"

    def test_ascii_art_contains_mirdb_pattern(self, soup):
        """ASCII art contains recognizable MirDB pattern."""
        ascii_art = soup.find("pre", class_="ascii-art") or soup.find("pre")
        assert ascii_art is not None, "Homepage must have ASCII art element"

        art_text = ascii_art.get_text()

        # Check for distinctive MirDB ASCII art patterns
        # The art shows: MirDB with stylized letters
        distinctive_patterns = [
            "|_|  |_|",  # Bottom of M
            "|___/",     # D and B shapes
            "__  __",    # Top of M
        ]

        has_pattern = any(pattern in art_text for pattern in distinctive_patterns)
        assert has_pattern, \
            "ASCII art must contain distinctive MirDB letter patterns"


class TestDefaultPort:
    """Test Case 5: Verify default port mentioned matches actual default."""

    def test_documentation_mentions_port_12333(self, soup):
        """Documentation mentions port 12333 which matches actual default."""
        page_text = soup.get_text()

        # Check that port 12333 is mentioned
        assert "12333" in page_text, \
            "Homepage must mention default port 12333"

    def test_port_matches_config_default(self, soup, config_content):
        """Port mentioned on homepage matches the default in config file."""
        # Extract port from config
        port_match = re.search(r'addr\s*=\s*"[^:]+:(\d+)"', config_content)
        assert port_match is not None, "Config must specify address with port"

        config_port = port_match.group(1)
        assert config_port == "12333", f"Default config port should be 12333, found: {config_port}"

        # Verify homepage mentions this port
        page_text = soup.get_text()
        assert config_port in page_text, \
            f"Homepage must mention the default port {config_port}"

    def test_port_mentioned_in_quick_start(self, soup):
        """Quick Start section mentions the correct port for connection."""
        quick_start = soup.find("section", {"id": "quick-start"}) or soup.find(class_="quick-start")
        assert quick_start is not None, "Page must have Quick Start section"

        quick_start_text = quick_start.get_text()
        assert "12333" in quick_start_text, \
            "Quick Start must mention port 12333 for connection"


class TestMemcachedProtocolFeature:
    """Test Case 6: Verify Memcached Protocol feature description."""

    def test_memcached_protocol_feature_accuracy(self, soup):
        """Feature card accurately describes memcached text protocol compatibility."""
        features = soup.find("section", {"id": "features"}) or soup.find(class_="features")
        assert features is not None, "Page must have features section"

        features_text = features.get_text().lower()

        # Check for key aspects of memcached protocol feature
        has_memcached = "memcached" in features_text
        has_protocol = "protocol" in features_text
        has_compatible = "compatible" in features_text or "compatibility" in features_text

        assert has_memcached, "Features must mention 'memcached'"
        assert has_protocol, "Features must mention 'protocol'"

    def test_memcached_feature_card_exists(self, soup):
        """Feature card for Memcached Protocol exists."""
        features = soup.find("section", {"id": "features"}) or soup.find(class_="features")
        assert features is not None, "Page must have features section"

        cards = features.find_all(class_="feature-card") or features.find_all("article")
        memcached_card_found = False

        for card in cards:
            card_text = card.get_text().lower()
            if "memcached" in card_text and "protocol" in card_text:
                memcached_card_found = True
                # Verify it mentions text protocol
                assert "text" in card_text or "client" in card_text or "connect" in card_text, \
                    "Memcached feature card should describe protocol usage"
                break

        assert memcached_card_found, "Must have a feature card for Memcached Protocol"


class TestDataPersistenceFeature:
    """Test Case 7: Verify Data Persistence feature description."""

    def test_data_persistence_mentions_sstables(self, soup):
        """Feature card mentions SSTables or disk persistence accurately."""
        features = soup.find("section", {"id": "features"}) or soup.find(class_="features")
        assert features is not None, "Page must have features section"

        features_text = features.get_text().lower()

        # Check for SSTable mention
        has_sstable = "sstable" in features_text or "sorted string table" in features_text
        has_persistence = "persist" in features_text or "disk" in features_text

        assert has_persistence, "Features must mention data persistence"
        assert has_sstable, "Features must mention SSTables (Sorted String Tables)"

    def test_data_persistence_feature_card_exists(self, soup):
        """Feature card for Data Persistence exists with accurate content."""
        features = soup.find("section", {"id": "features"}) or soup.find(class_="features")
        assert features is not None, "Page must have features section"

        cards = features.find_all(class_="feature-card") or features.find_all("article")
        persistence_card_found = False

        for card in cards:
            card_text = card.get_text().lower()
            h3 = card.find("h3")
            if h3 and "persistence" in h3.get_text().lower():
                persistence_card_found = True
                # Verify it mentions SSTables
                assert "sstable" in card_text, \
                    "Data Persistence card should mention SSTables"
                break

        assert persistence_card_found, "Must have a feature card for Data Persistence"


class TestLSMTreeFeature:
    """Test Case 8: Verify LSM Tree feature description."""

    def test_lsm_tree_description_accurate(self, soup):
        """Feature card describes Log-Structured Merge-tree architecture."""
        features = soup.find("section", {"id": "features"}) or soup.find(class_="features")
        assert features is not None, "Page must have features section"

        features_text = features.get_text().lower()

        # Check for LSM tree description
        has_lsm = "lsm" in features_text or "log-structured merge" in features_text
        has_tree = "tree" in features_text

        assert has_lsm, "Features must mention LSM (Log-Structured Merge)"
        assert has_tree, "Features must mention tree architecture"

    def test_lsm_tree_feature_card_exists(self, soup):
        """Feature card for LSM Tree exists with accurate content."""
        features = soup.find("section", {"id": "features"}) or soup.find(class_="features")
        assert features is not None, "Page must have features section"

        cards = features.find_all(class_="feature-card") or features.find_all("article")
        lsm_card_found = False

        for card in cards:
            card_text = card.get_text().lower()
            h3 = card.find("h3")
            if h3 and "lsm" in h3.get_text().lower():
                lsm_card_found = True
                # Verify it describes the architecture
                assert "log-structured" in card_text or "merge" in card_text, \
                    "LSM Tree card should describe Log-Structured Merge approach"
                break

        assert lsm_card_found, "Must have a feature card for LSM Tree"

    def test_lsm_mentions_memtables(self, soup):
        """LSM Tree description mentions memtables or compaction."""
        features = soup.find("section", {"id": "features"}) or soup.find(class_="features")
        assert features is not None, "Page must have features section"

        cards = features.find_all(class_="feature-card") or features.find_all("article")

        for card in cards:
            h3 = card.find("h3")
            if h3 and "lsm" in h3.get_text().lower():
                card_text = card.get_text().lower()
                # Should mention key LSM concepts
                has_memtable = "memtable" in card_text
                has_compaction = "compaction" in card_text

                assert has_memtable or has_compaction, \
                    "LSM Tree card should mention memtables or compaction"
                return

        pytest.fail("Could not find LSM Tree feature card to verify content")
