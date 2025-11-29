#!/usr/bin/env python3
"""
Validation script to check if README.md contains all required project status sections.
This script validates the manual test cases from the scenario definition.
"""

import os
import re
import sys


def test_readme_exists():
    """Test that README.md exists"""
    if not os.path.exists('README.md'):
        print("❌ FAIL: README.md does not exist")
        return False
    print("✅ PASS: README.md exists")
    return True


def test_readme_contains_status_section():
    """Test that README.md contains project status section"""
    with open('README.md', 'r') as f:
        content = f.read()

    if '## Project Status' not in content:
        print("❌ FAIL: README.md does not contain 'Project Status' section")
        return False
    print("✅ PASS: README.md contains 'Project Status' section")
    return True


def test_readme_contains_implemented_components():
    """Test that README.md lists implemented components"""
    with open('README.md', 'r') as f:
        content = f.read()

    implemented_components = ['Server', 'Persistence', 'LSM Tree', 'Commands', 'Compaction']
    missing = []

    for component in implemented_components:
        if component.lower() not in content.lower():
            missing.append(component)

    if missing:
        print(f"❌ FAIL: README.md missing implemented component(s): {', '.join(missing)}")
        return False
    print("✅ PASS: README.md lists all major implemented components (server, persistence, LSM tree, commands, compaction)")
    return True


def test_readme_contains_roadmap_section():
    """Test that README.md contains roadmap section"""
    with open('README.md', 'r') as f:
        content = f.read()

    if '### Roadmap' not in content:
        print("❌ FAIL: README.md does not contain 'Roadmap' section")
        return False
    print("✅ PASS: README.md contains 'Roadmap' section")
    return True


def test_readme_contains_raft_consensus():
    """Test that README.md mentions Raft consensus"""
    with open('README.md', 'r') as f:
        content = f.read()

    if 'Raft' not in content and 'raft' not in content:
        print("❌ FAIL: README.md does not mention Raft consensus")
        return False
    print("✅ PASS: README.md documents planned Raft consensus feature for distributed deployment")
    return True


def test_readme_contains_contribution_guidelines():
    """Test that README.md contains contribution guidelines"""
    with open('README.md', 'r') as f:
        content = f.read()

    if '### Contributing' not in content and 'Contributing' not in content:
        print("❌ FAIL: README.md does not contain contribution guidelines")
        return False
    print("✅ PASS: README.md contains contribution guidelines section")
    return True


def test_readme_contains_github_issues_link():
    """Test that README.md contains GitHub issues link"""
    with open('README.md', 'r') as f:
        content = f.read()

    if 'github.com/yetone/mirdb/issues' not in content:
        print("❌ FAIL: README.md does not contain GitHub issues link")
        return False
    print("✅ PASS: README.md contains GitHub issues link")
    return True


def test_readme_github_issues_link_is_clickable():
    """Test that GitHub issues link is in clickable format"""
    with open('README.md', 'r') as f:
        content = f.read()

    # Check for markdown link format
    has_markdown_link = re.search(r'\[.*?\]\(https://github\.com/yetone/mirdb/issues\)', content)

    if not has_markdown_link:
        print("❌ FAIL: GitHub issues link should be in markdown clickable format: [Text](url)")
        return False
    print("✅ PASS: GitHub issues link is in clickable markdown format")
    return True


def run_all_tests():
    """Run all validation tests and report results"""
    tests = [
        test_readme_exists,
        test_readme_contains_status_section,
        test_readme_contains_implemented_components,
        test_readme_contains_roadmap_section,
        test_readme_contains_raft_consensus,
        test_readme_contains_contribution_guidelines,
        test_readme_contains_github_issues_link,
        test_readme_github_issues_link_is_clickable
    ]

    results = []
    print("Running README validation tests...\n")

    for test in tests:
        try:
            result = test()
            results.append(result)
        except Exception as e:
            print(f"❌ ERROR: {test.__name__} failed with exception: {e}")
            results.append(False)
        print()

    passed = sum(results)
    total = len(results)

    print(f"\n{'='*50}")
    print(f"Test Results: {passed}/{total} passed")
    print(f"{'='*50}\n")

    if passed == total:
        print("✅ All tests passed!")
        sys.exit(0)
    else:
        print("❌ Some tests failed")
        sys.exit(1)


if __name__ == '__main__':
    run_all_tests()
