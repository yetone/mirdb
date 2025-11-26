#!/bin/bash

# Hero Section Validation Script
# This script performs basic validation checks on the hero section implementation

set -e

echo "====================================="
echo "Hero Section Validation Tests"
echo "====================================="
echo ""

PASS_COUNT=0
FAIL_COUNT=0

# Test 1: Check if index.html exists
if [ -f "../index.html" ]; then
    echo "✓ index.html exists"
    PASS_COUNT=$((PASS_COUNT + 1))
else
    echo "✗ index.html not found"
    FAIL_COUNT=$((FAIL_COUNT + 1))
fi

# Test 2: Check if styles.css exists
if [ -f "../styles.css" ]; then
    echo "✓ styles.css exists"
    PASS_COUNT=$((PASS_COUNT + 1))
else
    echo "✗ styles.css not found"
    FAIL_COUNT=$((FAIL_COUNT + 1))
fi

# Test 3: Check if script.js exists
if [ -f "../script.js" ]; then
    echo "✓ script.js exists"
    PASS_COUNT=$((PASS_COUNT + 1))
else
    echo "✗ script.js not found"
    FAIL_COUNT=$((FAIL_COUNT + 1))
fi

# Test 4: Check for hero section content
echo ""
echo "Checking hero section content..."

if grep -q "MirDB" ../index.html; then
    echo "✓ Product name 'MirDB' found in HTML"
    PASS_COUNT=$((PASS_COUNT + 1))
else
    echo "✗ Product name 'MirDB' not found"
    FAIL_COUNT=$((FAIL_COUNT + 1))
fi

if grep -q "Persistent Key-Value Store with Memcached Protocol Support" ../index.html; then
    echo "✓ Tagline found in HTML"
    PASS_COUNT=$((PASS_COUNT + 1))
else
    echo "✗ Tagline not found"
    FAIL_COUNT=$((FAIL_COUNT + 1))
fi

if grep -q "logo.gif" ../index.html; then
    echo "✓ Animated logo (logo.gif) referenced"
    PASS_COUNT=$((PASS_COUNT + 1))
else
    echo "✗ Logo not referenced"
    FAIL_COUNT=$((FAIL_COUNT + 1))
fi

if grep -q "\\.hero-title" ../styles.css; then
    echo "✓ Hero title styling defined"
    PASS_COUNT=$((PASS_COUNT + 1))
else
    echo "✗ Hero title styling not defined"
    FAIL_COUNT=$((FAIL_COUNT + 1))
fi

if grep -q "\\.hero-tagline" ../styles.css; then
    echo "✓ Hero tagline styling defined"
    PASS_COUNT=$((PASS_COUNT + 1))
else
    echo "✗ Hero tagline styling not defined"
    FAIL_COUNT=$((FAIL_COUNT + 1))
fi

if grep -q "btn-primary" ../styles.css; then
    echo "✓ Primary button styling defined"
    PASS_COUNT=$((PASS_COUNT + 1))
else
    echo "✗ Primary button styling not defined"
    FAIL_COUNT=$((FAIL_COUNT + 1))
fi

if grep -q "btn-secondary" ../styles.css; then
    echo "✓ Secondary button styling defined"
    PASS_COUNT=$((PASS_COUNT + 1))
else
    echo "✗ Secondary button styling not defined"
    FAIL_COUNT=$((FAIL_COUNT + 1))
fi

# Test 5: Check for Get Started button functionality
if grep -q "get-started-btn" ../index.html; then
    echo "✓ 'Get Started' button found"
    PASS_COUNT=$((PASS_COUNT + 1))
else
    echo "✗ 'Get Started' button not found"
    FAIL_COUNT=$((FAIL_COUNT + 1))
fi

if grep -q "Get Started" ../index.html; then
    echo "✓ 'Get Started' button text found"
    PASS_COUNT=$((PASS_COUNT + 1))
else
    echo "✗ 'Get Started' button text not found"
    FAIL_COUNT=$((FAIL_COUNT + 1))
fi

# Test 6: Check for GitHub button
if grep -q "github-btn" ../index.html; then
    echo "✓ GitHub button found"
    PASS_COUNT=$((PASS_COUNT + 1))
else
    echo "✗ GitHub button not found"
    FAIL_COUNT=$((FAIL_COUNT + 1))
fi

if grep -q "View on GitHub" ../index.html; then
    echo "✓ 'View on GitHub' button text found"
    PASS_COUNT=$((PASS_COUNT + 1))
else
    echo "✗ 'View on GitHub' button text not found"
    FAIL_COUNT=$((FAIL_COUNT + 1))
fi

if grep -q "https://github.com/yetone/mirdb" ../index.html; then
    echo "✓ Correct GitHub URL reference"
    PASS_COUNT=$((PASS_COUNT + 1))
else
    echo "✗ GitHub URL not correct"
    FAIL_COUNT=$((FAIL_COUNT + 1))
fi

# Test 7: Check for smooth scroll functionality
if grep -q "scrollToSection" ../script.js; then
    echo "✓ Smooth scroll function implemented"
    PASS_COUNT=$((PASS_COUNT + 1))
else
    echo "✗ Smooth scroll function not implemented"
    FAIL_COUNT=$((FAIL_COUNT + 1))
fi

if grep -q "scroll-behavior: smooth" ../styles.css; then
    echo "✓ CSS smooth scroll enabled"
    PASS_COUNT=$((PASS_COUNT + 1))
else
    echo "✗ CSS smooth scroll not enabled"
    FAIL_COUNT=$((FAIL_COUNT + 1))
fi

# Test 8: Check for accessibility features
if grep -q 'alt=' ../index.html; then
    echo "✓ Alt attributes found (accessibility)"
    PASS_COUNT=$((PASS_COUNT + 1))
else
    echo "✗ No alt attributes found"
    FAIL_COUNT=$((FAIL_COUNT + 1))
fi

if grep -q 'prefers-reduced-motion' ../styles.css; then
    echo "✓ Reduced motion support found (accessibility)"
    PASS_COUNT=$((PASS_COUNT + 1))
else
    echo "✗ No reduced motion support"
    FAIL_COUNT=$((FAIL_COUNT + 1))
fi

# Test 9: Check for responsive design
if grep -q '@media' ../styles.css; then
    echo "✓ Responsive media queries found"
    PASS_COUNT=$((PASS_COUNT + 1))
else
    echo "✗ No responsive media queries"
    FAIL_COUNT=$((FAIL_COUNT + 1))
fi

# Summary
echo ""
echo "====================================="
echo "TEST SUMMARY"
echo "====================================="
echo "Tests passed: $PASS_COUNT"
echo "Tests failed: $FAIL_COUNT"
echo "Total tests: $((PASS_COUNT + FAIL_COUNT))"
echo "====================================="

if [ $FAIL_COUNT -eq 0 ]; then
    echo "✓ All tests passed!"
    exit 0
else
    echo "✗ Some tests failed"
    exit 1
fi
