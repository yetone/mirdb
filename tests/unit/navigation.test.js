/**
 * Unit tests for Navigation Functionality
 * Scenario 17: Verify all navigation links work correctly and point to intended destinations
 *
 * Test Cases:
 * TC1 (unit): Check for navigation element - Page has navigation menu or section links
 */

const { test, describe } = require('node:test');
const assert = require('node:assert');
const fs = require('fs');
const path = require('path');

const indexHtmlPath = path.resolve(__dirname, '../../index.html');

describe('Navigation Functionality Unit Tests', () => {
  let html;

  // Load HTML once before all tests
  test.beforeEach(() => {
    html = fs.readFileSync(indexHtmlPath, 'utf8');
  });

  // Test Case 1: Check for navigation element
  test('TC1: Page has navigation menu or section links', () => {
    // Check for anchor links to sections (href="#...")
    const anchorLinkRegex = /<a[^>]*href=["']#[^"']+["'][^>]*>/gi;
    const anchorMatches = html.match(anchorLinkRegex) || [];

    // Check for nav element
    const hasNavElement = /<nav[^>]*>/i.test(html);

    // Check for navigation class or id
    const hasNavigationClass = /class=["'][^"']*nav[^"']*["']/i.test(html);
    const hasNavigationId = /id=["'][^"']*nav[^"']*["']/i.test(html);

    // Page should have at least one type of navigation:
    // - Anchor links to sections (like #getting-started, #features)
    // - Or a nav element
    // - Or navigation class/id
    const hasNavigation =
      anchorMatches.length > 0 ||
      hasNavElement ||
      hasNavigationClass ||
      hasNavigationId;

    assert.ok(
      hasNavigation,
      'Page should have navigation menu or section links (anchor links, nav element, or navigation class/id)'
    );

    // If we have anchor links, verify they point to section IDs that exist
    if (anchorMatches.length > 0) {
      const sectionIdRegex = /href=["']#([^"']+)["']/gi;
      const sectionIds = [];
      let match;

      while ((match = sectionIdRegex.exec(html)) !== null) {
        sectionIds.push(match[1]);
      }

      // Verify at least one anchor link exists
      assert.ok(
        sectionIds.length > 0,
        `Expected at least one anchor link to section, found ${sectionIds.length}`
      );

      // Verify each linked section ID exists in the HTML
      sectionIds.forEach((sectionId) => {
        const idRegex = new RegExp(`id=["']${sectionId}["']`, 'i');
        assert.ok(
          idRegex.test(html),
          `Section with id="${sectionId}" should exist for anchor link`
        );
      });
    }
  });

  // Additional test: Verify hero section has CTA links to sections
  test('Hero section has navigation CTA buttons', () => {
    // Find hero section
    const heroSectionRegex = /<header[^>]*class="[^"]*hero[^"]*"[^>]*>[\s\S]*?<\/header>/gi;
    const heroMatch = html.match(heroSectionRegex);

    assert.ok(heroMatch, 'Hero section should exist');

    const heroHtml = heroMatch[0];

    // Check for "Get Started" link in hero
    assert.ok(
      /Get Started/i.test(heroHtml),
      'Hero section should have a "Get Started" CTA'
    );

    // Check that Get Started links to a section
    const getStartedLinkRegex = /<a[^>]*href=["']#[^"']+["'][^>]*>[^<]*Get Started[^<]*<\/a>/i;
    const hasGetStartedSection = getStartedLinkRegex.test(heroHtml);

    // Or check if there's any anchor link in hero
    const hasAnchorInHero = /<a[^>]*href=["']#[^"']+["'][^>]*>/i.test(heroHtml);

    assert.ok(
      hasGetStartedSection || hasAnchorInHero,
      'Hero section should have anchor links for navigation'
    );
  });

  // Verify all internal anchor links point to existing sections
  test('All internal anchor links have corresponding section IDs', () => {
    // Find all anchor links
    const anchorLinkRegex = /href=["']#([^"']+)["']/gi;
    const matches = [...html.matchAll(anchorLinkRegex)];

    assert.ok(
      matches.length > 0,
      'Page should have at least one internal anchor link'
    );

    matches.forEach((match) => {
      const sectionId = match[1];
      const idRegex = new RegExp(`id=["']${sectionId}["']`, 'i');
      assert.ok(
        idRegex.test(html),
        `Section with id="${sectionId}" should exist in the document`
      );
    });
  });

  // Verify external links have proper attributes
  test('External links have proper security attributes', () => {
    // Find all external links (starting with http)
    const externalLinkRegex = /<a[^>]*href=["'](https?:\/\/[^"']+)["'][^>]*>/gi;
    const matches = [...html.matchAll(externalLinkRegex)];

    assert.ok(
      matches.length > 0,
      'Page should have at least one external link'
    );

    // Each external link should have rel="noopener" or rel="noopener noreferrer"
    matches.forEach((match) => {
      const fullMatch = match[0];
      const url = match[1];

      const hasNoopener = /rel=["'][^"']*noopener[^"']*["']/i.test(fullMatch);
      const hasTargetBlank = /target=["']_blank["']/i.test(fullMatch);

      // External links that open in new tab should have noopener
      if (hasTargetBlank) {
        assert.ok(
          hasNoopener,
          `External link to ${url} with target="_blank" should have rel="noopener noreferrer" for security`
        );
      }
    });
  });
});
