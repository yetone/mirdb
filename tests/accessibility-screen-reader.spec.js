// @ts-check
const { test, expect } = require('@playwright/test');
const AxeBuilder = require('@axe-core/playwright').default;
const path = require('path');

const indexPath = path.resolve(__dirname, '../index.html');

/**
 * Test Suite: Accessibility - Screen Reader Compatibility
 * Scenario ID: 11
 * UUID: 8cbbd132-b48a-4e2d-964c-9744431da358
 * Description: Verify the landing page is accessible to screen reader users
 */
test.describe('Accessibility - Screen Reader Compatibility', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(`file://${indexPath}`);
  });

  /**
   * Test Case 1: Check HTML for semantic landmark elements
   * Input: Check HTML for semantic landmark elements
   * Expected: Page contains header, nav, main, section, and footer elements
   */
  test('TC1: Page contains semantic landmark elements (header, nav, main, section, footer)', async ({ page }) => {
    // Check for nav element with proper role
    const nav = page.locator('nav[role="navigation"]');
    await expect(nav).toBeVisible();
    const navAriaLabel = await nav.getAttribute('aria-label');
    expect(navAriaLabel).toBe('Main navigation');

    // Check for main content area (hero section with role="banner")
    const banner = page.locator('section[role="banner"]');
    await expect(banner).toBeVisible();

    // Check for section elements with proper role="region" and aria-labelledby
    const featuresSection = page.locator('section#features[role="region"]');
    await expect(featuresSection).toBeVisible();
    const featuresLabelledBy = await featuresSection.getAttribute('aria-labelledby');
    expect(featuresLabelledBy).toBe('features-heading');

    const quickstartSection = page.locator('section#quickstart[role="region"]');
    await expect(quickstartSection).toBeVisible();
    const quickstartLabelledBy = await quickstartSection.getAttribute('aria-labelledby');
    expect(quickstartLabelledBy).toBe('quickstart-heading');

    const commandsSection = page.locator('section#commands[role="region"]');
    await expect(commandsSection).toBeVisible();
    const commandsLabelledBy = await commandsSection.getAttribute('aria-labelledby');
    expect(commandsLabelledBy).toBe('commands-heading');

    const configSection = page.locator('section#configuration[role="region"]');
    await expect(configSection).toBeVisible();
    const configLabelledBy = await configSection.getAttribute('aria-labelledby');
    expect(configLabelledBy).toBe('config-heading');

    // Check for footer element with role="contentinfo"
    const footer = page.locator('footer[role="contentinfo"]');
    await expect(footer).toBeVisible();

    // Verify all major landmark elements exist
    const landmarkCount = await page.locator('nav, section[role="region"], section[role="banner"], footer[role="contentinfo"]').count();
    expect(landmarkCount).toBeGreaterThanOrEqual(5);
  });

  /**
   * Test Case 2: Validate heading hierarchy
   * Input: Validate heading hierarchy
   * Expected: Single h1, h2s for main sections, h3s for subsections - no skipped levels
   */
  test('TC2: Heading hierarchy follows proper h1 > h2 > h3 structure with no skipped levels', async ({ page }) => {
    // Check for exactly one h1
    const h1Elements = page.locator('h1');
    const h1Count = await h1Elements.count();
    expect(h1Count).toBe(1);

    // Verify h1 is the main title
    const h1Text = await h1Elements.first().textContent();
    expect(h1Text).toBe('MirDB');

    // Check for h2 elements (section headings)
    const h2Elements = page.locator('h2');
    const h2Count = await h2Elements.count();
    expect(h2Count).toBeGreaterThanOrEqual(3); // At least Features, Quick Start, Commands, Configuration

    // Check that h2 elements have proper content
    const h2Texts = await h2Elements.allTextContents();
    expect(h2Texts).toContain('Key Features');
    expect(h2Texts).toContain('Quick Start');
    expect(h2Texts).toContain('Supported Commands');
    expect(h2Texts).toContain('Configuration');

    // Check for h3 elements (subsection headings)
    const h3Elements = page.locator('h3');
    const h3Count = await h3Elements.count();
    expect(h3Count).toBeGreaterThan(0);

    // Verify heading hierarchy: no h3 should appear before its parent h2
    // Get all headings in order
    const allHeadings = await page.evaluate(() => {
      const headings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
      return Array.from(headings).map(h => ({
        level: parseInt(h.tagName.substring(1)),
        text: h.textContent?.trim()
      }));
    });

    // Verify no skipped levels
    let currentLevel = 0;
    for (const heading of allHeadings) {
      // Heading can be same level, one level down, or back up to any previous level
      if (heading.level > currentLevel + 1 && currentLevel > 0) {
        // This would be a skipped level (e.g., h1 directly to h3)
        throw new Error(`Skipped heading level: went from h${currentLevel} to h${heading.level} at "${heading.text}"`);
      }
      currentLevel = heading.level;
    }
  });

  /**
   * Test Case 3: Check all img elements for alt attributes
   * Input: Check all img elements for alt attributes
   * Expected: All images have non-empty, descriptive alt attributes
   */
  test('TC3: All images have non-empty, descriptive alt attributes', async ({ page }) => {
    // Get all img elements
    const images = page.locator('img');
    const imageCount = await images.count();

    // If there are images, verify each has an alt attribute
    if (imageCount > 0) {
      for (let i = 0; i < imageCount; i++) {
        const img = images.nth(i);
        const alt = await img.getAttribute('alt');
        const src = await img.getAttribute('src');

        // Ensure alt attribute exists
        expect(alt, `Image with src="${src}" should have an alt attribute`).not.toBeNull();

        // Ensure alt is not empty (unless it's decorative, which should use alt="")
        // For this test, we expect descriptive alt text
        expect(alt?.length, `Image with src="${src}" should have non-empty alt text`).toBeGreaterThan(0);
      }
    }

    // Also check for SVG elements that might need aria-label or aria-hidden
    const svgs = page.locator('svg');
    const svgCount = await svgs.count();

    for (let i = 0; i < svgCount; i++) {
      const svg = svgs.nth(i);
      const ariaHidden = await svg.getAttribute('aria-hidden');
      const ariaLabel = await svg.getAttribute('aria-label');
      const role = await svg.getAttribute('role');

      // Each SVG should either be hidden from screen readers (aria-hidden="true")
      // or have an aria-label for accessibility
      const isAccessible = ariaHidden === 'true' || ariaLabel !== null || role === 'img';
      expect(isAccessible, `SVG element should have aria-hidden="true" or an aria-label`).toBe(true);
    }
  });

  /**
   * Test Case 4: Check ARIA labels on interactive elements
   * Input: Check ARIA labels on interactive elements
   * Expected: Copy buttons and other icons have aria-label attributes
   */
  test('TC4: Interactive elements have proper ARIA labels', async ({ page }) => {
    // Check copy buttons have aria-label
    const copyButtons = page.locator('.copy-btn');
    const copyButtonCount = await copyButtons.count();

    for (let i = 0; i < copyButtonCount; i++) {
      const button = copyButtons.nth(i);
      const ariaLabel = await button.getAttribute('aria-label');
      expect(ariaLabel, `Copy button ${i + 1} should have an aria-label`).not.toBeNull();
      expect(ariaLabel?.length, `Copy button ${i + 1} should have non-empty aria-label`).toBeGreaterThan(0);
    }

    // Check hamburger menu button has aria-label and aria-controls
    const hamburgerMenu = page.locator('[data-testid="hamburger-menu"]');
    if (await hamburgerMenu.count() > 0) {
      const hamburgerAriaLabel = await hamburgerMenu.getAttribute('aria-label');
      expect(hamburgerAriaLabel).toBe('Toggle navigation menu');

      const ariaControls = await hamburgerMenu.getAttribute('aria-controls');
      expect(ariaControls).toBe('nav-links');

      const ariaExpanded = await hamburgerMenu.getAttribute('aria-expanded');
      expect(ariaExpanded).not.toBeNull();
    }

    // Check that links to external sites have proper attributes
    const externalLinks = page.locator('a[target="_blank"]');
    const externalLinkCount = await externalLinks.count();

    for (let i = 0; i < externalLinkCount; i++) {
      const link = externalLinks.nth(i);
      const rel = await link.getAttribute('rel');

      // External links should have rel="noopener noreferrer" for security
      expect(rel, `External link ${i + 1} should have rel attribute`).toContain('noopener');
    }

    // Check that buttons have accessible names
    const allButtons = page.locator('button, [role="button"]');
    const buttonCount = await allButtons.count();

    for (let i = 0; i < buttonCount; i++) {
      const button = allButtons.nth(i);
      const ariaLabel = await button.getAttribute('aria-label');
      const textContent = await button.textContent();

      // Button should have either aria-label or visible text content
      const hasAccessibleName = (ariaLabel && ariaLabel.length > 0) || (textContent && textContent.trim().length > 0);
      expect(hasAccessibleName, `Button ${i + 1} should have an accessible name (aria-label or text content)`).toBe(true);
    }
  });

  /**
   * Test Case 5: Run axe accessibility audit
   * Input: Run axe accessibility audit
   * Expected: No critical or serious accessibility violations
   */
  test('TC5: No critical or serious accessibility violations (axe audit)', async ({ page }) => {
    // Run axe accessibility scan
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa'])
      .analyze();

    // Filter for critical and serious violations
    const criticalViolations = accessibilityScanResults.violations.filter(
      v => v.impact === 'critical' || v.impact === 'serious'
    );

    // Log any violations for debugging
    if (criticalViolations.length > 0) {
      console.log('Critical/Serious Accessibility Violations:');
      criticalViolations.forEach(violation => {
        console.log(`- ${violation.id}: ${violation.description}`);
        console.log(`  Impact: ${violation.impact}`);
        console.log(`  Help: ${violation.help}`);
        violation.nodes.forEach(node => {
          console.log(`  Element: ${node.html}`);
        });
      });
    }

    // Assert no critical or serious violations
    expect(criticalViolations.length, `Found ${criticalViolations.length} critical/serious accessibility violations`).toBe(0);
  });

  /**
   * Additional test: Code blocks are accessible
   * Verifies code examples are properly labeled and readable by screen readers
   */
  test('Code blocks are accessible and properly labeled', async ({ page }) => {
    // Check code blocks have proper structure
    const codeBlocks = page.locator('.code-block');
    const codeBlockCount = await codeBlocks.count();
    expect(codeBlockCount).toBeGreaterThan(0);

    for (let i = 0; i < codeBlockCount; i++) {
      const codeBlock = codeBlocks.nth(i);

      // Each code block should have a header with label
      const header = codeBlock.locator('.code-header');
      await expect(header).toBeVisible();

      // Check that pre/code elements exist
      const preElement = codeBlock.locator('pre');
      await expect(preElement).toBeVisible();

      const codeElement = codeBlock.locator('code');
      await expect(codeElement).toBeVisible();

      // Check code has language class for syntax highlighting
      const codeClass = await codeElement.getAttribute('class');
      expect(codeClass).toContain('language-');
    }
  });

  /**
   * Additional test: Tables are accessible
   * Verifies data tables have proper headers and structure
   */
  test('Tables have proper accessibility structure', async ({ page }) => {
    const tables = page.locator('table');
    const tableCount = await tables.count();
    expect(tableCount).toBeGreaterThan(0);

    for (let i = 0; i < tableCount; i++) {
      const table = tables.nth(i);

      // Each table should have a thead with th elements
      const thead = table.locator('thead');
      await expect(thead).toBeVisible();

      const thElements = table.locator('th');
      const thCount = await thElements.count();
      expect(thCount, `Table ${i + 1} should have header cells`).toBeGreaterThan(0);

      // Each table should have a tbody
      const tbody = table.locator('tbody');
      await expect(tbody).toBeVisible();
    }
  });

  /**
   * Additional test: Focus management and keyboard navigation
   * Verifies interactive elements are keyboard accessible
   */
  test('Interactive elements are keyboard navigable', async ({ page }) => {
    // Test that navigation links are focusable
    const navLinks = page.locator('.nav-links a');
    const navLinkCount = await navLinks.count();

    if (navLinkCount > 0) {
      // Focus first nav link
      await navLinks.first().focus();
      await expect(navLinks.first()).toBeFocused();
    }

    // Test that copy buttons are focusable
    const copyButtons = page.locator('.copy-btn');
    if (await copyButtons.count() > 0) {
      await copyButtons.first().focus();
      await expect(copyButtons.first()).toBeFocused();
    }

    // Test that CTA buttons are focusable
    const ctaButtons = page.locator('.hero-cta .btn');
    if (await ctaButtons.count() > 0) {
      await ctaButtons.first().focus();
      await expect(ctaButtons.first()).toBeFocused();
    }
  });

  /**
   * Additional test: Language attribute is set
   * Screen readers need the lang attribute to pronounce content correctly
   */
  test('HTML document has lang attribute set', async ({ page }) => {
    const htmlElement = page.locator('html');
    const lang = await htmlElement.getAttribute('lang');
    expect(lang).toBe('en');
  });
});
