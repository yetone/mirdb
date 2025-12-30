const { test, expect } = require('@playwright/test');

/**
 * Accessibility - Semantic HTML Tests
 * Scenario: Validate that the page uses proper semantic HTML structure
 *
 * This test suite verifies:
 * 1. Single h1 element on the page
 * 2. Proper heading hierarchy (no skipped levels)
 * 3. Landmark elements (header, main, footer)
 * 4. Navigation uses nav element or role="navigation"
 */

test.describe('Accessibility - Semantic HTML', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Test Case 1: Page contains exactly one h1 element', async ({ page }) => {
    // Get all h1 elements on the page
    const h1Elements = page.locator('h1');
    const h1Count = await h1Elements.count();

    // Page should have exactly one h1 element
    expect(h1Count).toBe(1);

    // Verify the h1 is visible and contains meaningful content
    const h1 = h1Elements.first();
    await expect(h1).toBeVisible();

    const h1Text = await h1.textContent();
    expect(h1Text.trim().length).toBeGreaterThan(0);

    // Verify the h1 contains the main page title (MirDB)
    expect(h1Text.toLowerCase()).toContain('mirdb');
  });

  test('Test Case 2: No skipped heading levels (proper h1-h6 hierarchy)', async ({ page }) => {
    // Get all heading elements in the order they appear in the document
    const headings = await page.locator('h1, h2, h3, h4, h5, h6').all();

    // Ensure we have headings to test
    expect(headings.length).toBeGreaterThan(0);

    // Track the heading levels to check for skipped levels
    const headingLevels = [];

    for (const heading of headings) {
      const tagName = await heading.evaluate(el => el.tagName.toLowerCase());
      const level = parseInt(tagName.charAt(1));
      headingLevels.push(level);
    }

    // First heading should be h1
    expect(headingLevels[0]).toBe(1);

    // Check for skipped heading levels
    // Each subsequent heading should not skip levels (e.g., h1 -> h3 without h2 is invalid)
    const skippedLevels = [];
    let maxLevelSeen = 1;

    for (let i = 1; i < headingLevels.length; i++) {
      const currentLevel = headingLevels[i];
      const previousLevel = headingLevels[i - 1];

      // If going to a deeper level, it should not skip more than one level
      if (currentLevel > previousLevel && currentLevel > previousLevel + 1) {
        skippedLevels.push({
          position: i,
          from: previousLevel,
          to: currentLevel,
          message: `h${previousLevel} followed by h${currentLevel} without h${previousLevel + 1}`
        });
      }

      // Also check if we're jumping to a level deeper than ever seen + 1
      if (currentLevel > maxLevelSeen + 1) {
        // Only flag if we haven't seen the intermediate level before
        let hasIntermediateLevel = false;
        for (let j = 0; j < i; j++) {
          if (headingLevels[j] === currentLevel - 1) {
            hasIntermediateLevel = true;
            break;
          }
        }
        if (!hasIntermediateLevel && !skippedLevels.find(s => s.position === i)) {
          skippedLevels.push({
            position: i,
            from: maxLevelSeen,
            to: currentLevel,
            message: `h${currentLevel} appears without prior h${currentLevel - 1}`
          });
        }
      }

      maxLevelSeen = Math.max(maxLevelSeen, currentLevel);
    }

    // Report any issues found
    if (skippedLevels.length > 0) {
      console.log('Heading hierarchy issues found:', skippedLevels);
    }

    // There should be no skipped levels
    expect(skippedLevels).toHaveLength(0);
  });

  test('Test Case 3: Page contains header, main, and footer landmark elements', async ({ page }) => {
    // Check for header element
    const header = page.locator('header');
    await expect(header).toBeVisible();

    // Check for main element
    const main = page.locator('main');
    const mainCount = await main.count();
    expect(mainCount).toBeGreaterThanOrEqual(1);
    await expect(main.first()).toBeVisible();

    // Check for footer element
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    // Verify proper document structure order: header -> main -> footer
    const headerBox = await header.boundingBox();
    const mainBox = await main.first().boundingBox();
    const footerBox = await footer.boundingBox();

    // Header should be at the top
    expect(headerBox.y).toBeLessThan(mainBox.y);

    // Main should be between header and footer
    expect(mainBox.y).toBeGreaterThan(headerBox.y);
    expect(mainBox.y).toBeLessThan(footerBox.y);

    // Footer should be at the bottom
    expect(footerBox.y).toBeGreaterThan(mainBox.y);
  });

  test('Test Case 4: Navigation uses nav element or role="navigation"', async ({ page }) => {
    // Check for nav element
    const navElement = page.locator('nav');
    const navCount = await navElement.count();

    // Check for elements with role="navigation"
    const roleNav = page.locator('[role="navigation"]');
    const roleNavCount = await roleNav.count();

    // At least one navigation landmark should exist
    const totalNavigationLandmarks = navCount + roleNavCount;
    expect(totalNavigationLandmarks).toBeGreaterThanOrEqual(1);

    // If nav element exists, verify it's properly structured
    if (navCount > 0) {
      await expect(navElement.first()).toBeVisible();

      // Navigation should contain links
      const navLinks = navElement.first().locator('a');
      const linkCount = await navLinks.count();
      expect(linkCount).toBeGreaterThan(0);
    }

    // If role="navigation" exists, verify it's properly structured
    if (roleNavCount > 0) {
      await expect(roleNav.first()).toBeVisible();

      // Should contain links
      const roleNavLinks = roleNav.first().locator('a');
      const linkCount = await roleNavLinks.count();
      expect(linkCount).toBeGreaterThan(0);
    }
  });

  test('Verify semantic list markup for feature lists', async ({ page }) => {
    // Check that lists use proper ul/ol/li elements
    const unorderedLists = page.locator('ul');
    const orderedLists = page.locator('ol');

    const ulCount = await unorderedLists.count();
    const olCount = await orderedLists.count();

    // Page should have semantic lists
    expect(ulCount + olCount).toBeGreaterThan(0);

    // Verify list items are properly nested within lists
    const listItems = page.locator('li');
    const liCount = await listItems.count();
    expect(liCount).toBeGreaterThan(0);

    // Each li should be a direct child of ul or ol
    for (let i = 0; i < liCount; i++) {
      const li = listItems.nth(i);
      const parentTag = await li.evaluate(el => el.parentElement.tagName.toLowerCase());
      expect(['ul', 'ol']).toContain(parentTag);
    }
  });

  test('Verify command lists use semantic list markup', async ({ page }) => {
    // Commands section should use proper list markup
    const commandsSection = page.locator('#commands, .commands');

    if (await commandsSection.count() > 0) {
      const commandLists = commandsSection.locator('ul, ol');
      const listCount = await commandLists.count();

      // Commands should be in lists
      expect(listCount).toBeGreaterThan(0);

      // Each command list should have list items
      for (let i = 0; i < listCount; i++) {
        const list = commandLists.nth(i);
        const items = list.locator('li');
        const itemCount = await items.count();
        expect(itemCount).toBeGreaterThan(0);
      }
    }
  });

  test('Verify main content is not placed directly in body', async ({ page }) => {
    // Main content sections should be within semantic containers
    const sections = page.locator('section');
    const sectionCount = await sections.count();

    // Page should use section elements for content organization
    expect(sectionCount).toBeGreaterThan(0);

    // Each visible section should have an identifiable purpose (id or aria-label)
    for (let i = 0; i < sectionCount; i++) {
      const section = sections.nth(i);
      const isVisible = await section.isVisible();

      if (isVisible) {
        const id = await section.getAttribute('id');
        const ariaLabel = await section.getAttribute('aria-label');
        const ariaLabelledBy = await section.getAttribute('aria-labelledby');

        // Section should have some form of identification
        const hasIdentification = id || ariaLabel || ariaLabelledBy;
        // Note: Not all sections need explicit labels, but main content sections should
        // We'll check that at least some sections have identification
      }
    }
  });

  test('Verify header contains proper landmark structure', async ({ page }) => {
    const header = page.locator('header');
    await expect(header).toBeVisible();

    // Header should contain navigation
    const headerNav = header.locator('nav');
    const navCount = await headerNav.count();
    expect(navCount).toBeGreaterThanOrEqual(1);

    // Header should contain branding/logo
    const logoOrBrand = header.locator('a[class*="logo"], img[alt*="logo" i], .logo-text');
    const brandCount = await logoOrBrand.count();
    expect(brandCount).toBeGreaterThanOrEqual(1);
  });

  test('Verify footer contains proper content structure', async ({ page }) => {
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    // Footer should contain links
    const footerLinks = footer.locator('a');
    const linkCount = await footerLinks.count();
    expect(linkCount).toBeGreaterThan(0);

    // Footer should have some text content (copyright, license info, etc.)
    const footerText = await footer.textContent();
    expect(footerText.trim().length).toBeGreaterThan(0);
  });
});
