// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

const indexPath = 'file://' + path.resolve(__dirname, '../index.html');

test.describe('Accessibility - Semantic HTML (NFR-3)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(indexPath);
  });

  // Test Case 1: Check for semantic header element
  // Expected: Page contains semantic <header> element
  test('TC1: Page contains semantic <header> element', async ({ page }) => {
    // Check that a semantic <header> element exists
    const header = page.locator('header');
    await expect(header).toBeVisible();

    // Verify the header element is actually a <header> tag
    const tagName = await header.evaluate(el => el.tagName.toLowerCase());
    expect(tagName).toBe('header');

    // Verify header contains meaningful content (hero section)
    const headerContent = await header.textContent();
    expect(headerContent.length).toBeGreaterThan(0);
  });

  // Test Case 2: Check for semantic main element
  // Expected: Page contains semantic <main> element
  test('TC2: Page contains semantic <main> element', async ({ page }) => {
    // Check that a semantic <main> element exists
    const main = page.locator('main');
    await expect(main).toBeVisible();

    // Verify the main element is actually a <main> tag
    const tagName = await main.evaluate(el => el.tagName.toLowerCase());
    expect(tagName).toBe('main');

    // Verify main contains the primary content sections
    const mainContent = await main.textContent();
    expect(mainContent.length).toBeGreaterThan(0);

    // Verify there is exactly one <main> element
    const mainCount = await page.locator('main').count();
    expect(mainCount).toBe(1);
  });

  // Test Case 3: Check for semantic footer element
  // Expected: Page contains semantic <footer> element
  test('TC3: Page contains semantic <footer> element', async ({ page }) => {
    // Check that a semantic <footer> element exists
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    // Verify the footer element is actually a <footer> tag
    const tagName = await footer.evaluate(el => el.tagName.toLowerCase());
    expect(tagName).toBe('footer');

    // Verify footer contains content
    const footerContent = await footer.textContent();
    expect(footerContent.length).toBeGreaterThan(0);
  });

  // Test Case 4: Verify single h1 element
  // Expected: Page contains exactly one <h1> element
  test('TC4: Page contains exactly one <h1> element', async ({ page }) => {
    // Count h1 elements
    const h1Elements = page.locator('h1');
    const h1Count = await h1Elements.count();

    // Verify exactly one h1 exists
    expect(h1Count).toBe(1);

    // Verify the h1 is visible
    await expect(h1Elements.first()).toBeVisible();

    // Verify the h1 contains the main title
    const h1Text = await h1Elements.first().textContent();
    expect(h1Text.trim()).toBe('MirDB');
  });

  // Test Case 5: Verify heading hierarchy
  // Expected: Headings follow proper hierarchy without skipping levels
  test('TC5: Headings follow proper hierarchy without skipping levels', async ({ page }) => {
    // Get all heading elements in order
    const headings = await page.locator('h1, h2, h3, h4, h5, h6').all();

    // Track previous heading level
    let previousLevel = 0;
    const headingLevels = [];

    for (const heading of headings) {
      const tagName = await heading.evaluate(el => el.tagName.toLowerCase());
      const level = parseInt(tagName.charAt(1));
      headingLevels.push(level);

      // If this is not the first heading, check the hierarchy
      if (previousLevel > 0) {
        // The next heading should either:
        // 1. Be a deeper level (but only by 1)
        // 2. Be the same level
        // 3. Be a higher level (going back up the tree)
        if (level > previousLevel) {
          // Skipping levels is not allowed (e.g., h1 -> h3)
          expect(level - previousLevel).toBe(1);
        }
      }

      previousLevel = level;
    }

    // Verify we have a proper hierarchy starting with h1
    expect(headingLevels[0]).toBe(1);

    // Verify we have multiple heading levels for proper structure
    expect(headingLevels.length).toBeGreaterThan(1);

    // Verify h2 elements exist for section headings
    const h2Count = headingLevels.filter(l => l === 2).length;
    expect(h2Count).toBeGreaterThan(0);
  });

  // Additional test: Verify semantic sections are properly structured
  test('Semantic sections are properly structured', async ({ page }) => {
    // Check for section elements
    const sections = page.locator('section');
    const sectionCount = await sections.count();
    expect(sectionCount).toBeGreaterThan(0);

    // Verify sections have identifiable IDs for navigation
    for (let i = 0; i < sectionCount; i++) {
      const section = sections.nth(i);
      const id = await section.getAttribute('id');
      expect(id).toBeTruthy();
    }

    // Verify the document structure follows a logical order:
    // header -> main (with sections) -> footer
    const header = page.locator('header');
    const main = page.locator('main');
    const footer = page.locator('footer');

    const headerBox = await header.boundingBox();
    const mainBox = await main.boundingBox();
    const footerBox = await footer.boundingBox();

    expect(headerBox).not.toBeNull();
    expect(mainBox).not.toBeNull();
    expect(footerBox).not.toBeNull();

    // Header should be above main
    expect(headerBox.y + headerBox.height).toBeLessThanOrEqual(mainBox.y + 5);

    // Main should be above footer
    expect(mainBox.y + mainBox.height).toBeLessThanOrEqual(footerBox.y + 5);
  });
});
