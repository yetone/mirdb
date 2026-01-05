const { test, expect } = require('@playwright/test');

test.describe('Semantic HTML Structure (NFR-5)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Page has exactly one h1 element containing MirDB', async ({ page }) => {
    // Find all h1 elements on the page
    const h1Elements = page.locator('h1');

    // Verify there is exactly one h1
    await expect(h1Elements).toHaveCount(1);

    // Verify the h1 contains 'MirDB'
    const h1Text = await h1Elements.textContent();
    expect(h1Text).toContain('MirDB');
  });

  test('TC2: Page uses <header> element for navigation area', async ({ page }) => {
    // Verify header element exists
    const headerElement = page.locator('header');
    await expect(headerElement).toBeVisible();

    // Verify header contains navigation
    const navInHeader = headerElement.locator('nav');
    await expect(navInHeader).toBeVisible();

    // Verify header contains the logo/brand
    const logo = headerElement.locator('.logo');
    await expect(logo).toBeVisible();
  });

  test('TC3: Page uses <main> element for primary content', async ({ page }) => {
    // Verify main element exists
    const mainElement = page.locator('main');
    await expect(mainElement).toBeVisible();

    // Verify main contains the primary content sections
    const sectionsInMain = mainElement.locator('section');
    const sectionCount = await sectionsInMain.count();
    expect(sectionCount).toBeGreaterThanOrEqual(1);

    // Verify main contains the hero section
    const heroSection = mainElement.locator('.hero');
    await expect(heroSection).toBeVisible();
  });

  test('TC4: Page uses <nav> element for navigation links', async ({ page }) => {
    // Verify nav element exists
    const navElement = page.locator('nav');
    await expect(navElement).toBeVisible();

    // Verify nav contains navigation links
    const navLinks = navElement.locator('a');
    const linkCount = await navLinks.count();
    expect(linkCount).toBeGreaterThanOrEqual(1);

    // Verify nav links include important sections
    const navText = await navElement.textContent();
    expect(navText).toContain('GitHub');
  });

  test('TC5: Page uses <footer> element at bottom of page', async ({ page }) => {
    // Verify footer element exists
    const footerElement = page.locator('footer');
    await expect(footerElement).toBeVisible();

    // Verify footer contains links
    const footerLinks = footerElement.locator('a');
    const linkCount = await footerLinks.count();
    expect(linkCount).toBeGreaterThanOrEqual(1);

    // Verify footer contains copyright information
    const copyrightText = await footerElement.textContent();
    expect(copyrightText.toLowerCase()).toMatch(/©|copyright|mit|license/i);

    // Verify footer is positioned at the bottom (after main content)
    const mainElement = page.locator('main');
    const mainBox = await mainElement.boundingBox();
    const footerBox = await footerElement.boundingBox();
    expect(footerBox.y).toBeGreaterThan(mainBox.y + mainBox.height - 1);
  });

  test('TC6: Page uses <section> elements for distinct content areas', async ({ page }) => {
    // Verify section elements exist
    const sectionElements = page.locator('section');
    const sectionCount = await sectionElements.count();

    // Page should have multiple sections for different content areas
    expect(sectionCount).toBeGreaterThanOrEqual(3);

    // Verify key sections exist
    const heroSection = page.locator('section.hero, section#hero');
    await expect(heroSection).toBeVisible();

    const featuresSection = page.locator('section.features, section#features');
    await expect(featuresSection).toBeVisible();

    const quickstartSection = page.locator('section.quickstart, section#quickstart');
    await expect(quickstartSection).toBeVisible();
  });

  test('TC7: Headings follow logical h1 > h2 > h3 hierarchy without skipping levels', async ({ page }) => {
    // Get all heading elements in document order
    const headings = await page.evaluate(() => {
      const allHeadings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
      return Array.from(allHeadings).map((h, index) => ({
        level: parseInt(h.tagName.charAt(1)),
        text: h.textContent.trim().substring(0, 50),
        index
      }));
    });

    // Verify we have headings
    expect(headings.length).toBeGreaterThan(0);

    // Verify first heading is h1
    expect(headings[0].level).toBe(1);

    // Verify heading hierarchy - no level should skip more than 1 from previous
    // (e.g., can go from h1 to h2, h2 to h3, but not h1 to h3 directly)
    for (let i = 1; i < headings.length; i++) {
      const currentLevel = headings[i].level;
      const previousLevel = headings[i - 1].level;

      // When going deeper, should not skip levels (e.g., h1 directly to h3)
      if (currentLevel > previousLevel) {
        const skippedLevels = currentLevel - previousLevel;
        expect(skippedLevels).toBeLessThanOrEqual(1);
      }
    }

    // Count h1 elements - should be exactly 1
    const h1Count = headings.filter(h => h.level === 1).length;
    expect(h1Count).toBe(1);
  });

  test('All sections have appropriate heading structure', async ({ page }) => {
    // Verify each major section has a heading
    const sections = await page.evaluate(() => {
      const sectionElements = document.querySelectorAll('main section');
      return Array.from(sectionElements).map(section => {
        const heading = section.querySelector('h1, h2, h3');
        return {
          hasHeading: !!heading,
          headingLevel: heading ? parseInt(heading.tagName.charAt(1)) : null,
          headingText: heading ? heading.textContent.trim().substring(0, 50) : null
        };
      });
    });

    // All content sections should have headings
    for (const section of sections) {
      expect(section.hasHeading).toBe(true);
    }
  });

  test('Document has proper semantic structure order', async ({ page }) => {
    // Verify the semantic elements appear in correct order
    const structureOrder = await page.evaluate(() => {
      const body = document.body;
      const children = Array.from(body.children);
      return children.map(el => el.tagName.toLowerCase());
    });

    // Find indexes
    const headerIndex = structureOrder.indexOf('header');
    const mainIndex = structureOrder.indexOf('main');
    const footerIndex = structureOrder.indexOf('footer');

    // Verify all semantic elements exist
    expect(headerIndex).toBeGreaterThanOrEqual(0);
    expect(mainIndex).toBeGreaterThanOrEqual(0);
    expect(footerIndex).toBeGreaterThanOrEqual(0);

    // Verify correct order: header < main < footer
    expect(headerIndex).toBeLessThan(mainIndex);
    expect(mainIndex).toBeLessThan(footerIndex);
  });
});
