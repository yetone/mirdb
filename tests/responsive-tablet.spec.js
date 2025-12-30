const { test, expect } = require('@playwright/test');

/**
 * Responsive Design - Tablet Tests
 * Scenario: Validate the homepage displays correctly on tablet devices (768px-1023px viewport)
 */

test.describe('Responsive Design - Tablet', () => {
  // Configure all tests in this suite to use tablet viewport (768px width)
  test.use({
    viewport: { width: 768, height: 1024 }
  });

  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Test Case 1: Page renders correctly with appropriate tablet layout at 768px width viewport', async ({ page }) => {
    // Check that the page doesn't have horizontal overflow
    const bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth);
    const viewportWidth = await page.evaluate(() => window.innerWidth);

    // Body scroll width should not exceed viewport width (no horizontal scroll)
    expect(bodyScrollWidth).toBeLessThanOrEqual(viewportWidth);

    // Verify the page is fully visible and functional
    await expect(page.locator('header')).toBeVisible();
    await expect(page.locator('.hero')).toBeVisible();
    await expect(page.locator('#value-proposition')).toBeVisible();
    await expect(page.locator('#features')).toBeVisible();
    await expect(page.locator('#commands')).toBeVisible();
    await expect(page.locator('#demo')).toBeVisible();
    await expect(page.locator('#quickstart')).toBeVisible();
    await expect(page.locator('footer')).toBeVisible();

    // Verify hero section elements
    const heroLogo = page.locator('.hero-logo');
    await expect(heroLogo).toBeVisible();

    const heroHeadline = page.locator('.hero-headline');
    await expect(heroHeadline).toBeVisible();

    const heroSubheadline = page.locator('.hero-subheadline');
    await expect(heroSubheadline).toBeVisible();

    // Verify CTA buttons are visible
    const ctaButtons = page.locator('.hero-cta .btn');
    expect(await ctaButtons.count()).toBe(2);
    await expect(ctaButtons.nth(0)).toBeVisible();
    await expect(ctaButtons.nth(1)).toBeVisible();

    // Verify no content is cut off by checking all sections are within viewport width
    const sections = ['header', '.hero', '#value-proposition', '#features', '#commands', '#demo', '#quickstart', 'footer'];
    for (const section of sections) {
      const sectionElement = page.locator(section);
      const box = await sectionElement.boundingBox();
      expect(box.width).toBeLessThanOrEqual(viewportWidth);
    }
  });

  test('Test Case 2: Value proposition section displays in two or three columns on tablet', async ({ page }) => {
    // Navigate to value proposition section
    const valuePropositionSection = page.locator('#value-proposition');
    await expect(valuePropositionSection).toBeVisible();

    // Get the value proposition columns
    const columns = page.locator('.value-prop-column');
    const columnCount = await columns.count();
    expect(columnCount).toBe(3);

    // On tablet (768px), the grid should use 3 columns layout
    const gridStyle = await page.locator('.value-prop-grid').evaluate((el) => {
      return window.getComputedStyle(el).gridTemplateColumns;
    });

    // Check that we have either 2 or 3 column layout (not single column)
    // Grid template columns will show pixel values like "200px 200px 200px" for 3 columns
    const columnWidths = gridStyle.split(' ').filter(val => val.includes('px'));
    expect(columnWidths.length).toBeGreaterThanOrEqual(2);
    expect(columnWidths.length).toBeLessThanOrEqual(3);

    // Get column positions to verify layout
    const firstColumnBox = await columns.nth(0).boundingBox();
    const secondColumnBox = await columns.nth(1).boundingBox();
    const thirdColumnBox = await columns.nth(2).boundingBox();

    // In a multi-column layout, columns should have different x positions
    // (they should be arranged side by side, not stacked)
    expect(secondColumnBox.x).toBeGreaterThan(firstColumnBox.x);

    // All columns should be visible
    for (let i = 0; i < 3; i++) {
      await expect(columns.nth(i)).toBeVisible();
    }

    // Verify column content is visible
    const columnTitles = page.locator('.value-prop-column h3');
    for (let i = 0; i < 3; i++) {
      await expect(columnTitles.nth(i)).toBeVisible();
    }

    const columnDescriptions = page.locator('.value-prop-column p');
    for (let i = 0; i < 3; i++) {
      await expect(columnDescriptions.nth(i)).toBeVisible();
    }
  });

  test('Test Case 3: Buttons and links have adequate touch target size (44px minimum)', async ({ page }) => {
    // According to WCAG and mobile UX guidelines, touch targets should be at least 44x44 pixels

    // Check CTA buttons
    const ctaButtons = page.locator('.hero-cta .btn');
    const buttonCount = await ctaButtons.count();

    for (let i = 0; i < buttonCount; i++) {
      const button = ctaButtons.nth(i);
      const buttonBox = await button.boundingBox();

      // Buttons should have at least 44px height
      expect(buttonBox.height).toBeGreaterThanOrEqual(44);
    }

    // Check navigation links
    const navLinks = page.locator('nav a');
    const navLinkCount = await navLinks.count();

    for (let i = 0; i < navLinkCount; i++) {
      const link = navLinks.nth(i);
      const linkBox = await link.boundingBox();

      // Navigation links should have at least 44px height on tablet
      expect(linkBox.height).toBeGreaterThanOrEqual(44);
    }

    // Check footer links
    const footerLinks = page.locator('.footer-links a');
    const footerLinkCount = await footerLinks.count();

    for (let i = 0; i < footerLinkCount; i++) {
      const link = footerLinks.nth(i);
      const linkBox = await link.boundingBox();

      // Footer links should have at least 44px height on tablet
      expect(linkBox.height).toBeGreaterThanOrEqual(44);
    }
  });

  // Additional test for layout verification at different tablet widths
  test('Layout adapts correctly across tablet viewport range (768px-1023px)', async ({ page }) => {
    const tabletWidths = [768, 900, 1023];

    for (const width of tabletWidths) {
      await page.setViewportSize({ width, height: 1024 });
      await page.goto('/');

      // Verify no horizontal overflow
      const bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth);
      expect(bodyScrollWidth).toBeLessThanOrEqual(width);

      // Verify all major sections are visible
      await expect(page.locator('header')).toBeVisible();
      await expect(page.locator('.hero')).toBeVisible();
      await expect(page.locator('#value-proposition')).toBeVisible();
      await expect(page.locator('#features')).toBeVisible();
      await expect(page.locator('footer')).toBeVisible();

      // Verify value proposition maintains multi-column layout
      const columns = page.locator('.value-prop-column');
      const firstColumnBox = await columns.nth(0).boundingBox();
      const secondColumnBox = await columns.nth(1).boundingBox();

      // Columns should be side by side (different x positions)
      expect(secondColumnBox.x).toBeGreaterThan(firstColumnBox.x);
    }
  });

  // Test that content is not overlapping
  test('All sections render without content overlap', async ({ page }) => {
    // Get bounding boxes for main sections
    const heroSection = page.locator('.hero');
    const valueProposition = page.locator('#value-proposition');
    const featuresSection = page.locator('#features');
    const commandsSection = page.locator('#commands');
    const demoSection = page.locator('#demo');
    const quickstartSection = page.locator('#quickstart');
    const footer = page.locator('footer');

    const heroBox = await heroSection.boundingBox();
    const valueBox = await valueProposition.boundingBox();
    const featuresBox = await featuresSection.boundingBox();
    const commandsBox = await commandsSection.boundingBox();
    const demoBox = await demoSection.boundingBox();
    const quickstartBox = await quickstartSection.boundingBox();
    const footerBox = await footer.boundingBox();

    // Verify sections are in correct order (each section starts after the previous one ends)
    expect(valueBox.y).toBeGreaterThanOrEqual(heroBox.y + heroBox.height);
    expect(featuresBox.y).toBeGreaterThanOrEqual(valueBox.y + valueBox.height);
    expect(commandsBox.y).toBeGreaterThanOrEqual(featuresBox.y + featuresBox.height);
    expect(demoBox.y).toBeGreaterThanOrEqual(commandsBox.y + commandsBox.height);
    expect(quickstartBox.y).toBeGreaterThanOrEqual(demoBox.y + demoBox.height);
    expect(footerBox.y).toBeGreaterThanOrEqual(quickstartBox.y + quickstartBox.height);
  });
});
