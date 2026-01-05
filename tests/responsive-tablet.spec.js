const { test, expect } = require('@playwright/test');

test.describe('Responsive Design - Tablet Viewport', () => {
  // Set tablet viewport for all tests in this describe block
  test.use({ viewport: { width: 768, height: 1024 } });

  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Page renders correctly at 768x1024 viewport without horizontal overflow', async ({ page }) => {
    // Wait for page to fully load
    await page.waitForLoadState('domcontentloaded');

    // Check that there is no horizontal scrollbar / overflow
    const hasHorizontalOverflow = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalOverflow).toBe(false);

    // Verify viewport dimensions
    const viewportSize = page.viewportSize();
    expect(viewportSize.width).toBe(768);
    expect(viewportSize.height).toBe(1024);

    // Verify key sections are visible
    const heroSection = page.locator('.hero');
    await expect(heroSection).toBeVisible();

    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeVisible();

    const footer = page.locator('footer');
    await expect(footer).toBeVisible();
  });

  test('TC2: Feature cards stack or display in 2-column layout at tablet viewport', async ({ page }) => {
    // Navigate to features section
    await page.locator('#features').scrollIntoViewIfNeeded();

    // Get all feature cards
    const featuresGrid = page.locator('.features-grid');
    await expect(featuresGrid).toBeVisible();

    const featureCards = page.locator('.feature-card');
    const count = await featureCards.count();
    expect(count).toBe(3);

    // Get the bounding boxes of all feature cards
    const cards = await featureCards.all();
    const boundingBoxes = await Promise.all(cards.map(card => card.boundingBox()));

    // Filter out null bounding boxes
    const validBoxes = boundingBoxes.filter(box => box !== null);
    expect(validBoxes.length).toBe(3);

    // Check the grid layout - at tablet viewport, cards should either:
    // 1. Stack vertically (1 column) - each card starts on a new row
    // 2. Display in 2-column layout - some cards share the same Y position

    // Get unique Y positions (with 5px tolerance for minor differences)
    const getApproxYPositions = (boxes) => {
      const yPositions = new Set();
      boxes.forEach(box => {
        const roundedY = Math.round(box.y / 5) * 5;
        yPositions.add(roundedY);
      });
      return yPositions;
    };

    const uniqueYPositions = getApproxYPositions(validBoxes);

    // With 3 cards:
    // - 1-column layout: 3 unique Y positions
    // - 2-column layout: 2 unique Y positions (2 cards on first row, 1 on second)
    // Both are acceptable for tablet
    expect(uniqueYPositions.size).toBeGreaterThanOrEqual(2);
    expect(uniqueYPositions.size).toBeLessThanOrEqual(3);

    // Verify cards don't overflow the viewport horizontally
    for (const box of validBoxes) {
      expect(box.x).toBeGreaterThanOrEqual(0);
      expect(box.x + box.width).toBeLessThanOrEqual(768);
    }
  });

  test('TC3: CTA buttons are at least 44x44 pixels for touch targets', async ({ page }) => {
    // Check primary CTA button
    const primaryCTA = page.locator('#primary-cta');
    await expect(primaryCTA).toBeVisible();
    const primaryBox = await primaryCTA.boundingBox();
    expect(primaryBox).not.toBeNull();
    expect(primaryBox.width).toBeGreaterThanOrEqual(44);
    expect(primaryBox.height).toBeGreaterThanOrEqual(44);

    // Check secondary CTA button
    const secondaryCTA = page.locator('#secondary-cta');
    await expect(secondaryCTA).toBeVisible();
    const secondaryBox = await secondaryCTA.boundingBox();
    expect(secondaryBox).not.toBeNull();
    expect(secondaryBox.width).toBeGreaterThanOrEqual(44);
    expect(secondaryBox.height).toBeGreaterThanOrEqual(44);

    // Check nav links - they should be accessible touch targets
    const navLinks = page.locator('.nav-links a');
    const navLinkCount = await navLinks.count();

    // Navigation may be hidden on tablet viewport (current design hides nav at 768px)
    // If visible, verify touch target sizes
    if (navLinkCount > 0) {
      const isVisible = await navLinks.first().isVisible();
      if (isVisible) {
        for (let i = 0; i < navLinkCount; i++) {
          const navLink = navLinks.nth(i);
          const linkBox = await navLink.boundingBox();
          if (linkBox) {
            // Touch targets should be at least 44x44 or have adequate spacing
            expect(linkBox.height).toBeGreaterThanOrEqual(20); // Minimum clickable height
          }
        }
      }
    }

    // Check footer links touch targets
    const footerLinks = page.locator('.footer-links a');
    const footerLinkCount = await footerLinks.count();
    for (let i = 0; i < footerLinkCount; i++) {
      const link = footerLinks.nth(i);
      await link.scrollIntoViewIfNeeded();
      const linkBox = await link.boundingBox();
      if (linkBox) {
        // Footer links should have reasonable touch targets
        expect(linkBox.height).toBeGreaterThanOrEqual(20);
      }
    }
  });

  test('TC4: Text remains readable without horizontal scrolling', async ({ page }) => {
    // Verify no horizontal scroll is required
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScroll).toBe(false);

    // Check that main text elements are within viewport bounds
    const hero = page.locator('.hero-content');
    await expect(hero).toBeVisible();
    const heroBox = await hero.boundingBox();
    expect(heroBox.x).toBeGreaterThanOrEqual(0);
    expect(heroBox.x + heroBox.width).toBeLessThanOrEqual(768);

    // Check hero title is readable (font-size >= 24px for tablet)
    const heroTitle = page.locator('.hero h1');
    const heroFontSize = await heroTitle.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });
    expect(heroFontSize).toBeGreaterThanOrEqual(24);

    // Check tagline is readable
    const tagline = page.locator('.hero .tagline');
    const taglineFontSize = await tagline.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });
    expect(taglineFontSize).toBeGreaterThanOrEqual(14);

    // Check section titles are readable
    const sectionTitles = page.locator('.section-title');
    const titleCount = await sectionTitles.count();
    for (let i = 0; i < titleCount; i++) {
      const title = sectionTitles.nth(i);
      await title.scrollIntoViewIfNeeded();
      const fontSize = await title.evaluate((el) => {
        return parseFloat(window.getComputedStyle(el).fontSize);
      });
      expect(fontSize).toBeGreaterThanOrEqual(20);
    }

    // Check feature card descriptions are readable
    const featureDescriptions = page.locator('.feature-card p');
    const descCount = await featureDescriptions.count();
    for (let i = 0; i < descCount; i++) {
      const desc = featureDescriptions.nth(i);
      await desc.scrollIntoViewIfNeeded();
      const fontSize = await desc.evaluate((el) => {
        return parseFloat(window.getComputedStyle(el).fontSize);
      });
      expect(fontSize).toBeGreaterThanOrEqual(14);
    }

    // Check code blocks don't cause horizontal overflow
    const codeBlocks = page.locator('.code-block');
    const codeBlockCount = await codeBlocks.count();
    for (let i = 0; i < codeBlockCount; i++) {
      const codeBlock = codeBlocks.nth(i);
      await codeBlock.scrollIntoViewIfNeeded();
      const box = await codeBlock.boundingBox();
      if (box) {
        // Code block should fit within viewport (may have internal scroll)
        expect(box.x).toBeGreaterThanOrEqual(0);
        expect(box.x + box.width).toBeLessThanOrEqual(768 + 1); // 1px tolerance for rounding
      }
    }
  });
});
