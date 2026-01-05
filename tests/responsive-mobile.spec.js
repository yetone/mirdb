const { test, expect } = require('@playwright/test');

test.describe('Responsive Design - Mobile Viewport', () => {
  // Set mobile viewport for all tests in this describe block (iPhone SE size)
  test.use({ viewport: { width: 375, height: 667 } });

  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Page renders correctly at 375x667 viewport without horizontal scrollbar', async ({ page }) => {
    // Wait for page to fully load
    await page.waitForLoadState('domcontentloaded');

    // Check that there is no horizontal scrollbar / overflow
    const hasHorizontalOverflow = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalOverflow).toBe(false);

    // Verify viewport dimensions
    const viewportSize = page.viewportSize();
    expect(viewportSize.width).toBe(375);
    expect(viewportSize.height).toBe(667);

    // Verify key sections are visible and within viewport width
    const sections = [
      { selector: 'header', name: 'Header' },
      { selector: '.hero', name: 'Hero' },
      { selector: '#features', name: 'Features' },
      { selector: '#quickstart', name: 'Quick Start' },
      { selector: '#commands', name: 'Commands' },
      { selector: 'footer', name: 'Footer' }
    ];

    for (const section of sections) {
      const element = page.locator(section.selector);
      await expect(element).toBeVisible();

      const box = await element.boundingBox();
      expect(box.x).toBeGreaterThanOrEqual(0);
      expect(box.x + box.width).toBeLessThanOrEqual(375 + 1); // 1px tolerance
    }
  });

  test('TC2: Hero section stacks vertically and remains readable at mobile viewport', async ({ page }) => {
    const heroContent = page.locator('.hero-content');
    await expect(heroContent).toBeVisible();

    // Check hero title visibility and font size
    const heroTitle = page.locator('.hero h1');
    await expect(heroTitle).toBeVisible();
    const heroTitleFontSize = await heroTitle.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });
    // At mobile (below 768px), hero h1 should be 2.5rem = 40px
    expect(heroTitleFontSize).toBeGreaterThanOrEqual(32);
    expect(heroTitleFontSize).toBeLessThanOrEqual(48);

    // Check tagline visibility and font size
    const tagline = page.locator('.hero .tagline');
    await expect(tagline).toBeVisible();
    const taglineFontSize = await tagline.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });
    // At mobile, tagline should be 1.125rem = 18px
    expect(taglineFontSize).toBeGreaterThanOrEqual(14);

    // Check CTA buttons are visible and stacked or wrapped
    const ctaButtons = page.locator('.cta-buttons');
    await expect(ctaButtons).toBeVisible();

    // Verify CTA buttons have flex-wrap
    const ctaFlexWrap = await ctaButtons.evaluate((el) => {
      return window.getComputedStyle(el).flexWrap;
    });
    expect(ctaFlexWrap).toBe('wrap');

    // Verify hero content fits within viewport
    const heroBox = await heroContent.boundingBox();
    expect(heroBox.x).toBeGreaterThanOrEqual(0);
    expect(heroBox.x + heroBox.width).toBeLessThanOrEqual(375);

    // Verify buttons meet touch target size (44x44 minimum)
    const primaryCTA = page.locator('#primary-cta');
    const primaryBox = await primaryCTA.boundingBox();
    expect(primaryBox.height).toBeGreaterThanOrEqual(44);
    expect(primaryBox.width).toBeGreaterThanOrEqual(44);

    const secondaryCTA = page.locator('#secondary-cta');
    const secondaryBox = await secondaryCTA.boundingBox();
    expect(secondaryBox.height).toBeGreaterThanOrEqual(44);
    expect(secondaryBox.width).toBeGreaterThanOrEqual(44);
  });

  test('TC3: Feature cards stack in single column at mobile viewport', async ({ page }) => {
    // Navigate to features section
    await page.locator('#features').scrollIntoViewIfNeeded();

    const featuresGrid = page.locator('.features-grid');
    await expect(featuresGrid).toBeVisible();

    // Check that grid is set to single column at mobile
    const gridTemplateColumns = await featuresGrid.evaluate((el) => {
      return window.getComputedStyle(el).gridTemplateColumns;
    });
    // At mobile with grid-template-columns: 1fr, the computed value should be a single column width
    // The value will be like "327px" (single column) not "327px 327px" (multiple columns)
    const columnCount = gridTemplateColumns.split(' ').filter(col => col.trim() !== '').length;
    expect(columnCount).toBe(1);

    // Get all feature cards
    const featureCards = page.locator('.feature-card');
    const count = await featureCards.count();
    expect(count).toBe(3);

    // Get bounding boxes of all cards
    const cards = await featureCards.all();
    const boundingBoxes = await Promise.all(cards.map(card => card.boundingBox()));
    const validBoxes = boundingBoxes.filter(box => box !== null);
    expect(validBoxes.length).toBe(3);

    // Verify cards are stacked vertically (each card on its own row)
    // Each card should have a different Y position
    const yPositions = validBoxes.map(box => box.y);
    const uniqueYPositions = new Set(yPositions.map(y => Math.round(y)));
    expect(uniqueYPositions.size).toBe(3); // 3 unique Y positions = 3 rows

    // Verify cards don't overflow horizontally
    for (const box of validBoxes) {
      expect(box.x).toBeGreaterThanOrEqual(0);
      expect(box.x + box.width).toBeLessThanOrEqual(375);
    }

    // Verify cards are ordered top to bottom
    for (let i = 1; i < validBoxes.length; i++) {
      expect(validBoxes[i].y).toBeGreaterThan(validBoxes[i - 1].y);
    }
  });

  test('TC4: Navigation is accessible via hamburger menu or collapses appropriately at mobile viewport', async ({ page }) => {
    // Check that desktop nav-links is hidden at mobile viewport
    const navLinks = page.locator('.nav-links');

    // Desktop nav should be hidden via CSS media query
    const navLinksDisplay = await navLinks.evaluate((el) => {
      return window.getComputedStyle(el).display;
    });
    expect(navLinksDisplay).toBe('none');

    // Logo should still be visible
    const logo = page.locator('.logo');
    await expect(logo).toBeVisible();

    // Verify navigation header is present and accessible
    const header = page.locator('header');
    await expect(header).toBeVisible();

    // Verify header fits within mobile viewport
    const headerBox = await header.boundingBox();
    expect(headerBox.x).toBeGreaterThanOrEqual(0);
    expect(headerBox.x + headerBox.width).toBeLessThanOrEqual(375);

    // Check if hamburger menu exists (optional - could be implemented)
    // For now, verify that navigation collapses appropriately by hiding desktop nav
    const hamburger = page.locator('.hamburger, .mobile-menu, .menu-toggle, [aria-label="Menu"], .mobile-nav-toggle');
    const hamburgerCount = await hamburger.count();

    // Either hamburger menu exists, or nav is simply hidden (both are valid mobile approaches)
    // The current implementation hides nav at mobile - this is a valid responsive pattern
    if (hamburgerCount === 0) {
      // If no hamburger, verify desktop nav is hidden
      expect(navLinksDisplay).toBe('none');
    } else {
      // If hamburger exists, verify it's visible and clickable
      await expect(hamburger.first()).toBeVisible();
    }

    // Verify footer links remain accessible (alternative navigation path)
    const footerLinks = page.locator('.footer-links a');
    await page.locator('footer').scrollIntoViewIfNeeded();

    const footerLinkCount = await footerLinks.count();
    expect(footerLinkCount).toBeGreaterThan(0);

    // Footer links should be accessible and have adequate touch targets
    for (let i = 0; i < footerLinkCount; i++) {
      const link = footerLinks.nth(i);
      await expect(link).toBeVisible();
      const linkBox = await link.boundingBox();
      expect(linkBox.height).toBeGreaterThanOrEqual(44); // 44px minimum touch target
    }
  });

  test('TC5: Code blocks have horizontal scroll if needed, not page-wide overflow', async ({ page }) => {
    // Navigate to quickstart section
    await page.locator('#quickstart').scrollIntoViewIfNeeded();

    const codeBlocks = page.locator('.code-block');
    const codeBlockCount = await codeBlocks.count();
    expect(codeBlockCount).toBeGreaterThan(0);

    // Verify page doesn't have horizontal overflow
    const pageHasOverflow = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(pageHasOverflow).toBe(false);

    for (let i = 0; i < codeBlockCount; i++) {
      const codeBlock = codeBlocks.nth(i);
      await codeBlock.scrollIntoViewIfNeeded();

      // Verify code block fits within viewport
      const box = await codeBlock.boundingBox();
      expect(box.x).toBeGreaterThanOrEqual(0);
      expect(box.x + box.width).toBeLessThanOrEqual(375 + 1); // 1px tolerance

      // Verify code block has overflow-x: auto for internal scrolling
      const overflowX = await codeBlock.evaluate((el) => {
        return window.getComputedStyle(el).overflowX;
      });
      expect(['auto', 'scroll']).toContain(overflowX);

      // Verify pre elements inside code blocks don't cause overflow
      const pre = codeBlock.locator('pre');
      const preCount = await pre.count();

      if (preCount > 0) {
        // Pre content may overflow code block, but should scroll internally
        const preElement = pre.first();
        const codeBlockRect = await codeBlock.evaluate((el) => {
          const rect = el.getBoundingClientRect();
          return { width: rect.width, scrollWidth: el.scrollWidth };
        });

        // If content overflows, code block should handle it with scrolling
        if (codeBlockRect.scrollWidth > codeBlockRect.width) {
          expect(['auto', 'scroll']).toContain(overflowX);
        }
      }
    }

    // Also check command items don't overflow
    const commandItems = page.locator('.command-item');
    await page.locator('#commands').scrollIntoViewIfNeeded();

    const commandCount = await commandItems.count();
    for (let i = 0; i < commandCount; i++) {
      const item = commandItems.nth(i);
      const box = await item.boundingBox();
      if (box) {
        expect(box.x).toBeGreaterThanOrEqual(0);
        expect(box.x + box.width).toBeLessThanOrEqual(375 + 1);
      }
    }

    // Verify config grid items don't overflow
    await page.locator('#configuration').scrollIntoViewIfNeeded();
    const configItems = page.locator('.config-item');
    const configCount = await configItems.count();

    for (let i = 0; i < configCount; i++) {
      const item = configItems.nth(i);
      const box = await item.boundingBox();
      if (box) {
        expect(box.x).toBeGreaterThanOrEqual(0);
        expect(box.x + box.width).toBeLessThanOrEqual(375 + 1);
      }
    }
  });
});
