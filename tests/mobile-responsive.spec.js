// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Mobile Responsive Design Tests
 * Tests that verify the homepage displays correctly on mobile devices
 * with proper layout adjustments (375x667 viewport - iPhone SE dimensions)
 */

test.describe('Responsive Design - Mobile', () => {
  // Set viewport to mobile size (iPhone SE dimensions)
  test.use({ viewport: { width: 375, height: 667 } });

  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Page renders without horizontal scrollbar at 375px viewport width', async ({ page }) => {
    // Check that the document width matches the viewport width (no horizontal overflow)
    const documentWidth = await page.evaluate(() => document.documentElement.scrollWidth);
    const viewportWidth = await page.evaluate(() => window.innerWidth);

    // Document width should not exceed viewport width (no horizontal scrollbar)
    expect(documentWidth).toBeLessThanOrEqual(viewportWidth);

    // Also verify by checking if horizontal scrollbar is present
    const hasHorizontalScrollbar = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScrollbar).toBe(false);
  });

  test('TC2: Hero content is readable and CTAs are tappable on mobile', async ({ page }) => {
    const heroSection = page.locator('.hero');
    await expect(heroSection).toBeVisible();

    // Check that hero heading is visible and readable
    const h1 = heroSection.locator('h1');
    await expect(h1).toBeVisible();
    await expect(h1).toContainText('MirDB');

    // Check that tagline is visible and readable
    const tagline = heroSection.locator('.tagline');
    await expect(tagline).toBeVisible();

    // Check that description is visible
    const description = heroSection.locator('.description');
    await expect(description).toBeVisible();

    // Verify CTA buttons are visible and have adequate tap target size (minimum 44x44px recommended)
    const primaryCta = heroSection.locator('.btn-primary');
    const secondaryCta = heroSection.locator('.btn-secondary');

    await expect(primaryCta).toBeVisible();
    await expect(secondaryCta).toBeVisible();

    // Check that buttons have adequate touch target size (at least 44px height)
    const primaryBox = await primaryCta.boundingBox();
    const secondaryBox = await secondaryCta.boundingBox();

    expect(primaryBox).not.toBeNull();
    expect(secondaryBox).not.toBeNull();
    expect(primaryBox.height).toBeGreaterThanOrEqual(44);
    expect(secondaryBox.height).toBeGreaterThanOrEqual(44);

    // Verify buttons are fully within viewport (not cut off)
    expect(primaryBox.x).toBeGreaterThanOrEqual(0);
    expect(primaryBox.x + primaryBox.width).toBeLessThanOrEqual(375);
    expect(secondaryBox.x).toBeGreaterThanOrEqual(0);
    expect(secondaryBox.x + secondaryBox.width).toBeLessThanOrEqual(375);
  });

  test('TC3: Feature cards stack vertically and are fully visible on mobile', async ({ page }) => {
    // Scroll to features section
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();
    await expect(featuresSection).toBeVisible();

    // Get all feature cards
    const featureCards = featuresSection.locator('.feature-card');
    await expect(featureCards).toHaveCount(3);

    // Get bounding boxes for all cards to check vertical stacking
    const cards = await featureCards.all();
    const cardBoxes = await Promise.all(cards.map(card => card.boundingBox()));

    // Verify all cards are visible
    for (const box of cardBoxes) {
      expect(box).not.toBeNull();
    }

    // On mobile (375px), cards should stack vertically
    // Check that each card starts below the previous one (vertical stacking)
    for (let i = 1; i < cardBoxes.length; i++) {
      // Each card's top should be at or below the previous card's bottom
      expect(cardBoxes[i].y).toBeGreaterThanOrEqual(cardBoxes[i - 1].y + cardBoxes[i - 1].height - 5);
    }

    // Verify cards are fully within viewport width (not cut off horizontally)
    for (const box of cardBoxes) {
      expect(box.x).toBeGreaterThanOrEqual(0);
      expect(box.x + box.width).toBeLessThanOrEqual(375 + 5); // Small tolerance for borders
    }

    // Check that feature card content is visible
    for (const card of cards) {
      const title = card.locator('.feature-title');
      const description = card.locator('.feature-description');
      await expect(title).toBeVisible();
      await expect(description).toBeVisible();
    }
  });

  test('TC4: Code blocks are scrollable or wrap appropriately on mobile', async ({ page }) => {
    // Scroll to getting-started section with code blocks
    const quickStartSection = page.locator('#getting-started');
    await quickStartSection.scrollIntoViewIfNeeded();
    await expect(quickStartSection).toBeVisible();

    // Get all code blocks
    const codeBlocks = quickStartSection.locator('.code-block');
    const codeBlockCount = await codeBlocks.count();
    expect(codeBlockCount).toBeGreaterThan(0);

    // Check each code block
    const blocks = await codeBlocks.all();
    for (const block of blocks) {
      await block.scrollIntoViewIfNeeded();
      await expect(block).toBeVisible();

      const box = await block.boundingBox();
      expect(box).not.toBeNull();

      // Code block should be within viewport width
      expect(box.x).toBeGreaterThanOrEqual(0);
      expect(box.x + box.width).toBeLessThanOrEqual(375 + 20); // Allow small padding

      // Check that code block has horizontal scroll if content overflows
      const pre = block.locator('pre');
      const preOverflowX = await pre.evaluate(el => {
        const styles = window.getComputedStyle(el);
        return styles.overflowX;
      });

      // Either auto, scroll, or the content fits without overflow
      const contentFits = await pre.evaluate(el => el.scrollWidth <= el.clientWidth);
      const hasHorizontalScroll = preOverflowX === 'auto' || preOverflowX === 'scroll';

      // Code blocks should either have scroll capability or content should fit
      expect(contentFits || hasHorizontalScroll).toBe(true);
    }
  });

  test('TC5: All buttons and links are tappable with adequate touch targets', async ({ page }) => {
    // Minimum recommended touch target size is 44x44px
    const MIN_TOUCH_TARGET = 44;

    // Test hero section buttons
    const heroButtons = page.locator('.hero .btn');
    const heroButtonsCount = await heroButtons.count();

    for (let i = 0; i < heroButtonsCount; i++) {
      const button = heroButtons.nth(i);
      await expect(button).toBeVisible();

      const box = await button.boundingBox();
      expect(box).not.toBeNull();
      expect(box.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
    }

    // Test footer links
    await page.locator('.footer').scrollIntoViewIfNeeded();
    const footerLinks = page.locator('.footer-links a');
    const footerLinksCount = await footerLinks.count();

    for (let i = 0; i < footerLinksCount; i++) {
      const link = footerLinks.nth(i);
      await expect(link).toBeVisible();

      const box = await link.boundingBox();
      expect(box).not.toBeNull();
      // Links should have reasonable touch target (we check height and line-height)
      expect(box.height).toBeGreaterThanOrEqual(24); // Minimum for links with proper line-height
    }

    // Test copy buttons in code blocks
    await page.locator('#getting-started').scrollIntoViewIfNeeded();
    const copyButtons = page.locator('.copy-btn');
    const copyButtonsCount = await copyButtons.count();

    for (let i = 0; i < copyButtonsCount; i++) {
      const button = copyButtons.nth(i);
      const isVisible = await button.isVisible();

      if (isVisible) {
        const box = await button.boundingBox();
        if (box) {
          // Copy buttons should have reasonable touch target
          expect(box.height).toBeGreaterThanOrEqual(24);
          expect(box.width).toBeGreaterThanOrEqual(40);
        }
      }
    }
  });

  test('Commands section displays properly on mobile', async ({ page }) => {
    // Scroll to commands section
    const commandsSection = page.locator('#commands');
    await commandsSection.scrollIntoViewIfNeeded();
    await expect(commandsSection).toBeVisible();

    // Get all command cards
    const commandCards = commandsSection.locator('.command-card');
    const commandCardsCount = await commandCards.count();
    expect(commandCardsCount).toBeGreaterThan(0);

    // Check that command cards are within viewport
    const cards = await commandCards.all();
    for (const card of cards) {
      await card.scrollIntoViewIfNeeded();
      await expect(card).toBeVisible();

      const box = await card.boundingBox();
      expect(box).not.toBeNull();

      // Cards should fit within mobile viewport width
      expect(box.x).toBeGreaterThanOrEqual(0);
      expect(box.x + box.width).toBeLessThanOrEqual(375 + 20);
    }
  });

  test('Footer displays properly on mobile', async ({ page }) => {
    // Scroll to footer
    const footer = page.locator('.footer');
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();

    // Check footer links are visible and accessible
    const footerLinks = footer.locator('.footer-links');
    await expect(footerLinks).toBeVisible();

    // Check that footer links wrap or fit within mobile viewport
    const footerLinksBox = await footerLinks.boundingBox();
    expect(footerLinksBox).not.toBeNull();
    expect(footerLinksBox.width).toBeLessThanOrEqual(375);

    // Check copyright text is visible
    const copyright = footer.locator('.copyright');
    await expect(copyright).toBeVisible();
  });

  test('Page has proper mobile viewport meta tag', async ({ page }) => {
    // Check that the viewport meta tag is present and configured correctly
    const viewportMeta = await page.locator('meta[name="viewport"]').getAttribute('content');
    expect(viewportMeta).toBeTruthy();
    expect(viewportMeta).toContain('width=device-width');
    expect(viewportMeta).toContain('initial-scale=1');
  });
});
