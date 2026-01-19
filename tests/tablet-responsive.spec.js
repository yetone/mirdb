// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

const indexPath = 'file://' + path.resolve(__dirname, '../index.html');

// Tablet viewport dimensions (iPad portrait mode)
const TABLET_VIEWPORT = { width: 768, height: 1024 };

test.describe('Responsive Design - Tablet', () => {
  test.beforeEach(async ({ page }) => {
    // Set tablet viewport before loading page
    await page.setViewportSize(TABLET_VIEWPORT);
    await page.goto(indexPath);
  });

  // Test Case 1: Page renders correctly at 768px width viewport
  test('TC1: Page renders correctly with appropriate layout at 768px width', async ({ page }) => {
    // Wait for page content to load
    await page.waitForLoadState('domcontentloaded');

    // Check that there's no horizontal scrollbar
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });

    expect(hasHorizontalScroll).toBe(false);

    // Verify body doesn't overflow
    const bodyOverflows = await page.evaluate(() => {
      const body = document.body;
      return body.scrollWidth > body.clientWidth;
    });

    expect(bodyOverflows).toBe(false);

    // Verify hero section is visible and properly rendered
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // Verify hero title is visible
    const heroTitle = page.locator('.hero-title');
    await expect(heroTitle).toBeVisible();
    const titleText = await heroTitle.textContent();
    expect(titleText).toContain('MirDB');

    // Verify hero subtitle is visible
    const heroSubtitle = page.locator('.hero-subtitle');
    await expect(heroSubtitle).toBeVisible();

    // Verify CTA buttons are visible
    const ctaPrimary = page.locator('#cta-primary');
    const ctaSecondary = page.locator('#cta-secondary');
    await expect(ctaPrimary).toBeVisible();
    await expect(ctaSecondary).toBeVisible();

    // Verify features section is present
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Verify code examples section is present
    const codeExamples = page.locator('#code-examples');
    await expect(codeExamples).toBeVisible();

    // Verify quick start section is present
    const quickStart = page.locator('#quick-start');
    await expect(quickStart).toBeVisible();

    // Verify footer is present
    const footer = page.locator('.footer');
    await expect(footer).toBeVisible();
  });

  // Test Case 2: Feature cards display in 2-column or adaptive grid layout
  test('TC2: Feature cards display in 2-column or adaptive grid layout at tablet width', async ({ page }) => {
    // Scroll to features section
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();
    await expect(featuresSection).toBeVisible();

    // Get all feature cards
    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();

    expect(cardCount).toBe(4); // There are 4 feature cards

    // Get bounding boxes for all cards
    const cardBoxes = [];
    for (let i = 0; i < cardCount; i++) {
      const box = await featureCards.nth(i).boundingBox();
      expect(box).toBeTruthy();
      cardBoxes.push(box);
    }

    // At tablet width (768px), with grid-template-columns: repeat(auto-fit, minmax(250px, 1fr))
    // Cards should be in a 2-column layout (2 cards per row)
    // Verify cards are arranged in grid pattern

    // Check first row: card 0 and card 1 should have same y position (approximately)
    const yTolerance = 10; // tolerance for floating point differences
    expect(Math.abs(cardBoxes[0].y - cardBoxes[1].y)).toBeLessThan(yTolerance);

    // Card 0 should be to the left of card 1
    expect(cardBoxes[0].x).toBeLessThan(cardBoxes[1].x);

    // Check second row: card 2 and card 3 should have same y position
    expect(Math.abs(cardBoxes[2].y - cardBoxes[3].y)).toBeLessThan(yTolerance);

    // Card 2 should be to the left of card 3
    expect(cardBoxes[2].x).toBeLessThan(cardBoxes[3].x);

    // Second row should be below first row
    expect(cardBoxes[2].y).toBeGreaterThan(cardBoxes[0].y);
    expect(cardBoxes[3].y).toBeGreaterThan(cardBoxes[1].y);

    // Verify each card fits within viewport width
    for (const box of cardBoxes) {
      expect(box.x + box.width).toBeLessThanOrEqual(TABLET_VIEWPORT.width);
    }

    // Verify cards have visible content
    for (let i = 0; i < cardCount; i++) {
      const card = featureCards.nth(i);
      const icon = card.locator('.feature-icon');
      const title = card.locator('h3');
      const description = card.locator('p');

      await expect(icon).toBeVisible();
      await expect(title).toBeVisible();
      await expect(description).toBeVisible();
    }
  });

  // Test Case 3: Navigation is accessible and usable at tablet width
  test('TC3: Navigation is accessible and usable at tablet width', async ({ page }) => {
    // Verify main navigation links (CTAs in hero) are visible and accessible
    const ctaPrimary = page.locator('#cta-primary');
    const ctaSecondary = page.locator('#cta-secondary');

    await expect(ctaPrimary).toBeVisible();
    await expect(ctaSecondary).toBeVisible();

    // Check that buttons have adequate size for touch interaction
    const primaryBox = await ctaPrimary.boundingBox();
    const secondaryBox = await ctaSecondary.boundingBox();

    expect(primaryBox).toBeTruthy();
    expect(secondaryBox).toBeTruthy();

    // Minimum touch target size
    const MIN_TOUCH_TARGET = 44;
    expect(primaryBox.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
    expect(primaryBox.width).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
    expect(secondaryBox.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
    expect(secondaryBox.width).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);

    // Verify buttons are side by side on tablet (not stacked)
    // At 768px, buttons should have space to be side by side
    const yDiff = Math.abs(primaryBox.y - secondaryBox.y);
    expect(yDiff).toBeLessThan(primaryBox.height); // Same row

    // Verify links are clickable (have proper href)
    const primaryHref = await ctaPrimary.getAttribute('href');
    const secondaryHref = await ctaSecondary.getAttribute('href');

    expect(primaryHref).toBe('https://github.com/yetone/mirdb');
    expect(secondaryHref).toBe('#quick-start');

    // Verify footer links are accessible
    const footer = page.locator('.footer');
    await footer.scrollIntoViewIfNeeded();

    const footerLinks = page.locator('.footer-links a');
    const footerLinkCount = await footerLinks.count();

    for (let i = 0; i < footerLinkCount; i++) {
      const link = footerLinks.nth(i);
      await expect(link).toBeVisible();

      const box = await link.boundingBox();
      expect(box).toBeTruthy();
      expect(box.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
      expect(box.width).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
    }

    // Verify internal anchor link works
    await ctaSecondary.click();

    // Wait for scroll to complete
    await page.waitForTimeout(500);

    // Quick start section should now be in viewport
    const quickStart = page.locator('#quick-start');
    await expect(quickStart).toBeInViewport();
  });

  // Additional test: Verify hero section adapts appropriately for tablet
  test('Hero section adapts appropriately for tablet viewport', async ({ page }) => {
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    const heroContent = page.locator('.hero-content');
    const contentBox = await heroContent.boundingBox();
    expect(contentBox).toBeTruthy();

    // Hero content should be centered and fit within viewport
    expect(contentBox.x + contentBox.width).toBeLessThanOrEqual(TABLET_VIEWPORT.width);

    // Check logo is visible and sized appropriately
    const logo = page.locator('.hero-logo');
    await expect(logo).toBeVisible();

    const logoBox = await logo.boundingBox();
    expect(logoBox).toBeTruthy();
    // Logo should not exceed viewport
    expect(logoBox.width).toBeLessThanOrEqual(TABLET_VIEWPORT.width);
  });

  // Additional test: Verify code examples section layout on tablet
  test('Code examples section displays correctly on tablet', async ({ page }) => {
    const codeSection = page.locator('#code-examples');
    await codeSection.scrollIntoViewIfNeeded();
    await expect(codeSection).toBeVisible();

    // Check code block wrapper
    const wrapper = page.locator('.code-block-wrapper');
    await expect(wrapper).toBeVisible();

    const wrapperBox = await wrapper.boundingBox();
    expect(wrapperBox).toBeTruthy();
    expect(wrapperBox.x + wrapperBox.width).toBeLessThanOrEqual(TABLET_VIEWPORT.width);

    // Verify copy button is visible and accessible
    const copyBtn = page.locator('#copy-code-btn');
    await expect(copyBtn).toBeVisible();

    const copyBtnBox = await copyBtn.boundingBox();
    expect(copyBtnBox).toBeTruthy();
    expect(copyBtnBox.height).toBeGreaterThanOrEqual(44);
    expect(copyBtnBox.width).toBeGreaterThanOrEqual(44);
  });

  // Additional test: Verify quick start section layout on tablet
  test('Quick start section displays in appropriate layout on tablet', async ({ page }) => {
    const quickStart = page.locator('#quick-start');
    await quickStart.scrollIntoViewIfNeeded();
    await expect(quickStart).toBeVisible();

    // Get the installation and usage blocks
    const installation = page.locator('.installation');
    const usage = page.locator('.usage');

    await expect(installation).toBeVisible();
    await expect(usage).toBeVisible();

    const installBox = await installation.boundingBox();
    const usageBox = await usage.boundingBox();

    expect(installBox).toBeTruthy();
    expect(usageBox).toBeTruthy();

    // At tablet width with grid-template-columns: repeat(auto-fit, minmax(300px, 1fr))
    // The two blocks should be in 2-column layout (side by side)
    // Check they are on the same row (similar y position)
    const yDiff = Math.abs(installBox.y - usageBox.y);
    expect(yDiff).toBeLessThan(installBox.height);

    // Installation should be to the left of usage
    expect(installBox.x).toBeLessThan(usageBox.x);

    // Both should fit within viewport
    expect(installBox.x + installBox.width).toBeLessThanOrEqual(TABLET_VIEWPORT.width);
    expect(usageBox.x + usageBox.width).toBeLessThanOrEqual(TABLET_VIEWPORT.width);
  });

  // Additional test: Verify status section layout on tablet
  test('Status section displays in appropriate layout on tablet', async ({ page }) => {
    const statusSection = page.locator('#status');
    await statusSection.scrollIntoViewIfNeeded();
    await expect(statusSection).toBeVisible();

    // Get implemented and planned columns
    const implemented = page.locator('.implemented');
    const planned = page.locator('.planned');

    await expect(implemented).toBeVisible();
    await expect(planned).toBeVisible();

    const implBox = await implemented.boundingBox();
    const plannedBox = await planned.boundingBox();

    expect(implBox).toBeTruthy();
    expect(plannedBox).toBeTruthy();

    // At 768px width, status-content uses repeat(auto-fit, minmax(250px, 1fr))
    // Should display in 2-column layout
    // Check they are on the same row
    const yDiff = Math.abs(implBox.y - plannedBox.y);
    expect(yDiff).toBeLessThan(implBox.height);

    // Implemented should be to the left of planned
    expect(implBox.x).toBeLessThan(plannedBox.x);

    // Both should fit within viewport
    expect(implBox.x + implBox.width).toBeLessThanOrEqual(TABLET_VIEWPORT.width);
    expect(plannedBox.x + plannedBox.width).toBeLessThanOrEqual(TABLET_VIEWPORT.width);
  });

  // Additional test: Verify demo section scales properly on tablet
  test('Demo section displays correctly on tablet', async ({ page }) => {
    const demoSection = page.locator('#demo');
    await demoSection.scrollIntoViewIfNeeded();
    await expect(demoSection).toBeVisible();

    const demoGif = page.locator('.demo-gif');
    await expect(demoGif).toBeVisible();

    const gifBox = await demoGif.boundingBox();
    expect(gifBox).toBeTruthy();

    // GIF should not exceed viewport width
    expect(gifBox.x + gifBox.width).toBeLessThanOrEqual(TABLET_VIEWPORT.width);
  });

  // Additional test: Verify footer displays correctly on tablet
  test('Footer displays correctly on tablet', async ({ page }) => {
    const footer = page.locator('.footer');
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();

    const footerContent = page.locator('.footer-content');
    const footerBox = await footerContent.boundingBox();
    expect(footerBox).toBeTruthy();

    // Footer should fit within viewport
    expect(footerBox.x + footerBox.width).toBeLessThanOrEqual(TABLET_VIEWPORT.width);

    // Verify footer links are displayed horizontally
    const footerLinks = page.locator('.footer-links');
    await expect(footerLinks).toBeVisible();

    // Verify license and copyright text are visible
    const license = page.locator('.license');
    const copyright = page.locator('.copyright');
    await expect(license).toBeVisible();
    await expect(copyright).toBeVisible();
  });
});
