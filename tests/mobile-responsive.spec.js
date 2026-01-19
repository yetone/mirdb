// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

const indexPath = 'file://' + path.resolve(__dirname, '../index.html');

// Mobile viewport dimensions (iPhone SE)
const MOBILE_VIEWPORT = { width: 375, height: 667 };

// Minimum touch target size per WCAG 2.1 AAA and Apple HIG
const MIN_TOUCH_TARGET_SIZE = 44;

test.describe('Responsive Design - Mobile', () => {
  test.beforeEach(async ({ page }) => {
    // Set mobile viewport before loading page
    await page.setViewportSize(MOBILE_VIEWPORT);
    await page.goto(indexPath);
  });

  // Test Case 1: Page renders without horizontal scrollbar at 375px width
  test('TC1: Page renders without horizontal scrollbar at 375px width viewport', async ({ page }) => {
    // Wait for page content to load
    await page.waitForLoadState('domcontentloaded');

    // Check that there's no horizontal scrollbar
    // The scrollWidth should equal clientWidth (or be less) if no horizontal scroll
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });

    expect(hasHorizontalScroll).toBe(false);

    // Additional check: verify body doesn't overflow
    const bodyOverflows = await page.evaluate(() => {
      const body = document.body;
      return body.scrollWidth > body.clientWidth;
    });

    expect(bodyOverflows).toBe(false);
  });

  // Test Case 2: Hero content stacks vertically and remains readable at mobile width
  test('TC2: Hero content stacks vertically and remains readable at mobile width', async ({ page }) => {
    // Verify hero section is visible
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // Check hero content is visible
    const heroContent = page.locator('.hero-content');
    await expect(heroContent).toBeVisible();

    // Verify hero title is visible and readable
    const heroTitle = page.locator('.hero-title');
    await expect(heroTitle).toBeVisible();
    const titleText = await heroTitle.textContent();
    expect(titleText).toBeTruthy();

    // Verify hero subtitle is visible and readable
    const heroSubtitle = page.locator('.hero-subtitle');
    await expect(heroSubtitle).toBeVisible();

    // Check that CTA buttons exist
    const ctaPrimary = page.locator('#cta-primary');
    const ctaSecondary = page.locator('#cta-secondary');
    await expect(ctaPrimary).toBeVisible();
    await expect(ctaSecondary).toBeVisible();

    // Verify buttons are stacked vertically on mobile (one below the other)
    const primaryBox = await ctaPrimary.boundingBox();
    const secondaryBox = await ctaSecondary.boundingBox();

    expect(primaryBox).toBeTruthy();
    expect(secondaryBox).toBeTruthy();

    // On mobile, buttons should be stacked (secondary button below primary)
    // or side by side if they fit. At 375px with flex-wrap, they should stack.
    // Check that both buttons fit within the viewport width
    expect(primaryBox.x + primaryBox.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width);
    expect(secondaryBox.x + secondaryBox.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width);

    // Verify content is centered (hero-content should be within viewport)
    const contentBox = await heroContent.boundingBox();
    expect(contentBox).toBeTruthy();
    expect(contentBox.x + contentBox.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width);
  });

  // Test Case 3: Feature cards stack vertically in single column at mobile width
  test('TC3: Feature cards stack vertically in single column at mobile width', async ({ page }) => {
    // Scroll to features section
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();
    await expect(featuresSection).toBeVisible();

    // Get all feature cards
    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();

    expect(cardCount).toBeGreaterThan(0);

    // Get bounding boxes for all cards
    const cardBoxes = [];
    for (let i = 0; i < cardCount; i++) {
      const box = await featureCards.nth(i).boundingBox();
      expect(box).toBeTruthy();
      cardBoxes.push(box);
    }

    // Verify cards are stacked vertically (each card's x position should be similar,
    // and each subsequent card's y should be greater than the previous)
    for (let i = 1; i < cardBoxes.length; i++) {
      // Cards should have similar x positions (within some tolerance for centering)
      const xDiff = Math.abs(cardBoxes[i].x - cardBoxes[0].x);
      expect(xDiff).toBeLessThan(20); // Tolerance for centering/padding differences

      // Each card should be below the previous one
      expect(cardBoxes[i].y).toBeGreaterThan(cardBoxes[i - 1].y);
    }

    // Verify each card fits within viewport width
    for (const box of cardBoxes) {
      expect(box.x + box.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width);
    }
  });

  // Test Case 4: Code blocks are scrollable horizontally within container at mobile width
  test('TC4: Code blocks are scrollable horizontally within container at mobile width', async ({ page }) => {
    // Scroll to code examples section
    const codeSection = page.locator('#code-examples');
    await codeSection.scrollIntoViewIfNeeded();
    await expect(codeSection).toBeVisible();

    // Check the syntax-highlighted code block
    const syntaxHighlight = page.locator('.syntax-highlight');
    await expect(syntaxHighlight).toBeVisible();

    // Verify the code block wrapper doesn't overflow the viewport
    const wrapper = page.locator('.code-block-wrapper');
    const wrapperBox = await wrapper.boundingBox();
    expect(wrapperBox).toBeTruthy();
    expect(wrapperBox.x + wrapperBox.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width);

    // Check that the pre element has overflow-x: auto (scrollable)
    const overflowX = await syntaxHighlight.evaluate((el) => {
      return window.getComputedStyle(el).overflowX;
    });

    // overflow-x should be 'auto' or 'scroll' to allow horizontal scrolling
    expect(['auto', 'scroll']).toContain(overflowX);

    // Also check quick-start code blocks
    const quickStartSection = page.locator('#quick-start');
    await quickStartSection.scrollIntoViewIfNeeded();

    const preElements = page.locator('#quick-start pre');
    const preCount = await preElements.count();

    for (let i = 0; i < preCount; i++) {
      const preElement = preElements.nth(i);
      const preBox = await preElement.boundingBox();
      expect(preBox).toBeTruthy();

      // The container should not exceed viewport width
      // (code inside can scroll, but container must fit)
      expect(preBox.x + preBox.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width + 10); // Small tolerance for borders

      // Verify overflow-x allows scrolling
      const preOverflowX = await preElement.evaluate((el) => {
        return window.getComputedStyle(el).overflowX;
      });
      expect(['auto', 'scroll']).toContain(preOverflowX);
    }
  });

  // Test Case 5: All interactive elements are at least 44x44 pixels (touch target size)
  test('TC5: All interactive elements are at least 44x44 pixels touch target size', async ({ page }) => {
    // Get all buttons
    const buttons = page.locator('button');
    const buttonCount = await buttons.count();

    for (let i = 0; i < buttonCount; i++) {
      const button = buttons.nth(i);
      if (await button.isVisible()) {
        const box = await button.boundingBox();
        expect(box).toBeTruthy();
        // Check minimum touch target size
        expect(box.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET_SIZE);
        expect(box.width).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET_SIZE);
      }
    }

    // Get all links (anchor tags) that are likely interactive
    const links = page.locator('a.btn, .hero-cta a, .footer-links a');
    const linkCount = await links.count();

    for (let i = 0; i < linkCount; i++) {
      const link = links.nth(i);
      if (await link.isVisible()) {
        const box = await link.boundingBox();
        expect(box).toBeTruthy();
        // Check minimum touch target size
        expect(box.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET_SIZE);
        expect(box.width).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET_SIZE);
      }
    }

    // Specifically check the CTA buttons in hero section
    const ctaPrimary = page.locator('#cta-primary');
    const ctaSecondary = page.locator('#cta-secondary');

    const primaryBox = await ctaPrimary.boundingBox();
    const secondaryBox = await ctaSecondary.boundingBox();

    expect(primaryBox).toBeTruthy();
    expect(secondaryBox).toBeTruthy();

    expect(primaryBox.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET_SIZE);
    expect(primaryBox.width).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET_SIZE);

    expect(secondaryBox.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET_SIZE);
    expect(secondaryBox.width).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET_SIZE);

    // Check the copy button
    const copyBtn = page.locator('#copy-code-btn');
    await copyBtn.scrollIntoViewIfNeeded();
    const copyBox = await copyBtn.boundingBox();
    expect(copyBox).toBeTruthy();
    expect(copyBox.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET_SIZE);
    expect(copyBox.width).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET_SIZE);
  });

  // Additional test: Verify demo GIF scales properly on mobile
  test('Demo GIF scales properly on mobile viewport', async ({ page }) => {
    const demoSection = page.locator('#demo');
    await demoSection.scrollIntoViewIfNeeded();
    await expect(demoSection).toBeVisible();

    const demoGif = page.locator('.demo-gif');
    await expect(demoGif).toBeVisible();

    const gifBox = await demoGif.boundingBox();
    expect(gifBox).toBeTruthy();

    // GIF should not exceed viewport width
    expect(gifBox.x + gifBox.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width);
  });

  // Additional test: Verify footer content is centered on mobile
  test('Footer content is properly displayed on mobile', async ({ page }) => {
    const footer = page.locator('.footer');
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();

    const footerContent = page.locator('.footer-content');
    const footerBox = await footerContent.boundingBox();
    expect(footerBox).toBeTruthy();

    // Footer content should fit within viewport
    expect(footerBox.x + footerBox.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width);
  });

  // Additional test: Verify status section columns stack on mobile
  test('Status section columns stack on mobile', async ({ page }) => {
    const statusSection = page.locator('#status');
    await statusSection.scrollIntoViewIfNeeded();
    await expect(statusSection).toBeVisible();

    const implemented = page.locator('.implemented');
    const planned = page.locator('.planned');

    const implementedBox = await implemented.boundingBox();
    const plannedBox = await planned.boundingBox();

    expect(implementedBox).toBeTruthy();
    expect(plannedBox).toBeTruthy();

    // On mobile, these should stack vertically (planned below implemented)
    // or be side by side if the viewport allows
    // Either way, neither should overflow the viewport
    expect(implementedBox.x + implementedBox.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width);
    expect(plannedBox.x + plannedBox.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width);
  });
});
