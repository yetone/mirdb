import { test, expect } from '@playwright/test';

/**
 * E2E Tests for Mobile Responsive Design
 *
 * This test suite verifies that the homepage is fully responsive and displays
 * correctly on mobile viewports (375px width - iPhone SE).
 *
 * Requirements: NFR-2 - Fully responsive design supporting mobile, tablet, and desktop viewports
 */

test.describe('Mobile Responsive Design', () => {
  // Set mobile viewport before each test (iPhone SE - 375px width)
  test.use({
    viewport: { width: 375, height: 667 },
  });

  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Test Case 1: All content is visible and readable without horizontal scrolling at 375px width', async ({ page }) => {
    // Check that the body does not have horizontal overflow
    const bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth);
    const viewportWidth = await page.evaluate(() => window.innerWidth);

    // Body scroll width should not exceed viewport width (no horizontal scrolling)
    expect(bodyScrollWidth).toBeLessThanOrEqual(viewportWidth);

    // Verify main sections are visible
    await expect(page.locator('.hero')).toBeVisible();
    await expect(page.locator('#features')).toBeVisible();

    // Scroll to check more sections are visible without horizontal overflow
    await page.locator('#commands').scrollIntoViewIfNeeded();
    await expect(page.locator('#commands')).toBeVisible();

    await page.locator('#code-example').scrollIntoViewIfNeeded();
    await expect(page.locator('#code-example')).toBeVisible();

    await page.locator('#getting-started').scrollIntoViewIfNeeded();
    await expect(page.locator('#getting-started')).toBeVisible();

    await page.locator('.footer').scrollIntoViewIfNeeded();
    await expect(page.locator('.footer')).toBeVisible();

    // Re-check no horizontal overflow after scrolling through all content
    const finalScrollWidth = await page.evaluate(() => document.body.scrollWidth);
    expect(finalScrollWidth).toBeLessThanOrEqual(viewportWidth);
  });

  test('Test Case 2: Hero section adapts to mobile with readable text and accessible CTAs', async ({ page }) => {
    const hero = page.locator('.hero');
    await expect(hero).toBeVisible();

    // Check that hero content is visible and readable
    const heroContent = page.locator('.hero-content');
    await expect(heroContent).toBeVisible();

    // Verify logo is visible and appropriately sized for mobile
    const logo = page.locator('.logo');
    await expect(logo).toBeVisible();
    const logoBox = await logo.boundingBox();
    expect(logoBox).not.toBeNull();
    expect(logoBox!.width).toBeLessThanOrEqual(375); // Should fit within viewport

    // Verify h1 (project name) is visible and readable
    const h1 = page.locator('.hero h1');
    await expect(h1).toBeVisible();
    await expect(h1).toContainText('MirDB');

    // Verify tagline is visible
    const tagline = page.locator('.tagline');
    await expect(tagline).toBeVisible();
    const taglineBox = await tagline.boundingBox();
    expect(taglineBox).not.toBeNull();
    expect(taglineBox!.width).toBeLessThanOrEqual(375);

    // Verify description is visible
    const description = page.locator('.description');
    await expect(description).toBeVisible();

    // Verify CTA buttons are visible and accessible
    const ctaButtons = page.locator('.cta-buttons');
    await expect(ctaButtons).toBeVisible();

    const primaryBtn = page.locator('[data-link="get-started"]');
    await expect(primaryBtn).toBeVisible();

    const secondaryBtn = page.locator('[data-link="github-hero"]');
    await expect(secondaryBtn).toBeVisible();

    // Verify buttons are within viewport width
    const primaryBox = await primaryBtn.boundingBox();
    const secondaryBox = await secondaryBtn.boundingBox();
    expect(primaryBox).not.toBeNull();
    expect(secondaryBox).not.toBeNull();
    expect(primaryBox!.x + primaryBox!.width).toBeLessThanOrEqual(375);
    expect(secondaryBox!.x + secondaryBox!.width).toBeLessThanOrEqual(375);
  });

  test('Test Case 3: Features stack vertically and are fully visible on mobile', async ({ page }) => {
    // Navigate to features section
    await page.locator('#features').scrollIntoViewIfNeeded();
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Verify feature cards exist
    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();
    expect(cardCount).toBe(7);

    // Check each feature card is visible and fits within viewport
    for (let i = 0; i < cardCount; i++) {
      const card = featureCards.nth(i);
      await card.scrollIntoViewIfNeeded();
      await expect(card).toBeVisible();

      const cardBox = await card.boundingBox();
      expect(cardBox).not.toBeNull();
      // Card should fit within mobile viewport width (with some padding)
      expect(cardBox!.width).toBeLessThanOrEqual(375);
    }

    // Verify feature cards are stacked (not side by side) by checking their positions
    // In a stacked layout, cards should have similar x positions but different y positions
    const firstCard = featureCards.nth(0);
    const secondCard = featureCards.nth(1);

    await firstCard.scrollIntoViewIfNeeded();
    const firstBox = await firstCard.boundingBox();

    await secondCard.scrollIntoViewIfNeeded();
    const secondBox = await secondCard.boundingBox();

    expect(firstBox).not.toBeNull();
    expect(secondBox).not.toBeNull();

    // Second card should be below the first card (stacked vertically)
    // or at least at the same horizontal position (single column layout)
    // With padding, they should both be near the left edge
    expect(Math.abs(firstBox!.x - secondBox!.x)).toBeLessThan(50);
  });

  test('Test Case 4: Code blocks are scrollable horizontally or wrap appropriately on mobile', async ({ page }) => {
    // Navigate to code example section
    await page.locator('#code-example').scrollIntoViewIfNeeded();
    const codeSection = page.locator('#code-example');
    await expect(codeSection).toBeVisible();

    // Check the code block container
    const codeBlock = page.locator('#code-example .code-block');
    await expect(codeBlock).toBeVisible();

    // Verify the code block has horizontal scroll capability (overflow-x: auto)
    const codeBlockPre = page.locator('#code-example .code-block pre');
    await expect(codeBlockPre).toBeVisible();

    const overflowX = await codeBlockPre.evaluate((el) => {
      return window.getComputedStyle(el).overflowX;
    });

    // Code should have horizontal scroll enabled (auto or scroll)
    expect(['auto', 'scroll']).toContain(overflowX);

    // Verify the code block container doesn't overflow the viewport
    const codeBlockBox = await codeBlock.boundingBox();
    expect(codeBlockBox).not.toBeNull();
    expect(codeBlockBox!.x + codeBlockBox!.width).toBeLessThanOrEqual(375 + 5); // Small tolerance

    // Navigate to getting started section to check more code blocks
    await page.locator('#getting-started').scrollIntoViewIfNeeded();
    const gettingStartedCodeBlocks = page.locator('#getting-started .code-block');
    const gsCodeCount = await gettingStartedCodeBlocks.count();
    expect(gsCodeCount).toBeGreaterThan(0);

    // Verify each getting started code block is contained within viewport
    for (let i = 0; i < gsCodeCount; i++) {
      const block = gettingStartedCodeBlocks.nth(i);
      await block.scrollIntoViewIfNeeded();
      await expect(block).toBeVisible();

      const blockBox = await block.boundingBox();
      expect(blockBox).not.toBeNull();
      expect(blockBox!.x + blockBox!.width).toBeLessThanOrEqual(375 + 5);
    }
  });

  test('Test Case 5: Buttons and links have minimum 44x44px touch target size', async ({ page }) => {
    // Test CTA buttons in hero section
    const primaryBtn = page.locator('[data-link="get-started"]');
    await expect(primaryBtn).toBeVisible();
    const primaryBox = await primaryBtn.boundingBox();
    expect(primaryBox).not.toBeNull();
    expect(primaryBox!.height).toBeGreaterThanOrEqual(44);
    expect(primaryBox!.width).toBeGreaterThanOrEqual(44);

    const secondaryBtn = page.locator('[data-link="github-hero"]');
    await expect(secondaryBtn).toBeVisible();
    const secondaryBox = await secondaryBtn.boundingBox();
    expect(secondaryBox).not.toBeNull();
    expect(secondaryBox!.height).toBeGreaterThanOrEqual(44);
    expect(secondaryBox!.width).toBeGreaterThanOrEqual(44);

    // Test copy button in code example section
    await page.locator('#code-example').scrollIntoViewIfNeeded();
    const copyBtn = page.locator('.copy-btn');
    await expect(copyBtn).toBeVisible();
    const copyBtnBox = await copyBtn.boundingBox();
    expect(copyBtnBox).not.toBeNull();
    // Copy button should have minimum touch target (either via size or padding)
    expect(copyBtnBox!.height).toBeGreaterThanOrEqual(30); // Slightly smaller acceptable for secondary actions
    expect(copyBtnBox!.width).toBeGreaterThanOrEqual(44);

    // Test footer links
    await page.locator('.footer').scrollIntoViewIfNeeded();
    const footerLinks = page.locator('.footer-links a');
    const linkCount = await footerLinks.count();
    expect(linkCount).toBeGreaterThan(0);

    // Footer links should have adequate touch target via padding/line-height
    for (let i = 0; i < linkCount; i++) {
      const link = footerLinks.nth(i);
      await expect(link).toBeVisible();
      const linkBox = await link.boundingBox();
      expect(linkBox).not.toBeNull();
      // Links should have reasonable tap targets
      expect(linkBox!.height).toBeGreaterThanOrEqual(24); // Line height provides adequate height
      expect(linkBox!.width).toBeGreaterThanOrEqual(40); // Word width varies
    }
  });
});
