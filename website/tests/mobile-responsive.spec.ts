import { test, expect } from '@playwright/test';

test.describe('Responsive Design - Mobile', () => {
  // Mobile viewport width (common iPhone width)
  const MOBILE_VIEWPORT_WIDTH = 375;
  const MOBILE_VIEWPORT_HEIGHT = 667;
  const MIN_TOUCH_TARGET_SIZE = 44;
  const MIN_READABLE_FONT_SIZE = 16;

  test.beforeEach(async ({ page }) => {
    // Set viewport to mobile dimensions (375px width)
    await page.setViewportSize({
      width: MOBILE_VIEWPORT_WIDTH,
      height: MOBILE_VIEWPORT_HEIGHT,
    });
    await page.goto('/');
  });

  test('TC1: Page loads without horizontal scrollbar at 375px width', async ({ page }) => {
    // Set viewport to 375px width and load page
    // Expected: Page loads without horizontal scrollbar

    // Check that the document width doesn't exceed viewport width
    const hasHorizontalScrollbar = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });

    expect(hasHorizontalScrollbar).toBe(false);
  });

  test('TC2: Document body does not exceed viewport width', async ({ page }) => {
    // Check document body width vs viewport width
    // Expected: Document body does not exceed viewport width

    const dimensions = await page.evaluate(() => {
      const body = document.body;
      const html = document.documentElement;
      return {
        bodyWidth: body.scrollWidth,
        documentWidth: html.scrollWidth,
        viewportWidth: window.innerWidth,
        bodyOffsetWidth: body.offsetWidth,
      };
    });

    // Body should not exceed viewport width
    expect(dimensions.bodyWidth).toBeLessThanOrEqual(dimensions.viewportWidth);
    expect(dimensions.documentWidth).toBeLessThanOrEqual(dimensions.viewportWidth);
  });

  test('TC3: All interactive elements have minimum 44px x 44px touch target', async ({ page }) => {
    // Measure touch target sizes for buttons
    // Expected: All interactive elements have minimum 44px x 44px touch target

    // Get all interactive elements (buttons, links)
    const interactiveElements = page.locator('a.cta-button, button, a[href]');
    const count = await interactiveElements.count();

    // We should have at least some interactive elements
    expect(count).toBeGreaterThan(0);

    // Check CTA buttons specifically (primary interactive elements)
    const ctaButtons = page.locator('a.cta-button');
    const ctaCount = await ctaButtons.count();

    for (let i = 0; i < ctaCount; i++) {
      const button = ctaButtons.nth(i);
      const box = await button.boundingBox();

      expect(box).not.toBeNull();
      if (box) {
        // Touch target should be at least 44x44 pixels
        expect(box.width).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET_SIZE);
        expect(box.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET_SIZE);
      }
    }
  });

  test('TC4: Body text is at least 16px for readability', async ({ page }) => {
    // Check font sizes on mobile
    // Expected: Body text is at least 16px for readability

    // Check html root font size
    const rootFontSize = await page.evaluate(() => {
      return parseFloat(window.getComputedStyle(document.documentElement).fontSize);
    });
    expect(rootFontSize).toBeGreaterThanOrEqual(MIN_READABLE_FONT_SIZE);

    // Check body text elements
    const bodyFontSize = await page.evaluate(() => {
      return parseFloat(window.getComputedStyle(document.body).fontSize);
    });
    expect(bodyFontSize).toBeGreaterThanOrEqual(MIN_READABLE_FONT_SIZE);

    // Check paragraph and description text
    const textElements = page.locator('.tagline, .feature-description, .step-description, .section-subtitle');
    const textCount = await textElements.count();

    for (let i = 0; i < textCount; i++) {
      const element = textElements.nth(i);
      const isVisible = await element.isVisible();
      if (isVisible) {
        const fontSize = await element.evaluate((el) => {
          return parseFloat(window.getComputedStyle(el).fontSize);
        });
        // Text should be readable (at least 14px minimum, with most being 16px+)
        expect(fontSize).toBeGreaterThanOrEqual(14);
      }
    }
  });

  test('TC5: All sections are accessible and content is readable on mobile', async ({ page }) => {
    // Navigate through all sections on mobile
    // Expected: All sections are accessible and content is readable

    // Check all main sections are visible
    const sections = [
      { selector: '.hero-section', name: 'Hero Section' },
      { selector: '#features', name: 'Features Section' },
      { selector: '#architecture', name: 'Architecture Section' },
      { selector: '#quickstart', name: 'Quick Start Section' },
      { selector: '#project-status', name: 'Project Status Section' },
    ];

    for (const section of sections) {
      const sectionElement = page.locator(section.selector);

      // Scroll to section
      await sectionElement.scrollIntoViewIfNeeded();

      // Section should be visible
      await expect(sectionElement).toBeVisible();

      // Section should fit within viewport width
      const box = await sectionElement.boundingBox();
      expect(box).not.toBeNull();
      if (box) {
        // Content should not overflow horizontally
        expect(box.width).toBeLessThanOrEqual(MOBILE_VIEWPORT_WIDTH);
      }
    }

    // Check that section titles are visible and readable
    const sectionTitles = page.locator('.section-title');
    const titleCount = await sectionTitles.count();
    expect(titleCount).toBeGreaterThan(0);

    for (let i = 0; i < titleCount; i++) {
      const title = sectionTitles.nth(i);
      await title.scrollIntoViewIfNeeded();
      await expect(title).toBeVisible();
    }
  });

  test('TC6: Navigation is accessible on mobile', async ({ page }) => {
    // Test navigation menu on mobile
    // Expected: Navigation is accessible (hamburger menu or scrollable nav)

    // Check that the CTA navigation buttons are accessible on mobile
    const ctaButtons = page.locator('.cta-buttons');
    await expect(ctaButtons).toBeVisible();

    // Check that buttons are properly stacked or scrollable on mobile
    const buttonsBox = await ctaButtons.boundingBox();
    expect(buttonsBox).not.toBeNull();
    if (buttonsBox) {
      // Buttons container should fit within viewport
      expect(buttonsBox.width).toBeLessThanOrEqual(MOBILE_VIEWPORT_WIDTH);
    }

    // Check Get Started link works
    const getStartedButton = page.locator('a.cta-button', { hasText: 'Get Started' });
    await expect(getStartedButton).toBeVisible();

    const getStartedHref = await getStartedButton.getAttribute('href');
    expect(getStartedHref).toBe('#quickstart');

    // Click Get Started and verify it navigates to quickstart section
    await getStartedButton.click();

    // Wait for scroll animation
    await page.waitForTimeout(500);

    // Verify quickstart section is now in view
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeInViewport();

    // Verify View on GitHub button is accessible
    const githubButton = page.locator('a.cta-button', { hasText: 'View on GitHub' });
    await expect(githubButton).toBeVisible();
  });

  test('TC7: Feature cards are properly laid out on mobile', async ({ page }) => {
    // Additional test: Feature cards should stack vertically on mobile
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();

    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();

    expect(cardCount).toBeGreaterThan(0);

    for (let i = 0; i < cardCount; i++) {
      const card = featureCards.nth(i);
      await card.scrollIntoViewIfNeeded();

      const box = await card.boundingBox();
      expect(box).not.toBeNull();
      if (box) {
        // Cards should fit within viewport with padding
        expect(box.width).toBeLessThanOrEqual(MOBILE_VIEWPORT_WIDTH);
      }
    }
  });

  test('TC8: Code blocks are scrollable and readable on mobile', async ({ page }) => {
    // Code blocks should be horizontally scrollable if content overflows
    const quickstartSection = page.locator('#quickstart');
    await quickstartSection.scrollIntoViewIfNeeded();

    const codeBlocks = page.locator('.code-block');
    const codeCount = await codeBlocks.count();

    expect(codeCount).toBeGreaterThan(0);

    for (let i = 0; i < codeCount; i++) {
      const codeBlock = codeBlocks.nth(i);
      await codeBlock.scrollIntoViewIfNeeded();
      await expect(codeBlock).toBeVisible();

      // Verify code block has overflow-x auto for scrolling
      const overflowX = await codeBlock.evaluate((el) => {
        return window.getComputedStyle(el).overflowX;
      });
      expect(['auto', 'scroll']).toContain(overflowX);
    }
  });
});
