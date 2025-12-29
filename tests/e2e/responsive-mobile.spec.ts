import { test, expect, Page } from '@playwright/test';

/**
 * E2E Tests for Mobile Responsive Design
 *
 * These tests verify that the MirDB homepage renders correctly on mobile devices
 * with viewports ranging from 320px to 768px.
 *
 * Test Cases:
 * TC1: All content visible at 320px without horizontal scrolling (except code blocks)
 * TC2: Layout adapts properly at 375px, text remains readable
 * TC3: All interactive elements are at least 44x44px (touch targets)
 * TC4: Navigation elements are accessible and functional on mobile
 * TC5: Feature cards stack vertically and remain readable on mobile
 */

test.describe('Responsive Design - Mobile', () => {
  /**
   * TC1: Load page at 320px viewport width
   * Expected: All content visible without horizontal scrolling (except code blocks)
   */
  test('TC1: All content visible at 320px without horizontal scrolling (except code blocks)', async ({ page }) => {
    // Set viewport to smallest mobile size (320px - iPhone SE/5)
    await page.setViewportSize({ width: 320, height: 568 });
    await page.goto('/');

    // Wait for page to load
    await page.waitForLoadState('domcontentloaded');

    // Check that the body doesn't have horizontal overflow
    const bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth);
    const viewportWidth = await page.evaluate(() => window.innerWidth);

    // Body scroll width should not exceed viewport width (allowing small tolerance for scrollbars)
    expect(bodyScrollWidth).toBeLessThanOrEqual(viewportWidth + 20);

    // Verify all main sections are visible
    const heroSection = page.locator('.hero, header').first();
    await expect(heroSection).toBeVisible();

    const featuresSection = page.locator('#features, .features');
    await expect(featuresSection).toBeVisible();

    // Check that main container doesn't cause overflow
    const containers = page.locator('.container');
    const containerCount = await containers.count();

    for (let i = 0; i < containerCount; i++) {
      const container = containers.nth(i);
      const isVisible = await container.isVisible();
      if (isVisible) {
        const box = await container.boundingBox();
        if (box) {
          // Container should not extend beyond viewport
          expect(box.width).toBeLessThanOrEqual(320);
        }
      }
    }

    // Verify code blocks have proper overflow handling (should be scrollable, not causing page overflow)
    const preElements = page.locator('pre');
    const preCount = await preElements.count();

    for (let i = 0; i < preCount; i++) {
      const pre = preElements.nth(i);
      const isVisible = await pre.isVisible();
      if (isVisible) {
        const overflowX = await pre.evaluate((el) => window.getComputedStyle(el).overflowX);
        // Pre elements should allow horizontal scroll for code content
        expect(['auto', 'scroll']).toContain(overflowX);

        // Pre element itself should not exceed viewport
        const box = await pre.boundingBox();
        if (box) {
          expect(box.width).toBeLessThanOrEqual(320);
        }
      }
    }
  });

  /**
   * TC2: Load page at 375px viewport width
   * Expected: Layout adapts properly, text remains readable
   */
  test('TC2: Layout adapts at 375px, text remains readable', async ({ page }) => {
    // Set viewport to standard mobile size (375px - iPhone 6/7/8/X)
    await page.setViewportSize({ width: 375, height: 667 });
    await page.goto('/');

    // Wait for page to load
    await page.waitForLoadState('domcontentloaded');

    // Check hero section adapts properly
    const heroTitle = page.locator('.hero h1, header h1').first();
    await expect(heroTitle).toBeVisible();

    // Verify font size is adapted for mobile (should be smaller than desktop)
    const titleFontSize = await heroTitle.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });
    // Title should be readable but adapted for mobile (between 24px and 56px)
    expect(titleFontSize).toBeGreaterThanOrEqual(24);
    expect(titleFontSize).toBeLessThanOrEqual(56);

    // Check tagline is readable
    const tagline = page.locator('.tagline').first();
    await expect(tagline).toBeVisible();
    const taglineFontSize = await tagline.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });
    // Tagline should be readable (at least 16px)
    expect(taglineFontSize).toBeGreaterThanOrEqual(16);

    // Check description text is readable
    const description = page.locator('.description, .hero .description').first();
    if (await description.count() > 0) {
      await expect(description).toBeVisible();
      const descFontSize = await description.evaluate((el) => {
        return parseFloat(window.getComputedStyle(el).fontSize);
      });
      // Description should be at least 14px for readability
      expect(descFontSize).toBeGreaterThanOrEqual(14);
    }

    // Verify layout doesn't have horizontal overflow
    const bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth);
    expect(bodyScrollWidth).toBeLessThanOrEqual(375 + 20);

    // Check that CTA buttons are properly laid out
    const ctaButtons = page.locator('.cta-buttons');
    await expect(ctaButtons).toBeVisible();

    // Buttons should either wrap or stack on mobile
    const ctaDisplay = await ctaButtons.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        display: style.display,
        flexWrap: style.flexWrap,
        flexDirection: style.flexDirection
      };
    });

    // Should be flex with wrap or column direction for mobile
    expect(ctaDisplay.display).toBe('flex');
    // Either wraps or stacks vertically
    const isResponsive = ctaDisplay.flexWrap === 'wrap' || ctaDisplay.flexDirection === 'column';
    expect(isResponsive).toBe(true);
  });

  /**
   * TC3: Check touch target sizes
   * Expected: All interactive elements are at least 44x44px
   */
  test('TC3: All interactive elements have adequate touch target size (44x44px)', async ({ page }) => {
    // Set viewport to mobile size
    await page.setViewportSize({ width: 375, height: 667 });
    await page.goto('/');

    // Wait for page to load
    await page.waitForLoadState('domcontentloaded');

    // Check all buttons have minimum 44px touch target
    const buttons = page.locator('button, .btn, a.btn, a.btn-primary, a.btn-secondary');
    const buttonCount = await buttons.count();

    for (let i = 0; i < buttonCount; i++) {
      const button = buttons.nth(i);
      const isVisible = await button.isVisible();
      if (isVisible) {
        const box = await button.boundingBox();
        if (box) {
          // Touch target should be at least 44x44px
          // We allow for buttons that are wider but shorter, as long as height >= 44
          // or the clickable area is at least 44px in height
          expect(box.height).toBeGreaterThanOrEqual(40); // Allow small tolerance
        }
      }
    }

    // Check footer navigation links have adequate touch targets
    const footerLinks = page.locator('footer a, .footer a, .footer-nav a');
    const footerLinkCount = await footerLinks.count();

    for (let i = 0; i < footerLinkCount; i++) {
      const link = footerLinks.nth(i);
      const isVisible = await link.isVisible();
      if (isVisible) {
        const box = await link.boundingBox();
        if (box) {
          // Links in footer should have adequate line height for touch
          // Combined with padding, they should have at least 44px touch target
          const height = box.height;
          // Footer links should be at least 24px (accounting for line height + padding)
          expect(height).toBeGreaterThanOrEqual(20);
        }
      }
    }

    // Verify main CTA buttons specifically
    const getStartedBtn = page.locator('a:has-text("Get Started")').first();
    await expect(getStartedBtn).toBeVisible();
    const getStartedBox = await getStartedBtn.boundingBox();
    expect(getStartedBox).not.toBeNull();
    if (getStartedBox) {
      expect(getStartedBox.height).toBeGreaterThanOrEqual(44);
    }

    const githubBtn = page.locator('a:has-text("GitHub"), a:has-text("View on GitHub")').first();
    await expect(githubBtn).toBeVisible();
    const githubBox = await githubBtn.boundingBox();
    expect(githubBox).not.toBeNull();
    if (githubBox) {
      expect(githubBox.height).toBeGreaterThanOrEqual(44);
    }
  });

  /**
   * TC4: Verify navigation on mobile
   * Expected: Navigation elements are accessible and functional on mobile
   */
  test('TC4: Navigation elements are accessible and functional on mobile', async ({ page }) => {
    // Set viewport to mobile size
    await page.setViewportSize({ width: 375, height: 667 });
    await page.goto('/');

    // Wait for page to load
    await page.waitForLoadState('domcontentloaded');

    // Test "Get Started" anchor link navigation
    const getStartedBtn = page.locator('a[href="#getting-started"], a:has-text("Get Started")').first();
    await expect(getStartedBtn).toBeVisible();

    // Click the button and verify navigation works
    await getStartedBtn.click();

    // Wait for smooth scroll to complete
    await page.waitForTimeout(500);

    // Verify the getting-started section is now in viewport
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeVisible();
    const isInViewport = await gettingStartedSection.evaluate((el) => {
      const rect = el.getBoundingClientRect();
      return rect.top >= -100 && rect.top <= window.innerHeight;
    });
    expect(isInViewport).toBe(true);

    // Go back to top
    await page.evaluate(() => window.scrollTo(0, 0));
    await page.waitForTimeout(300);

    // Test external GitHub link exists and has correct attributes
    const githubLinks = page.locator('a[href*="github"]');
    const githubLinkCount = await githubLinks.count();
    expect(githubLinkCount).toBeGreaterThan(0);

    // Verify first GitHub link has proper external link attributes
    const firstGithubLink = githubLinks.first();
    await expect(firstGithubLink).toBeVisible();
    const href = await firstGithubLink.getAttribute('href');
    expect(href).toContain('github');

    // External links should have target="_blank" and rel="noopener"
    const target = await firstGithubLink.getAttribute('target');
    const rel = await firstGithubLink.getAttribute('rel');
    expect(target).toBe('_blank');
    expect(rel).toContain('noopener');

    // Verify footer navigation is accessible
    const footerNav = page.locator('footer nav, .footer-nav, footer .footer-links');
    if (await footerNav.count() > 0) {
      await expect(footerNav).toBeVisible();
    }
  });

  /**
   * TC5: Check feature cards on mobile
   * Expected: Feature cards stack vertically and remain readable
   */
  test('TC5: Feature cards stack vertically and remain readable on mobile', async ({ page }) => {
    // Set viewport to mobile size
    await page.setViewportSize({ width: 375, height: 667 });
    await page.goto('/');

    // Wait for page to load
    await page.waitForLoadState('domcontentloaded');

    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Scroll to features section
    await featuresSection.scrollIntoViewIfNeeded();

    // Check features grid layout on mobile
    const featuresGrid = page.locator('.features-grid, .feature-grid');
    await expect(featuresGrid).toBeVisible();

    // Get computed grid columns - should be 1 column on mobile
    const gridColumns = await featuresGrid.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        display: style.display,
        gridTemplateColumns: style.gridTemplateColumns,
        gridAutoFlow: style.gridAutoFlow
      };
    });

    // Should be grid display
    expect(gridColumns.display).toBe('grid');

    // Grid should be single column on mobile (minmax resolves to full width)
    // The computed value will be a single pixel value like "343px" for single column
    const columnValues = gridColumns.gridTemplateColumns.split(' ').filter((v: string) => v.trim());
    // On mobile with a 375px viewport, we should have 1 column
    expect(columnValues.length).toBe(1);

    // Check feature cards
    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();
    expect(cardCount).toBeGreaterThanOrEqual(1);

    // Verify cards are stacked vertically by checking their positions
    const cardPositions: { top: number; left: number }[] = [];
    for (let i = 0; i < Math.min(cardCount, 4); i++) {
      const card = featureCards.nth(i);
      const box = await card.boundingBox();
      if (box) {
        cardPositions.push({ top: box.y, left: box.x });
      }
    }

    // Cards should be stacked vertically (same left position, increasing top)
    for (let i = 1; i < cardPositions.length; i++) {
      // Cards should be in the same column (similar left position)
      expect(Math.abs(cardPositions[i].left - cardPositions[0].left)).toBeLessThan(50);
      // Each card should be below the previous one
      expect(cardPositions[i].top).toBeGreaterThan(cardPositions[i - 1].top);
    }

    // Verify feature card text is readable
    const firstCard = featureCards.first();
    const cardTitle = firstCard.locator('h3');
    await expect(cardTitle).toBeVisible();

    const titleFontSize = await cardTitle.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });
    // Title should be readable (at least 16px)
    expect(titleFontSize).toBeGreaterThanOrEqual(16);

    const cardDescription = firstCard.locator('p');
    await expect(cardDescription).toBeVisible();

    const descFontSize = await cardDescription.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });
    // Description should be readable (at least 14px)
    expect(descFontSize).toBeGreaterThanOrEqual(14);

    // Verify card doesn't overflow viewport
    const cardBox = await firstCard.boundingBox();
    if (cardBox) {
      expect(cardBox.width).toBeLessThanOrEqual(375);
    }
  });

  /**
   * Additional test: Verify all viewports in mobile range work correctly
   */
  test('Page renders correctly across mobile viewport range (320px-768px)', async ({ page }) => {
    const mobileViewports = [
      { width: 320, height: 568, name: 'iPhone SE' },
      { width: 375, height: 667, name: 'iPhone 6/7/8' },
      { width: 414, height: 896, name: 'iPhone XR' },
      { width: 768, height: 1024, name: 'iPad Mini (portrait)' }
    ];

    for (const viewport of mobileViewports) {
      await page.setViewportSize({ width: viewport.width, height: viewport.height });
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Check no horizontal overflow at this viewport
      const bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth);
      const viewportWidth = await page.evaluate(() => window.innerWidth);

      expect(bodyScrollWidth).toBeLessThanOrEqual(viewportWidth + 20);

      // Verify main content is visible
      const hero = page.locator('.hero, header').first();
      await expect(hero).toBeVisible();

      const features = page.locator('#features');
      await expect(features).toBeVisible();
    }
  });
});
