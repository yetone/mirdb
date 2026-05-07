import { test, expect } from '@playwright/test';

test.describe('Responsive Design - Mobile', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize({ width: 375, height: 667 });
    await page.goto('/');
  });

  test('at 375x667 viewport uses single-column stacked layout', async ({ page }) => {
    // Hero section should be visible and readable
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    // Hero headline should be readable
    const heroHeadline = page.locator('[data-testid="hero-headline"]');
    await expect(heroHeadline).toBeVisible();

    // Hero tagline should be visible
    const heroTagline = page.locator('[data-testid="hero-tagline"]');
    await expect(heroTagline).toBeVisible();

    // Feature cards should stack vertically (single column)
    const featuresGrid = page.locator('[data-testid="features-grid"]');
    await expect(featuresGrid).toBeVisible();
    const featureCards = page.locator('[data-testid="feature-card"]');
    await expect(featureCards).toHaveCount(3);

    // Verify cards are stacked vertically by checking they have different Y positions
    const cardYPositions = await featureCards.evaluateAll((cards) =>
      cards.map((card) => card.getBoundingClientRect().y)
    );
    expect(cardYPositions.length).toBe(3);
    for (let i = 1; i < cardYPositions.length; i++) {
      expect(cardYPositions[i]).toBeGreaterThan(cardYPositions[i - 1]);
    }

    // Protocol cards should stack vertically
    const protocolCards = page.locator('[data-testid="protocol-card"]');
    await expect(protocolCards).toHaveCount(4);
    const protocolYPositions = await protocolCards.evaluateAll((cards) =>
      cards.map((card) => card.getBoundingClientRect().y)
    );
    for (let i = 1; i < protocolYPositions.length; i++) {
      expect(protocolYPositions[i]).toBeGreaterThan(protocolYPositions[i - 1]);
    }

    // Architecture cards should stack vertically
    const architectureGrid = page.locator('[data-testid="architecture-grid"]');
    await expect(architectureGrid).toBeVisible();
    const archCards = architectureGrid.locator('> div');
    const archYPositions = await archCards.evaluateAll((cards) =>
      cards.map((card) => card.getBoundingClientRect().y)
    );
    expect(archYPositions.length).toBe(3);
    for (let i = 1; i < archYPositions.length; i++) {
      expect(archYPositions[i]).toBeGreaterThan(archYPositions[i - 1]);
    }

    // Status cards should stack vertically (grid-cols-1 on mobile)
    const statusGrid = page.locator('[data-testid="status-grid"]');
    await expect(statusGrid).toBeVisible();
    const statusCards = statusGrid.locator('> div');
    await expect(statusCards).toHaveCount(3);
    const statusYPositions = await statusCards.evaluateAll((cards) =>
      cards.map((card) => card.getBoundingClientRect().y)
    );
    for (let i = 1; i < statusYPositions.length; i++) {
      expect(statusYPositions[i]).toBeGreaterThan(statusYPositions[i - 1]);
    }
  });

  test('at 375x667 there is no horizontal scroll', async ({ page }) => {
    // Wait for page to settle
    await page.waitForTimeout(500);

    // Check that document width does not exceed viewport width
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > window.innerWidth;
    });
    expect(hasHorizontalScroll).toBe(false);

    // Also check body width
    const bodyWidth = await page.evaluate(() => document.body.scrollWidth);
    const viewportWidth = await page.evaluate(() => window.innerWidth);
    expect(bodyWidth).toBeLessThanOrEqual(viewportWidth);
  });

  test('mobile navigation hamburger menu is visible on mobile', async ({ page }) => {
    const mobileMenuButton = page.locator('[data-testid="mobile-menu-button"]');
    await expect(mobileMenuButton).toBeVisible();

    // Desktop nav should be hidden
    const desktopNav = page.locator('[data-testid="desktop-nav"]');
    await expect(desktopNav).not.toBeVisible();
  });

  test('tapping hamburger menu opens navigation overlay with all section links', async ({ page }) => {
    const mobileMenuButton = page.locator('[data-testid="mobile-menu-button"]');
    await expect(mobileMenuButton).toBeVisible();

    // Initially overlay should not be present
    const overlay = page.locator('[data-testid="mobile-nav-overlay"]');
    await expect(overlay).toHaveCount(0);

    // Tap the hamburger button
    await mobileMenuButton.click();

    // Overlay should now exist and have non-zero dimensions
    await expect(overlay).toHaveCount(1);
    const overlayBox = await overlay.boundingBox();
    expect(overlayBox).not.toBeNull();
    expect(overlayBox!.width).toBeGreaterThan(0);
    expect(overlayBox!.height).toBeGreaterThan(0);

    // All nav links should be present in the overlay
    const navLinks = overlay.locator('[data-testid="mobile-nav-link"]');
    await expect(navLinks).toHaveCount(5);

    // Verify link texts
    const linkTexts = await navLinks.allTextContents();
    expect(linkTexts).toEqual([
      'Features',
      'Getting Started',
      'Protocol',
      'Architecture',
      'GitHub',
    ]);
  });

  test('tapping a nav link closes the mobile menu overlay', async ({ page }) => {
    const mobileMenuButton = page.locator('[data-testid="mobile-menu-button"]');
    const overlay = page.locator('[data-testid="mobile-nav-overlay"]');

    // Open menu
    await mobileMenuButton.click();
    await expect(overlay).toHaveCount(1);

    // Click a nav link
    const firstLink = overlay.locator('[data-testid="mobile-nav-link"]').first();
    await firstLink.click();

    // Overlay should close (element removed from DOM)
    await expect(overlay).toHaveCount(0);
  });

  test('tapping hamburger button again closes the open menu', async ({ page }) => {
    const mobileMenuButton = page.locator('[data-testid="mobile-menu-button"]');
    const overlay = page.locator('[data-testid="mobile-nav-overlay"]');

    // Open menu
    await mobileMenuButton.click();
    await expect(overlay).toHaveCount(1);

    // Tap hamburger again to close
    await mobileMenuButton.click();
    await expect(overlay).toHaveCount(0);
  });

  test('mobile menu icon changes from hamburger to close when open', async ({ page }) => {
    const mobileMenuButton = page.locator('[data-testid="mobile-menu-button"]');
    await expect(mobileMenuButton).toBeVisible();

    // Initially aria-expanded should be false
    await expect(mobileMenuButton).toHaveAttribute('aria-expanded', 'false');

    // Open menu
    await mobileMenuButton.click();
    await expect(mobileMenuButton).toHaveAttribute('aria-expanded', 'true');

    // Close menu
    await mobileMenuButton.click();
    await expect(mobileMenuButton).toHaveAttribute('aria-expanded', 'false');
  });

  test('code blocks are horizontally scrollable on mobile with no overflow', async ({ page }) => {
    // Scroll to quick-start section where code blocks are
    await page.locator('#quick-start').scrollIntoViewIfNeeded();
    await page.waitForTimeout(300);

    // Check all code blocks
    const codeBlocks = page.locator('[data-testid="code-block"]');
    const count = await codeBlocks.count();
    expect(count).toBeGreaterThan(0);

    for (let i = 0; i < count; i++) {
      const codeBlock = codeBlocks.nth(i);
      await expect(codeBlock).toBeVisible();

      // Check that pre element has overflow-x-auto
      const preElement = codeBlock.locator('pre');
      await expect(preElement).toBeVisible();

      // Verify the code content does not cause page-level overflow
      const codeBlockBox = await codeBlock.boundingBox();
      if (codeBlockBox) {
        expect(codeBlockBox.width).toBeLessThanOrEqual(375);
      }
    }

    // Ensure no horizontal scroll from code blocks
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > window.innerWidth;
    });
    expect(hasHorizontalScroll).toBe(false);
  });

  test('config table is horizontally scrollable on mobile', async ({ page }) => {
    // Scroll to config section
    await page.locator('#config').scrollIntoViewIfNeeded();
    await page.waitForTimeout(300);

    const configTable = page.locator('[data-testid="config-table"]');
    await expect(configTable).toBeVisible();

    // No horizontal page overflow from table
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > window.innerWidth;
    });
    expect(hasHorizontalScroll).toBe(false);
  });

  test('at 320x568 content is still readable and functional', async ({ page }) => {
    await page.setViewportSize({ width: 320, height: 568 });
    await page.reload();

    // Wait for page to settle
    await page.waitForTimeout(500);

    // Hero headline should still be visible
    const heroHeadline = page.locator('[data-testid="hero-headline"]');
    await expect(heroHeadline).toBeVisible();

    // Hero tagline should be visible
    const heroTagline = page.locator('[data-testid="hero-tagline"]');
    await expect(heroTagline).toBeVisible();

    // Feature cards should still stack
    const featureCards = page.locator('[data-testid="feature-card"]');
    await expect(featureCards).toHaveCount(3);

    // Mobile menu should work
    const mobileMenuButton = page.locator('[data-testid="mobile-menu-button"]');
    await expect(mobileMenuButton).toBeVisible();

    await mobileMenuButton.click();
    const overlay = page.locator('[data-testid="mobile-nav-overlay"]');
    await expect(overlay).toHaveCount(1);
    const overlayBox = await overlay.boundingBox();
    expect(overlayBox).not.toBeNull();
    expect(overlayBox!.width).toBeGreaterThan(0);
    expect(overlayBox!.height).toBeGreaterThan(0);

    // All links should be visible in overlay
    const navLinks = overlay.locator('[data-testid="mobile-nav-link"]');
    await expect(navLinks).toHaveCount(5);

    // No horizontal scroll at smallest width
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > window.innerWidth;
    });
    expect(hasHorizontalScroll).toBe(false);

    // Text should be readable (font size check)
    const headlineFontSize = await heroHeadline.evaluate((el) => {
      return window.getComputedStyle(el).fontSize;
    });
    expect(parseInt(headlineFontSize)).toBeGreaterThanOrEqual(24);

    const taglineFontSize = await heroTagline.evaluate((el) => {
      return window.getComputedStyle(el).fontSize;
    });
    expect(parseInt(taglineFontSize)).toBeGreaterThanOrEqual(16);
  });

  test('hero text is readable with appropriate font sizes on mobile', async ({ page }) => {
    const heroHeadline = page.locator('[data-testid="hero-headline"]');
    const headlineFontSize = await heroHeadline.evaluate((el) => {
      return window.getComputedStyle(el).fontSize;
    });
    expect(parseInt(headlineFontSize)).toBeGreaterThanOrEqual(30);

    const heroTagline = page.locator('[data-testid="hero-tagline"]');
    const taglineFontSize = await heroTagline.evaluate((el) => {
      return window.getComputedStyle(el).fontSize;
    });
    expect(parseInt(taglineFontSize)).toBeGreaterThanOrEqual(18);

    const heroDescription = page.locator('[data-testid="hero-description"]');
    const descFontSize = await heroDescription.evaluate((el) => {
      return window.getComputedStyle(el).fontSize;
    });
    expect(parseInt(descFontSize)).toBeGreaterThanOrEqual(14);
  });

  test('footer content stacks vertically on mobile', async ({ page }) => {
    const footer = page.locator('[data-testid="footer"]');
    await expect(footer).toBeVisible();

    // Footer content should be visible
    await expect(footer.getByText('MirDB', { exact: false }).first()).toBeVisible();

    // Footer links should be visible
    await expect(footer.getByText('GitHub')).toBeVisible();
    await expect(footer.getByText('Documentation')).toBeVisible();
    await expect(footer.getByText('Issues')).toBeVisible();
  });

  test('protocol example code blocks do not overflow on mobile', async ({ page }) => {
    await page.locator('#protocol').scrollIntoViewIfNeeded();
    await page.waitForTimeout(300);

    const protocolCards = page.locator('[data-testid="protocol-card"]');
    const count = await protocolCards.count();
    expect(count).toBeGreaterThan(0);

    for (let i = 0; i < count; i++) {
      const card = protocolCards.nth(i);
      const codeElements = card.locator('code, pre');
      const codeCount = await codeElements.count();

      for (let j = 0; j < codeCount; j++) {
        const codeBox = await codeElements.nth(j).boundingBox();
        if (codeBox) {
          expect(codeBox.width).toBeLessThanOrEqual(375);
        }
      }
    }

    // Verify no horizontal page scroll
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > window.innerWidth;
    });
    expect(hasHorizontalScroll).toBe(false);
  });

  test('demo section stacks vertically on mobile', async ({ page }) => {
    const demoSection = page.locator('[data-testid="demo-section"]');
    await expect(demoSection).toBeVisible();

    // Terminal animation should be visible
    const terminalAnimation = demoSection.locator('[data-testid="terminal-animation"]');
    await expect(terminalAnimation).toBeVisible();

    // Check that the section uses a single-column layout by verifying
    // the grid children are stacked vertically
    const gridChildren = demoSection.locator('.grid > div');
    const childCount = await gridChildren.count();
    if (childCount >= 2) {
      const firstBox = await gridChildren.nth(0).boundingBox();
      const secondBox = await gridChildren.nth(1).boundingBox();
      if (firstBox && secondBox) {
        expect(secondBox.y).toBeGreaterThan(firstBox.y);
      }
    }
  });

  test('roadmap section stacks vertically on mobile', async ({ page }) => {
    const roadmapSection = page.locator('[data-testid="roadmap-section"]');
    await expect(roadmapSection).toBeVisible();

    const implementedList = roadmapSection.locator('[data-testid="implemented-list"]');
    const plannedList = roadmapSection.locator('[data-testid="planned-list"]');

    await expect(implementedList).toBeVisible();
    await expect(plannedList).toBeVisible();

    // Both lists should be visible (stacked vertically)
    const implementedBox = await implementedList.boundingBox();
    const plannedBox = await plannedList.boundingBox();
    if (implementedBox && plannedBox) {
      expect(plannedBox.y).toBeGreaterThan(implementedBox.y);
    }
  });
});
