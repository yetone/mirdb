// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * E2E Tests for MirDB Homepage Desktop Responsive Design
 *
 * Scenario: Verify the homepage displays correctly on desktop screens
 *
 * Test Case 1: Page renders with full desktop layout at 1280px viewport
 * Test Case 2: Full navigation menu is visible without hamburger
 * Test Case 3: Feature cards display in multi-column grid (3-4 columns)
 * Test Case 4: Content is constrained to reasonable max-width for readability
 */

test.describe('Responsive Design - Desktop View', () => {
  // Use desktop viewport for all tests in this suite (1280px width)
  test.use({ viewport: { width: 1280, height: 800 } });

  test.beforeEach(async ({ page }) => {
    // Navigate to homepage before each test
    await page.goto('/');
    // Wait for page to be fully loaded
    await page.waitForLoadState('networkidle');
  });

  test('Test Case 1: Page renders with full desktop layout at 1280px viewport', async ({ page }) => {
    // Verify viewport is set to 1280px
    const viewportWidth = await page.evaluate(() => window.innerWidth);
    expect(viewportWidth).toBe(1280);

    // Verify no horizontal scrollbar (scroll width should not exceed viewport width)
    const scrollWidth = await page.evaluate(() => document.documentElement.scrollWidth);
    expect(scrollWidth).toBeLessThanOrEqual(viewportWidth + 1);

    // Verify the body doesn't overflow horizontally
    const bodyOverflow = await page.evaluate(() => {
      const body = document.body;
      return body.scrollWidth <= body.clientWidth + 1; // Allow 1px tolerance
    });
    expect(bodyOverflow).toBe(true);

    // Verify no element extends beyond viewport
    const hasOverflow = await page.evaluate(() => {
      const elements = document.querySelectorAll('*');
      for (const el of elements) {
        const rect = el.getBoundingClientRect();
        if (rect.right > window.innerWidth + 1) { // Allow 1px tolerance
          return true;
        }
      }
      return false;
    });
    expect(hasOverflow).toBe(false);

    // Verify hero section is visible and properly rendered
    const heroSection = page.getByTestId('hero-section');
    await expect(heroSection).toBeVisible();

    // Verify hero content is within viewport bounds
    const heroContent = page.locator('.hero-content');
    const heroBox = await heroContent.boundingBox();
    expect(heroBox).not.toBeNull();
    if (heroBox) {
      expect(heroBox.x).toBeGreaterThanOrEqual(0);
      expect(heroBox.x + heroBox.width).toBeLessThanOrEqual(1280 + 1);
    }

    // Verify features section is visible
    const featuresSection = page.getByTestId('features-section');
    await featuresSection.scrollIntoViewIfNeeded();
    await expect(featuresSection).toBeVisible();

    // Verify footer is visible
    const footer = page.locator('footer.footer');
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();
  });

  test('Test Case 2: Full navigation menu is visible without hamburger', async ({ page }) => {
    // Verify desktop navigation links are visible
    const desktopNavLinks = page.locator('.nav-links');
    await expect(desktopNavLinks).toBeVisible();

    // Verify all navigation links are visible in the desktop nav
    const featuresLink = desktopNavLinks.locator('a[href="#features"]');
    const docsLink = desktopNavLinks.locator('a:has-text("Documentation")');
    const githubLink = desktopNavLinks.locator('a:has-text("GitHub")');

    await expect(featuresLink).toBeVisible();
    await expect(docsLink).toBeVisible();
    await expect(githubLink).toBeVisible();

    // Verify hamburger menu toggle is hidden on desktop
    const hamburgerButton = page.getByTestId('mobile-menu-toggle');
    await expect(hamburgerButton).toBeHidden();

    // Verify the mobile navigation menu is hidden
    const mobileNav = page.getByTestId('mobile-nav-menu');
    await expect(mobileNav).toBeHidden();

    // Verify navigation links are readable and links are clickable
    const navLinksBox = await desktopNavLinks.boundingBox();
    expect(navLinksBox).not.toBeNull();
    if (navLinksBox) {
      // Navigation should fit within viewport
      expect(navLinksBox.x + navLinksBox.width).toBeLessThanOrEqual(1280);
    }

    // Verify the logo is visible
    const navLogo = page.locator('.nav-logo');
    await expect(navLogo).toBeVisible();
    const logoText = await navLogo.textContent();
    expect(logoText).toContain('MirDB');
  });

  test('Test Case 3: Feature cards display in multi-column grid (3-4 columns)', async ({ page }) => {
    // Scroll to features section
    const featuresSection = page.getByTestId('features-section');
    await featuresSection.scrollIntoViewIfNeeded();

    // Verify features section is visible
    await expect(featuresSection).toBeVisible();

    // Get all feature cards
    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();

    // Verify we have feature cards (should be 8 based on the HTML)
    expect(cardCount).toBeGreaterThanOrEqual(4);

    // Collect card positions to analyze layout
    const cardPositions = [];
    for (let i = 0; i < Math.min(cardCount, 8); i++) {
      const card = featureCards.nth(i);
      await card.scrollIntoViewIfNeeded();
      const box = await card.boundingBox();
      if (box) {
        cardPositions.push({
          index: i,
          top: Math.round(box.y),
          left: Math.round(box.x),
          width: Math.round(box.width),
          right: Math.round(box.x + box.width)
        });
      }
    }

    // Verify we collected positions
    expect(cardPositions.length).toBeGreaterThanOrEqual(4);

    // Group cards by row (cards with similar Y position are in the same row)
    const rows = [];
    const rowTolerance = 10; // Allow 10px tolerance for row grouping

    for (const card of cardPositions) {
      let foundRow = false;
      for (const row of rows) {
        if (Math.abs(row[0].top - card.top) < rowTolerance) {
          row.push(card);
          foundRow = true;
          break;
        }
      }
      if (!foundRow) {
        rows.push([card]);
      }
    }

    // Analyze layout - at 1280px desktop viewport, we expect 3-4 columns
    // Check if any row has 3 or 4 cards (indicating multi-column layout)
    const rowsWith3OrMoreCards = rows.filter(row => row.length >= 3);
    const maxColumnsInAnyRow = Math.max(...rows.map(row => row.length));

    // On desktop (1280px), feature cards should display in 3-4 columns
    expect(maxColumnsInAnyRow).toBeGreaterThanOrEqual(3);
    expect(maxColumnsInAnyRow).toBeLessThanOrEqual(4);

    // Verify at least one row has 3 or more cards (multi-column layout)
    expect(rowsWith3OrMoreCards.length).toBeGreaterThan(0);

    // Verify all cards fit within viewport
    for (let i = 0; i < Math.min(cardCount, 8); i++) {
      const card = featureCards.nth(i);
      await card.scrollIntoViewIfNeeded();

      const box = await card.boundingBox();
      expect(box).not.toBeNull();
      if (box) {
        // Card should fit within viewport width
        expect(box.x).toBeGreaterThanOrEqual(0);
        expect(box.x + box.width).toBeLessThanOrEqual(1280 + 1); // Allow 1px tolerance
      }

      // Verify card content is visible
      const title = card.locator('[data-testid="feature-title"]');
      const description = card.locator('[data-testid="feature-description"]');
      const icon = card.locator('[data-testid="feature-icon"]');

      await expect(title).toBeVisible();
      await expect(description).toBeVisible();
      await expect(icon).toBeVisible();
    }
  });

  test('Test Case 4: Content is constrained to reasonable max-width for readability', async ({ page }) => {
    // Verify the navigation container has a max-width constraint
    const nav = page.locator('.nav');
    const navComputedMaxWidth = await nav.evaluate((el) => {
      return window.getComputedStyle(el).maxWidth;
    });
    // Max-width should be set (not "none")
    expect(navComputedMaxWidth).not.toBe('none');

    // Verify the nav max-width value is reasonable (1200px as per CSS)
    const navMaxWidthPx = parseFloat(navComputedMaxWidth);
    expect(navMaxWidthPx).toBeLessThanOrEqual(1400); // Should be reasonable max-width
    expect(navMaxWidthPx).toBeGreaterThanOrEqual(800); // Should not be too narrow

    // Verify hero content has a max-width constraint
    const heroContent = page.locator('.hero-content');
    const heroMaxWidth = await heroContent.evaluate((el) => {
      return window.getComputedStyle(el).maxWidth;
    });
    expect(heroMaxWidth).not.toBe('none');

    // Verify hero content max-width is reasonable for readability
    const heroMaxWidthPx = parseFloat(heroMaxWidth);
    expect(heroMaxWidthPx).toBeLessThanOrEqual(1000); // Should not be too wide for readability
    expect(heroMaxWidthPx).toBeGreaterThanOrEqual(600); // Should not be too narrow

    // Verify features container has a max-width constraint
    const featuresContainer = page.locator('.features-container');
    await featuresContainer.scrollIntoViewIfNeeded();
    const featuresMaxWidth = await featuresContainer.evaluate((el) => {
      return window.getComputedStyle(el).maxWidth;
    });
    expect(featuresMaxWidth).not.toBe('none');

    // Verify features container max-width is reasonable
    const featuresMaxWidthPx = parseFloat(featuresMaxWidth);
    expect(featuresMaxWidthPx).toBeLessThanOrEqual(1400); // Should be reasonable max-width
    expect(featuresMaxWidthPx).toBeGreaterThanOrEqual(800); // Should not be too narrow

    // Verify content is centered on the page
    const heroBox = await heroContent.boundingBox();
    expect(heroBox).not.toBeNull();
    if (heroBox) {
      // Content should be approximately centered (left margin should be similar to right margin)
      const leftMargin = heroBox.x;
      const rightMargin = 1280 - (heroBox.x + heroBox.width);
      const marginDifference = Math.abs(leftMargin - rightMargin);

      // Allow for some variance, but content should be roughly centered
      expect(marginDifference).toBeLessThan(100);
    }
  });

  test('Hero section displays properly at desktop viewport', async ({ page }) => {
    // Verify hero section is visible
    const heroSection = page.getByTestId('hero-section');
    await expect(heroSection).toBeVisible();

    // Verify hero title is visible and appropriately sized for desktop
    const heroTitle = page.getByTestId('product-name');
    await expect(heroTitle).toBeVisible();

    const titleFontSize = await heroTitle.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });
    // Title should be larger on desktop (3rem = 48px based on CSS)
    expect(titleFontSize).toBeGreaterThanOrEqual(40);

    // Verify hero tagline is visible
    const heroTagline = page.getByTestId('hero-tagline');
    await expect(heroTagline).toBeVisible();

    const taglineFontSize = await heroTagline.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });
    // Tagline should be appropriately sized (1.5rem = 24px based on CSS)
    expect(taglineFontSize).toBeGreaterThanOrEqual(20);

    // Verify CTA buttons are visible and displayed side-by-side
    const ctaSection = page.getByTestId('hero-cta');
    await expect(ctaSection).toBeVisible();

    const getStartedBtn = page.getByTestId('cta-get-started');
    const docsBtn = page.getByTestId('cta-documentation');

    await expect(getStartedBtn).toBeVisible();
    await expect(docsBtn).toBeVisible();

    // Check button layout - they should be side-by-side on desktop
    const btn1Box = await getStartedBtn.boundingBox();
    const btn2Box = await docsBtn.boundingBox();

    expect(btn1Box).not.toBeNull();
    expect(btn2Box).not.toBeNull();

    if (btn1Box && btn2Box) {
      // Buttons should be on the same row (similar Y position)
      const yDifference = Math.abs(btn1Box.y - btn2Box.y);
      expect(yDifference).toBeLessThan(50); // Should be on same row

      // Both buttons should be within viewport
      expect(btn1Box.x + btn1Box.width).toBeLessThanOrEqual(1280);
      expect(btn2Box.x + btn2Box.width).toBeLessThanOrEqual(1280);
    }
  });

  test('Footer displays correctly at desktop viewport', async ({ page }) => {
    // Scroll to footer
    const footer = page.locator('footer.footer');
    await footer.scrollIntoViewIfNeeded();

    // Verify footer is visible
    await expect(footer).toBeVisible();

    // Verify footer spans the full width
    const footerBox = await footer.boundingBox();
    expect(footerBox).not.toBeNull();
    if (footerBox) {
      // Footer should span close to full viewport width (allowing for body margins)
      expect(footerBox.width).toBeGreaterThanOrEqual(1000);
    }

    // Verify footer content is readable
    const footerText = await footer.textContent();
    expect(footerText).toBeTruthy();
    expect(footerText?.toLowerCase()).toMatch(/mirdb|mit|license|copyright|©/);
  });
});
