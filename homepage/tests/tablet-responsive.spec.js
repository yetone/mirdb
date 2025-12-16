// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * E2E Tests for MirDB Homepage Tablet Responsive Design
 *
 * Scenario: Verify the homepage displays correctly on tablet devices
 *
 * Test Case 1: Page renders correctly with appropriate tablet layout at 768px viewport
 * Test Case 2: Navigation displays appropriately (full or hamburger menu)
 * Test Case 3: Feature cards display in 2-column grid or appropriate layout
 */

test.describe('Responsive Design - Tablet View', () => {
  // Use tablet viewport for all tests in this suite (768px width)
  test.use({ viewport: { width: 768, height: 1024 } });

  test.beforeEach(async ({ page }) => {
    // Navigate to homepage before each test
    await page.goto('/');
    // Wait for page to be fully loaded
    await page.waitForLoadState('networkidle');
  });

  test('Test Case 1: Page renders correctly with appropriate tablet layout at 768px viewport', async ({ page }) => {
    // Get the document scroll width and viewport width
    const scrollWidth = await page.evaluate(() => document.documentElement.scrollWidth);
    const viewportWidth = await page.evaluate(() => window.innerWidth);

    // Verify viewport is set to 768px
    expect(viewportWidth).toBe(768);

    // Verify no horizontal scrollbar (scroll width should not exceed viewport width significantly)
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
      expect(heroBox.x + heroBox.width).toBeLessThanOrEqual(768 + 1);
    }

    // Verify features section is visible
    const featuresSection = page.getByTestId('features-section');
    await featuresSection.scrollIntoViewIfNeeded();
    await expect(featuresSection).toBeVisible();
  });

  test('Test Case 2: Navigation displays appropriately (full or hamburger menu)', async ({ page }) => {
    // At tablet viewport (768px), which is typically the breakpoint,
    // the navigation may show either full desktop nav or mobile hamburger menu
    // depending on implementation choice

    // Check if desktop navigation is visible
    const desktopNavLinks = page.locator('.nav-links');
    const isDesktopNavVisible = await desktopNavLinks.isVisible();

    // Check if hamburger menu is visible
    const hamburgerButton = page.getByTestId('mobile-menu-toggle');
    const isHamburgerVisible = await hamburgerButton.isVisible();

    // At tablet size, either full nav OR hamburger should be visible (not both hidden)
    expect(isDesktopNavVisible || isHamburgerVisible).toBe(true);

    if (isDesktopNavVisible) {
      // If desktop nav is showing, verify all links are visible
      const featuresLink = desktopNavLinks.locator('a[href="#features"]');
      const docsLink = desktopNavLinks.locator('a:has-text("Documentation")');
      const githubLink = desktopNavLinks.locator('a:has-text("GitHub")');

      await expect(featuresLink).toBeVisible();
      await expect(docsLink).toBeVisible();
      await expect(githubLink).toBeVisible();

      // Verify navigation is readable and links are clickable
      const navLinksBox = await desktopNavLinks.boundingBox();
      expect(navLinksBox).not.toBeNull();
      if (navLinksBox) {
        // Navigation should fit within viewport
        expect(navLinksBox.x + navLinksBox.width).toBeLessThanOrEqual(768);
      }
    } else if (isHamburgerVisible) {
      // If hamburger menu is showing, verify it's functional
      await expect(hamburgerButton).toBeEnabled();

      // Verify hamburger menu can be opened
      await hamburgerButton.click();
      await page.waitForTimeout(300);

      // Verify mobile nav menu is visible
      const mobileNav = page.getByTestId('mobile-nav-menu');
      await expect(mobileNav).toBeVisible();

      // Verify all navigation links are accessible
      const featuresLink = mobileNav.locator('a[href="#features"]');
      const docsLink = mobileNav.locator('a:has-text("Documentation")');
      const githubLink = mobileNav.locator('a:has-text("GitHub")');

      await expect(featuresLink).toBeVisible();
      await expect(docsLink).toBeVisible();
      await expect(githubLink).toBeVisible();

      // Close menu
      await hamburgerButton.click();
      await page.waitForTimeout(300);
      await expect(mobileNav).toBeHidden();
    }

    // Verify the logo is always visible regardless of nav type
    const navLogo = page.locator('.nav-logo');
    await expect(navLogo).toBeVisible();
    const logoText = await navLogo.textContent();
    expect(logoText).toContain('MirDB');
  });

  test('Test Case 3: Feature cards display in 2-column grid or appropriate layout', async ({ page }) => {
    // Scroll to features section
    const featuresSection = page.getByTestId('features-section');
    await featuresSection.scrollIntoViewIfNeeded();

    // Verify features section is visible
    await expect(featuresSection).toBeVisible();

    // Get all feature cards
    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();

    // Verify we have feature cards
    expect(cardCount).toBeGreaterThan(0);

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

    // Analyze layout
    // At 768px tablet viewport, appropriate layouts include:
    // - 2-column grid (most cards have 2 per row)
    // - Single column (all cards stacked)
    // - Mixed layout (responsive grid)

    // Check if any row has 2 cards (indicating 2-column layout)
    const rowsWith2Cards = rows.filter(row => row.length === 2);
    const rowsWith1Card = rows.filter(row => row.length === 1);

    // The layout is appropriate if:
    // 1. Cards are in a 2-column grid (multiple rows with 2 cards), OR
    // 2. Cards are in single column (all rows have 1 card), OR
    // 3. Auto-fit grid with reasonable column count

    const is2ColumnLayout = rowsWith2Cards.length >= 2;
    const isSingleColumnLayout = rowsWith1Card.length === rows.length;
    const isValidAutoFitLayout = rows.every(row => row.length <= 3); // Max 3 columns at tablet

    expect(is2ColumnLayout || isSingleColumnLayout || isValidAutoFitLayout).toBe(true);

    // Verify all cards fit within viewport
    for (let i = 0; i < Math.min(cardCount, 8); i++) {
      const card = featureCards.nth(i);
      await card.scrollIntoViewIfNeeded();

      const box = await card.boundingBox();
      expect(box).not.toBeNull();
      if (box) {
        // Card should fit within viewport width
        expect(box.x).toBeGreaterThanOrEqual(0);
        expect(box.x + box.width).toBeLessThanOrEqual(768 + 1); // Allow 1px tolerance
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

  test('Hero section adapts appropriately for tablet viewport', async ({ page }) => {
    // Verify hero section is visible
    const heroSection = page.getByTestId('hero-section');
    await expect(heroSection).toBeVisible();

    // Verify hero title is visible and appropriately sized
    const heroTitle = page.getByTestId('product-name');
    await expect(heroTitle).toBeVisible();

    const titleFontSize = await heroTitle.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });
    // Title should be reasonably sized for tablet (between mobile and desktop)
    expect(titleFontSize).toBeGreaterThanOrEqual(28);

    // Verify hero tagline is visible
    const heroTagline = page.getByTestId('hero-tagline');
    await expect(heroTagline).toBeVisible();

    // Verify CTA buttons are visible and properly arranged
    const ctaSection = page.getByTestId('hero-cta');
    await expect(ctaSection).toBeVisible();

    const getStartedBtn = page.getByTestId('cta-get-started');
    const docsBtn = page.getByTestId('cta-documentation');

    await expect(getStartedBtn).toBeVisible();
    await expect(docsBtn).toBeVisible();

    // Check button layout - they may be side-by-side or stacked
    const btn1Box = await getStartedBtn.boundingBox();
    const btn2Box = await docsBtn.boundingBox();

    expect(btn1Box).not.toBeNull();
    expect(btn2Box).not.toBeNull();

    if (btn1Box && btn2Box) {
      // Both buttons should be within viewport
      expect(btn1Box.x + btn1Box.width).toBeLessThanOrEqual(768);
      expect(btn2Box.x + btn2Box.width).toBeLessThanOrEqual(768);
    }
  });

  test('Footer displays correctly at tablet viewport', async ({ page }) => {
    // Scroll to footer
    const footer = page.locator('footer.footer');
    await footer.scrollIntoViewIfNeeded();

    // Verify footer is visible
    await expect(footer).toBeVisible();

    // Verify footer is within viewport bounds
    const footerBox = await footer.boundingBox();
    expect(footerBox).not.toBeNull();
    if (footerBox) {
      expect(footerBox.width).toBeLessThanOrEqual(768 + 1);
    }

    // Verify footer content is readable
    const footerText = await footer.textContent();
    expect(footerText).toBeTruthy();
    expect(footerText?.toLowerCase()).toMatch(/mirdb|mit|license|copyright|©/);
  });
});
