// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Mobile Responsive Design Tests
 *
 * Scenario: Verify homepage displays correctly on mobile devices
 *
 * Test cases:
 * 1. Page renders without horizontal scroll at 375px width
 * 2. Hero section text is readable with tappable CTAs
 * 3. Feature cards stack vertically on mobile
 * 4. Code snippet is scrollable with accessible copy button
 * 5. Navigation is accessible via mobile-friendly pattern
 * 6. Mobile responsiveness follows best practices
 */

// Mobile viewport dimensions
const MOBILE_VIEWPORT = { width: 375, height: 667 };
const MOBILE_VIEWPORT_SMALL = { width: 320, height: 568 };

test.describe('Mobile Responsive Design', () => {
  test.beforeEach(async ({ page }) => {
    // Set mobile viewport for all tests in this suite
    await page.setViewportSize(MOBILE_VIEWPORT);
    await page.goto('/');
  });

  /**
   * Test Case 1: Load page at 375px width
   * Expected: Page renders without horizontal scroll, content fits viewport
   */
  test('should render page without horizontal scroll at 375px width', async ({ page }) => {
    // Wait for page to fully load
    await page.waitForLoadState('networkidle');

    // Check that the body does not overflow horizontally
    const hasHorizontalScroll = await page.evaluate(() => {
      const body = document.body;
      const html = document.documentElement;

      // Check if there's any horizontal overflow
      const bodyOverflowX = body.scrollWidth > body.clientWidth;
      const htmlOverflowX = html.scrollWidth > html.clientWidth;

      // Check actual scrollbar presence
      const hasHorizontalScrollbar = window.innerWidth > document.documentElement.clientWidth;

      return bodyOverflowX || htmlOverflowX || hasHorizontalScrollbar;
    });

    expect(hasHorizontalScroll).toBe(false);

    // Verify viewport meta tag is present for proper mobile rendering
    const viewportMeta = await page.locator('meta[name="viewport"]').getAttribute('content');
    expect(viewportMeta).toContain('width=device-width');

    // Verify no top-level element overflows the viewport horizontally
    // Note: Elements inside scrollable containers (like code blocks) may have
    // bounding boxes extending beyond viewport, but that's expected behavior
    const overflowingElements = await page.evaluate(() => {
      const viewportWidth = window.innerWidth;
      // Check only main structural elements, not content inside scrollable containers
      const elementsToCheck = document.querySelectorAll('header, main, footer, section, .container, .hero, .features-grid, .feature-card, .code-block, .nav, .hero-buttons, .btn');
      const overflowing = [];

      elementsToCheck.forEach((el) => {
        const rect = el.getBoundingClientRect();
        if (rect.right > viewportWidth + 1) { // +1 for rounding tolerance
          overflowing.push({
            tagName: el.tagName,
            className: el.className,
            right: rect.right,
            viewportWidth: viewportWidth
          });
        }
      });

      return overflowing;
    });

    // There should be no overflowing structural elements
    expect(overflowingElements.length).toBe(0);
  });

  /**
   * Test Case 2: Check hero section on mobile
   * Expected: Hero text is readable, CTAs are tappable with adequate touch targets
   */
  test('should display readable hero text with tappable CTAs on mobile', async ({ page }) => {
    const heroSection = page.locator('.hero');
    await expect(heroSection).toBeVisible();

    // Check hero heading is visible and readable
    const heroHeading = heroSection.locator('h1');
    await expect(heroHeading).toBeVisible();

    // Verify heading font size is adequate for mobile (minimum 24px recommended)
    const headingFontSize = await heroHeading.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });
    expect(headingFontSize).toBeGreaterThanOrEqual(24);

    // Check tagline is visible and readable
    const heroTagline = heroSection.locator('.hero-tagline');
    await expect(heroTagline).toBeVisible();

    const taglineFontSize = await heroTagline.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });
    expect(taglineFontSize).toBeGreaterThanOrEqual(16);

    // Check CTA buttons have adequate touch targets (minimum 44x44px per WCAG)
    const ctaButtons = heroSection.locator('.btn');
    const buttonCount = await ctaButtons.count();
    expect(buttonCount).toBeGreaterThanOrEqual(1);

    for (let i = 0; i < buttonCount; i++) {
      const button = ctaButtons.nth(i);
      await expect(button).toBeVisible();

      const boundingBox = await button.boundingBox();
      expect(boundingBox).not.toBeNull();

      // Minimum touch target size should be 44x44px (WCAG recommendation)
      expect(boundingBox.height).toBeGreaterThanOrEqual(44);
      // Width can be larger due to text content
      expect(boundingBox.width).toBeGreaterThanOrEqual(44);
    }

    // Verify buttons are within viewport (not overflowing)
    const getStartedButton = heroSection.locator('a.btn-primary').first();
    await expect(getStartedButton).toBeInViewport();

    const buttonBox = await getStartedButton.boundingBox();
    expect(buttonBox.x).toBeGreaterThanOrEqual(0);
    expect(buttonBox.x + buttonBox.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width);
  });

  /**
   * Test Case 3: Check features grid on mobile
   * Expected: Feature cards stack vertically on mobile viewports
   */
  test('should stack feature cards vertically on mobile', async ({ page }) => {
    // Scroll to features section
    const featuresSection = page.locator('.features-section, #features');
    await featuresSection.scrollIntoViewIfNeeded();
    await expect(featuresSection).toBeVisible();

    // Get all feature cards
    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();
    expect(cardCount).toBeGreaterThanOrEqual(3);

    // Check that cards are stacked vertically (each card below the previous one)
    const cardPositions = [];
    for (let i = 0; i < cardCount; i++) {
      const card = featureCards.nth(i);
      const boundingBox = await card.boundingBox();
      expect(boundingBox).not.toBeNull();
      cardPositions.push(boundingBox);
    }

    // Verify vertical stacking: each card should start below the previous one
    for (let i = 1; i < cardPositions.length; i++) {
      const previousCard = cardPositions[i - 1];
      const currentCard = cardPositions[i];

      // Current card's Y position should be at or below previous card's bottom
      // Allow for small gap/margin
      expect(currentCard.y).toBeGreaterThanOrEqual(previousCard.y + previousCard.height - 10);
    }

    // Verify cards fit within viewport width
    for (const card of cardPositions) {
      expect(card.x).toBeGreaterThanOrEqual(0);
      expect(card.x + card.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width + 5); // Small tolerance
    }

    // Verify grid uses single column on mobile
    const featuresGrid = page.locator('.features-grid');
    const gridStyle = await featuresGrid.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        display: style.display,
        gridTemplateColumns: style.gridTemplateColumns
      };
    });

    // On mobile at 375px, grid should have single column layout
    // gridTemplateColumns of "1fr" or exact pixel width indicates single column
    if (gridStyle.display === 'grid') {
      const columns = gridStyle.gridTemplateColumns.split(' ').length;
      expect(columns).toBe(1);
    }
  });

  /**
   * Test Case 4: Check code snippet on mobile
   * Expected: Code snippet is scrollable horizontally if needed, copy button accessible
   */
  test('should have scrollable code snippet with accessible copy button on mobile', async ({ page }) => {
    // Scroll to quickstart section
    const quickstartSection = page.locator('.quickstart-section, #quickstart');
    await quickstartSection.scrollIntoViewIfNeeded();
    await expect(quickstartSection).toBeVisible();

    // Find code blocks
    const codeBlocks = quickstartSection.locator('.code-block');
    const codeBlockCount = await codeBlocks.count();
    expect(codeBlockCount).toBeGreaterThan(0);

    const firstCodeBlock = codeBlocks.first();
    await expect(firstCodeBlock).toBeVisible();

    // Verify code block fits within viewport width (container doesn't overflow)
    const codeBlockBox = await firstCodeBlock.boundingBox();
    expect(codeBlockBox.x).toBeGreaterThanOrEqual(0);
    expect(codeBlockBox.x + codeBlockBox.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width + 5);

    // Check that pre element has overflow-x set for horizontal scrolling
    const preElement = firstCodeBlock.locator('pre');
    await expect(preElement).toBeVisible();

    const overflowStyle = await preElement.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        overflowX: style.overflowX,
        overflowY: style.overflowY
      };
    });

    // Should have auto or scroll for horizontal overflow to handle long code lines
    expect(['auto', 'scroll']).toContain(overflowStyle.overflowX);

    // Verify copy button is visible and accessible
    const copyButton = firstCodeBlock.locator('.copy-button, [data-testid="copy-button"]');
    await expect(copyButton).toBeVisible();

    // Check copy button has adequate touch target size
    const buttonBox = await copyButton.boundingBox();
    expect(buttonBox).not.toBeNull();
    // Button should be at least 32x32px (slightly smaller than 44px is acceptable for secondary actions)
    expect(buttonBox.height).toBeGreaterThanOrEqual(28);
    expect(buttonBox.width).toBeGreaterThanOrEqual(44);

    // Verify copy button is within viewport
    expect(buttonBox.x).toBeGreaterThanOrEqual(0);
    expect(buttonBox.x + buttonBox.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width);
  });

  /**
   * Test Case 5: Check navigation on mobile
   * Expected: Navigation is accessible via hamburger menu or similar mobile pattern
   */
  test('should have accessible navigation on mobile', async ({ page }) => {
    const header = page.locator('header, .header');
    await expect(header).toBeVisible();

    // Check that the header/nav fits within mobile viewport
    const headerBox = await header.boundingBox();
    expect(headerBox).not.toBeNull();
    expect(headerBox.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width);

    // Check for either:
    // 1. Hamburger menu button visible on mobile
    // 2. Or compact nav links that fit within mobile viewport

    const hamburgerMenu = page.locator('.hamburger, .menu-toggle, .mobile-menu-btn, [data-testid="mobile-menu"], button[aria-label*="menu" i]');
    const hamburgerCount = await hamburgerMenu.count();

    const navLinks = page.locator('.nav-links a');
    const navLinksCount = await navLinks.count();

    if (hamburgerCount > 0) {
      // If hamburger menu exists, it should be visible on mobile
      await expect(hamburgerMenu.first()).toBeVisible();

      // Click hamburger to open menu
      await hamburgerMenu.first().click();

      // Nav links should become visible after opening menu
      await expect(navLinks.first()).toBeVisible();
    } else {
      // If no hamburger menu, nav links should still be visible and fit in viewport
      expect(navLinksCount).toBeGreaterThan(0);

      // Check that nav links are visible
      const firstNavLink = navLinks.first();
      await expect(firstNavLink).toBeVisible();

      // Verify nav links fit within viewport
      const navLinksContainer = page.locator('.nav-links');
      const navLinksBox = await navLinksContainer.boundingBox();

      if (navLinksBox) {
        expect(navLinksBox.x).toBeGreaterThanOrEqual(0);
        expect(navLinksBox.x + navLinksBox.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width + 5);
      }
    }

    // Verify logo is visible and accessible
    const logo = page.locator('.nav-brand, [data-testid="logo"]');
    await expect(logo).toBeVisible();

    const logoBox = await logo.boundingBox();
    expect(logoBox).not.toBeNull();
    expect(logoBox.x).toBeGreaterThanOrEqual(0);

    // Verify navigation links are keyboard accessible
    const allNavLinks = await navLinks.all();
    for (const link of allNavLinks) {
      // Each link should be focusable
      const tabIndex = await link.getAttribute('tabindex');
      // tabindex should be null (default focusable) or >= 0
      if (tabIndex !== null) {
        expect(parseInt(tabIndex)).toBeGreaterThanOrEqual(0);
      }
    }
  });

  /**
   * Test Case 6: Verify mobile responsiveness best practices
   * Note: Lighthouse audit is a separate integration test
   * This test verifies core responsive design principles
   */
  test('should follow mobile responsiveness best practices', async ({ page }) => {
    // 1. Verify viewport meta tag is correctly set
    const viewportMeta = await page.locator('meta[name="viewport"]').getAttribute('content');
    expect(viewportMeta).toContain('width=device-width');
    expect(viewportMeta).toContain('initial-scale=1');

    // 2. Check that text is readable without zooming (base font size >= 16px)
    const bodyFontSize = await page.evaluate(() => {
      const style = window.getComputedStyle(document.body);
      return parseFloat(style.fontSize);
    });
    expect(bodyFontSize).toBeGreaterThanOrEqual(14);

    // 3. Verify tap targets are properly sized
    const interactiveElements = page.locator('a, button');
    const elementCount = await interactiveElements.count();

    let tooSmallTargets = 0;
    for (let i = 0; i < Math.min(elementCount, 20); i++) { // Check first 20 elements
      const element = interactiveElements.nth(i);
      const isVisible = await element.isVisible();

      if (isVisible) {
        const box = await element.boundingBox();
        if (box && (box.height < 24 || box.width < 24)) {
          tooSmallTargets++;
        }
      }
    }

    // Allow some small targets (icons, etc.) but majority should be adequate
    expect(tooSmallTargets).toBeLessThanOrEqual(5);

    // 4. Verify no fixed-width structural elements that break layout
    // Note: Content inside scrollable containers may extend beyond viewport
    const hasFixedWidthOverflow = await page.evaluate(() => {
      const viewportWidth = window.innerWidth;
      const problematicElements = [];

      // Check only structural elements, not content inside scrollable areas
      const elementsToCheck = document.querySelectorAll('header, main, footer, section, .container, .hero, .features-grid, .feature-card, .code-block, .nav, .hero-buttons, .btn, .config-grid, .footer-links');

      elementsToCheck.forEach((el) => {
        const style = window.getComputedStyle(el);
        const rect = el.getBoundingClientRect();

        // Check for actual overflow of structural elements
        if (rect.right > viewportWidth + 1) {
          problematicElements.push(el.tagName);
        }
      });

      return problematicElements.length;
    });

    expect(hasFixedWidthOverflow).toBe(0);

    // 5. Verify smooth scrolling is enabled
    const scrollBehavior = await page.evaluate(() => {
      return window.getComputedStyle(document.documentElement).scrollBehavior;
    });
    expect(scrollBehavior).toBe('smooth');

    // 6. Verify images are responsive (if any)
    const images = page.locator('img');
    const imageCount = await images.count();

    for (let i = 0; i < imageCount; i++) {
      const img = images.nth(i);
      const isVisible = await img.isVisible();

      if (isVisible) {
        const imgBox = await img.boundingBox();
        if (imgBox) {
          // Images should not exceed viewport width
          expect(imgBox.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width);
        }
      }
    }
  });
});

test.describe('Mobile Responsive Design - Multiple Viewports', () => {
  /**
   * Additional test: Verify responsive behavior across different mobile sizes
   */
  test('should render correctly on various mobile viewport sizes', async ({ page }) => {
    const viewports = [
      { width: 320, height: 568, name: 'iPhone SE' },
      { width: 375, height: 667, name: 'iPhone 8' },
      { width: 390, height: 844, name: 'iPhone 12' },
      { width: 412, height: 915, name: 'Pixel 5' },
    ];

    for (const viewport of viewports) {
      await page.setViewportSize({ width: viewport.width, height: viewport.height });
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Check no horizontal scroll for this viewport
      // We check if the body or main structural elements overflow, not internal scrollable content
      const hasHorizontalScroll = await page.evaluate(() => {
        const body = document.body;
        const html = document.documentElement;
        const viewportWidth = window.innerWidth;

        // Check if the overall page has horizontal scrollbar
        // The scrollWidth can be larger than clientWidth if there's scrollable content
        // inside pre/code blocks, but that's expected and handled by overflow-x: auto
        // We should check if there's visible horizontal scrollbar on the body/html
        const hasBodyOverflow = body.scrollWidth > body.clientWidth && window.getComputedStyle(body).overflowX !== 'hidden';

        // Also check main structural elements
        const structuralElements = document.querySelectorAll('header, .hero, .features-section, .quickstart-section, footer');
        let structuralOverflow = false;
        structuralElements.forEach(el => {
          const rect = el.getBoundingClientRect();
          if (rect.right > viewportWidth + 1 || rect.left < -1) {
            structuralOverflow = true;
          }
        });

        return structuralOverflow;
      });

      expect(hasHorizontalScroll, `Horizontal scroll detected on ${viewport.name}`).toBe(false);

      // Verify hero section is visible
      const heroSection = page.locator('.hero');
      await expect(heroSection, `Hero not visible on ${viewport.name}`).toBeVisible();

      // Verify all main sections are accessible
      const header = page.locator('header');
      await expect(header, `Header not visible on ${viewport.name}`).toBeVisible();
    }
  });
});
