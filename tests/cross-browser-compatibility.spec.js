// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Test Suite: Cross-Browser Compatibility
 * Scenario: Verify the page displays correctly across major browsers
 * This test suite validates that the MirDB landing page renders correctly
 * and all features are functional across Chrome, Firefox, Safari (WebKit), and Edge.
 *
 * Test browsers are configured in playwright.config.js:
 * - chromium (Chrome)
 * - firefox (Firefox)
 * - webkit (Safari)
 * - edge (Microsoft Edge - uses Chromium)
 */

test.describe('Cross-Browser Compatibility', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 1: Page renders correctly with all features functional
   * Input: Load page in browser
   * Expected: Page renders correctly with all features functional
   * Validates: Basic rendering, no JavaScript errors, main sections visible
   */
  test('TC1: Page loads and renders correctly with no console errors', async ({ page, browserName }) => {
    // Collect console errors during page load
    const consoleErrors = [];
    page.on('console', msg => {
      if (msg.type() === 'error') {
        consoleErrors.push(msg.text());
      }
    });

    // Verify page has loaded successfully
    const body = page.locator('body');
    await expect(body).toBeVisible();

    // Verify page title is correct
    await expect(page).toHaveTitle('MirDB - Persistent Key-Value Store with Memcached Protocol');

    // Check for critical console errors (filter out expected third-party errors)
    const criticalErrors = consoleErrors.filter(err =>
      !err.includes('favicon') &&
      !err.includes('third-party') &&
      !err.includes('net::ERR_')
    );

    // Log browser name for debugging
    console.log(`Running cross-browser test on: ${browserName}`);

    // No critical JavaScript errors should occur
    expect(criticalErrors.length).toBe(0);
  });

  /**
   * Test Case 2: All main sections are visible and properly rendered
   * Input: Load page in browser
   * Expected: All main sections render correctly
   */
  test('TC2: All main page sections are visible', async ({ page, browserName }) => {
    // Header section
    const header = page.locator('header.header');
    await expect(header).toBeVisible();

    // Logo and navigation
    const logo = page.locator('.logo-section');
    await expect(logo).toBeVisible();

    const mainNav = page.locator('nav.main-nav');
    await expect(mainNav).toBeVisible();

    // Hero section
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    const heroHeading = page.locator('.hero-section h1');
    await expect(heroHeading).toBeVisible();
    await expect(heroHeading).toContainText('Persistent Memcached');

    // Features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // How It Works section
    const howItWorksSection = page.locator('#how-it-works');
    await expect(howItWorksSection).toBeVisible();

    // Getting Started section
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeVisible();

    // Documentation section
    const documentationSection = page.locator('#documentation');
    await expect(documentationSection).toBeVisible();

    // Footer
    const footer = page.locator('footer.footer');
    await expect(footer).toBeVisible();

    console.log(`All sections visible on: ${browserName}`);
  });

  /**
   * Test Case 3: Navigation links are functional across browsers
   * Input: Click navigation links
   * Expected: Navigation works correctly
   */
  test('TC3: Navigation links work correctly', async ({ page, browserName }) => {
    // Test internal navigation links
    const navLinks = [
      { selector: '.nav-link[href="#features"]', target: '#features' },
      { selector: '.nav-link[href="#how-it-works"]', target: '#how-it-works' },
      { selector: '.nav-link[href="#getting-started"]', target: '#getting-started' },
      { selector: '.nav-link[href="#documentation"]', target: '#documentation' },
    ];

    for (const link of navLinks) {
      const navLink = page.locator(link.selector);
      await expect(navLink).toBeVisible();

      await navLink.click();

      // Verify the target section is in view
      const targetSection = page.locator(link.target);
      await expect(targetSection).toBeInViewport({ ratio: 0.3 });
    }

    console.log(`Navigation links work on: ${browserName}`);
  });

  /**
   * Test Case 4: CSS styles are applied correctly across browsers
   * Input: Check CSS rendering
   * Expected: Styles render consistently
   */
  test('TC4: CSS styles are applied correctly', async ({ page, browserName }) => {
    // Check header styling
    const header = page.locator('header.header');
    const headerBg = await header.evaluate(el =>
      window.getComputedStyle(el).backgroundColor
    );
    expect(headerBg).toBeTruthy();

    // Check that CSS custom properties are working
    const body = page.locator('body');
    const bodyColor = await body.evaluate(el =>
      window.getComputedStyle(el).color
    );
    expect(bodyColor).toBeTruthy();

    // Check feature cards have proper styling
    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();
    expect(cardCount).toBeGreaterThan(0);

    for (let i = 0; i < Math.min(cardCount, 3); i++) {
      const card = featureCards.nth(i);
      const cardStyles = await card.evaluate(el => {
        const styles = window.getComputedStyle(el);
        return {
          padding: styles.padding,
          backgroundColor: styles.backgroundColor,
          borderRadius: styles.borderRadius
        };
      });

      // Cards should have some styling applied
      expect(cardStyles.backgroundColor).toBeTruthy();
    }

    // Check button styling
    const primaryBtn = page.locator('.btn-primary').first();
    await expect(primaryBtn).toBeVisible();
    const btnStyles = await primaryBtn.evaluate(el => {
      const styles = window.getComputedStyle(el);
      return {
        backgroundColor: styles.backgroundColor,
        color: styles.color,
        padding: styles.padding
      };
    });
    expect(btnStyles.backgroundColor).toBeTruthy();
    expect(btnStyles.color).toBeTruthy();

    console.log(`CSS styles render correctly on: ${browserName}`);
  });

  /**
   * Test Case 5: Code blocks render correctly across browsers
   * Input: Check code block rendering
   * Expected: Code blocks display properly with correct formatting
   */
  test('TC5: Code blocks render correctly', async ({ page, browserName }) => {
    // Navigate to getting started section
    const gettingStartedSection = page.locator('#getting-started');
    await gettingStartedSection.scrollIntoViewIfNeeded();

    // Check code blocks
    const codeBlocks = page.locator('pre');
    const codeCount = await codeBlocks.count();
    expect(codeCount).toBeGreaterThan(0);

    for (let i = 0; i < codeCount; i++) {
      const codeBlock = codeBlocks.nth(i);
      await expect(codeBlock).toBeVisible();

      // Verify code content is visible and not empty
      const codeContent = await codeBlock.textContent();
      expect(codeContent).toBeTruthy();
      expect(codeContent.length).toBeGreaterThan(0);

      // Check code block styling
      const codeStyles = await codeBlock.evaluate(el => {
        const styles = window.getComputedStyle(el);
        return {
          fontFamily: styles.fontFamily,
          overflow: styles.overflow
        };
      });

      // Code blocks should use monospace font
      const hasMonospace = codeStyles.fontFamily.toLowerCase().includes('mono') ||
                          codeStyles.fontFamily.toLowerCase().includes('courier') ||
                          codeStyles.fontFamily.toLowerCase().includes('consolas');
      expect(hasMonospace).toBe(true);
    }

    console.log(`Code blocks render correctly on: ${browserName}`);
  });

  /**
   * Test Case 6: Images load correctly across browsers
   * Input: Check image loading
   * Expected: All images load without errors
   */
  test('TC6: Images load correctly', async ({ page, browserName }) => {
    // Check logo image
    const logoImage = page.locator('.logo-image');
    await expect(logoImage).toBeVisible();

    // Verify image has loaded (has natural dimensions)
    const imgLoaded = await logoImage.evaluate(img => {
      return img.complete && img.naturalWidth > 0;
    });
    expect(imgLoaded).toBe(true);

    console.log(`Images load correctly on: ${browserName}`);
  });

  /**
   * Test Case 7: Table renders correctly across browsers
   * Input: Check table rendering in documentation section
   * Expected: Table displays properly with all rows visible
   */
  test('TC7: Documentation table renders correctly', async ({ page, browserName }) => {
    // Navigate to documentation section
    const docSection = page.locator('#documentation');
    await docSection.scrollIntoViewIfNeeded();

    // Check commands table
    const commandsTable = page.locator('.commands-table');
    await expect(commandsTable).toBeVisible();

    // Verify table header
    const tableHeader = commandsTable.locator('thead');
    await expect(tableHeader).toBeVisible();

    // Verify table has expected number of rows
    const tableRows = commandsTable.locator('tbody tr');
    const rowCount = await tableRows.count();
    expect(rowCount).toBeGreaterThanOrEqual(8); // Should have at least 8 commands

    // Check that table cells are properly styled
    const firstCell = tableRows.first().locator('td').first();
    await expect(firstCell).toBeVisible();
    const cellStyles = await firstCell.evaluate(el => {
      const styles = window.getComputedStyle(el);
      return {
        padding: styles.padding,
        borderBottom: styles.borderBottom
      };
    });
    expect(cellStyles.padding).toBeTruthy();

    console.log(`Table renders correctly on: ${browserName}`);
  });

  /**
   * Test Case 8: Layout is consistent across browsers (no horizontal scroll)
   * Input: Check page layout
   * Expected: No horizontal scrollbar, content fits viewport
   */
  test('TC8: Layout is consistent with no horizontal scroll', async ({ page, browserName }) => {
    // Check for horizontal scroll
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScroll).toBe(false);

    // Check that main containers are properly sized
    const containers = [
      '.header-container',
      '.section-container',
      '.footer-container'
    ];

    for (const containerSelector of containers) {
      const containers = page.locator(containerSelector);
      const count = await containers.count();

      for (let i = 0; i < count; i++) {
        const container = containers.nth(i);
        const box = await container.boundingBox();
        if (box) {
          // Container should not exceed viewport width
          expect(box.width).toBeLessThanOrEqual(await page.evaluate(() => window.innerWidth) + 10);
        }
      }
    }

    console.log(`Layout is consistent on: ${browserName}`);
  });

  /**
   * Test Case 9: Flexbox/Grid layouts work across browsers
   * Input: Check CSS layout features
   * Expected: Modern CSS layout features work correctly
   */
  test('TC9: Flexbox and Grid layouts work correctly', async ({ page, browserName }) => {
    // Check header flexbox layout
    const headerContainer = page.locator('.header-container');
    const headerDisplay = await headerContainer.evaluate(el =>
      window.getComputedStyle(el).display
    );
    expect(headerDisplay).toBe('flex');

    // Check features grid
    const featuresGrid = page.locator('.features-grid');
    await featuresGrid.scrollIntoViewIfNeeded();
    const gridDisplay = await featuresGrid.evaluate(el =>
      window.getComputedStyle(el).display
    );
    expect(gridDisplay).toBe('grid');

    // Verify grid items are properly positioned
    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();

    if (cardCount >= 2) {
      const firstBox = await featureCards.nth(0).boundingBox();
      const secondBox = await featureCards.nth(1).boundingBox();

      expect(firstBox).not.toBeNull();
      expect(secondBox).not.toBeNull();

      // Cards should have reasonable dimensions
      expect(firstBox.width).toBeGreaterThan(100);
      expect(firstBox.height).toBeGreaterThan(50);
    }

    console.log(`Flexbox and Grid layouts work on: ${browserName}`);
  });

  /**
   * Test Case 10: External links have correct attributes
   * Input: Check external link attributes
   * Expected: External links have target="_blank" and rel="noopener noreferrer"
   */
  test('TC10: External links have correct attributes for security', async ({ page, browserName }) => {
    // Check GitHub links
    const githubLinks = page.locator('a[href*="github.com"]');
    const linkCount = await githubLinks.count();
    expect(linkCount).toBeGreaterThan(0);

    for (let i = 0; i < linkCount; i++) {
      const link = githubLinks.nth(i);

      // Check target attribute
      const target = await link.getAttribute('target');
      expect(target).toBe('_blank');

      // Check rel attribute for security
      const rel = await link.getAttribute('rel');
      expect(rel).toContain('noopener');
      expect(rel).toContain('noreferrer');
    }

    console.log(`External links are secure on: ${browserName}`);
  });

  /**
   * Test Case 11: Smooth scrolling works across browsers
   * Input: Click anchor links
   * Expected: Smooth scroll to target sections
   */
  test('TC11: Anchor navigation scrolls to correct sections', async ({ page, browserName }) => {
    // Start at top of page
    await page.evaluate(() => window.scrollTo(0, 0));

    // Click on getting started link
    const gettingStartedLink = page.locator('.nav-link[href="#getting-started"]');
    await gettingStartedLink.click();

    // Wait for any scroll animation
    await page.waitForTimeout(500);

    // Verify we've scrolled to the getting started section
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeInViewport({ ratio: 0.5 });

    console.log(`Smooth scrolling works on: ${browserName}`);
  });

  /**
   * Test Case 12: Hero CTA buttons are clickable and visible
   * Input: Check CTA buttons in hero section
   * Expected: Both primary and secondary buttons are visible and clickable
   */
  test('TC12: Hero CTA buttons are functional', async ({ page, browserName }) => {
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // Check primary CTA button
    const primaryBtn = page.locator('.hero-cta .btn-primary');
    await expect(primaryBtn).toBeVisible();
    await expect(primaryBtn).toHaveText('Get Started');

    // Check secondary CTA button
    const secondaryBtn = page.locator('.hero-cta .btn-secondary');
    await expect(secondaryBtn).toBeVisible();
    await expect(secondaryBtn).toHaveText('View on GitHub');

    // Verify buttons are clickable by checking they are anchor elements with href
    // Note: WebKit/Safari may return 'auto' for computed cursor on anchor elements
    const primaryHref = await primaryBtn.getAttribute('href');
    expect(primaryHref).toBeTruthy();
    expect(primaryHref).toBe('#getting-started');

    const secondaryHref = await secondaryBtn.getAttribute('href');
    expect(secondaryHref).toBeTruthy();
    expect(secondaryHref).toContain('github.com');

    // Verify buttons are enabled (not disabled)
    await expect(primaryBtn).toBeEnabled();
    await expect(secondaryBtn).toBeEnabled();

    console.log(`Hero CTA buttons are functional on: ${browserName}`);
  });
});
