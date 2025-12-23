// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * E2E Tests for Desktop Responsive Design
 * Scenario: Verify the homepage displays correctly on desktop (>1024px) as specified in NFR-1
 * Related Requirements: NFR-1 (Responsive Design), US-6
 */

const DESKTOP_WIDTH = 1280;
const DESKTOP_HEIGHT = 800;

test.describe('Responsive Design - Desktop', () => {
  test.beforeEach(async ({ page }) => {
    // Set viewport to desktop size (1280px width, >1024px)
    await page.setViewportSize({ width: DESKTOP_WIDTH, height: DESKTOP_HEIGHT });
    await page.goto('/');
  });

  /**
   * Test Case 1: Load page at 1280px width (desktop)
   * Input: Load page at 1280px width (desktop)
   * Expected: Full desktop layout is displayed with all elements properly positioned
   */
  test('TC1: Full desktop layout is displayed with all elements properly positioned at 1280px width', async ({ page }) => {
    // Verify the page loads correctly at desktop width
    const body = page.locator('body');
    await expect(body).toBeVisible();

    // Verify hero section is visible and properly sized
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    const heroBox = await heroSection.boundingBox();
    expect(heroBox).toBeTruthy();
    // Hero should span the full width of the viewport (allowing for padding)
    expect(heroBox.width).toBeGreaterThanOrEqual(DESKTOP_WIDTH - 50);

    // Verify main content sections are visible
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeVisible();

    const projectStatusSection = page.locator('#project-status');
    await expect(projectStatusSection).toBeVisible();

    const codeExamplesSection = page.locator('#code-examples');
    await expect(codeExamplesSection).toBeVisible();

    // Verify footer is present
    const footer = page.locator('footer.footer');
    await expect(footer).toBeVisible();

    // Verify no horizontal scrolling is required
    const scrollWidth = await page.evaluate(() => document.documentElement.scrollWidth);
    const clientWidth = await page.evaluate(() => document.documentElement.clientWidth);
    expect(scrollWidth).toBeLessThanOrEqual(clientWidth + 5); // Small tolerance for rounding
  });

  /**
   * Test Case 2: Check feature cards layout on desktop
   * Input: Check feature cards layout on desktop
   * Expected: Feature cards display in multi-column grid (3-4 columns) on desktop
   */
  test('TC2: Feature cards display in multi-column grid (3-4 columns) on desktop', async ({ page }) => {
    // Scroll to features section
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();

    // Get the feature grid
    const featureGrid = page.locator('.feature-grid');
    await expect(featureGrid).toBeVisible();

    // Get all feature cards
    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();
    expect(cardCount).toBeGreaterThanOrEqual(3); // Need at least 3 cards to verify multi-column

    // Get bounding boxes for first three cards to verify multi-column layout
    const card1 = featureCards.nth(0);
    const card2 = featureCards.nth(1);
    const card3 = featureCards.nth(2);

    await expect(card1).toBeVisible();
    await expect(card2).toBeVisible();
    await expect(card3).toBeVisible();

    const card1Box = await card1.boundingBox();
    const card2Box = await card2.boundingBox();
    const card3Box = await card3.boundingBox();

    expect(card1Box).toBeTruthy();
    expect(card2Box).toBeTruthy();
    expect(card3Box).toBeTruthy();

    // On desktop (1280px), with auto-fit and minmax(280px, 1fr), we should have 3-4 columns
    // All first 3 (or at least 3) cards should be on the same row
    // Check that they are positioned horizontally (different X positions, same Y)
    expect(Math.abs(card1Box.y - card2Box.y)).toBeLessThan(10);
    expect(card2Box.x).toBeGreaterThan(card1Box.x);

    // Third card should also be on the same row (different X, same Y as first two)
    expect(Math.abs(card2Box.y - card3Box.y)).toBeLessThan(10);
    expect(card3Box.x).toBeGreaterThan(card2Box.x);

    // Verify grid computed style confirms multi-column layout (3-4 columns)
    const gridStyle = await featureGrid.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        display: style.display,
        gridTemplateColumns: style.gridTemplateColumns
      };
    });

    expect(gridStyle.display).toBe('grid');
    // The grid-template-columns should have 3-4 columns on desktop
    const columnCount = gridStyle.gridTemplateColumns.split(' ').filter(v => v && v !== '0px').length;
    expect(columnCount).toBeGreaterThanOrEqual(3);
    expect(columnCount).toBeLessThanOrEqual(4);
  });

  /**
   * Test Case 3: Verify navigation bar on desktop
   * Input: Verify navigation bar on desktop
   * Expected: Full horizontal navigation bar is visible with all links
   */
  test('TC3: Full horizontal navigation bar is visible with all links on desktop', async ({ page }) => {
    // Get the header/navigation
    const header = page.locator('header.header');
    await expect(header).toBeVisible();

    const nav = page.locator('nav.nav');
    await expect(nav).toBeVisible();

    // Check if the navigation links are visible
    const navLinks = page.locator('.nav-links');
    await expect(navLinks).toBeVisible();

    // Verify the logo is visible
    const logo = page.locator('.logo');
    await expect(logo).toBeVisible();
    await expect(logo).toContainText('MirDB');

    // Check for all navigation items
    const featuresLink = navLinks.locator('a[href="#features"]');
    const gettingStartedLink = navLinks.locator('a[href="#getting-started"]');
    const docsLink = navLinks.locator('a:has-text("Documentation")');
    const githubLink = navLinks.locator('a:has-text("GitHub")');

    // On desktop, all navigation links should be fully visible
    await expect(featuresLink).toBeVisible();
    await expect(gettingStartedLink).toBeVisible();
    await expect(docsLink).toBeVisible();
    await expect(githubLink).toBeVisible();

    // Verify theme toggle is visible
    const themeToggle = page.locator('.theme-toggle');
    await expect(themeToggle).toBeVisible();

    // Verify navigation links are on a single horizontal line (same Y position)
    const featuresBox = await featuresLink.boundingBox();
    const gettingStartedBox = await gettingStartedLink.boundingBox();
    const docsBox = await docsLink.boundingBox();
    const githubBox = await githubLink.boundingBox();

    expect(featuresBox).toBeTruthy();
    expect(gettingStartedBox).toBeTruthy();
    expect(docsBox).toBeTruthy();
    expect(githubBox).toBeTruthy();

    // All nav links should be at approximately the same Y position (horizontal layout)
    const yPositions = [featuresBox.y, gettingStartedBox.y, docsBox.y, githubBox.y];
    const avgY = yPositions.reduce((a, b) => a + b, 0) / yPositions.length;
    yPositions.forEach(y => {
      expect(Math.abs(y - avgY)).toBeLessThan(10); // Allow small variation
    });

    // Verify links are positioned from left to right
    expect(gettingStartedBox.x).toBeGreaterThan(featuresBox.x);
    expect(docsBox.x).toBeGreaterThan(gettingStartedBox.x);
    expect(githubBox.x).toBeGreaterThan(docsBox.x);

    // Verify navigation functionality - click on Features link
    await featuresLink.click();
    await page.waitForTimeout(500);

    // Verify the Features section is in view
    const featuresSection = page.locator('#features');
    const featuresBoundingBox = await featuresSection.boundingBox();
    expect(featuresBoundingBox).toBeTruthy();
    expect(featuresBoundingBox.y).toBeLessThan(200);
  });

  /**
   * Test Case 4: Check content max-width
   * Input: Check content max-width
   * Expected: Content is constrained to readable width, not stretching full screen
   */
  test('TC4: Content is constrained to readable width, not stretching full screen', async ({ page }) => {
    // Check that the main container has a max-width constraint
    // The CSS defines --max-width: 1200px
    const MAX_CONTENT_WIDTH = 1200;

    // Check hero content max-width
    const heroContent = page.locator('.hero-content');
    await expect(heroContent).toBeVisible();
    const heroContentBox = await heroContent.boundingBox();
    expect(heroContentBox).toBeTruthy();
    expect(heroContentBox.width).toBeLessThanOrEqual(MAX_CONTENT_WIDTH + 50); // Allow some padding

    // Check feature grid max-width
    const featureGrid = page.locator('.feature-grid');
    await featureGrid.scrollIntoViewIfNeeded();
    const featureGridBox = await featureGrid.boundingBox();
    expect(featureGridBox).toBeTruthy();
    expect(featureGridBox.width).toBeLessThanOrEqual(MAX_CONTENT_WIDTH + 50);

    // Check getting-started section max-width
    const gettingStarted = page.locator('.getting-started');
    await gettingStarted.scrollIntoViewIfNeeded();

    // The getting-started section has max-width: var(--max-width)
    const gettingStartedStyle = await gettingStarted.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        maxWidth: style.maxWidth
      };
    });
    expect(gettingStartedStyle.maxWidth).toBe('1200px');

    // Check code examples grid max-width
    const codeExamplesGrid = page.locator('.code-examples-grid');
    await codeExamplesGrid.scrollIntoViewIfNeeded();
    const codeExamplesGridBox = await codeExamplesGrid.boundingBox();
    expect(codeExamplesGridBox).toBeTruthy();
    expect(codeExamplesGridBox.width).toBeLessThanOrEqual(MAX_CONTENT_WIDTH + 50);

    // Check navigation max-width
    const nav = page.locator('nav.nav');
    const navStyle = await nav.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        maxWidth: style.maxWidth
      };
    });
    expect(navStyle.maxWidth).toBe('1200px');

    // Verify content is centered (left margin approximately equals right margin)
    const viewportWidth = await page.evaluate(() => window.innerWidth);
    const expectedMargin = (viewportWidth - MAX_CONTENT_WIDTH) / 2;

    // Feature grid should be roughly centered
    if (featureGridBox.width <= MAX_CONTENT_WIDTH) {
      const leftMargin = featureGridBox.x;
      const rightMargin = viewportWidth - (featureGridBox.x + featureGridBox.width);
      // Margins should be approximately equal (centered)
      expect(Math.abs(leftMargin - rightMargin)).toBeLessThan(100);
    }
  });

  /**
   * Additional Test: Verify code examples grid uses 3-column layout on desktop
   */
  test('Code examples grid displays in 3-column layout on desktop', async ({ page }) => {
    // Scroll to code examples section
    const codeExamplesSection = page.locator('#code-examples');
    await codeExamplesSection.scrollIntoViewIfNeeded();

    // Get the code examples grid
    const codeExamplesGrid = page.locator('.code-examples-grid');
    await expect(codeExamplesGrid).toBeVisible();

    // Get all code example cards
    const codeExampleCards = page.locator('.code-example-card');
    const cardCount = await codeExampleCards.count();
    expect(cardCount).toBe(3); // SET, GET, DELETE examples

    // Get bounding boxes for all three cards
    const card1 = codeExampleCards.nth(0);
    const card2 = codeExampleCards.nth(1);
    const card3 = codeExampleCards.nth(2);

    const card1Box = await card1.boundingBox();
    const card2Box = await card2.boundingBox();
    const card3Box = await card3.boundingBox();

    expect(card1Box).toBeTruthy();
    expect(card2Box).toBeTruthy();
    expect(card3Box).toBeTruthy();

    // All three cards should be on the same row (same Y position)
    expect(Math.abs(card1Box.y - card2Box.y)).toBeLessThan(10);
    expect(Math.abs(card2Box.y - card3Box.y)).toBeLessThan(10);

    // Cards should be positioned from left to right
    expect(card2Box.x).toBeGreaterThan(card1Box.x);
    expect(card3Box.x).toBeGreaterThan(card2Box.x);

    // Verify grid computed style confirms 3-column layout
    const gridStyle = await codeExamplesGrid.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        display: style.display,
        gridTemplateColumns: style.gridTemplateColumns
      };
    });

    expect(gridStyle.display).toBe('grid');
    const columnCount = gridStyle.gridTemplateColumns.split(' ').filter(v => v && v !== '0px').length;
    expect(columnCount).toBe(3);
  });

  /**
   * Additional Test: Verify hero CTA buttons display side by side on desktop
   */
  test('Hero CTA buttons display side by side on desktop', async ({ page }) => {
    // Get the CTA buttons container
    const heroCta = page.locator('.hero-cta');
    await expect(heroCta).toBeVisible();

    // Get the buttons
    const getStartedBtn = page.locator('[data-testid="cta-get-started"]');
    const githubBtn = page.locator('[data-testid="cta-github"]');

    await expect(getStartedBtn).toBeVisible();
    await expect(githubBtn).toBeVisible();

    const btn1Box = await getStartedBtn.boundingBox();
    const btn2Box = await githubBtn.boundingBox();

    expect(btn1Box).toBeTruthy();
    expect(btn2Box).toBeTruthy();

    // On desktop, buttons should be side by side (same Y position, different X)
    expect(Math.abs(btn1Box.y - btn2Box.y)).toBeLessThan(10);
    expect(btn2Box.x).toBeGreaterThan(btn1Box.x);
  });

  /**
   * Additional Test: Verify status grid uses 2-column layout on desktop
   */
  test('Status grid displays in 2-column layout on desktop', async ({ page }) => {
    // Scroll to project status section
    const statusSection = page.locator('#project-status');
    await statusSection.scrollIntoViewIfNeeded();

    // Get the status grid
    const statusGrid = page.locator('.status-grid');
    await expect(statusGrid).toBeVisible();

    // Get status cards
    const statusCards = page.locator('.status-card');
    const cardCount = await statusCards.count();
    expect(cardCount).toBe(2); // Implemented and Planned features

    const card1 = statusCards.nth(0);
    const card2 = statusCards.nth(1);

    const card1Box = await card1.boundingBox();
    const card2Box = await card2.boundingBox();

    expect(card1Box).toBeTruthy();
    expect(card2Box).toBeTruthy();

    // On desktop, status cards should be side by side
    expect(Math.abs(card1Box.y - card2Box.y)).toBeLessThan(10);
    expect(card2Box.x).toBeGreaterThan(card1Box.x);

    // Verify grid computed style confirms 2-column layout
    const gridStyle = await statusGrid.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        display: style.display,
        gridTemplateColumns: style.gridTemplateColumns
      };
    });

    expect(gridStyle.display).toBe('grid');
    const columnCount = gridStyle.gridTemplateColumns.split(' ').filter(v => v && v !== '0px').length;
    expect(columnCount).toBe(2);
  });

  /**
   * Additional Test: Verify commands grid uses 4-column layout on desktop
   */
  test('Commands grid displays in multi-column layout on desktop', async ({ page }) => {
    // Scroll to commands section
    const commandsSection = page.locator('[data-testid="supported-commands-section"]');
    await commandsSection.scrollIntoViewIfNeeded();

    // Get the commands grid
    const commandsGrid = page.locator('.commands-grid');
    await expect(commandsGrid).toBeVisible();

    // Get command groups
    const commandGroups = page.locator('.command-group');
    const groupCount = await commandGroups.count();
    expect(groupCount).toBe(4); // Storage, Retrieval, Deletion, MirDB-specific

    // Verify grid computed style
    const gridStyle = await commandsGrid.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        display: style.display,
        gridTemplateColumns: style.gridTemplateColumns
      };
    });

    expect(gridStyle.display).toBe('grid');
    // On desktop with minmax(250px, 1fr), should have multiple columns
    const columnCount = gridStyle.gridTemplateColumns.split(' ').filter(v => v && v !== '0px').length;
    expect(columnCount).toBeGreaterThanOrEqual(2);
  });

  /**
   * Additional Test: Verify no horizontal scrolling on desktop viewport
   */
  test('No horizontal scrolling required on desktop viewport', async ({ page }) => {
    // Scroll through the entire page
    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
    await page.waitForTimeout(100);

    // Check for horizontal overflow
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });

    expect(hasHorizontalScroll).toBe(false);
  });

  /**
   * Additional Test: Verify larger viewport (1920px)
   */
  test('Layout scales properly at larger desktop viewport (1920px)', async ({ page }) => {
    // Set viewport to larger desktop size
    await page.setViewportSize({ width: 1920, height: 1080 });
    await page.goto('/');

    // Verify content is still constrained to max-width
    const featureGrid = page.locator('.feature-grid');
    await featureGrid.scrollIntoViewIfNeeded();
    const featureGridBox = await featureGrid.boundingBox();
    expect(featureGridBox).toBeTruthy();
    expect(featureGridBox.width).toBeLessThanOrEqual(1200 + 50); // max-width: 1200px

    // Verify content is centered
    const viewportWidth = await page.evaluate(() => window.innerWidth);
    const leftMargin = featureGridBox.x;
    const rightMargin = viewportWidth - (featureGridBox.x + featureGridBox.width);
    // Margins should be approximately equal (centered)
    expect(Math.abs(leftMargin - rightMargin)).toBeLessThan(100);

    // Verify no horizontal scrolling
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScroll).toBe(false);
  });
});
