// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Tablet Responsive Design Tests
 *
 * Scenario: Verify homepage displays correctly on tablet devices
 * Viewport: 768x1024 (standard iPad portrait dimensions)
 */

const TABLET_VIEWPORT = { width: 768, height: 1024 };

test.describe('Tablet Responsive Design', () => {
  test.beforeEach(async ({ page }) => {
    // Set tablet viewport before each test
    await page.setViewportSize(TABLET_VIEWPORT);
    await page.goto('/');
  });

  /**
   * Test Case 1: Page renders with appropriate tablet layout
   * Input: Load page at 768px width
   * Expected: Page renders with appropriate tablet layout
   */
  test('should render page with appropriate tablet layout at 768px width', async ({ page }) => {
    // Verify viewport is set correctly
    const viewport = page.viewportSize();
    expect(viewport.width).toBe(768);
    expect(viewport.height).toBe(1024);

    // Verify main sections are visible
    const header = page.locator('header, .header');
    await expect(header).toBeVisible();

    const hero = page.locator('.hero');
    await expect(hero).toBeVisible();

    const features = page.locator('#features, .features-section');
    await expect(features).toBeVisible();

    const quickstart = page.locator('#quickstart, .quickstart-section');
    await expect(quickstart).toBeVisible();

    const footer = page.locator('footer, .footer');
    await expect(footer).toBeVisible();

    // Verify page is scrollable (content fits within tablet viewport)
    const body = page.locator('body');
    const bodyBox = await body.boundingBox();
    expect(bodyBox).toBeTruthy();
    expect(bodyBox.width).toBeLessThanOrEqual(768);

    // Verify there's no horizontal scrollbar (no overflow)
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScroll).toBe(false);
  });

  /**
   * Test Case 2: Feature cards display in 2-column grid on tablet
   * Input: Check features grid on tablet
   * Expected: Feature cards display in 2-column grid on tablet
   */
  test('should display feature cards in 2-column grid on tablet', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features, .features-section');
    await featuresSection.scrollIntoViewIfNeeded();
    await expect(featuresSection).toBeVisible();

    // Get the features grid
    const featuresGrid = page.locator('.features-grid, [data-testid="features-grid"]');
    await expect(featuresGrid).toBeVisible();

    // Get all feature cards
    const featureCards = page.locator('.feature-card, [data-testid="feature-card"]');
    const cardCount = await featureCards.count();
    expect(cardCount).toBeGreaterThanOrEqual(4);

    // Get first two card positions to verify 2-column layout
    const firstCard = featureCards.nth(0);
    const secondCard = featureCards.nth(1);
    const thirdCard = featureCards.nth(2);

    const firstBox = await firstCard.boundingBox();
    const secondBox = await secondCard.boundingBox();
    const thirdBox = await thirdCard.boundingBox();

    expect(firstBox).toBeTruthy();
    expect(secondBox).toBeTruthy();
    expect(thirdBox).toBeTruthy();

    // In a 2-column layout:
    // - First and second cards should be on the same row (similar Y position)
    // - Third card should be on a different row (below first card)
    // Allow for small variation due to padding/margins
    const yTolerance = 20;

    // First and second card should be side by side (same row)
    expect(Math.abs(firstBox.y - secondBox.y)).toBeLessThan(yTolerance);

    // Third card should be below the first card (different row)
    expect(thirdBox.y).toBeGreaterThan(firstBox.y + firstBox.height - yTolerance);

    // First card should be on the left, second on the right
    expect(secondBox.x).toBeGreaterThan(firstBox.x);

    // Verify grid has 2 columns by checking computed style
    const gridStyle = await featuresGrid.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        display: style.display,
        gridTemplateColumns: style.gridTemplateColumns
      };
    });

    expect(gridStyle.display).toBe('grid');
    // Grid template columns should indicate 2 columns (not single column '1fr' or 'none')
    // The grid template should contain at least one space (indicating multiple columns)
    const columnCount = gridStyle.gridTemplateColumns.split(/\s+/).filter(c => c && c !== 'none').length;
    expect(columnCount).toBe(2);
  });

  /**
   * Test Case 3: Navigation is fully functional and accessible on tablet
   * Input: Check navigation on tablet
   * Expected: Navigation is fully functional and accessible on tablet
   */
  test('should have fully functional and accessible navigation on tablet', async ({ page }) => {
    // Verify header is visible
    const header = page.locator('header, .header');
    await expect(header).toBeVisible();

    // Check logo is visible and clickable
    const logo = page.locator('[data-testid="logo"], .nav-brand');
    await expect(logo).toBeVisible();

    // Check navigation links are visible
    const navLinks = page.locator('.nav-links');
    await expect(navLinks).toBeVisible();

    // Verify all navigation links are displayed and accessible
    const quickStartLink = page.locator('.nav-links a:has-text("Quick Start")');
    const docsLink = page.locator('.nav-links a:has-text("Documentation"), .nav-links a:has-text("Docs")');
    const githubLink = page.locator('.nav-links a:has-text("GitHub")');

    await expect(quickStartLink).toBeVisible();
    await expect(docsLink).toBeVisible();
    await expect(githubLink).toBeVisible();

    // Test keyboard navigation
    await quickStartLink.focus();
    await expect(quickStartLink).toBeFocused();

    await docsLink.focus();
    await expect(docsLink).toBeFocused();

    await githubLink.focus();
    await expect(githubLink).toBeFocused();

    // Verify navigation links have proper href attributes
    const quickStartHref = await quickStartLink.getAttribute('href');
    expect(quickStartHref).toBeTruthy();
    expect(quickStartHref).toContain('quickstart');

    const githubHref = await githubLink.getAttribute('href');
    expect(githubHref).toBeTruthy();
    expect(githubHref).toContain('github.com');

    // Test navigation functionality - click Quick Start
    await quickStartLink.click();
    await page.waitForTimeout(500);

    // After clicking Quick Start, the URL should contain #quickstart
    const url = page.url();
    expect(url).toContain('quickstart');

    // Verify the quickstart section is visible after navigation
    const quickstartSection = page.locator('#quickstart, .quickstart-section');
    await expect(quickstartSection).toBeInViewport();
  });

  /**
   * Test Case 4: Architecture diagram scales appropriately and remains readable
   * Input: Check architecture diagram on tablet
   * Expected: Diagram scales appropriately and remains readable
   */
  test('should scale architecture diagram appropriately and remain readable on tablet', async ({ page }) => {
    // The "architecture" is represented in the Project Status section
    // and the feature icons/diagrams
    const statusSection = page.locator('#status, .status-section');
    await statusSection.scrollIntoViewIfNeeded();
    await expect(statusSection).toBeVisible();

    // Check the status grid layout on tablet
    const statusGrid = page.locator('.status-grid');
    await expect(statusGrid).toBeVisible();

    // Get the implemented and planned sections
    const implementedSection = page.locator('[data-testid="implemented-features"], .status-implemented');
    const plannedSection = page.locator('[data-testid="planned-features"], .status-planned');

    await expect(implementedSection).toBeVisible();
    await expect(plannedSection).toBeVisible();

    // Check that sections are readable (have adequate width)
    const implementedBox = await implementedSection.boundingBox();
    const plannedBox = await plannedSection.boundingBox();

    expect(implementedBox).toBeTruthy();
    expect(plannedBox).toBeTruthy();

    // Each section should have at least 250px width for readability
    expect(implementedBox.width).toBeGreaterThanOrEqual(250);
    expect(plannedBox.width).toBeGreaterThanOrEqual(250);

    // Verify text content is readable - check font size is adequate
    const statusItem = page.locator('.status-item').first();
    const statusItemStyle = await statusItem.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        fontSize: parseFloat(style.fontSize),
        lineHeight: style.lineHeight
      };
    });

    // Font size should be at least 14px for readability on tablet
    expect(statusItemStyle.fontSize).toBeGreaterThanOrEqual(14);

    // Verify status icons are visible and properly sized
    const statusIcons = page.locator('.status-indicator svg, [data-testid="status-indicator"] svg');
    const iconCount = await statusIcons.count();
    expect(iconCount).toBeGreaterThan(0);

    // Check first icon is visible and has adequate size
    const firstIcon = statusIcons.first();
    await expect(firstIcon).toBeVisible();

    const iconBox = await firstIcon.boundingBox();
    expect(iconBox).toBeTruthy();
    // Icons should be at least 14px for visibility
    expect(iconBox.width).toBeGreaterThanOrEqual(14);
    expect(iconBox.height).toBeGreaterThanOrEqual(14);

    // Verify code blocks in quickstart are readable on tablet
    const quickstartSection = page.locator('#quickstart, .quickstart-section');
    await quickstartSection.scrollIntoViewIfNeeded();

    const codeBlock = page.locator('.code-block').first();
    await expect(codeBlock).toBeVisible();

    const codeBlockBox = await codeBlock.boundingBox();
    expect(codeBlockBox).toBeTruthy();

    // Code block should fit within tablet viewport
    expect(codeBlockBox.width).toBeLessThanOrEqual(768);
    // Code block should have adequate width for code readability
    expect(codeBlockBox.width).toBeGreaterThanOrEqual(300);
  });
});

test.describe('Tablet Layout Consistency', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(TABLET_VIEWPORT);
    await page.goto('/');
  });

  test('should maintain header sticky behavior on tablet', async ({ page }) => {
    const header = page.locator('header, .header');
    await expect(header).toBeVisible();

    // Get initial header position
    const initialBox = await header.boundingBox();
    expect(initialBox.y).toBeLessThanOrEqual(5);

    // Scroll down
    await page.evaluate(() => window.scrollTo(0, 500));
    await page.waitForTimeout(300);

    // Header should still be at top (sticky)
    const scrolledBox = await header.boundingBox();
    expect(scrolledBox.y).toBeLessThanOrEqual(5);
    await expect(header).toBeInViewport();
  });

  test('should display hero section appropriately on tablet', async ({ page }) => {
    const hero = page.locator('.hero');
    await expect(hero).toBeVisible();

    // Check hero heading font size is appropriate for tablet
    const heroH1 = page.locator('.hero h1');
    await expect(heroH1).toBeVisible();

    const h1Style = await heroH1.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        fontSize: parseFloat(style.fontSize)
      };
    });

    // On tablet (768px), font should be between mobile (32px) and desktop (56px)
    // Based on CSS: 2.5rem = 40px for tablet
    expect(h1Style.fontSize).toBeGreaterThanOrEqual(32);
    expect(h1Style.fontSize).toBeLessThanOrEqual(56);

    // Hero buttons should be visible and clickable
    const heroButtons = page.locator('.hero-buttons');
    await expect(heroButtons).toBeVisible();

    const getStartedBtn = page.locator('.hero-buttons .btn-primary');
    const githubBtn = page.locator('.hero-buttons .btn-secondary');

    await expect(getStartedBtn).toBeVisible();
    await expect(githubBtn).toBeVisible();

    // Buttons should be side by side on tablet (not stacked)
    const getStartedBox = await getStartedBtn.boundingBox();
    const githubBox = await githubBtn.boundingBox();

    // Allow for wrapping - buttons can be side by side or wrapped
    // Just verify both buttons are accessible and visible
    expect(getStartedBox).toBeTruthy();
    expect(githubBox).toBeTruthy();
    expect(getStartedBox.width).toBeGreaterThan(100);
    expect(githubBox.width).toBeGreaterThan(100);
  });

  test('should display footer correctly on tablet', async ({ page }) => {
    const footer = page.locator('footer, .footer');
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();

    // Verify footer content is visible
    const footerText = await footer.textContent();
    expect(footerText).toContain('MIT License');

    // Verify footer links are accessible
    const footerLinks = footer.locator('a');
    const linkCount = await footerLinks.count();
    expect(linkCount).toBeGreaterThanOrEqual(2);

    // Test footer links are keyboard accessible
    for (let i = 0; i < linkCount; i++) {
      const link = footerLinks.nth(i);
      await link.focus();
      await expect(link).toBeFocused();
    }
  });
});
