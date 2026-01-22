// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Responsive Design - Desktop', () => {
  test.use({
    viewport: { width: 1440, height: 900 }
  });

  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Content has appropriate max-width to prevent excessive line lengths at 1440px width', async ({ page }) => {
    // Verify viewport is at desktop width
    const viewportSize = page.viewportSize();
    expect(viewportSize.width).toBe(1440);

    // Check the container has a max-width that prevents excessive stretching
    const container = page.locator('.container').first();
    await expect(container).toBeVisible();

    // Get the computed max-width of the container
    const containerBox = await container.boundingBox();
    expect(containerBox).not.toBeNull();

    // The container should have a max-width less than the full viewport
    // to prevent text from stretching too wide (max-width: 1200px in CSS)
    expect(containerBox.width).toBeLessThanOrEqual(1200);

    // Verify the container is centered (has margins on both sides at 1440px)
    const marginLeft = containerBox.x;
    const marginRight = 1440 - (containerBox.x + containerBox.width);

    // Both margins should be approximately equal (centered layout)
    expect(Math.abs(marginLeft - marginRight)).toBeLessThan(50);

    // Verify hero content also has max-width constraint
    const heroContent = page.locator('.hero-content');
    await expect(heroContent).toBeVisible();

    const heroBox = await heroContent.boundingBox();
    expect(heroBox).not.toBeNull();
    // Hero content max-width is 800px in CSS
    expect(heroBox.width).toBeLessThanOrEqual(800);
  });

  test('TC2: Features display in multi-column grid (3 or 6 columns) on desktop', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();
    await expect(featuresSection).toBeVisible();

    // Get the features grid
    const featuresGrid = page.locator('.features-grid');
    await expect(featuresGrid).toBeVisible();

    // Get all feature cards
    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();
    expect(cardCount).toBe(6);

    // Get positions of all cards to determine column layout
    const cardPositions = [];
    for (let i = 0; i < cardCount; i++) {
      const card = featureCards.nth(i);
      const box = await card.boundingBox();
      expect(box).not.toBeNull();
      cardPositions.push({ x: box.x, y: box.y, width: box.width });
    }

    // Group cards by their Y position (same row)
    const rows = {};
    for (const pos of cardPositions) {
      // Round Y to handle small variations
      const rowKey = Math.round(pos.y / 10) * 10;
      if (!rows[rowKey]) {
        rows[rowKey] = [];
      }
      rows[rowKey].push(pos);
    }

    const rowKeys = Object.keys(rows);
    const cardsInFirstRow = rows[rowKeys[0]].length;

    // On desktop at 1440px, with minmax(300px, 1fr) grid and 6 cards,
    // we expect either 3 columns (2 rows of 3) or potentially more columns
    // CSS: grid-template-columns: repeat(auto-fit, minmax(300px, 1fr))
    // At 1440px with 1200px container: floor(1200/300) = 4 columns possible,
    // but 6 cards would fit in 2 rows of 3 each or similar
    expect(cardsInFirstRow).toBeGreaterThanOrEqual(2);
    expect(cardsInFirstRow).toBeLessThanOrEqual(6);

    // Verify that cards are arranged in a proper grid (not stacked vertically)
    const uniqueRows = rowKeys.length;
    // With 6 cards and at least 2 columns, we should have at most 3 rows
    expect(uniqueRows).toBeLessThanOrEqual(3);
  });

  test('TC3: Hero and demo sections may use side-by-side layout where appropriate on desktop', async ({ page }) => {
    // Test Hero Section layout at desktop width
    const heroSection = page.locator('.hero');
    await expect(heroSection).toBeVisible();

    const heroContent = page.locator('.hero-content');
    await expect(heroContent).toBeVisible();

    // Get hero dimensions
    const heroBox = await heroSection.boundingBox();
    expect(heroBox).not.toBeNull();

    // On desktop, hero should use full width of viewport
    expect(heroBox.width).toBeGreaterThanOrEqual(1400);

    // Verify hero CTAs are displayed side-by-side (inline)
    const heroCtas = page.locator('.hero-ctas');
    await expect(heroCtas).toBeVisible();

    const ctaButtons = page.locator('.hero-ctas .btn');
    const buttonCount = await ctaButtons.count();
    expect(buttonCount).toBeGreaterThanOrEqual(2);

    // Get positions of CTA buttons
    const button1 = await ctaButtons.first().boundingBox();
    const button2 = await ctaButtons.nth(1).boundingBox();

    expect(button1).not.toBeNull();
    expect(button2).not.toBeNull();

    // On desktop, buttons should be side-by-side (similar Y position)
    expect(Math.abs(button1.y - button2.y)).toBeLessThan(10);

    // Test Demo Section layout
    const demoSection = page.locator('#demo');
    await demoSection.scrollIntoViewIfNeeded();
    await expect(demoSection).toBeVisible();

    const demoContent = page.locator('.demo-content');
    await expect(demoContent).toBeVisible();

    // Demo content should have reasonable max-width (max-width: 900px in CSS)
    const demoBox = await demoContent.boundingBox();
    expect(demoBox).not.toBeNull();
    expect(demoBox.width).toBeLessThanOrEqual(900);

    // Verify demo content is centered within the section
    const demoSectionBox = await demoSection.boundingBox();
    const demoContentCenterX = demoBox.x + (demoBox.width / 2);
    const sectionCenterX = demoSectionBox.x + (demoSectionBox.width / 2);

    // Demo content should be approximately centered
    expect(Math.abs(demoContentCenterX - sectionCenterX)).toBeLessThan(50);
  });

  test('Navigation is fully visible on desktop viewport', async ({ page }) => {
    // Verify navigation elements are visible
    const nav = page.locator('.nav');
    await expect(nav).toBeVisible();

    const navBrand = page.locator('.nav-brand');
    await expect(navBrand).toBeVisible();

    const navLinks = page.locator('.nav-links');
    await expect(navLinks).toBeVisible();

    // All nav link items should be visible and arranged horizontally
    const navItems = page.locator('.nav-links li');
    const itemCount = await navItems.count();
    expect(itemCount).toBeGreaterThanOrEqual(3);

    // Get positions of nav items to verify horizontal layout
    const firstItem = await navItems.first().boundingBox();
    const lastItem = await navItems.last().boundingBox();

    expect(firstItem).not.toBeNull();
    expect(lastItem).not.toBeNull();

    // Items should be on the same horizontal line (similar Y position)
    // Allow small tolerance for subpixel rendering differences
    expect(Math.abs(firstItem.y - lastItem.y)).toBeLessThan(15);
  });

  test('Footer content is laid out properly on desktop', async ({ page }) => {
    // Scroll to footer
    const footer = page.locator('.footer');
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();

    // Verify footer content layout
    const footerContent = page.locator('.footer-content');
    await expect(footerContent).toBeVisible();

    const footerBrand = page.locator('.footer-brand');
    const footerLinks = page.locator('.footer-links');

    await expect(footerBrand).toBeVisible();
    await expect(footerLinks).toBeVisible();

    // On desktop, footer brand and links should be side-by-side
    const brandBox = await footerBrand.boundingBox();
    const linksBox = await footerLinks.boundingBox();

    expect(brandBox).not.toBeNull();
    expect(linksBox).not.toBeNull();

    // They should be on approximately the same vertical position
    expect(Math.abs(brandBox.y - linksBox.y)).toBeLessThan(30);

    // Links should be to the right of brand
    expect(linksBox.x).toBeGreaterThan(brandBox.x);
  });

  test('Roadmap section uses multi-column layout on desktop', async ({ page }) => {
    // Scroll to roadmap section
    const roadmapSection = page.locator('#roadmap');
    await roadmapSection.scrollIntoViewIfNeeded();
    await expect(roadmapSection).toBeVisible();

    // Get the roadmap content grid
    const roadmapContent = page.locator('.roadmap-content');
    await expect(roadmapContent).toBeVisible();

    // Get the two roadmap sections (Completed and Planned)
    const roadmapSections = page.locator('.roadmap-section');
    const sectionCount = await roadmapSections.count();
    expect(sectionCount).toBe(2);

    // On desktop, sections should be side-by-side
    const completedSection = roadmapSections.first();
    const plannedSection = roadmapSections.last();

    const completedBox = await completedSection.boundingBox();
    const plannedBox = await plannedSection.boundingBox();

    expect(completedBox).not.toBeNull();
    expect(plannedBox).not.toBeNull();

    // Sections should be on approximately the same vertical position (side-by-side)
    expect(Math.abs(completedBox.y - plannedBox.y)).toBeLessThan(30);

    // Planned section should be to the right of completed section
    expect(plannedBox.x).toBeGreaterThan(completedBox.x);
  });
});
