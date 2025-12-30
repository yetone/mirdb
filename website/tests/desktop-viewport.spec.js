// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Responsive Design - Desktop Viewport', () => {
  // Test Case 1: Load page at 1920x1080 viewport
  test('TC1: All sections display correctly with proper spacing and alignment at 1920x1080', async ({ page }) => {
    // Set viewport to 1920x1080 (Full HD)
    await page.setViewportSize({ width: 1920, height: 1080 });
    await page.goto('/');

    // Verify page loads successfully
    await expect(page).toHaveTitle(/MirDB/);

    // Verify all major sections are visible
    const navbar = page.locator('header.navbar');
    await expect(navbar).toBeVisible();

    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    const featuresSection = page.locator('[data-testid="features-section"]');
    await expect(featuresSection).toBeVisible();

    const commandsSection = page.locator('[data-testid="commands-section"]');
    await expect(commandsSection).toBeVisible();

    const quickStartSection = page.locator('[data-testid="quick-start"]');
    await expect(quickStartSection).toBeVisible();

    const statusSection = page.locator('[data-testid="status-section"]');
    await expect(statusSection).toBeVisible();

    const footer = page.locator('footer.footer');
    await expect(footer).toBeVisible();

    // Verify sections have proper spacing (padding)
    const heroBox = await heroSection.boundingBox();
    expect(heroBox.width).toBeGreaterThan(0);
    expect(heroBox.height).toBeGreaterThan(100); // Ensure hero has proper height

    // Verify hero content is centered
    const heroContent = page.locator('.hero-content');
    const heroContentBox = await heroContent.boundingBox();
    const heroContentCenterX = heroContentBox.x + heroContentBox.width / 2;
    const viewportCenterX = 1920 / 2;
    // Content should be roughly centered (within 10% of viewport width)
    expect(Math.abs(heroContentCenterX - viewportCenterX)).toBeLessThan(192);

    // Verify features grid has appropriate width
    const featuresGrid = page.locator('.features-grid');
    const featuresGridBox = await featuresGrid.boundingBox();
    expect(featuresGridBox.width).toBeLessThanOrEqual(1200); // max-width constraint
  });

  // Test Case 2: Load page at 2560x1440 viewport
  test('TC2: Layout scales appropriately for large screens at 2560x1440 without breaking', async ({ page }) => {
    // Set viewport to 2560x1440 (QHD/1440p)
    await page.setViewportSize({ width: 2560, height: 1440 });
    await page.goto('/');

    // Verify page loads successfully
    await expect(page).toHaveTitle(/MirDB/);

    // Verify all major sections are visible and not broken
    const navbar = page.locator('header.navbar');
    await expect(navbar).toBeVisible();

    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    const featuresSection = page.locator('[data-testid="features-section"]');
    await expect(featuresSection).toBeVisible();

    // Verify navbar spans full width but content is constrained
    const navbarNav = page.locator('.navbar nav');
    const navbarNavBox = await navbarNav.boundingBox();
    expect(navbarNavBox.width).toBeLessThanOrEqual(1200); // max-width constraint

    // Verify hero section spans full viewport width (as background)
    const heroBox = await heroSection.boundingBox();
    expect(heroBox.width).toBeGreaterThanOrEqual(2560);

    // Verify content containers have proper max-width constraints
    const heroContent = page.locator('.hero-content');
    const heroContentBox = await heroContent.boundingBox();
    expect(heroContentBox.width).toBeLessThanOrEqual(800); // max-width: 800px

    // Verify features grid is constrained
    const featuresGrid = page.locator('.features-grid');
    const featuresGridBox = await featuresGrid.boundingBox();
    expect(featuresGridBox.width).toBeLessThanOrEqual(1200);

    // Verify text is readable (not stretched)
    const tagline = page.locator('.tagline');
    await expect(tagline).toBeVisible();
    const taglineBox = await tagline.boundingBox();
    expect(taglineBox.width).toBeLessThan(800); // Text should not span too wide

    // Verify no horizontal scrollbar (layout not breaking)
    const documentWidth = await page.evaluate(() => document.documentElement.scrollWidth);
    expect(documentWidth).toBeLessThanOrEqual(2560);
  });

  // Test Case 3: Check feature cards layout at 1920px
  test('TC3: Feature cards display in multi-column grid layout at 1920px', async ({ page }) => {
    // Set viewport to 1920px width
    await page.setViewportSize({ width: 1920, height: 1080 });
    await page.goto('/');

    // Navigate to features section
    const featuresSection = page.locator('[data-testid="features-section"]');
    await expect(featuresSection).toBeVisible();

    // Get all feature cards
    const featureCards = page.locator('[data-testid="feature-card"]');
    const cardCount = await featureCards.count();
    expect(cardCount).toBe(5); // Should have 5 feature cards

    // Get bounding boxes for all cards to verify grid layout
    const cardBoxes = [];
    for (let i = 0; i < cardCount; i++) {
      const box = await featureCards.nth(i).boundingBox();
      cardBoxes.push(box);
    }

    // Verify multi-column layout by checking cards are positioned in a grid
    // With minmax(280px, 1fr), at 1920px we should have multiple columns

    // Get unique Y positions (rows)
    const uniqueYPositions = [...new Set(cardBoxes.map(box => Math.round(box.y)))];

    // At 1920px, with max-width 1200px and min card width 280px,
    // we should have at most 2 rows for 5 cards (3 in first row, 2 in second)
    // or potentially 1-2 rows depending on exact layout
    expect(uniqueYPositions.length).toBeLessThanOrEqual(2);

    // Verify cards on the same row have different X positions (multiple columns)
    const firstRowY = uniqueYPositions[0];
    const cardsInFirstRow = cardBoxes.filter(box => Math.round(box.y) === firstRowY);

    // Should have at least 3 cards in the first row at desktop size
    expect(cardsInFirstRow.length).toBeGreaterThanOrEqual(3);

    // Verify cards in first row have different X positions (side by side)
    const xPositions = cardsInFirstRow.map(box => box.x);
    const uniqueXPositions = [...new Set(xPositions)];
    expect(uniqueXPositions.length).toBeGreaterThanOrEqual(3);

    // Verify cards have proper width (not full width)
    for (const box of cardBoxes) {
      expect(box.width).toBeLessThan(600); // Cards should not be too wide
      expect(box.width).toBeGreaterThan(200); // Cards should have minimum width
    }
  });

  // Test Case 4: Check navigation at desktop size
  test('TC4: Navigation displays full menu items (not hamburger menu) at desktop size', async ({ page }) => {
    // Set viewport to desktop size (1920x1080)
    await page.setViewportSize({ width: 1920, height: 1080 });
    await page.goto('/');

    // Verify navbar is visible
    const navbar = page.locator('header.navbar');
    await expect(navbar).toBeVisible();

    // Verify nav-links (full menu) is visible
    const navLinks = page.locator('.nav-links');
    await expect(navLinks).toBeVisible();

    // Verify all menu items are visible
    const menuItems = page.locator('.nav-links li');
    const menuCount = await menuItems.count();
    expect(menuCount).toBe(4); // Features, Commands, Quick Start, GitHub

    // Verify each menu item is visible
    for (let i = 0; i < menuCount; i++) {
      await expect(menuItems.nth(i)).toBeVisible();
    }

    // Verify specific navigation links are present and visible
    const featuresLink = page.locator('.nav-links a[href="#features"]');
    await expect(featuresLink).toBeVisible();
    await expect(featuresLink).toHaveText('Features');

    const commandsLink = page.locator('.nav-links a[href="#commands"]');
    await expect(commandsLink).toBeVisible();
    await expect(commandsLink).toHaveText('Commands');

    const quickStartLink = page.locator('.nav-links a[href="#quick-start"]');
    await expect(quickStartLink).toBeVisible();
    await expect(quickStartLink).toHaveText('Quick Start');

    const githubLink = page.locator('.nav-links a:has-text("GitHub")');
    await expect(githubLink).toBeVisible();

    // Verify navigation is laid out horizontally (not stacked vertically like hamburger menu)
    const navLinksBox = await navLinks.boundingBox();

    // Get positions of first and last menu items
    const firstItemBox = await menuItems.first().boundingBox();
    const lastItemBox = await menuItems.last().boundingBox();

    // Items should be on the same vertical level (horizontal layout)
    expect(Math.abs(firstItemBox.y - lastItemBox.y)).toBeLessThan(20);

    // Items should be spread horizontally
    expect(lastItemBox.x).toBeGreaterThan(firstItemBox.x);

    // Verify nav-links has display: flex (not display: none which would be for mobile hamburger)
    const navLinksDisplay = await navLinks.evaluate((el) => window.getComputedStyle(el).display);
    expect(navLinksDisplay).toBe('flex');

    // Verify no hamburger menu button is visible (should not exist or be hidden at desktop)
    const hamburgerButton = page.locator('.hamburger, .menu-toggle, [data-testid="hamburger-menu"]');
    const hamburgerCount = await hamburgerButton.count();
    if (hamburgerCount > 0) {
      // If hamburger exists, it should be hidden at desktop size
      await expect(hamburgerButton.first()).not.toBeVisible();
    }
  });

  // Additional test: Verify layout at minimum desktop breakpoint (1024px)
  test('TC5: Layout displays correctly at minimum desktop breakpoint (1024px)', async ({ page }) => {
    // Set viewport to minimum desktop size
    await page.setViewportSize({ width: 1024, height: 768 });
    await page.goto('/');

    // Verify page loads successfully
    await expect(page).toHaveTitle(/MirDB/);

    // Verify nav-links is still visible (not hidden like at mobile)
    const navLinks = page.locator('.nav-links');
    await expect(navLinks).toBeVisible();

    // Verify feature cards are in multi-column layout
    const featureCards = page.locator('[data-testid="feature-card"]');
    const cardBoxes = [];
    const cardCount = await featureCards.count();

    for (let i = 0; i < cardCount; i++) {
      const box = await featureCards.nth(i).boundingBox();
      cardBoxes.push(box);
    }

    // Get unique Y positions to determine number of rows
    const uniqueYPositions = [...new Set(cardBoxes.map(box => Math.round(box.y)))];

    // At 1024px, should still have multi-column layout (not single column)
    // With minmax(280px, 1fr), we should have 3 columns max
    const firstRowY = uniqueYPositions[0];
    const cardsInFirstRow = cardBoxes.filter(box => Math.round(box.y) === firstRowY);

    // Should have at least 2 cards in a row (multi-column)
    expect(cardsInFirstRow.length).toBeGreaterThanOrEqual(2);
  });
});
