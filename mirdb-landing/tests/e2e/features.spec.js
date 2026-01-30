/**
 * Features Section E2E Tests
 * Owner: Scenario 3 - Features Showcase Section
 *
 * Tests:
 * - Feature cards content and visibility
 * - Grid layout at different breakpoints
 * - Scroll animations
 * - Reduced motion preference
 */

const { test, expect } = require('@playwright/test');

test.describe('Features Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  // Test Case 1: Features section displays heading and 4 feature cards
  test('displays heading and 4 feature cards', async ({ page }) => {
    // Navigate to features section
    await page.locator('#features').scrollIntoViewIfNeeded();

    // Verify section title
    const sectionTitle = page.locator('#features .section-title');
    await expect(sectionTitle).toBeVisible();
    await expect(sectionTitle).toHaveText('Features');

    // Verify 4 feature cards exist
    const featureCards = page.locator('#features .feature-card');
    await expect(featureCards).toHaveCount(4);

    // Verify feature card titles
    const cardTitles = page.locator('#features .feature-card__title');
    await expect(cardTitles.nth(0)).toContainText('Memcached Protocol Compatibility');
    await expect(cardTitles.nth(1)).toContainText('Persistence Architecture');
    await expect(cardTitles.nth(2)).toContainText('LSM Tree Implementation');
    await expect(cardTitles.nth(3)).toContainText('Performance Characteristics');
  });

  // Test Case 2: Verify Memcached Protocol feature content
  test('Memcached Protocol card displays correct content', async ({ page }) => {
    await page.locator('#features').scrollIntoViewIfNeeded();

    const memcachedCard = page.locator('#features .feature-card').first();

    // Verify icon exists
    const icon = memcachedCard.locator('.feature-card__icon svg');
    await expect(icon).toBeVisible();

    // Verify title
    const title = memcachedCard.locator('.feature-card__title');
    await expect(title).toHaveText('Memcached Protocol Compatibility');

    // Verify description contains key information
    const description = memcachedCard.locator('.feature-card__description');
    await expect(description).toContainText('memcached');
    await expect(description).toContainText('text protocol');
    await expect(description).toContainText('existing');
    await expect(description).toContainText('clients');
  });

  // Test Case 3: Verify Persistence Architecture feature content
  test('Persistence Architecture card displays correct content', async ({ page }) => {
    await page.locator('#features').scrollIntoViewIfNeeded();

    const persistenceCard = page.locator('#features .feature-card').nth(1);

    // Verify title
    const title = persistenceCard.locator('.feature-card__title');
    await expect(title).toHaveText('Persistence Architecture');

    // Verify description contains key information about SSTable and durability
    const description = persistenceCard.locator('.feature-card__description');
    await expect(description).toContainText('SSTable');
    await expect(description).toContainText('durability');
    await expect(description).toContainText('in-memory');
  });

  // Test Case 4: Verify LSM Tree feature content with diagram
  test('LSM Tree card displays diagram and correct content', async ({ page }) => {
    await page.locator('#features').scrollIntoViewIfNeeded();

    const lsmCard = page.locator('#features .feature-card').nth(2);

    // Verify title
    const title = lsmCard.locator('.feature-card__title');
    await expect(title).toHaveText('LSM Tree Implementation');

    // Verify icon/diagram exists (contains LSM diagram SVG)
    const icon = lsmCard.locator('.feature-card__icon svg');
    await expect(icon).toBeVisible();

    // Verify description mentions memtable and SSTables
    const description = lsmCard.locator('.feature-card__description');
    await expect(description).toContainText('memtable');
    await expect(description).toContainText('SSTable');
  });

  // Test Case 5: Desktop (1280px) - 3-column grid layout
  test('displays 3-column grid on desktop (1280px)', async ({ page }) => {
    await page.setViewportSize({ width: 1280, height: 800 });
    await page.goto('/');
    await page.locator('#features').scrollIntoViewIfNeeded();

    const grid = page.locator('#features .features__grid');
    await expect(grid).toBeVisible();

    // Check computed grid-template-columns
    const gridStyle = await grid.evaluate((el) => {
      return window.getComputedStyle(el).gridTemplateColumns;
    });

    // Should have 3 columns (3 equal values)
    const columns = gridStyle.split(' ').filter(c => c !== '');
    expect(columns.length).toBe(3);

    // Verify cards have equal heights (using align-items: stretch behavior)
    const cards = page.locator('#features .feature-card');
    const firstCardHeight = await cards.nth(0).evaluate(el => el.offsetHeight);
    const secondCardHeight = await cards.nth(1).evaluate(el => el.offsetHeight);
    const thirdCardHeight = await cards.nth(2).evaluate(el => el.offsetHeight);

    // Heights should be equal (within small tolerance for subpixel rendering)
    expect(Math.abs(firstCardHeight - secondCardHeight)).toBeLessThan(5);
    expect(Math.abs(secondCardHeight - thirdCardHeight)).toBeLessThan(5);
  });

  // Test Case 6: Tablet (768px) - 2-column grid layout
  test('displays 2-column grid on tablet (768px)', async ({ page }) => {
    await page.setViewportSize({ width: 768, height: 1024 });
    await page.goto('/');
    await page.locator('#features').scrollIntoViewIfNeeded();

    const grid = page.locator('#features .features__grid');

    // Check computed grid-template-columns
    const gridStyle = await grid.evaluate((el) => {
      return window.getComputedStyle(el).gridTemplateColumns;
    });

    // Should have 2 columns
    const columns = gridStyle.split(' ').filter(c => c !== '');
    expect(columns.length).toBe(2);
  });

  // Test Case 7: Mobile (375px) - single column layout
  test('displays single column on mobile (375px)', async ({ page }) => {
    await page.setViewportSize({ width: 375, height: 667 });
    await page.goto('/');
    await page.locator('#features').scrollIntoViewIfNeeded();

    const grid = page.locator('#features .features__grid');

    // Check computed grid-template-columns
    const gridStyle = await grid.evaluate((el) => {
      return window.getComputedStyle(el).gridTemplateColumns;
    });

    // Should have 1 column (single value)
    const columns = gridStyle.split(' ').filter(c => c !== '');
    expect(columns.length).toBe(1);
  });

  // Test Case 8: Scroll animations - cards fade in when entering viewport
  test('feature cards fade in on scroll', async ({ page }) => {
    // Scroll to top first
    await page.evaluate(() => window.scrollTo(0, 0));

    // Wait for page to settle
    await page.waitForTimeout(100);

    // Check initial state - cards should not be visible yet if they're below fold
    const cards = page.locator('#features .feature-card.animate-on-scroll');

    // Scroll to features section to trigger animation
    await page.locator('#features').scrollIntoViewIfNeeded();

    // Wait for animation to complete
    await page.waitForTimeout(600);

    // Check that cards now have is-visible class
    const firstCard = cards.first();
    await expect(firstCard).toHaveClass(/is-visible/);
  });

  // Test Case 9: Reduced motion - cards appear immediately without animation
  test('cards appear immediately with prefers-reduced-motion', async ({ page }) => {
    // Emulate prefers-reduced-motion: reduce
    await page.emulateMedia({ reducedMotion: 'reduce' });

    await page.goto('/');
    await page.locator('#features').scrollIntoViewIfNeeded();

    // Cards should immediately have is-visible class
    const cards = page.locator('#features .feature-card.animate-on-scroll');
    const firstCard = cards.first();

    // With reduced motion, should be visible immediately
    await expect(firstCard).toHaveClass(/is-visible/);

    // Check that transitions are disabled via CSS
    const transitionDuration = await firstCard.evaluate((el) => {
      return window.getComputedStyle(el).transitionDuration;
    });

    // Transition should be effectively disabled (0.01ms or 0ms or scientific notation like 1e-05s)
    // Parse the value and check it's near zero (less than 1ms)
    const durationMatch = transitionDuration.match(/([\d.e+-]+)(m?s)/);
    expect(durationMatch).not.toBeNull();
    const durationValue = parseFloat(durationMatch[1]);
    const unit = durationMatch[2];
    // Convert to ms if needed
    const durationMs = unit === 's' ? durationValue * 1000 : durationValue;
    // Should be less than 1ms (effectively instant)
    expect(durationMs).toBeLessThan(1);
  });
});
