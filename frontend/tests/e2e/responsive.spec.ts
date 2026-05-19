import { test, expect } from '@playwright/test';

/**
 * Helper to inject a feature cards section into the page for responsive layout testing.
 * Uses the responsive.css rules defined in src/styles/responsive.css.
 */
async function injectFeatureCards(page: any) {
  await page.evaluate(() => {
    const container = document.createElement('div');
    container.className = 'feature-cards-container';
    container.setAttribute('data-testid', 'feature-cards-container');
    container.innerHTML = `
      <div class="feature-card glass-card" data-testid="feature-card">
        <h3>Real-time Analytics</h3>
        <p>Track clicks, geographic data, and referrer insights in real time.</p>
      </div>
      <div class="feature-card glass-card" data-testid="feature-card">
        <h3>Secure &amp; Private</h3>
        <p>Enterprise-grade encryption and privacy controls for every link.</p>
      </div>
      <div class="feature-card glass-card" data-testid="feature-card">
        <h3>Lightning Fast Redirects</h3>
        <p>Sub-millisecond redirects powered by global edge infrastructure.</p>
      </div>
    `;
    document.body.appendChild(container);
  });
}

/**
 * Helper to get the bounding box of an element.
 */
async function getBoundingBox(page: any, selector: string) {
  const element = page.locator(selector).first();
  await element.waitFor();
  return element.boundingBox();
}

test.describe('Responsive Layout Across Viewports', () => {

  test('Feature cards horizontal layout at 1440px desktop', async ({ page }) => {
    await page.setViewportSize({ width: 1440, height: 900 });
    await page.goto('/');
    await injectFeatureCards(page);

    const container = page.locator('[data-testid="feature-cards-container"]');
    await container.waitFor();

    // Verify feature cards are in a horizontal (row) layout
    const box = await container.boundingBox();
    expect(box).not.toBeNull();
    expect(box!.width).toBeGreaterThan(0);

    const cards = page.locator('[data-testid="feature-card"]');
    await expect(cards).toHaveCount(3);

    // All cards should be visible horizontally (not stacked vertically)
    const firstCard = await cards.nth(0).boundingBox();
    const secondCard = await cards.nth(1).boundingBox();
    const thirdCard = await cards.nth(2).boundingBox();

    expect(firstCard).not.toBeNull();
    expect(secondCard).not.toBeNull();
    expect(thirdCard).not.toBeNull();

    // Cards should be side by side (second card starts to the right of first)
    expect(secondCard!.x).toBeGreaterThan(firstCard!.x);
    expect(thirdCard!.x).toBeGreaterThan(secondCard!.x);

    // Verify the container uses flex-direction row via computed style
    const flexDirection = await container.evaluate((el: HTMLElement) =>
      window.getComputedStyle(el).flexDirection
    );
    expect(flexDirection).toBe('row');
  });

  test('Feature cards vertical stacking and hamburger menu at 375px mobile', async ({ page }) => {
    await page.setViewportSize({ width: 375, height: 667 });
    await page.goto('/');
    await injectFeatureCards(page);

    // ── Feature cards should stack vertically ──
    const container = page.locator('[data-testid="feature-cards-container"]');
    await container.waitFor();

    const cards = page.locator('[data-testid="feature-card"]');
    await expect(cards).toHaveCount(3);

    const firstCard = await cards.nth(0).boundingBox();
    const secondCard = await cards.nth(1).boundingBox();
    const thirdCard = await cards.nth(2).boundingBox();

    expect(firstCard).not.toBeNull();
    expect(secondCard).not.toBeNull();
    expect(thirdCard).not.toBeNull();

    // Cards should stack vertically (second card is below first)
    expect(secondCard!.y).toBeGreaterThan(firstCard!.y);
    expect(thirdCard!.y).toBeGreaterThan(secondCard!.y);

    // Verify flex-direction column via computed style
    const flexDirection = await container.evaluate((el: HTMLElement) =>
      window.getComputedStyle(el).flexDirection
    );
    expect(flexDirection).toBe('column');

    // ── Navbar should collapse to hamburger menu ──
    const navbarToggle = page.locator('[data-testid="navbar-toggle"]');
    await expect(navbarToggle).toBeVisible();

    // Menu should be hidden by default
    const navbarMenu = page.locator('[data-testid="navbar-menu"]');
    await expect(navbarMenu).not.toBeVisible();

    // Click hamburger to open menu
    await navbarToggle.click();
    await expect(navbarMenu).toBeVisible();

    // Menu should display vertically
    const menuItems = navbarMenu.locator('li');
    await expect(menuItems).toHaveCount(3);
  });

  test('Tap targets minimum 44x44px at 375px viewport', async ({ page }) => {
    await page.setViewportSize({ width: 375, height: 667 });
    await page.goto('/');

    // Collect all interactive elements
    const interactiveElements = await page.locator('button, a, [role="button"]').all();
    expect(interactiveElements.length).toBeGreaterThan(0);

    for (const element of interactiveElements) {
      const box = await element.boundingBox();
      if (box) {
        expect(
          box.width,
          `Element ${await element.evaluate((el: HTMLElement) => el.outerHTML.substring(0, 80))} width is ${box.width}px, expected >= 44px`
        ).toBeGreaterThanOrEqual(44);
        expect(
          box.height,
          `Element height is ${box.height}px, expected >= 44px`
        ).toBeGreaterThanOrEqual(44);
      }
    }

    // Also verify via computed style that tap-target class enforces minimum
    const toggleBtn = page.locator('[data-testid="navbar-toggle"]');
    const computedWidth = await toggleBtn.evaluate((el: HTMLElement) =>
      parseFloat(window.getComputedStyle(el).minWidth)
    );
    const computedHeight = await toggleBtn.evaluate((el: HTMLElement) =>
      parseFloat(window.getComputedStyle(el).minHeight)
    );
    expect(computedWidth).toBeGreaterThanOrEqual(44);
    expect(computedHeight).toBeGreaterThanOrEqual(44);
  });

  test('No horizontal overflow at 768px tablet', async ({ page }) => {
    await page.setViewportSize({ width: 768, height: 1024 });
    await page.goto('/');
    await injectFeatureCards(page);

    // Check that the document body has no horizontal scroll
    const scrollWidth = await page.evaluate(() => document.documentElement.scrollWidth);
    const clientWidth = await page.evaluate(() => document.documentElement.clientWidth);
    expect(scrollWidth).toBeLessThanOrEqual(clientWidth + 1); // allow 1px rounding

    // Check body overflow-x is hidden
    const bodyOverflowX = await page.evaluate(() =>
      window.getComputedStyle(document.body).overflowX
    );
    expect(bodyOverflowX).toBe('hidden');

    // Verify no element exceeds viewport width
    const overflowingElements = await page.evaluate(() => {
      const allElements = document.querySelectorAll('*');
      const overflows: string[] = [];
      const viewportWidth = window.innerWidth;
      allElements.forEach((el) => {
        const rect = el.getBoundingClientRect();
        if (rect.right > viewportWidth + 1) {
          overflows.push(`${el.tagName}${el.id ? '#' + el.id : ''}${el.className ? '.' + el.className.split(' ').join('.') : ''}`);
        }
      });
      return overflows;
    });

    expect(overflowingElements, `Elements overflowing viewport: ${overflowingElements.join(', ')}`).toHaveLength(0);
  });

  test('Feature cards horizontal with equal spacing at 1024px', async ({ page }) => {
    await page.setViewportSize({ width: 1024, height: 768 });
    await page.goto('/');
    await injectFeatureCards(page);

    const container = page.locator('[data-testid="feature-cards-container"]');
    await container.waitFor();

    const cards = page.locator('[data-testid="feature-card"]');
    await expect(cards).toHaveCount(3);

    // Cards should be in horizontal layout
    const flexDirection = await container.evaluate((el: HTMLElement) =>
      window.getComputedStyle(el).flexDirection
    );
    expect(flexDirection).toBe('row');

    // Verify cards have equal spacing (gap)
    const firstCard = await cards.nth(0).boundingBox();
    const secondCard = await cards.nth(1).boundingBox();
    const thirdCard = await cards.nth(2).boundingBox();

    expect(firstCard).not.toBeNull();
    expect(secondCard).not.toBeNull();
    expect(thirdCard).not.toBeNull();

    // Cards should be side by side
    expect(secondCard!.x).toBeGreaterThan(firstCard!.x);
    expect(thirdCard!.x).toBeGreaterThan(secondCard!.x);

    // Calculate gaps between cards (should be roughly equal)
    const gap1 = secondCard!.x - (firstCard!.x + firstCard!.width);
    const gap2 = thirdCard!.x - (secondCard!.x + secondCard!.width);

    // Gaps should be positive (spacing exists)
    expect(gap1).toBeGreaterThan(0);
    expect(gap2).toBeGreaterThan(0);

    // Gaps should be roughly equal (within 2px tolerance for rounding)
    expect(Math.abs(gap1 - gap2)).toBeLessThanOrEqual(2);

    // Verify justify-content is center (equal spacing from edges)
    const justifyContent = await container.evaluate((el: HTMLElement) =>
      window.getComputedStyle(el).justifyContent
    );
    expect(justifyContent).toBe('center');
  });
});
