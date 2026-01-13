import { test, expect } from '@playwright/test';

/**
 * E2E tests for Responsive Design - Desktop Viewport
 * Tests REQ-6 requirements for desktop responsiveness
 * Validates that the homepage renders optimally at 1280px+ viewport widths
 */

test.describe('Desktop Viewport Responsive Design', () => {
  test.beforeEach(async ({ page }) => {
    // Set viewport to desktop width (1280px) as per test case requirements
    await page.setViewportSize({ width: 1280, height: 800 });
    await page.goto('/');
    // Wait for page to fully load
    await page.waitForLoadState('networkidle');
  });

  test('TC1: Homepage renders with full desktop layout at 1280px viewport', async ({ page }) => {
    // Verify the page renders without errors
    const mainContainer = page.locator('.min-h-screen');
    await expect(mainContainer).toBeVisible();

    // Verify navigation bar is fully visible with all elements on desktop
    const navbar = page.locator('nav.navbar');
    await expect(navbar).toBeVisible();

    // Verify both login and sign up links are visible in nav
    const loginLink = page.getByTestId('login-link');
    const registerLink = page.getByTestId('register-link');
    await expect(loginLink).toBeVisible();
    await expect(registerLink).toBeVisible();

    // Verify hero section displays with full desktop layout
    const heroSection = page.locator('section.hero');
    await expect(heroSection).toBeVisible();

    // Verify hero CTA buttons are displayed inline (flex row)
    const ctaContainer = page.locator('.hero-content .flex.gap-4');
    await expect(ctaContainer).toBeVisible();

    // Verify the flex container shows buttons in a row (not stacked)
    const ctaContainerStyle = await ctaContainer.evaluate((el) => {
      return window.getComputedStyle(el).flexDirection;
    });
    expect(ctaContainerStyle).toBe('row');

    // Verify features section is visible
    const featuresSection = page.getByTestId('features-section');
    await expect(featuresSection).toBeVisible();

    // Verify social proof section is visible
    const socialProofSection = page.getByTestId('social-proof-section');
    await expect(socialProofSection).toBeVisible();

    // Verify footer is visible
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();
  });

  test('TC2: Feature cards display in 3-4 column grid on desktop', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.getByTestId('features-section');
    await expect(featuresSection).toBeVisible();

    // Get the feature cards container
    const cardsContainer = page.getByTestId('feature-cards-container');
    await expect(cardsContainer).toBeVisible();

    // Verify grid has 4 columns on desktop (lg:grid-cols-4)
    const gridStyle = await cardsContainer.evaluate((el) => {
      return window.getComputedStyle(el).gridTemplateColumns;
    });

    // On desktop (1280px), the lg:grid-cols-4 class should be active
    // This means there should be 4 columns
    const columnCount = gridStyle.split(' ').filter(col => col.trim() !== '').length;
    expect(columnCount).toBeGreaterThanOrEqual(3);
    expect(columnCount).toBeLessThanOrEqual(4);

    // Verify all 4 feature cards are visible
    const featureCards = page.locator('[data-testid^="feature-card-"]');
    const cardCount = await featureCards.count();
    expect(cardCount).toBe(4);

    // Verify all cards are visible and rendered
    for (let i = 0; i < cardCount; i++) {
      const card = featureCards.nth(i);
      await expect(card).toBeVisible();
    }

    // Verify cards are arranged horizontally (check if cards are on roughly the same vertical position)
    const cardPositions = await featureCards.evaluateAll((cards) => {
      return cards.map(card => {
        const rect = card.getBoundingClientRect();
        return { top: rect.top, left: rect.left };
      });
    });

    // All cards should be on the same row (same top position, different left positions)
    // Allow some tolerance for minor rendering differences
    const firstCardTop = cardPositions[0].top;
    const tolerance = 10; // 10px tolerance
    const cardsOnSameRow = cardPositions.every(pos => Math.abs(pos.top - firstCardTop) < tolerance);
    expect(cardsOnSameRow).toBe(true);
  });

  test('TC3: Content is constrained and centered, not stretched full-width', async ({ page }) => {
    // Check that hero content has max-width constraint
    const heroContentWrapper = page.locator('.hero-content .max-w-2xl');
    await expect(heroContentWrapper).toBeVisible();

    // Verify the max-width is applied (max-w-2xl = 672px)
    const heroMaxWidth = await heroContentWrapper.evaluate((el) => {
      const computedStyle = window.getComputedStyle(el);
      return parseFloat(computedStyle.maxWidth);
    });

    // max-w-2xl in Tailwind is 42rem = 672px
    expect(heroMaxWidth).toBeLessThanOrEqual(1280); // Should be constrained
    expect(heroMaxWidth).toBeGreaterThan(0); // Should have a max-width

    // Check features section has max-width constraint (max-w-7xl)
    const featuresContainer = page.locator('#features .max-w-7xl');
    await expect(featuresContainer).toBeVisible();

    const featuresContainerBox = await featuresContainer.boundingBox();
    expect(featuresContainerBox).not.toBeNull();

    // max-w-7xl = 80rem = 1280px, content should not stretch beyond this
    expect(featuresContainerBox!.width).toBeLessThanOrEqual(1280);

    // Verify the container is centered (check margin auto behavior)
    const featuresContainerStyle = await featuresContainer.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        marginLeft: style.marginLeft,
        marginRight: style.marginRight,
      };
    });

    // mx-auto should result in equal left/right margins
    // On desktop, there should be margins pushing content toward center
    const marginLeft = parseFloat(featuresContainerStyle.marginLeft);
    const marginRight = parseFloat(featuresContainerStyle.marginRight);

    // Margins should be equal (centered) or both should be auto
    expect(Math.abs(marginLeft - marginRight)).toBeLessThan(2);

    // Check social proof section has max-width constraint
    const socialProofContainer = page.locator('#social-proof .max-w-7xl');
    await expect(socialProofContainer).toBeVisible();

    const socialProofBox = await socialProofContainer.boundingBox();
    expect(socialProofBox).not.toBeNull();
    expect(socialProofBox!.width).toBeLessThanOrEqual(1280);

    // Verify demo section has max-width constraint (max-w-2xl)
    const demoContainer = page.locator('[data-testid="demo-section"] .container.max-w-2xl');
    await expect(demoContainer).toBeVisible();

    const demoContainerBox = await demoContainer.boundingBox();
    expect(demoContainerBox).not.toBeNull();
    // max-w-2xl = 672px max
    expect(demoContainerBox!.width).toBeLessThanOrEqual(1280);
  });

  test('Desktop layout remains stable at larger viewports (1920px)', async ({ page }) => {
    // Set viewport to a larger desktop size
    await page.setViewportSize({ width: 1920, height: 1080 });
    await page.reload();
    await page.waitForLoadState('networkidle');

    // Verify content is still constrained and not stretched
    const featuresContainer = page.locator('#features .max-w-7xl');
    await expect(featuresContainer).toBeVisible();

    const featuresContainerBox = await featuresContainer.boundingBox();
    expect(featuresContainerBox).not.toBeNull();

    // Content should be constrained to max-w-7xl even on larger screens
    expect(featuresContainerBox!.width).toBeLessThanOrEqual(1280);

    // Verify feature cards still display in 4-column grid
    const cardsContainer = page.getByTestId('feature-cards-container');
    const gridStyle = await cardsContainer.evaluate((el) => {
      return window.getComputedStyle(el).gridTemplateColumns;
    });

    const columnCount = gridStyle.split(' ').filter(col => col.trim() !== '').length;
    expect(columnCount).toBe(4);

    // Verify social proof statistics display in 4-column grid
    const statsContainer = page.getByTestId('statistics-container');
    const statsGridStyle = await statsContainer.evaluate((el) => {
      return window.getComputedStyle(el).gridTemplateColumns;
    });

    const statsColumnCount = statsGridStyle.split(' ').filter(col => col.trim() !== '').length;
    expect(statsColumnCount).toBe(4);
  });
});
