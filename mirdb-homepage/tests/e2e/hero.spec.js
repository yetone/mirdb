/**
 * Hero Section E2E Tests
 * Owner: Scenario 1 - Hero Section Display
 *
 * Tests:
 * - Hero section visibility and content
 * - MirDB heading and tagline presence
 * - Get Started CTA functionality (scroll)
 * - View Source CTA functionality (external link)
 * - Semantic HTML structure
 */

const { test, expect } = require('@playwright/test');

test.describe('Hero Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');
  });

  test('TC1: Hero section displays MirDB heading and tagline', async ({ page }) => {
    // Verify hero section exists
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // Verify MirDB heading is present with h1
    const heading = page.locator('#hero h1');
    await expect(heading).toBeVisible();
    await expect(heading).toHaveText('MirDB');

    // Verify tagline is present
    const tagline = page.locator('.hero__tagline');
    await expect(tagline).toBeVisible();
    await expect(tagline).toContainText('A Persistent Key-Value Store with Memcached protocol');
  });

  test('TC2: Primary CTA Get Started button is visible and clickable', async ({ page }) => {
    // Verify Get Started button exists
    const getStartedButton = page.locator('a.hero__cta--primary');
    await expect(getStartedButton).toBeVisible();
    await expect(getStartedButton).toHaveText('Get Started');

    // Verify it's clickable (has proper href)
    await expect(getStartedButton).toHaveAttribute('href', '#quick-start');

    // Verify it's enabled and focusable
    await expect(getStartedButton).toBeEnabled();
  });

  test('TC3: Secondary CTA View Source button links to GitHub', async ({ page }) => {
    // Verify View Source button exists
    const viewSourceButton = page.locator('a.hero__cta--secondary');
    await expect(viewSourceButton).toBeVisible();
    await expect(viewSourceButton).toHaveText('View Source');

    // Verify it links to GitHub
    const href = await viewSourceButton.getAttribute('href');
    expect(href).toContain('github.com');

    // Verify it opens in new tab
    await expect(viewSourceButton).toHaveAttribute('target', '_blank');

    // Verify security attributes for external link
    await expect(viewSourceButton).toHaveAttribute('rel', /noopener/);
  });

  test('TC4: Get Started button scrolls to Quick Start section', async ({ page }) => {
    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY);

    // Click Get Started button
    const getStartedButton = page.locator('a.hero__cta--primary');
    await getStartedButton.click();

    // Wait for smooth scroll to complete
    await page.waitForTimeout(500);

    // Verify scroll position changed (scrolled down)
    const newScrollY = await page.evaluate(() => window.scrollY);

    // The page should have scrolled (or quick-start section should be in view)
    // Since quick-start is below hero, scroll position should increase or section should be visible
    const quickStartSection = page.locator('#quick-start');

    // Check that either we've scrolled or the section is now in the viewport
    const isInViewport = await page.evaluate(() => {
      const el = document.querySelector('#quick-start');
      if (!el) return false;
      const rect = el.getBoundingClientRect();
      return rect.top >= 0 && rect.top < window.innerHeight;
    });

    // Either scrolled or section is visible
    expect(newScrollY > initialScrollY || isInViewport).toBeTruthy();
  });

  test('TC5: Hero section uses semantic HTML with proper heading hierarchy', async ({ page }) => {
    // Verify h1 element is used for main heading
    const h1Element = page.locator('#hero h1');
    await expect(h1Element).toBeVisible();
    await expect(h1Element).toHaveText('MirDB');

    // Verify hero section uses section element with id
    const heroSection = page.locator('section#hero');
    await expect(heroSection).toBeVisible();

    // Verify h1 is the first heading in the document (no h1 before it in main)
    const h1Count = await page.locator('main h1').count();
    expect(h1Count).toBeGreaterThanOrEqual(1);

    // Verify there's only one h1 on the page (best practice)
    const allH1s = await page.locator('h1').count();
    expect(allH1s).toBe(1);
  });

  test('Hero section is visible above the fold on desktop', async ({ page }) => {
    // Set desktop viewport
    await page.setViewportSize({ width: 1280, height: 720 });

    // Verify hero content is visible without scrolling
    const heroTitle = page.locator('.hero__title');
    const heroTagline = page.locator('.hero__tagline');
    const heroCTAs = page.locator('.hero__cta-group');

    await expect(heroTitle).toBeVisible();
    await expect(heroTagline).toBeVisible();
    await expect(heroCTAs).toBeVisible();

    // Verify elements are in viewport
    const isHeroInViewport = await page.evaluate(() => {
      const title = document.querySelector('.hero__title');
      const tagline = document.querySelector('.hero__tagline');
      const ctas = document.querySelector('.hero__cta-group');

      const isInView = (el) => {
        if (!el) return false;
        const rect = el.getBoundingClientRect();
        return rect.top >= 0 && rect.bottom <= window.innerHeight;
      };

      return isInView(title) && isInView(tagline) && isInView(ctas);
    });

    expect(isHeroInViewport).toBeTruthy();
  });

  test('Hero section CTA buttons have proper focus states', async ({ page }) => {
    // Tab to primary CTA
    const primaryCTA = page.locator('a.hero__cta--primary');
    await primaryCTA.focus();

    // Verify focus is visible (element should be focusable)
    await expect(primaryCTA).toBeFocused();

    // Tab to secondary CTA
    const secondaryCTA = page.locator('a.hero__cta--secondary');
    await secondaryCTA.focus();
    await expect(secondaryCTA).toBeFocused();
  });

  test('Hero section description provides value proposition', async ({ page }) => {
    const description = page.locator('.hero__description');
    await expect(description).toBeVisible();

    // Should mention key features
    const text = await description.textContent();
    expect(text.toLowerCase()).toMatch(/key-value|storage|memcached|fast|durable|lsm/i);
  });
});
