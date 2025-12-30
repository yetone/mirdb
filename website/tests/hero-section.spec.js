// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Hero Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Product name MirDB is prominently displayed', async ({ page }) => {
    // Load homepage and inspect hero section
    const heroSection = page.locator('section.hero, [data-testid="hero-section"], #hero');
    await expect(heroSection).toBeVisible();

    // Check that MirDB product name is prominently displayed
    const productName = heroSection.locator('h1');
    await expect(productName).toBeVisible();
    await expect(productName).toContainText('MirDB');
  });

  test('TC2: Tagline communicates value proposition', async ({ page }) => {
    // Check tagline content
    const heroSection = page.locator('section.hero, [data-testid="hero-section"], #hero');
    await expect(heroSection).toBeVisible();

    // Verify tagline contains the value proposition about persistent key-value store with Memcached Protocol
    const tagline = heroSection.locator('.tagline, .subtitle, [data-testid="tagline"], p');
    await expect(tagline).toBeVisible();

    // Check for key terms in tagline
    const taglineText = await tagline.textContent();
    const hasKeyValueStore = taglineText?.toLowerCase().includes('key-value') || taglineText?.toLowerCase().includes('key value');
    const hasPersistent = taglineText?.toLowerCase().includes('persistent');
    const hasMemcached = taglineText?.toLowerCase().includes('memcached');

    expect(hasKeyValueStore || hasPersistent || hasMemcached).toBeTruthy();
  });

  test('TC3: Get Started button navigates to Quick Start section', async ({ page }) => {
    // Click 'Get Started' button
    const getStartedButton = page.locator('a:has-text("Get Started"), button:has-text("Get Started")');
    await expect(getStartedButton).toBeVisible();

    await getStartedButton.click();

    // Button should navigate to Quick Start section or appropriate documentation
    // Check URL contains quick-start hash or page scrolls to that section
    const url = page.url();
    const quickStartSection = page.locator('#quick-start, #quickstart, [data-testid="quick-start"]');

    // Either the URL has a hash pointing to quick-start or the section is visible in viewport
    const hasQuickStartHash = url.includes('quick-start') || url.includes('quickstart');
    const isQuickStartVisible = await quickStartSection.isVisible().catch(() => false);

    expect(hasQuickStartHash || isQuickStartVisible).toBeTruthy();
  });

  test('TC4: View on GitHub button opens repository in new tab', async ({ page, context }) => {
    // Get the hero section to scope our search
    const heroSection = page.locator('section.hero, [data-testid="hero-section"], #hero');
    await expect(heroSection).toBeVisible();

    // Click 'View on GitHub' button in the hero section specifically
    const githubButton = heroSection.locator('a:has-text("View on GitHub")');
    await expect(githubButton).toBeVisible();

    // Check that the button has target="_blank" to open in new tab
    const target = await githubButton.getAttribute('target');
    expect(target).toBe('_blank');

    // Check that the href points to GitHub repository
    const href = await githubButton.getAttribute('href');
    expect(href).toContain('github.com');

    // Additionally verify rel="noopener noreferrer" for security (best practice)
    const rel = await githubButton.getAttribute('rel');
    expect(rel).toContain('noopener');
  });
});
