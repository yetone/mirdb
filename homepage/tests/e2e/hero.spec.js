/**
 * Hero Section Tests
 * Owner: Scenario 2 - Hero Section
 *
 * Tests:
 * - Value proposition visibility
 * - CTA button presence and functionality
 * - Status badge loading
 */

const { test, expect } = require('@playwright/test');
const { SELECTORS } = require('./test-utils');

test.describe('Hero Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Hero section exists with headline describing MirDB value proposition', async ({ page }) => {
    // Check hero section exists
    const heroSection = page.locator(SELECTORS.hero);
    await expect(heroSection).toBeVisible();

    // Check headline contains MirDB value proposition
    const headline = heroSection.locator('.hero__title');
    await expect(headline).toBeVisible();
    await expect(headline).toContainText('MirDB');
    await expect(headline).toContainText('Persistent');
    await expect(headline).toContainText('Key-Value Store');
    await expect(headline).toContainText('Memcached');
  });

  test('TC2: Primary CTA button exists with text Get Started or View on GitHub', async ({ page }) => {
    const heroSection = page.locator(SELECTORS.hero);

    // Check for CTA buttons
    const ctaContainer = heroSection.locator('.hero__cta-container');
    await expect(ctaContainer).toBeVisible();

    // Check for primary CTA button
    const primaryCta = heroSection.locator('.hero__cta--primary');
    await expect(primaryCta).toBeVisible();

    // Verify CTA text is either 'Get Started' or 'View on GitHub'
    const ctaText = await primaryCta.textContent();
    const validCtaTexts = ['Get Started', 'View on GitHub'];
    expect(validCtaTexts.some(text => ctaText.includes(text))).toBeTruthy();
  });

  test('TC3: CTA button navigates to appropriate section or external link', async ({ page }) => {
    const heroSection = page.locator(SELECTORS.hero);

    // Test the primary CTA button (View on GitHub)
    const primaryCta = heroSection.locator('.hero__cta--primary');
    const href = await primaryCta.getAttribute('href');

    // If external link, verify it's a GitHub link
    if (href.startsWith('http')) {
      expect(href).toContain('github.com/yetone/mirdb');
    }

    // Test the secondary CTA button (Get Started - anchor link)
    const secondaryCta = heroSection.locator('.hero__cta--secondary');
    const secondaryHref = await secondaryCta.getAttribute('href');

    // If anchor link, verify it points to a valid section
    if (secondaryHref.startsWith('#')) {
      expect(secondaryHref).toBe('#getting-started');
    }
  });

  test('TC4: CircleCI badge image loads and links to CI pipeline', async ({ page }) => {
    const heroSection = page.locator(SELECTORS.hero);

    // Check badge link exists
    const badgeLink = heroSection.locator('.hero__badge-link');
    await expect(badgeLink).toBeVisible();

    // Verify badge link points to CircleCI
    const href = await badgeLink.getAttribute('href');
    expect(href).toBe('https://circleci.com/gh/yetone/mirdb');

    // Check badge image exists
    const badgeImage = heroSection.locator('.hero__badge');
    await expect(badgeImage).toBeVisible();

    // Verify badge image is from CircleCI
    const imgSrc = await badgeImage.getAttribute('src');
    expect(imgSrc).toContain('circleci.com/gh/yetone/mirdb');
  });

  test('Hero subtitle describes memcached compatibility and persistence', async ({ page }) => {
    const heroSection = page.locator(SELECTORS.hero);

    // Check subtitle contains key messaging
    const subtitle = heroSection.locator('.hero__subtitle');
    await expect(subtitle).toBeVisible();

    const subtitleText = await subtitle.textContent();
    expect(subtitleText.toLowerCase()).toContain('memcached');
    expect(subtitleText.toLowerCase()).toContain('persistent');
  });
});
