import { test, expect } from '@playwright/test';

test.describe('Hero Section Content and Branding', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Hero section displays MirDB logo image', async ({ page }) => {
    // Query hero section for MirDB logo image
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    const logo = heroSection.locator('img.hero__logo');
    await expect(logo).toBeVisible();

    // Logo image is present with alt text 'MirDB' or similar
    const altText = await logo.getAttribute('alt');
    expect(altText).toBeTruthy();
    expect(altText?.toLowerCase()).toContain('mirdb');

    // Verify logo src contains logo.gif
    const src = await logo.getAttribute('src');
    expect(src).toContain('logo.gif');
  });

  test('TC2: Hero headline contains Persistent and Memcached keywords', async ({ page }) => {
    // Get hero headline text content
    const heroTitle = page.locator('.hero__title');
    await expect(heroTitle).toBeVisible();

    const titleText = await heroTitle.textContent();
    expect(titleText).toBeTruthy();

    // Headline contains 'Persistent' and 'Memcached' keywords (case-insensitive)
    const lowerText = titleText?.toLowerCase() || '';
    expect(lowerText).toContain('persistent');
    expect(lowerText).toContain('memcached');
  });

  test('TC3: Subheadline mentions Rust and LSM technologies', async ({ page }) => {
    // Query for subheadline mentioning Rust and LSM Trees
    const heroSubtitle = page.locator('.hero__subtitle');
    await expect(heroSubtitle).toBeVisible();

    const subtitleText = await heroSubtitle.textContent();
    expect(subtitleText).toBeTruthy();

    // Subheadline mentions 'Rust' and 'LSM' technologies
    expect(subtitleText).toContain('Rust');
    expect(subtitleText).toContain('LSM');
  });

  test('TC4: Primary CTA button with Get Started text exists', async ({ page }) => {
    // Query for primary CTA button
    const getStartedBtn = page.locator('.hero__cta .btn-primary');
    await expect(getStartedBtn).toBeVisible();

    // Button with text 'Get Started' exists
    await expect(getStartedBtn).toContainText('Get Started');

    // Button is clickable
    await expect(getStartedBtn).toBeEnabled();

    // Verify it links to getting-started section
    const href = await getStartedBtn.getAttribute('href');
    expect(href).toContain('getting-started');
  });

  test('TC5: Secondary CTA button View on GitHub exists and links to GitHub', async ({ page }) => {
    // Query for secondary CTA button
    const githubBtn = page.locator('.hero__cta .btn-secondary');
    await expect(githubBtn).toBeVisible();

    // Button with text 'View on GitHub' exists
    await expect(githubBtn).toContainText('View on GitHub');

    // Button links to GitHub
    const href = await githubBtn.getAttribute('href');
    expect(href).toBeTruthy();
    expect(href).toContain('github.com');
    expect(href).toContain('mirdb');

    // Verify it opens in new tab
    const target = await githubBtn.getAttribute('target');
    expect(target).toBe('_blank');
  });

  test('TC6: Hero section is visible above fold on mobile viewport (375px)', async ({ page }) => {
    // Set mobile viewport
    await page.setViewportSize({ width: 375, height: 667 });
    await page.goto('/');

    // Check hero section visibility above fold on mobile
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // Tagline (hero title) is visible without scrolling on mobile viewport
    const heroTitle = page.locator('.hero__title');
    await expect(heroTitle).toBeVisible();
    await expect(heroTitle).toBeInViewport();

    // Subtitle should also be visible
    const heroSubtitle = page.locator('.hero__subtitle');
    await expect(heroSubtitle).toBeVisible();
  });

  test('Hero section has proper semantic structure', async ({ page }) => {
    // Verify hero section has proper ARIA labeling
    const heroSection = page.locator('#hero');
    const ariaLabelledBy = await heroSection.getAttribute('aria-labelledby');
    expect(ariaLabelledBy).toBe('hero-title');

    // Verify h1 exists and has correct id
    const h1 = page.locator('h1#hero-title');
    await expect(h1).toBeVisible();
  });

  test('CTA buttons are styled distinctly', async ({ page }) => {
    const primaryBtn = page.locator('.hero__cta .btn-primary');
    const secondaryBtn = page.locator('.hero__cta .btn-secondary');

    // Both buttons should be visible
    await expect(primaryBtn).toBeVisible();
    await expect(secondaryBtn).toBeVisible();

    // Get computed styles to verify distinct styling
    const primaryBg = await primaryBtn.evaluate(
      (el) => getComputedStyle(el).backgroundColor
    );
    const secondaryBg = await secondaryBtn.evaluate(
      (el) => getComputedStyle(el).backgroundColor
    );

    // Primary and secondary should have different background colors
    expect(primaryBg).not.toBe(secondaryBg);
  });
});
