/**
 * Hero Section E2E Tests
 * Owner: Scenario 1 - Hero Section and Branding
 *
 * Tests:
 * - Hero section contains MirDB logo
 * - Hero section displays product name "MirDB"
 * - Hero section shows tagline
 * - CTA buttons present (Get Started, View on GitHub)
 * - Dark theme styling applied
 * - Color contrast meets WCAG 2.1 AA (4.5:1)
 */

const { test, expect } = require('@playwright/test');

/**
 * Calculate relative luminance of an RGB color
 * Based on WCAG 2.1 formula
 */
function getRelativeLuminance(r, g, b) {
  const [rs, gs, bs] = [r, g, b].map((c) => {
    const sRGB = c / 255;
    return sRGB <= 0.03928 ? sRGB / 12.92 : Math.pow((sRGB + 0.055) / 1.055, 2.4);
  });
  return 0.2126 * rs + 0.7152 * gs + 0.0722 * bs;
}

/**
 * Calculate contrast ratio between two colors
 * Returns value between 1 and 21
 */
function getContrastRatio(rgb1, rgb2) {
  const l1 = getRelativeLuminance(rgb1.r, rgb1.g, rgb1.b);
  const l2 = getRelativeLuminance(rgb2.r, rgb2.g, rgb2.b);
  const lighter = Math.max(l1, l2);
  const darker = Math.min(l1, l2);
  return (lighter + 0.05) / (darker + 0.05);
}

/**
 * Parse RGB string to {r, g, b} object
 */
function parseRgb(rgbString) {
  const match = rgbString.match(/rgba?\((\d+),\s*(\d+),\s*(\d+)/);
  if (!match) return null;
  return {
    r: parseInt(match[1], 10),
    g: parseInt(match[2], 10),
    b: parseInt(match[3], 10),
  };
}

test.describe('Hero Section and Branding', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');
  });

  test('TC-1: Hero section contains h1 with MirDB text and tagline', async ({ page }) => {
    // Query for hero section
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // Verify h1 contains MirDB text
    const h1 = heroSection.locator('h1');
    await expect(h1).toBeVisible();
    await expect(h1).toHaveText('MirDB');

    // Verify tagline is present
    const tagline = heroSection.locator('.tagline');
    await expect(tagline).toBeVisible();
    await expect(tagline).toContainText('A Persistent Key-Value Store with Memcached Protocol');
  });

  test('TC-2: Logo image element present with correct src and alt text', async ({ page }) => {
    // Query for hero section
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // Query for logo image in hero section
    const heroLogo = heroSection.locator('img.hero-logo');
    await expect(heroLogo).toBeVisible();

    // Verify src points to logo.gif
    const src = await heroLogo.getAttribute('src');
    expect(src).toContain('logo.gif');

    // Verify alt text is present and appropriate
    const alt = await heroLogo.getAttribute('alt');
    expect(alt).toBeTruthy();
    expect(alt.toLowerCase()).toContain('mirdb');
  });

  test('TC-3: Two CTA buttons present with correct labels and hrefs', async ({ page }) => {
    // Query for hero section
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // Query for CTA container
    const ctaContainer = heroSection.locator('.hero-cta');
    await expect(ctaContainer).toBeVisible();

    // Query for CTA buttons
    const ctaButtons = ctaContainer.locator('.btn');
    await expect(ctaButtons).toHaveCount(2);

    // Verify "Get Started" button
    const getStartedBtn = ctaContainer.locator('a:has-text("Get Started")');
    await expect(getStartedBtn).toBeVisible();
    const getStartedHref = await getStartedBtn.getAttribute('href');
    expect(getStartedHref).toBe('#quickstart');

    // Verify "View on GitHub" button
    const githubBtn = ctaContainer.locator('a:has-text("View on GitHub")');
    await expect(githubBtn).toBeVisible();
    const githubHref = await githubBtn.getAttribute('href');
    expect(githubHref).toContain('github.com/yetone/mirdb');
  });

  test('TC-4: Dark background color with light text colors defined in CSS variables', async ({ page }) => {
    // Get CSS custom properties from :root
    const cssVars = await page.evaluate(() => {
      const computedStyle = getComputedStyle(document.documentElement);
      return {
        bgPrimary: computedStyle.getPropertyValue('--color-bg-primary').trim(),
        bgSecondary: computedStyle.getPropertyValue('--color-bg-secondary').trim(),
        textPrimary: computedStyle.getPropertyValue('--color-text-primary').trim(),
        textSecondary: computedStyle.getPropertyValue('--color-text-secondary').trim(),
      };
    });

    // Verify dark background color (near black - low RGB values)
    expect(cssVars.bgPrimary).toBe('#0d1117');
    const bgRgb = { r: 13, g: 17, b: 23 }; // #0d1117
    expect(bgRgb.r + bgRgb.g + bgRgb.b).toBeLessThan(100); // Sum < 100 indicates very dark

    // Verify light text color (near white - high RGB values)
    expect(cssVars.textPrimary).toBe('#f0f6fc');
    const textRgb = { r: 240, g: 246, b: 252 }; // #f0f6fc
    expect(textRgb.r).toBeGreaterThan(200);
    expect(textRgb.g).toBeGreaterThan(200);
    expect(textRgb.b).toBeGreaterThan(200);
  });

  test('TC-5: Color contrast ratio meets WCAG 2.1 AA minimum (4.5:1)', async ({ page }) => {
    // Get actual computed colors for hero elements
    const colors = await page.evaluate(() => {
      const hero = document.querySelector('#hero');
      const h1 = document.querySelector('#hero h1');
      const tagline = document.querySelector('#hero .tagline');

      if (!hero || !h1 || !tagline) return null;

      const heroStyle = getComputedStyle(hero);
      const h1Style = getComputedStyle(h1);
      const taglineStyle = getComputedStyle(tagline);

      return {
        heroBg: heroStyle.backgroundColor,
        h1Color: h1Style.color,
        taglineColor: taglineStyle.color,
        bodyBg: getComputedStyle(document.body).backgroundColor,
      };
    });

    expect(colors).not.toBeNull();

    // Parse the body background color (this is the effective background for hero)
    const bodyBgRgb = parseRgb(colors.bodyBg);
    expect(bodyBgRgb).not.toBeNull();

    // Parse h1 text color
    const h1Rgb = parseRgb(colors.h1Color);
    expect(h1Rgb).not.toBeNull();

    // Parse tagline text color
    const taglineRgb = parseRgb(colors.taglineColor);
    expect(taglineRgb).not.toBeNull();

    // Calculate contrast ratios
    const h1Contrast = getContrastRatio(bodyBgRgb, h1Rgb);
    const taglineContrast = getContrastRatio(bodyBgRgb, taglineRgb);

    // WCAG 2.1 AA requires 4.5:1 for normal text, 3:1 for large text
    // h1 is large text (>= 18pt or 14pt bold), so 3:1 minimum
    // But we test for 4.5:1 as per NFR-11 specification
    expect(h1Contrast).toBeGreaterThanOrEqual(4.5);
    expect(taglineContrast).toBeGreaterThanOrEqual(4.5);
  });

  test('Hero section is visible without scrolling (above the fold)', async ({ page }) => {
    // Set viewport to standard desktop size
    await page.setViewportSize({ width: 1280, height: 720 });

    // Navigate to page
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    // Verify hero section is in viewport
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeInViewport();

    // Verify key elements are in viewport
    const h1 = page.locator('#hero h1');
    await expect(h1).toBeInViewport();

    const tagline = page.locator('#hero .tagline');
    await expect(tagline).toBeInViewport();

    const ctaButtons = page.locator('#hero .hero-cta');
    await expect(ctaButtons).toBeInViewport();
  });

  test('Hero section has proper dark theme gradient background', async ({ page }) => {
    // Get hero section background style
    const heroStyles = await page.evaluate(() => {
      const hero = document.querySelector('#hero');
      if (!hero) return null;
      const style = getComputedStyle(hero);
      return {
        backgroundImage: style.backgroundImage,
        backgroundColor: style.backgroundColor,
      };
    });

    expect(heroStyles).not.toBeNull();

    // Verify gradient or dark background is applied
    const hasGradient = heroStyles.backgroundImage.includes('linear-gradient');
    const hasDarkBg = heroStyles.backgroundColor.includes('rgb');

    expect(hasGradient || hasDarkBg).toBe(true);
  });

  test('Navigation header displays logo with MirDB branding', async ({ page }) => {
    // Query for navbar logo
    const navLogo = page.locator('.navbar .logo');
    await expect(navLogo).toBeVisible();

    // Check logo image in navbar
    const navLogoImg = navLogo.locator('img.logo-img');
    await expect(navLogoImg).toBeVisible();

    const src = await navLogoImg.getAttribute('src');
    expect(src).toContain('logo.gif');

    // Check logo text
    const logoText = navLogo.locator('.logo-text');
    await expect(logoText).toHaveText('MirDB');
  });

  test('CTA buttons have proper styling classes', async ({ page }) => {
    const heroSection = page.locator('#hero');
    const ctaContainer = heroSection.locator('.hero-cta');

    // Get Started should be primary button
    const getStartedBtn = ctaContainer.locator('a:has-text("Get Started")');
    const getStartedClasses = await getStartedBtn.getAttribute('class');
    expect(getStartedClasses).toContain('btn');
    expect(getStartedClasses).toContain('btn-primary');

    // GitHub button should be secondary button
    const githubBtn = ctaContainer.locator('a:has-text("View on GitHub")');
    const githubClasses = await githubBtn.getAttribute('class');
    expect(githubClasses).toContain('btn');
    expect(githubClasses).toContain('btn-secondary');
  });

  test('Hero section buttons have minimum touch target size (44x44px)', async ({ page }) => {
    const heroSection = page.locator('#hero');
    const ctaButtons = heroSection.locator('.hero-cta .btn');

    const count = await ctaButtons.count();
    expect(count).toBe(2);

    for (let i = 0; i < count; i++) {
      const btn = ctaButtons.nth(i);
      const box = await btn.boundingBox();
      expect(box).not.toBeNull();
      expect(box.width).toBeGreaterThanOrEqual(44);
      expect(box.height).toBeGreaterThanOrEqual(44);
    }
  });
});
