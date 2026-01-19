// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Cross-Browser Compatibility Tests
 * NFR-4: Page must render correctly on modern browsers (Chrome, Firefox, Safari, Edge)
 *
 * These tests verify that the MirDB landing page renders correctly across all major browsers
 * without visual issues or console errors.
 */

test.describe('Cross-Browser Compatibility - Page Rendering', () => {

  // Capture console errors during page load
  let consoleErrors = [];

  test.beforeEach(async ({ page }) => {
    consoleErrors = [];

    // Listen for console errors
    page.on('console', msg => {
      if (msg.type() === 'error') {
        consoleErrors.push(msg.text());
      }
    });

    // Listen for page errors (uncaught exceptions)
    page.on('pageerror', error => {
      consoleErrors.push(error.message);
    });
  });

  test('page loads successfully without errors', async ({ page, browserName }) => {
    // Navigate to the landing page
    const response = await page.goto('/');

    // Verify successful HTTP response
    expect(response).not.toBeNull();
    expect(response.status()).toBe(200);

    // Wait for page to be fully loaded
    await page.waitForLoadState('networkidle');

    // Verify no JavaScript console errors occurred during page load
    // Filter out known acceptable errors (e.g., external resource loading issues)
    const criticalErrors = consoleErrors.filter(error =>
      !error.includes('favicon.ico') &&
      !error.includes('Failed to load resource') // External badge images may fail
    );

    expect(criticalErrors).toEqual([]);
  });

  test('hero section renders correctly', async ({ page, browserName }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Check hero section is visible
    const hero = page.locator('[data-testid="hero"]');
    await expect(hero).toBeVisible();

    // Check main heading (MirDB)
    const heading = page.locator('.hero h1');
    await expect(heading).toBeVisible();
    await expect(heading).toContainText('MirDB');

    // Check tagline
    const tagline = page.locator('.hero .tagline');
    await expect(tagline).toBeVisible();
    await expect(tagline).toContainText('Persistent Key-Value Store');

    // Check CTA buttons
    const primaryCta = page.locator('.hero .btn-primary');
    await expect(primaryCta).toBeVisible();

    const secondaryCta = page.locator('.hero .btn-secondary');
    await expect(secondaryCta).toBeVisible();

    // Check logo image
    const logo = page.locator('.hero .logo');
    await expect(logo).toBeVisible();
  });

  test('features section renders correctly', async ({ page, browserName }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Check features section
    const features = page.locator('[data-testid="features"]');
    await expect(features).toBeVisible();

    // Check section heading
    const heading = page.locator('.features h2');
    await expect(heading).toBeVisible();
    await expect(heading).toContainText('Key Features');

    // Check feature cards are rendered (should have 4)
    const featureCards = page.locator('[data-testid="feature-card"]');
    await expect(featureCards).toHaveCount(4);

    // Verify each card is visible
    for (let i = 0; i < 4; i++) {
      await expect(featureCards.nth(i)).toBeVisible();
    }

    // Check feature icons are rendered
    const featureIcons = page.locator('[data-testid="feature-icon"]');
    await expect(featureIcons).toHaveCount(4);
  });

  test('usage section renders correctly', async ({ page, browserName }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Check usage section
    const usage = page.locator('[data-testid="usage"]');
    await expect(usage).toBeVisible();

    // Check section heading
    const heading = page.locator('.usage h2');
    await expect(heading).toBeVisible();
    await expect(heading).toContainText('Quick Start');

    // Check code block is rendered
    const codeBlock = page.locator('[data-testid="code-block"]');
    await expect(codeBlock).toBeVisible();

    // Check usage demo image
    const usageDemo = page.locator('[data-testid="usage-demo"]');
    await expect(usageDemo).toBeVisible();
  });

  test('roadmap section renders correctly', async ({ page, browserName }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Check roadmap section
    const roadmap = page.locator('[data-testid="roadmap"]');
    await expect(roadmap).toBeVisible();

    // Check section heading
    const heading = page.locator('.roadmap h2');
    await expect(heading).toBeVisible();
    await expect(heading).toContainText('Roadmap');

    // Check roadmap items are rendered (should have 5)
    const roadmapItems = page.locator('[data-testid="roadmap-item"]');
    await expect(roadmapItems).toHaveCount(5);

    // Check roadmap icons
    const roadmapIcons = page.locator('[data-testid="roadmap-icon"]');
    await expect(roadmapIcons).toHaveCount(5);
  });

  test('footer section renders correctly', async ({ page, browserName }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Check footer
    const footer = page.locator('[data-testid="footer"]');
    await expect(footer).toBeVisible();

    // Check footer brand
    const footerLogo = page.locator('.footer-logo');
    await expect(footerLogo).toBeVisible();
    await expect(footerLogo).toContainText('MirDB');

    // Check GitHub link
    const githubLink = page.locator('.github-link');
    await expect(githubLink).toBeVisible();

    // Check license text
    const license = page.locator('.license');
    await expect(license).toBeVisible();
    await expect(license).toContainText('MIT License');
  });

  test('CSS styles are properly applied', async ({ page, browserName }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Check body background color (dark theme)
    const body = page.locator('body');
    const bodyBgColor = await body.evaluate(el =>
      window.getComputedStyle(el).backgroundColor
    );
    // Should be the dark theme color (#0d1117 = rgb(13, 17, 23))
    expect(bodyBgColor).toBe('rgb(13, 17, 23)');

    // Check hero section uses flexbox centering
    const hero = page.locator('.hero');
    const heroDisplay = await hero.evaluate(el =>
      window.getComputedStyle(el).display
    );
    expect(heroDisplay).toBe('flex');

    // Check features grid uses CSS grid
    const featuresGrid = page.locator('.features-grid');
    const gridDisplay = await featuresGrid.evaluate(el =>
      window.getComputedStyle(el).display
    );
    expect(gridDisplay).toBe('grid');
  });

  test('all page sections are present and in correct order', async ({ page, browserName }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Get all main sections in order
    const hero = page.locator('[data-testid="hero"]');
    const features = page.locator('[data-testid="features"]');
    const usage = page.locator('[data-testid="usage"]');
    const roadmap = page.locator('[data-testid="roadmap"]');
    const footer = page.locator('[data-testid="footer"]');

    // Verify all sections exist
    await expect(hero).toBeAttached();
    await expect(features).toBeAttached();
    await expect(usage).toBeAttached();
    await expect(roadmap).toBeAttached();
    await expect(footer).toBeAttached();

    // Verify sections are in correct vertical order by checking their positions
    const heroBox = await hero.boundingBox();
    const featuresBox = await features.boundingBox();
    const usageBox = await usage.boundingBox();
    const roadmapBox = await roadmap.boundingBox();
    const footerBox = await footer.boundingBox();

    expect(heroBox.y).toBeLessThan(featuresBox.y);
    expect(featuresBox.y).toBeLessThan(usageBox.y);
    expect(usageBox.y).toBeLessThan(roadmapBox.y);
    expect(roadmapBox.y).toBeLessThan(footerBox.y);
  });

  test('navigation links are functional', async ({ page, browserName }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Check "Learn More" button scrolls to features section
    const learnMoreBtn = page.locator('.btn-secondary');
    const featuresSection = page.locator('#features');

    await expect(learnMoreBtn).toHaveAttribute('href', '#features');

    // Check GitHub link has correct href
    const githubBtn = page.locator('.hero .btn-primary');
    await expect(githubBtn).toHaveAttribute('href', 'https://github.com/yetone/mirdb');
    await expect(githubBtn).toHaveAttribute('target', '_blank');
  });

  test('images load correctly', async ({ page, browserName }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Check logo image loads
    const logo = page.locator('.hero .logo');
    await expect(logo).toBeVisible();

    // Check usage demo image loads
    const usageDemo = page.locator('[data-testid="usage-demo"]');
    await expect(usageDemo).toBeVisible();

    // Verify images have valid src attributes
    await expect(logo).toHaveAttribute('src', 'assets/logo.gif');
    await expect(usageDemo).toHaveAttribute('src', 'assets/usage.gif');
  });

  test('SVG icons render correctly', async ({ page, browserName }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Check feature icons are SVG elements and visible
    const featureIcons = page.locator('[data-testid="feature-icon"]');
    const count = await featureIcons.count();

    expect(count).toBe(4);

    for (let i = 0; i < count; i++) {
      const icon = featureIcons.nth(i);
      await expect(icon).toBeVisible();

      // Verify it's an SVG element
      const tagName = await icon.evaluate(el => el.tagName.toLowerCase());
      expect(tagName).toBe('svg');
    }
  });

  test('page title and meta tags are correct', async ({ page, browserName }) => {
    await page.goto('/');

    // Check page title
    await expect(page).toHaveTitle('MirDB - Persistent Key-Value Store');

    // Check meta description
    const metaDescription = page.locator('meta[name="description"]');
    await expect(metaDescription).toHaveAttribute(
      'content',
      'MirDB - A Persistent Key-Value Store with Memcached Protocol'
    );

    // Check viewport meta tag (important for responsive design)
    const viewport = page.locator('meta[name="viewport"]');
    await expect(viewport).toHaveAttribute('content', 'width=device-width, initial-scale=1.0');
  });

  test('CSS custom properties (variables) are applied', async ({ page, browserName }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Check that CSS custom properties are defined and applied
    const rootStyles = await page.evaluate(() => {
      const root = document.documentElement;
      const styles = getComputedStyle(root);
      return {
        bgDark: styles.getPropertyValue('--bg-dark').trim(),
        bgSecondary: styles.getPropertyValue('--bg-secondary').trim(),
        textPrimary: styles.getPropertyValue('--text-primary').trim(),
        accentGreen: styles.getPropertyValue('--accent-green').trim(),
        accentBlue: styles.getPropertyValue('--accent-blue').trim()
      };
    });

    // Verify CSS variables are defined
    expect(rootStyles.bgDark).toBe('#0d1117');
    expect(rootStyles.bgSecondary).toBe('#161b22');
    expect(rootStyles.textPrimary).toBe('#c9d1d9');
    expect(rootStyles.accentGreen).toBe('#3fb950');
    expect(rootStyles.accentBlue).toBe('#58a6ff');
  });

  test('gradient text effect renders correctly', async ({ page, browserName }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    const heading = page.locator('.hero h1');

    // Check that the heading has background-clip applied
    // Note: Different browsers may report this differently
    const styles = await heading.evaluate(el => {
      const computed = window.getComputedStyle(el);
      return {
        backgroundClip: computed.backgroundClip || computed.webkitBackgroundClip,
        backgroundImage: computed.backgroundImage
      };
    });

    // Should have gradient background image
    expect(styles.backgroundImage).toContain('gradient');
  });

  test('transitions and hover effects are defined', async ({ page, browserName }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Check that feature cards have transition defined
    const featureCard = page.locator('.feature-card').first();
    const transition = await featureCard.evaluate(el =>
      window.getComputedStyle(el).transition
    );

    // Transition should be defined for smooth hover effects
    expect(transition).not.toBe('none');
    expect(transition).not.toBe('');
  });
});
