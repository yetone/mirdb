// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Error Handling - Broken Assets', () => {
  test('Test Case 1: Block logo.gif from loading - page displays alt text or fallback', async ({ page }) => {
    // Block the logo.gif asset from loading
    await page.route('**/assets/logo.gif', (route) => {
      route.abort('failed');
    });

    await page.goto('/');

    // Verify the page still loads and hero section is visible
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    // Verify the logo element exists and has proper alt text for accessibility
    const logo = page.locator('[data-testid="hero-logo"]');
    await expect(logo).toBeVisible();

    // Check that the alt text is properly set for screen readers
    const altText = await logo.getAttribute('alt');
    expect(altText).toBeTruthy();
    expect(altText).toContain('MirDB');

    // Verify the logo has fallback styling (background-color from CSS)
    const backgroundColor = await logo.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });
    expect(backgroundColor).toBeTruthy();

    // Verify that the hero title is still visible and readable
    const heroTitle = page.locator('[data-testid="hero-title"]');
    await expect(heroTitle).toBeVisible();
    await expect(heroTitle).toHaveText('MirDB');

    // Verify the page content remains functional
    const heroTagline = page.locator('[data-testid="hero-tagline"]');
    await expect(heroTagline).toBeVisible();
    await expect(heroTagline).toContainText('Persistent Key-Value Store');
  });

  test('Test Case 2: Block external badge images - page remains functional', async ({ page }) => {
    // Block all external badge images (CircleCI and shields.io)
    await page.route('**/circleci.com/**', (route) => {
      route.abort('failed');
    });
    await page.route('**/img.shields.io/**', (route) => {
      route.abort('failed');
    });

    await page.goto('/');

    // Verify the page loads successfully
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    // Verify the status badges container is still visible
    const statusBadges = page.locator('[data-testid="status-badges"]');
    await expect(statusBadges).toBeVisible();

    // Verify badge links are still functional (even if images fail)
    const buildBadge = page.locator('[data-testid="badge-build-status"]');
    await expect(buildBadge).toBeVisible();
    await expect(buildBadge).toHaveAttribute('href', 'https://circleci.com/gh/yetone/mirdb');

    const versionBadge = page.locator('[data-testid="badge-version"]');
    await expect(versionBadge).toBeVisible();

    const licenseBadge = page.locator('[data-testid="badge-license"]');
    await expect(licenseBadge).toBeVisible();

    // Verify badge images have proper alt text for accessibility
    const buildBadgeImg = page.locator('[data-testid="badge-build-status-img"]');
    const altText = await buildBadgeImg.getAttribute('alt');
    expect(altText).toBeTruthy();
    expect(altText).toContain('CircleCI');

    // Verify CTA buttons are still functional
    const getStartedBtn = page.locator('[data-testid="cta-get-started"]');
    await expect(getStartedBtn).toBeVisible();
    await expect(getStartedBtn).toBeEnabled();

    const githubBtn = page.locator('[data-testid="cta-github"]');
    await expect(githubBtn).toBeVisible();
    await expect(githubBtn).toBeEnabled();
  });

  test('Test Case 3: Block usage.gif from loading - page displays placeholder or alt text', async ({ page }) => {
    // Block the usage.gif asset from loading
    await page.route('**/assets/usage.gif', (route) => {
      route.abort('failed');
    });

    await page.goto('/');

    // Verify the usage demo section is still visible
    const usageDemoSection = page.locator('[data-testid="usage-demo-section"]');
    await expect(usageDemoSection).toBeVisible();

    // Verify the usage demo media element exists
    const usageDemoMedia = page.locator('[data-testid="usage-demo-media"]');
    await expect(usageDemoMedia).toBeVisible();

    // Check that the alt text is properly set for accessibility
    const altText = await usageDemoMedia.getAttribute('alt');
    expect(altText).toBeTruthy();
    expect(altText).toContain('MirDB');
    expect(altText.toLowerCase()).toContain('usage');

    // Verify the usage demo container has fallback styling
    const usageDemoContainer = page.locator('.usage-demo-container');
    await expect(usageDemoContainer).toBeVisible();

    // Verify section heading is still visible
    const sectionHeading = usageDemoSection.locator('h2');
    await expect(sectionHeading).toBeVisible();
    await expect(sectionHeading).toContainText('MirDB in Action');

    // Verify the rest of the page is functional
    const quickStartSection = page.locator('[data-testid="quick-start-section"]');
    await expect(quickStartSection).toBeVisible();
  });

  test('Test Case 4: Load page with all external resources blocked - core content remains accessible', async ({ page }) => {
    // Block ALL external resources (images, badges, etc.)
    await page.route('**/assets/logo.gif', (route) => {
      route.abort('failed');
    });
    await page.route('**/assets/usage.gif', (route) => {
      route.abort('failed');
    });
    await page.route('**/circleci.com/**', (route) => {
      route.abort('failed');
    });
    await page.route('**/img.shields.io/**', (route) => {
      route.abort('failed');
    });

    await page.goto('/');

    // Verify the page loads successfully
    await expect(page).toHaveTitle(/MirDB/);

    // Verify core hero section content is accessible
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    const heroTitle = page.locator('[data-testid="hero-title"]');
    await expect(heroTitle).toBeVisible();
    await expect(heroTitle).toHaveText('MirDB');

    const heroTagline = page.locator('[data-testid="hero-tagline"]');
    await expect(heroTagline).toBeVisible();
    await expect(heroTagline).toContainText('Persistent Key-Value Store');

    // Verify CTA buttons are functional
    const getStartedBtn = page.locator('[data-testid="cta-get-started"]');
    await expect(getStartedBtn).toBeVisible();
    await expect(getStartedBtn).toBeEnabled();

    const githubBtn = page.locator('[data-testid="cta-github"]');
    await expect(githubBtn).toBeVisible();
    await expect(githubBtn).toBeEnabled();

    // Verify navigation is functional
    const navigation = page.locator('[data-testid="navigation"]');
    await expect(navigation).toBeVisible();

    const navLinks = page.locator('[data-testid="nav-links"]');
    await expect(navLinks).toBeVisible();

    // Verify Quick Start section content is accessible
    const quickStartSection = page.locator('[data-testid="quick-start-section"]');
    await expect(quickStartSection).toBeVisible();

    // Verify code blocks are still visible and functional
    const installCodeBlock = page.locator('[data-testid="install-code-block"]');
    await expect(installCodeBlock).toBeVisible();

    const installCode = page.locator('[data-testid="install-code"]');
    await expect(installCode).toBeVisible();
    await expect(installCode).toContainText('cargo install mirdb-server');

    // Verify comparison table is accessible
    const comparisonTable = page.locator('[data-testid="comparison-table"]');
    await expect(comparisonTable).toBeVisible();

    // Verify architecture section (SVG diagram) is accessible
    const architectureSection = page.locator('[data-testid="architecture-section"]');
    await expect(architectureSection).toBeVisible();

    const architectureDiagram = page.locator('[data-testid="architecture-diagram"]');
    await expect(architectureDiagram).toBeVisible();

    // Verify footer is accessible
    const footer = page.locator('[data-testid="footer"]');
    await expect(footer).toBeVisible();

    const footerGithubLink = page.locator('[data-testid="footer-github-link"]');
    await expect(footerGithubLink).toBeVisible();
    await expect(footerGithubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');

    // Verify all text content is readable
    const pageText = await page.locator('body').textContent();
    expect(pageText).toContain('MirDB');
    expect(pageText).toContain('Persistent Key-Value Store');
    expect(pageText).toContain('Memcached Protocol');
    expect(pageText).toContain('Quick Start');
    expect(pageText).toContain('Feature Comparison');
    expect(pageText).toContain('Architecture');
  });
});
