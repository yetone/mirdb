/**
 * Progressive Enhancement Tests
 * Owner: Scenario 14 - Progressive Enhancement - No JavaScript
 *
 * Test cases:
 * - Page renders with JS disabled
 * - Navigation works without JS
 * - Code examples visible without JS
 * - All critical features accessible
 */

import { test, expect } from '@playwright/test';

// Create a test context with JavaScript disabled
test.describe('Progressive Enhancement - No JavaScript', () => {
  test.use({ javaScriptEnabled: false });

  test('TC1: Page renders with all text content visible when JavaScript is disabled', async ({ page }) => {
    await page.goto('/');

    // Verify page loads and basic structure is present
    await expect(page).toHaveTitle(/MirDB/);

    // Verify hero section content is visible
    await expect(page.locator('.hero-title')).toBeVisible();
    await expect(page.locator('.hero-title')).toHaveText('MirDB');
    await expect(page.locator('.hero-tagline')).toBeVisible();
    await expect(page.locator('.hero-tagline')).toContainText('Persistent Key-Value Store');

    // Verify main content area is visible
    await expect(page.locator('#main-content')).toBeVisible();

    // Verify features section is visible
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();
    await expect(featuresSection.locator('h2')).toHaveText('Key Features');

    // Verify all feature cards are visible
    const featureCards = page.locator('.feature-card');
    await expect(featureCards).toHaveCount(7);

    // Verify getting started section is visible
    const gettingStarted = page.locator('#getting-started');
    await expect(gettingStarted).toBeVisible();
    await expect(gettingStarted.locator('h2')).toHaveText('Getting Started');

    // Verify footer is visible
    await expect(page.locator('footer.footer')).toBeVisible();
  });

  test('TC2: Navigation links are functional without JavaScript', async ({ page }) => {
    await page.goto('/');

    // Verify navigation container is visible
    const nav = page.locator('nav.site-nav');
    await expect(nav).toBeVisible();

    // Verify nav links container is visible (not hidden via JS)
    const navLinks = page.locator('.nav-links');
    await expect(navLinks).toBeVisible();

    // Test anchor navigation links exist and have correct hrefs
    const featuresLink = page.locator('.nav-links a[href="#features"]');
    await expect(featuresLink).toBeVisible();
    await expect(featuresLink).toHaveText('Features');

    const gettingStartedLink = page.locator('.nav-links a[href="#getting-started"]');
    await expect(gettingStartedLink).toBeVisible();
    await expect(gettingStartedLink).toHaveText('Getting Started');

    // Test external navigation links
    const docsLink = page.locator('.nav-link-docs');
    await expect(docsLink).toBeVisible();
    await expect(docsLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb#readme');

    const githubLink = page.locator('.nav-link-github');
    await expect(githubLink).toBeVisible();
    await expect(githubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');

    // Verify hero CTA links work
    const getStartedBtn = page.locator('.hero-cta a[href="#getting-started"]');
    await expect(getStartedBtn).toBeVisible();
    await expect(getStartedBtn).toHaveText('Get Started');

    const viewGithubBtn = page.locator('.hero-cta a[href="https://github.com/yetone/mirdb"]');
    await expect(viewGithubBtn).toBeVisible();
    await expect(viewGithubBtn).toHaveText('View on GitHub');

    // Navigate to features section via anchor link click
    await featuresLink.click();
    // Wait for the URL to update with the anchor
    await page.waitForURL(/#features/, { timeout: 5000 });

    // Verify the features section is now in view
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Navigate back to top to test another link
    await page.goto('/');

    // Navigate to getting started section
    const gettingStartedLinkAgain = page.locator('.nav-links a[href="#getting-started"]');
    await gettingStartedLinkAgain.click();
    await page.waitForURL(/#getting-started/, { timeout: 5000 });
  });

  test('TC3: Code examples are visible and readable without JavaScript', async ({ page }) => {
    await page.goto('/');

    // Verify all code blocks in Getting Started section are visible
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeVisible();

    // Check telnet connection code block
    const connectCode = page.locator('#code-connect');
    await expect(connectCode).toBeVisible();
    await expect(connectCode).toContainText('telnet localhost 11211');

    // Check SET command code block
    const setCode = page.locator('#code-set');
    await expect(setCode).toBeVisible();
    await expect(setCode).toContainText('set mykey 0 3600 5');
    await expect(setCode).toContainText('STORED');

    // Check GET command code block
    const getCode = page.locator('#code-get');
    await expect(getCode).toBeVisible();
    await expect(getCode).toContainText('get mykey');
    await expect(getCode).toContainText('VALUE mykey 0 5');

    // Check complete session code block
    const completeCode = page.locator('#code-complete');
    await expect(completeCode).toBeVisible();
    await expect(completeCode).toContainText('telnet localhost 11211');

    // Verify code blocks in Installation section
    const installationSection = page.locator('#installation');
    await expect(installationSection).toBeVisible();

    // With noscript fallback, both tab panels should be visible
    const cargoPanel = page.locator('#cargo-panel');
    await expect(cargoPanel).toBeVisible();
    await expect(cargoPanel.locator('#code-cargo-install')).toBeVisible();
    await expect(page.locator('#code-cargo-install')).toContainText('cargo install mirdb-server');

    // Docker panel should also be visible without JS (noscript fallback)
    const dockerPanel = page.locator('#docker-panel');
    await expect(dockerPanel).toBeVisible();
    await expect(dockerPanel.locator('#code-docker-pull')).toBeVisible();
    await expect(page.locator('#code-docker-pull')).toContainText('docker pull yetone/mirdb:latest');

    // Verify code blocks in Configuration section
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    const configToml = page.locator('#code-config-toml');
    await expect(configToml).toBeVisible();
    await expect(configToml).toContainText('addr = "0.0.0.0:12333"');
  });

  test('TC4: All content sections are accessible without JavaScript', async ({ page }) => {
    await page.goto('/');

    // Verify all major sections are present and accessible
    const sections = [
      { selector: '#hero', name: 'Hero' },
      { selector: '#badges', name: 'Badges' },
      { selector: '#features', name: 'Features' },
      { selector: '#getting-started', name: 'Getting Started' },
      { selector: '#installation', name: 'Installation' },
      { selector: '#configuration', name: 'Configuration' },
      { selector: 'footer.footer', name: 'Footer' },
    ];

    for (const section of sections) {
      const element = page.locator(section.selector);
      await expect(element, `${section.name} section should be visible`).toBeVisible();
    }

    // Verify skip-to-content link exists for accessibility
    const skipLink = page.locator('.skip-link');
    await expect(skipLink).toHaveAttribute('href', '#main-content');

    // Verify main content landmark exists
    const mainContent = page.locator('#main-content');
    await expect(mainContent).toBeVisible();

    // Verify semantic structure is maintained
    // Check h1 exists
    const h1 = page.locator('h1');
    await expect(h1).toBeVisible();

    // Check heading hierarchy - h2 elements exist in main sections
    const h2Elements = page.locator('main h2');
    const h2Count = await h2Elements.count();
    expect(h2Count).toBeGreaterThanOrEqual(4); // Features, Getting Started, Installation, Configuration

    // Verify feature cards content is accessible
    const featureCards = page.locator('.feature-card');
    const featureCount = await featureCards.count();
    expect(featureCount).toBe(7);

    // Check that feature headings are readable
    const featureTitles = [
      'Memcached Protocol',
      'LSM-tree Storage',
      'Skip-list Memtable',
      'WAL Durability',
      'SSTable Storage',
      'Compaction',
      'TTL Support',
    ];

    for (const title of featureTitles) {
      const featureHeading = page.locator('.feature-card h3', { hasText: title });
      await expect(featureHeading, `Feature "${title}" should be visible`).toBeVisible();
    }

    // Verify configuration table is readable
    const configTable = page.locator('.config-table');
    await expect(configTable).toBeVisible();

    // Check table has headers and data
    const tableHeaders = configTable.locator('th');
    await expect(tableHeaders).toHaveCount(3); // Parameter, Description, Default

    // Verify footer links are accessible
    const footerLinks = page.locator('footer.footer a');
    const footerLinkCount = await footerLinks.count();
    expect(footerLinkCount).toBeGreaterThan(5); // Multiple links in footer

    // Check CircleCI badge is visible (image, not JS-loaded)
    const circleCIBadge = page.locator('.badge-image');
    await expect(circleCIBadge).toBeVisible();
    await expect(circleCIBadge).toHaveAttribute('alt', 'CircleCI Build Status');

    // Check version badge is visible
    const versionBadge = page.locator('.version-badge');
    await expect(versionBadge).toBeVisible();
  });

  test('Logo images load correctly without JavaScript', async ({ page }) => {
    await page.goto('/');

    // Check hero logo
    const heroLogo = page.locator('.hero-logo');
    await expect(heroLogo).toBeVisible();
    await expect(heroLogo).toHaveAttribute('alt', 'MirDB Logo');
    await expect(heroLogo).toHaveAttribute('src', /logo/);

    // Check nav logo
    const navLogo = page.locator('.nav-logo');
    await expect(navLogo).toBeVisible();

    // Check footer logo
    const footerLogo = page.locator('.footer-logo');
    await expect(footerLogo).toBeVisible();
  });

  test('Page structure is semantically correct without JavaScript', async ({ page }) => {
    await page.goto('/');

    // Verify proper semantic HTML elements
    await expect(page.locator('nav[aria-label="Main navigation"]')).toBeVisible();
    await expect(page.locator('header.hero')).toBeVisible();
    await expect(page.locator('main#main-content')).toBeVisible();
    await expect(page.locator('footer.footer')).toBeVisible();

    // Verify sections use proper HTML5 section elements
    const sections = page.locator('main section');
    const sectionCount = await sections.count();
    expect(sectionCount).toBeGreaterThanOrEqual(4);

    // Verify ARIA labels are present on key elements
    const navAriaLabel = await page.locator('nav').getAttribute('aria-label');
    expect(navAriaLabel).toBe('Main navigation');
  });

  test('Links to external resources work without JavaScript', async ({ page }) => {
    await page.goto('/');

    // Verify external links have correct attributes for security
    const externalLinks = page.locator('a[target="_blank"]');
    const externalLinkCount = await externalLinks.count();
    expect(externalLinkCount).toBeGreaterThan(0);

    // Check that external links have rel="noopener noreferrer"
    for (let i = 0; i < Math.min(externalLinkCount, 5); i++) {
      const link = externalLinks.nth(i);
      const rel = await link.getAttribute('rel');
      expect(rel).toContain('noopener');
    }

    // Verify GitHub link in hero is accessible
    const githubHeroLink = page.locator('.hero-cta a[href="https://github.com/yetone/mirdb"]');
    await expect(githubHeroLink).toBeVisible();

    // Verify footer GitHub links are accessible
    const footerGithubLink = page.locator('footer a[href="https://github.com/yetone/mirdb"]');
    await expect(footerGithubLink.first()).toBeVisible();
  });
});
