// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

const indexPath = 'file://' + path.resolve(__dirname, '../index.html');

test.describe('Progressive Enhancement - Core Content Without JavaScript (NFR-4)', () => {
  // Configure tests to run with JavaScript disabled
  test.use({ javaScriptEnabled: false });

  // Test Case 1: Page renders core content without JavaScript
  test('TC1: Page renders core content without JavaScript', async ({ page }) => {
    await page.goto(indexPath, { waitUntil: 'domcontentloaded' });

    // Verify the page has loaded (DOCTYPE and HTML structure present)
    const html = await page.locator('html');
    await expect(html).toBeVisible();

    // Verify body has content
    const body = await page.locator('body');
    await expect(body).toBeVisible();

    // Verify main content sections exist
    const header = page.locator('header');
    await expect(header).toBeVisible();

    const main = page.locator('main');
    await expect(main).toBeVisible();

    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    // Verify key sections are present
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeVisible();

    // Verify page title is correct
    const title = await page.title();
    expect(title).toContain('MirDB');
  });

  // Test Case 2: Hero section content is visible without JavaScript
  test('TC2: Hero section content is visible without JavaScript', async ({ page }) => {
    await page.goto(indexPath, { waitUntil: 'domcontentloaded' });

    // Verify hero section exists and is visible
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // Verify product name is visible
    const heroTitle = page.locator('.hero-title');
    await expect(heroTitle).toBeVisible();
    await expect(heroTitle).toHaveText('MirDB');

    // Verify hero subtitle/tagline is visible
    const heroSubtitle = page.locator('.hero-subtitle');
    await expect(heroSubtitle).toBeVisible();
    const subtitleText = await heroSubtitle.textContent();
    expect(subtitleText.toLowerCase()).toContain('persistent');
    expect(subtitleText.toLowerCase()).toContain('memcached');

    // Verify logo image is present (may not load without server, but element should exist)
    const logo = page.locator('.hero-logo');
    await expect(logo).toBeVisible();

    // Verify CTA buttons are present
    const primaryCta = page.locator('#cta-primary');
    await expect(primaryCta).toBeVisible();

    const secondaryCta = page.locator('#cta-secondary');
    await expect(secondaryCta).toBeVisible();
  });

  // Test Case 3: Features section is readable without JavaScript
  test('TC3: Features section is readable without JavaScript', async ({ page }) => {
    await page.goto(indexPath, { waitUntil: 'domcontentloaded' });

    // Scroll to features section
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();
    await expect(featuresSection).toBeVisible();

    // Verify features heading is visible
    const featuresHeading = page.locator('#features h2');
    await expect(featuresHeading).toBeVisible();
    await expect(featuresHeading).toHaveText('Key Features');

    // Verify feature cards are present
    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();
    expect(cardCount).toBeGreaterThanOrEqual(3);
    expect(cardCount).toBeLessThanOrEqual(4);

    // Verify each feature card has readable content
    for (let i = 0; i < cardCount; i++) {
      const card = featureCards.nth(i);
      await expect(card).toBeVisible();

      // Each card should have a heading (h3)
      const cardHeading = card.locator('h3');
      await expect(cardHeading).toBeVisible();
      const headingText = await cardHeading.textContent();
      expect(headingText.trim().length).toBeGreaterThan(0);

      // Each card should have a description (p)
      const cardDescription = card.locator('p');
      await expect(cardDescription).toBeVisible();
      const descText = await cardDescription.textContent();
      expect(descText.trim().length).toBeGreaterThan(0);
    }

    // Verify specific features are mentioned
    const memcachedFeature = page.locator('.feature-card').filter({ hasText: /memcached/i });
    await expect(memcachedFeature).toBeVisible();

    const persistentFeature = page.locator('.feature-card').filter({ hasText: /persistent/i });
    await expect(persistentFeature).toBeVisible();

    const lsmFeature = page.locator('.feature-card').filter({ hasText: /lsm/i });
    await expect(lsmFeature).toBeVisible();
  });

  // Test Case 4: Navigation links are functional without JavaScript
  test('TC4: Navigation links are functional without JavaScript', async ({ page }) => {
    await page.goto(indexPath, { waitUntil: 'domcontentloaded' });

    // Verify primary CTA link (GitHub) is functional
    const primaryCta = page.locator('#cta-primary');
    await expect(primaryCta).toBeVisible();
    const primaryHref = await primaryCta.getAttribute('href');
    expect(primaryHref).toBeTruthy();
    expect(primaryHref).toContain('github.com');

    // Verify secondary CTA link (documentation/quick-start) is functional
    const secondaryCta = page.locator('#cta-secondary');
    await expect(secondaryCta).toBeVisible();
    const secondaryHref = await secondaryCta.getAttribute('href');
    expect(secondaryHref).toBeTruthy();
    expect(secondaryHref).toContain('#quick-start');

    // Verify anchor link navigates correctly (click and check URL hash)
    await secondaryCta.click();
    const urlAfterClick = page.url();
    expect(urlAfterClick).toContain('#quick-start');

    // Verify quick-start section is now in view
    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeVisible();

    // Verify footer GitHub link is functional
    const footerGitHubLink = page.locator('footer a[href*="github"]').first();
    await expect(footerGitHubLink).toBeVisible();
    const footerHref = await footerGitHubLink.getAttribute('href');
    expect(footerHref).toBeTruthy();
    expect(footerHref).toContain('github.com');
  });

  // Additional test: Quick Start section is readable without JavaScript
  test('Quick Start section content is accessible without JavaScript', async ({ page }) => {
    await page.goto(indexPath, { waitUntil: 'domcontentloaded' });

    // Scroll to quick-start section
    const quickStartSection = page.locator('#quick-start');
    await quickStartSection.scrollIntoViewIfNeeded();
    await expect(quickStartSection).toBeVisible();

    // Verify heading
    const heading = page.locator('#quick-start h2');
    await expect(heading).toBeVisible();
    await expect(heading).toHaveText('Quick Start');

    // Verify installation instructions are visible
    const installationSection = page.locator('.installation');
    await expect(installationSection).toBeVisible();

    // Verify code blocks are readable
    const codeBlocks = page.locator('#quick-start pre');
    const codeCount = await codeBlocks.count();
    expect(codeCount).toBeGreaterThan(0);

    // Verify configuration info is visible
    const configInfo = page.locator('.config-info');
    await expect(configInfo).toBeVisible();
  });

  // Additional test: Code examples section is readable without JavaScript
  test('Code examples section displays correctly without JavaScript', async ({ page }) => {
    await page.goto(indexPath, { waitUntil: 'domcontentloaded' });

    // Scroll to code examples section
    const codeExamplesSection = page.locator('#code-examples');
    await codeExamplesSection.scrollIntoViewIfNeeded();
    await expect(codeExamplesSection).toBeVisible();

    // Verify heading
    const heading = page.locator('#code-examples h2');
    await expect(heading).toBeVisible();
    await expect(heading).toHaveText('Code Examples');

    // Verify code block is visible and has content
    const codeBlock = page.locator('#code-example');
    await expect(codeBlock).toBeVisible();
    const codeContent = await codeBlock.textContent();
    expect(codeContent).toContain('set');
    expect(codeContent).toContain('get');
    expect(codeContent).toContain('STORED');

    // Note: Copy button won't work without JS, but that's acceptable for progressive enhancement
    // The core content (the code itself) should still be visible and readable
  });

  // Additional test: Project status section is readable without JavaScript
  test('Project status section is visible without JavaScript', async ({ page }) => {
    await page.goto(indexPath, { waitUntil: 'domcontentloaded' });

    // Scroll to status section
    const statusSection = page.locator('#status');
    await statusSection.scrollIntoViewIfNeeded();
    await expect(statusSection).toBeVisible();

    // Verify heading
    const heading = page.locator('#status h2');
    await expect(heading).toBeVisible();
    await expect(heading).toHaveText('Project Status');

    // Verify implemented features list is visible
    const implementedSection = page.locator('.implemented');
    await expect(implementedSection).toBeVisible();

    const implementedItems = page.locator('.implemented li');
    const implementedCount = await implementedItems.count();
    expect(implementedCount).toBeGreaterThan(0);

    // Verify planned features list is visible
    const plannedSection = page.locator('.planned');
    await expect(plannedSection).toBeVisible();
  });

  // Additional test: Footer section is accessible without JavaScript
  test('Footer section is accessible without JavaScript', async ({ page }) => {
    await page.goto(indexPath, { waitUntil: 'domcontentloaded' });

    // Scroll to footer
    const footer = page.locator('footer');
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();

    // Verify license information is visible
    const license = page.locator('.license');
    await expect(license).toBeVisible();
    const licenseText = await license.textContent();
    expect(licenseText).toContain('ISC');

    // Verify copyright is visible
    const copyright = page.locator('.copyright');
    await expect(copyright).toBeVisible();
  });
});
