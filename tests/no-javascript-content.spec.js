// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

/**
 * No-JavaScript Core Content Test Suite
 *
 * Tests that verify the homepage is fully functional with JavaScript disabled.
 * This ensures progressive enhancement and accessibility for users who have
 * JavaScript disabled or for cases where scripts fail to load.
 *
 * Requirements tested:
 * - NFR-4: Must work without JavaScript for core content visibility
 * - REQ-1: Hero section displays content
 * - REQ-2: Key features are visible
 * - REQ-3: Quick start code examples are readable
 * - REQ-4: Navigation links work without JS
 * - REQ-10: Footer content is visible
 */

const HOMEPAGE_PATH = path.resolve(__dirname, '..', 'index.html');
const HOMEPAGE_URL = `file://${HOMEPAGE_PATH}`;

test.describe('No-JavaScript Core Content', () => {
  // Use a context with JavaScript disabled for all tests in this suite
  test.use({ javaScriptEnabled: false });

  test('TC1: Page renders and displays all content without JavaScript', async ({ page }) => {
    // Navigate to the page with JavaScript disabled
    await page.goto(HOMEPAGE_URL, { waitUntil: 'domcontentloaded' });

    // Verify the page loads and main structure is present
    await expect(page.locator('html')).toBeVisible();
    await expect(page.locator('body')).toBeVisible();

    // Verify all major sections are visible
    await expect(page.locator('header.hero')).toBeVisible();
    await expect(page.locator('#features')).toBeVisible();
    await expect(page.locator('#quick-start')).toBeVisible();
    await expect(page.locator('#architecture')).toBeVisible();
    await expect(page.locator('#commands')).toBeVisible();
    await expect(page.locator('footer.footer')).toBeVisible();

    // Verify main content container exists
    await expect(page.locator('main')).toBeVisible();

    // Verify page title is correctly set
    const title = await page.title();
    expect(title).toContain('MirDB');
  });

  test('TC2: Hero section content is fully visible without JavaScript', async ({ page }) => {
    await page.goto(HOMEPAGE_URL, { waitUntil: 'domcontentloaded' });

    // Verify hero section structure and content
    const heroSection = page.locator('header.hero');
    await expect(heroSection).toBeVisible();

    // Verify product name/title
    const heroTitle = page.locator('.hero-title');
    await expect(heroTitle).toBeVisible();
    await expect(heroTitle).toHaveText('MirDB');

    // Verify tagline
    const heroTagline = page.locator('.hero-tagline');
    await expect(heroTagline).toBeVisible();
    await expect(heroTagline).toContainText('Persistent Key-Value Store');
    await expect(heroTagline).toContainText('Memcached Protocol');

    // Verify description
    const heroDescription = page.locator('.hero-description');
    await expect(heroDescription).toBeVisible();
    const descriptionText = await heroDescription.textContent();
    expect(descriptionText).toContain('high-performance');
    expect(descriptionText).toContain('Rust');

    // Verify CTA buttons are visible
    const ctaSection = page.locator('.hero-cta');
    await expect(ctaSection).toBeVisible();

    // Primary CTA (GitHub link)
    const primaryBtn = page.locator('.btn-primary');
    await expect(primaryBtn).toBeVisible();
    await expect(primaryBtn).toContainText('GitHub');

    // Secondary CTA (Documentation)
    const secondaryBtn = page.locator('.btn-secondary');
    await expect(secondaryBtn).toBeVisible();
    await expect(secondaryBtn).toContainText('Documentation');
  });

  test('TC3: Navigation links are clickable and functional without JavaScript', async ({ page }) => {
    await page.goto(HOMEPAGE_URL, { waitUntil: 'domcontentloaded' });

    // Verify GitHub link in hero section
    const githubLink = page.locator('.hero-cta .btn-primary');
    await expect(githubLink).toBeVisible();
    const githubHref = await githubLink.getAttribute('href');
    expect(githubHref).toContain('github.com');
    expect(githubHref).toContain('mirdb');

    // Verify link has proper security attributes
    const relAttr = await githubLink.getAttribute('rel');
    expect(relAttr).toContain('noopener');
    expect(relAttr).toContain('noreferrer');

    // Verify target attribute for external link
    const targetAttr = await githubLink.getAttribute('target');
    expect(targetAttr).toBe('_blank');

    // Verify Documentation link (anchor link)
    const docsLink = page.locator('.hero-cta .btn-secondary');
    await expect(docsLink).toBeVisible();
    const docsHref = await docsLink.getAttribute('href');
    expect(docsHref).toBe('#features');

    // Verify footer GitHub link
    const footerGithubLink = page.locator('.footer-links a');
    await expect(footerGithubLink).toBeVisible();
    const footerGithubHref = await footerGithubLink.getAttribute('href');
    expect(footerGithubHref).toContain('github.com');

    // Test that anchor link works by clicking it
    await docsLink.click();
    // The features section should now be in view
    await expect(page.locator('#features')).toBeInViewport();
  });

  test('TC4: Code examples are visible and readable without JavaScript', async ({ page }) => {
    await page.goto(HOMEPAGE_URL, { waitUntil: 'domcontentloaded' });

    // Verify quick start section exists
    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeVisible();

    // Verify code blocks are present (3 code blocks: server start, usage, config)
    const codeBlocks = page.locator('.code-block');
    await expect(codeBlocks).toHaveCount(3);

    // Verify each code block is visible and has content
    for (let i = 0; i < 3; i++) {
      const codeBlock = codeBlocks.nth(i);
      await expect(codeBlock).toBeVisible();
      const content = await codeBlock.textContent();
      expect(content.length).toBeGreaterThan(0);
    }

    // Verify first code block contains server start command
    const serverStartBlock = codeBlocks.first();
    const serverStartContent = await serverStartBlock.textContent();
    expect(serverStartContent).toContain('mirdb');
    expect(serverStartContent).toContain('-c');

    // Verify usage code block contains SET and GET examples
    const usageBlock = codeBlocks.nth(1);
    const usageContent = await usageBlock.textContent();
    expect(usageContent).toContain('set');
    expect(usageContent).toContain('get');
    expect(usageContent).toContain('STORED');

    // Verify configuration block contains TOML config
    const configBlock = codeBlocks.nth(2);
    const configContent = await configBlock.textContent();
    expect(configContent).toContain('addr');
    expect(configContent).toContain('12333');
    expect(configContent).toContain('work_dir');

    // Verify code blocks have pre-rendered syntax highlighting classes
    // These should work without JavaScript since they're CSS-based
    const syntaxHighlightedElements = page.locator('.code-block [class^="code-"]');
    const highlightCount = await syntaxHighlightedElements.count();
    expect(highlightCount).toBeGreaterThan(10); // Multiple highlighted elements exist

    // Verify specific syntax highlighting classes exist
    await expect(page.locator('.code-comment').first()).toBeVisible();
    await expect(page.locator('.code-command').first()).toBeVisible();
    await expect(page.locator('.code-string').first()).toBeVisible();
  });

  test('Features section is visible without JavaScript', async ({ page }) => {
    await page.goto(HOMEPAGE_URL, { waitUntil: 'domcontentloaded' });

    // Verify features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Verify section title
    const sectionTitle = page.locator('#features .section-title');
    await expect(sectionTitle).toContainText('Key Features');

    // Verify all three feature cards
    const featureCards = page.locator('.feature-card');
    await expect(featureCards).toHaveCount(3);

    // Verify each feature card content
    // Memcached Compatible
    const memcachedCard = page.locator('[data-feature="memcached"]');
    await expect(memcachedCard).toBeVisible();
    await expect(memcachedCard.locator('.feature-title')).toContainText('Memcached Compatible');

    // Persistent Storage
    const persistenceCard = page.locator('[data-feature="persistence"]');
    await expect(persistenceCard).toBeVisible();
    await expect(persistenceCard.locator('.feature-title')).toContainText('Persistent Storage');

    // LSM Tree Architecture
    const lsmCard = page.locator('[data-feature="lsm"]');
    await expect(lsmCard).toBeVisible();
    await expect(lsmCard.locator('.feature-title')).toContainText('LSM Tree Architecture');

    // Verify icons are visible (SVG elements)
    const featureIcons = page.locator('.feature-icon svg');
    await expect(featureIcons).toHaveCount(3);
    for (let i = 0; i < 3; i++) {
      await expect(featureIcons.nth(i)).toBeVisible();
    }
  });

  test('Architecture section and diagram are visible without JavaScript', async ({ page }) => {
    await page.goto(HOMEPAGE_URL, { waitUntil: 'domcontentloaded' });

    // Verify architecture section
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    // Verify section title
    await expect(page.locator('#architecture .section-title')).toContainText('Architecture Overview');

    // Verify the SVG diagram is visible
    const architectureDiagram = page.locator('.architecture-diagram svg');
    await expect(architectureDiagram).toBeVisible();

    // Verify diagram has aria-label for accessibility
    const ariaLabel = await architectureDiagram.getAttribute('aria-label');
    expect(ariaLabel).toContain('LSM Tree');

    // Verify diagram contains key components (text labels)
    const diagramText = await architectureDiagram.textContent();
    expect(diagramText).toContain('WAL');
    expect(diagramText).toContain('Memtable');
    expect(diagramText).toContain('SSTable');

    // Verify architecture explanation is visible
    const explanation = page.locator('.architecture-explanation');
    await expect(explanation).toBeVisible();

    // Verify data flow list
    const dataFlowList = page.locator('.data-flow-list li');
    await expect(dataFlowList).toHaveCount(3);
  });

  test('Commands section is visible without JavaScript', async ({ page }) => {
    await page.goto(HOMEPAGE_URL, { waitUntil: 'domcontentloaded' });

    // Verify commands section
    const commandsSection = page.locator('#commands');
    await expect(commandsSection).toBeVisible();

    // Verify section title
    await expect(page.locator('#commands .section-title')).toContainText('Supported Commands');

    // Verify command categories are visible
    const commandCategories = page.locator('.command-category');
    await expect(commandCategories).toHaveCount(3); // Storage, Retrieval, Deletion

    // Verify specific commands are listed
    const commandNames = page.locator('.command-name');
    const commandCount = await commandNames.count();
    expect(commandCount).toBeGreaterThanOrEqual(8); // SET, ADD, REPLACE, APPEND, PREPEND, GET, GETS, DELETE

    // Verify key commands are present
    const commandText = await commandsSection.textContent();
    expect(commandText).toContain('SET');
    expect(commandText).toContain('GET');
    expect(commandText).toContain('DELETE');
    expect(commandText).toContain('ADD');
    expect(commandText).toContain('REPLACE');
  });

  test('Footer is visible and contains required information without JavaScript', async ({ page }) => {
    await page.goto(HOMEPAGE_URL, { waitUntil: 'domcontentloaded' });

    // Verify footer
    const footer = page.locator('footer.footer');
    await expect(footer).toBeVisible();

    // Verify GitHub link in footer
    const footerLinks = page.locator('.footer-links a');
    await expect(footerLinks).toBeVisible();
    await expect(footerLinks).toContainText('GitHub');

    // Verify license information
    const licenseText = page.locator('.footer-license');
    await expect(licenseText).toBeVisible();
    await expect(licenseText).toContainText('MIT');
  });

  test('Page styling is applied correctly without JavaScript', async ({ page }) => {
    await page.goto(HOMEPAGE_URL, { waitUntil: 'domcontentloaded' });

    // Verify CSS is loaded by checking computed styles
    const body = page.locator('body');
    const backgroundColor = await body.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });
    // Background should not be default white (CSS is applied)
    expect(backgroundColor).not.toBe('rgb(255, 255, 255)');
    expect(backgroundColor).not.toBe('rgba(0, 0, 0, 0)');

    // Verify hero section has styling
    const heroTitle = page.locator('.hero-title');
    const fontSize = await heroTitle.evaluate((el) => {
      return window.getComputedStyle(el).fontSize;
    });
    // Font size should be larger than default (16px)
    expect(parseInt(fontSize)).toBeGreaterThan(20);

    // Verify buttons have styling
    const primaryBtn = page.locator('.btn-primary');
    const btnBgColor = await primaryBtn.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });
    // Button should have a background color
    expect(btnBgColor).not.toBe('rgba(0, 0, 0, 0)');
  });

  test('All content is accessible via semantic HTML without JavaScript', async ({ page }) => {
    await page.goto(HOMEPAGE_URL, { waitUntil: 'domcontentloaded' });

    // Verify semantic structure
    // Header
    await expect(page.locator('header')).toBeVisible();

    // Main content area
    await expect(page.locator('main')).toBeVisible();

    // Footer
    await expect(page.locator('footer')).toBeVisible();

    // Sections with IDs for navigation
    const sections = ['features', 'quick-start', 'architecture', 'commands'];
    for (const sectionId of sections) {
      const section = page.locator(`section#${sectionId}`);
      await expect(section).toBeVisible();
    }

    // Article elements for feature cards
    const articles = page.locator('article.feature-card');
    await expect(articles).toHaveCount(3);

    // Heading hierarchy
    await expect(page.locator('h1')).toHaveCount(1); // One main heading
    const h2Count = await page.locator('h2').count();
    expect(h2Count).toBeGreaterThanOrEqual(4); // Multiple section headings
  });
});
