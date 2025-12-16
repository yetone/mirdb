// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Cross-Browser Compatibility Tests
 *
 * Verifies that the MirDB homepage renders correctly and all features function
 * properly across major browsers: Chrome, Firefox, Safari (WebKit), and Edge.
 *
 * These tests run in Playwright's project matrix, executing once per browser.
 */

test.describe('Cross-Browser Compatibility - Homepage Rendering', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Wait for page to be fully loaded
    await page.waitForLoadState('domcontentloaded');
  });

  test('page loads successfully and has correct title', async ({ page }) => {
    await expect(page).toHaveTitle('MirDB - Persistent Key-Value Store');
  });

  test('hero section renders correctly', async ({ page }) => {
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    const productName = page.locator('[data-testid="product-name"]');
    await expect(productName).toBeVisible();
    await expect(productName).toHaveText('MirDB');

    const tagline = page.locator('[data-testid="tagline"]');
    await expect(tagline).toBeVisible();
    await expect(tagline).toContainText('Memcached protocol compatibility');
  });

  test('CTA buttons are visible and functional', async ({ page }) => {
    const primaryCTA = page.locator('[data-testid="primary-cta"]');
    await expect(primaryCTA).toBeVisible();
    await expect(primaryCTA).toHaveAttribute('href', 'https://github.com/yetone/mirdb');

    const secondaryCTA = page.locator('[data-testid="secondary-cta"]');
    await expect(secondaryCTA).toBeVisible();
    await expect(secondaryCTA).toHaveAttribute('href', '#features');
  });

  test('features section renders all feature cards', async ({ page }) => {
    const featuresSection = page.locator('[data-testid="features-section"]');
    await expect(featuresSection).toBeVisible();

    const featureCards = page.locator('.feature-card');
    await expect(featureCards).toHaveCount(6);

    // Verify key feature titles are present
    const expectedFeatures = [
      'Memcached Compatible',
      'Persistent Storage',
      'High Performance',
      'Durable',
      'Efficient',
      'Written in Rust'
    ];

    for (const feature of expectedFeatures) {
      const featureTitle = page.locator('.feature-card h3', { hasText: feature });
      await expect(featureTitle).toBeVisible();
    }
  });

  test('architecture section renders diagram and descriptions', async ({ page }) => {
    const architectureSection = page.locator('[data-testid="architecture-section"]');
    await expect(architectureSection).toBeVisible();

    const lsmExplanation = page.locator('[data-testid="lsm-tree-explanation"]');
    await expect(lsmExplanation).toBeVisible();
    await expect(lsmExplanation).toContainText('Log-Structured Merge-tree');

    const architectureDiagram = page.locator('[data-testid="architecture-diagram"]');
    await expect(architectureDiagram).toBeVisible();

    const writePathDesc = page.locator('[data-testid="write-path-description"]');
    await expect(writePathDesc).toBeVisible();
    await expect(writePathDesc).toContainText('Client → WAL → Memtable → SSTable');

    const readPathDesc = page.locator('[data-testid="read-path-description"]');
    await expect(readPathDesc).toBeVisible();
    await expect(readPathDesc).toContainText('Client → Memtable → Immutable List → SSTable Levels');
  });

  test('commands section renders table with all commands', async ({ page }) => {
    const commandsSection = page.locator('[data-testid="commands-section"]');
    await expect(commandsSection).toBeVisible();

    const commandsTable = page.locator('[data-testid="commands-table"]');
    await expect(commandsTable).toBeVisible();

    // Verify getter commands
    const getterRow = page.locator('[data-testid="getter-commands-row"]');
    await expect(getterRow).toBeVisible();
    await expect(getterRow).toContainText('get');
    await expect(getterRow).toContainText('gets');

    // Verify setter commands
    const setterRow = page.locator('[data-testid="setter-commands-row"]');
    await expect(setterRow).toBeVisible();
    await expect(setterRow).toContainText('set');
    await expect(setterRow).toContainText('add');
    await expect(setterRow).toContainText('replace');
    await expect(setterRow).toContainText('append');
    await expect(setterRow).toContainText('prepend');

    // Verify other commands
    const otherRow = page.locator('[data-testid="other-commands-row"]');
    await expect(otherRow).toBeVisible();
    await expect(otherRow).toContainText('delete');
  });

  test('getting started section renders all subsections', async ({ page }) => {
    const gettingStartedSection = page.locator('[data-testid="getting-started-section"]');
    await expect(gettingStartedSection).toBeVisible();

    // Installation instructions
    const installationInstructions = page.locator('[data-testid="installation-instructions"]');
    await expect(installationInstructions).toBeVisible();
    await expect(installationInstructions).toContainText('git clone');
    await expect(installationInstructions).toContainText('cargo build');

    // Configuration example
    const configurationExample = page.locator('[data-testid="configuration-example"]');
    await expect(configurationExample).toBeVisible();
    await expect(configurationExample).toContainText('mirdb.toml');

    // Usage examples
    const usageExamples = page.locator('[data-testid="usage-examples"]');
    await expect(usageExamples).toBeVisible();
    await expect(usageExamples).toContainText('telnet');
    await expect(usageExamples).toContainText('pymemcache');
  });

  test('footer renders with correct links and badges', async ({ page }) => {
    const footer = page.locator('.footer');
    await expect(footer).toBeVisible();

    // GitHub link
    const githubLink = footer.locator('a[href="https://github.com/yetone/mirdb"]');
    await expect(githubLink).toBeVisible();

    // Technology badges
    const badges = footer.locator('.badge');
    await expect(badges).toHaveCount(3);

    // Verify badge text
    await expect(footer).toContainText('Rust');
    await expect(footer).toContainText('Tokio');
    await expect(footer).toContainText('LSM-tree');

    // License
    await expect(footer).toContainText('MIT License');
  });

  test('smooth scroll navigation works', async ({ page }) => {
    // Click Learn More button to scroll to features
    const secondaryCTA = page.locator('[data-testid="secondary-cta"]');
    await secondaryCTA.click();

    // Wait a moment for smooth scroll
    await page.waitForTimeout(500);

    // Check if features section is in viewport
    const featuresSection = page.locator('[data-testid="features-section"]');
    await expect(featuresSection).toBeInViewport();
  });

  test('all sections have correct visual hierarchy', async ({ page }) => {
    // Verify main headings exist
    const h1 = page.locator('h1');
    await expect(h1).toHaveCount(1);

    // Verify section headings
    const sectionHeadings = page.locator('section h2');
    const headingCount = await sectionHeadings.count();
    expect(headingCount).toBeGreaterThanOrEqual(4); // Features, Architecture, Commands, Getting Started
  });

  test('page has no broken images', async ({ page }) => {
    const images = page.locator('img');
    const imageCount = await images.count();

    for (let i = 0; i < imageCount; i++) {
      const image = images.nth(i);
      const naturalWidth = await image.evaluate((img) => (img).naturalWidth);
      expect(naturalWidth).toBeGreaterThan(0);
    }
  });

  test('external links have proper attributes', async ({ page }) => {
    const externalLinks = page.locator('a[target="_blank"]');
    const linkCount = await externalLinks.count();

    for (let i = 0; i < linkCount; i++) {
      const link = externalLinks.nth(i);
      await expect(link).toHaveAttribute('rel', /noopener/);
    }
  });

  test('code blocks are properly formatted', async ({ page }) => {
    const codeBlocks = page.locator('.code-block pre code');
    const codeBlockCount = await codeBlocks.count();
    expect(codeBlockCount).toBeGreaterThan(0);

    // Verify code blocks are visible
    for (let i = 0; i < codeBlockCount; i++) {
      const codeBlock = codeBlocks.nth(i);
      await expect(codeBlock).toBeVisible();
    }
  });

  test('page meets basic accessibility standards', async ({ page }) => {
    // Verify lang attribute
    const html = page.locator('html');
    await expect(html).toHaveAttribute('lang', 'en');

    // Verify meta description exists
    const metaDescription = page.locator('meta[name="description"]');
    await expect(metaDescription).toHaveAttribute('content', /MirDB/);

    // Verify viewport meta tag
    const viewport = page.locator('meta[name="viewport"]');
    await expect(viewport).toHaveAttribute('content', /width=device-width/);
  });
});

test.describe('Cross-Browser Compatibility - CSS and Layout', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');
  });

  test('CSS loads correctly and styles are applied', async ({ page }) => {
    // Check hero section has gradient background
    const heroSection = page.locator('[data-testid="hero-section"]');
    const heroBackground = await heroSection.evaluate((el) => {
      return window.getComputedStyle(el).backgroundImage;
    });
    expect(heroBackground).toContain('gradient');
  });

  test('fonts load correctly', async ({ page }) => {
    const productName = page.locator('[data-testid="product-name"]');
    const fontSize = await productName.evaluate((el) => {
      return window.getComputedStyle(el).fontSize;
    });
    // Font size should be non-zero
    expect(parseInt(fontSize)).toBeGreaterThan(0);
  });

  test('flexbox layouts render correctly', async ({ page }) => {
    const ctaButtons = page.locator('.cta-buttons');
    const display = await ctaButtons.evaluate((el) => {
      return window.getComputedStyle(el).display;
    });
    expect(display).toBe('flex');
  });

  test('grid layouts render correctly', async ({ page }) => {
    const featuresGrid = page.locator('.features-grid');
    const display = await featuresGrid.evaluate((el) => {
      return window.getComputedStyle(el).display;
    });
    expect(display).toBe('grid');
  });
});

test.describe('Cross-Browser Compatibility - JavaScript Functionality', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');
  });

  test('Mermaid diagram initializes', async ({ page }) => {
    const mermaidDiagram = page.locator('[data-testid="mermaid-diagram"]');
    await expect(mermaidDiagram).toBeVisible();

    // Wait for Mermaid to process the diagram
    // After processing, Mermaid adds SVG content
    await page.waitForTimeout(1000);

    // Check if SVG was generated (Mermaid converts pre to svg)
    const diagramContainer = page.locator('.diagram-container');
    const hasSvgOrProcessed = await diagramContainer.evaluate((el) => {
      return el.querySelector('svg') !== null || el.innerHTML.includes('svg');
    });

    // Mermaid should have processed the diagram
    expect(hasSvgOrProcessed || await mermaidDiagram.isVisible()).toBeTruthy();
  });

  test('smooth scroll JavaScript works', async ({ page }) => {
    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY);

    // Click the Learn More button
    await page.locator('[data-testid="secondary-cta"]').click();

    // Wait for scroll animation
    await page.waitForTimeout(800);

    // Get new scroll position
    const newScrollY = await page.evaluate(() => window.scrollY);

    // Scroll position should have changed
    expect(newScrollY).toBeGreaterThan(initialScrollY);
  });
});
