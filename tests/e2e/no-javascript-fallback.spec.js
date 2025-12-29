// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

/**
 * No-JavaScript Fallback Tests (NFR-4)
 *
 * Verifies that core content is visible without JavaScript enabled.
 * This ensures the page degrades gracefully and remains functional
 * for users who have JavaScript disabled.
 */

const indexPath = 'file://' + path.resolve(__dirname, '../../index.html');

// Test configuration: Create a separate context with JavaScript disabled
test.describe('No-JavaScript Fallback', () => {

  test.beforeEach(async ({ browser }) => {
    // All tests in this describe block will run with JS disabled
  });

  test('TC1: Hero section content is visible without JavaScript', async ({ browser }) => {
    // Create a new context with JavaScript disabled
    const context = await browser.newContext({ javaScriptEnabled: false });
    const page = await context.newPage();

    // Navigate to the page
    await page.goto(indexPath);

    // Verify the hero section is visible
    const heroSection = page.locator('.hero');
    await expect(heroSection).toBeVisible();

    // Verify hero title (MirDB)
    const heroTitle = page.locator('.hero-title');
    await expect(heroTitle).toBeVisible();
    await expect(heroTitle).toHaveText('MirDB');

    // Verify hero tagline
    const heroTagline = page.locator('.hero-tagline');
    await expect(heroTagline).toBeVisible();
    await expect(heroTagline).toContainText('Persistent Key-Value Store');

    // Verify hero description
    const heroDescription = page.locator('.hero-description');
    await expect(heroDescription).toBeVisible();
    await expect(heroDescription).toContainText('MirDB is a persistent key-value store');

    // Verify CTA buttons are visible
    const ctaButtons = page.locator('.hero-cta .btn');
    await expect(ctaButtons.first()).toBeVisible();
    await expect(ctaButtons.first()).toContainText('Get Started');

    await context.close();
  });

  test('TC2: Feature cards and content are visible without JavaScript', async ({ browser }) => {
    // Create a new context with JavaScript disabled
    const context = await browser.newContext({ javaScriptEnabled: false });
    const page = await context.newPage();

    await page.goto(indexPath);

    // Verify features section is visible
    const featuresSection = page.locator('.features');
    await expect(featuresSection).toBeVisible();

    // Verify section title
    const sectionTitle = page.locator('.features .section-title');
    await expect(sectionTitle).toBeVisible();
    await expect(sectionTitle).toHaveText('Key Features');

    // Verify all three feature cards are visible
    const featureCards = page.locator('.feature-card');
    await expect(featureCards).toHaveCount(3);

    // Verify first feature card (Memcached Compatible)
    const firstCard = featureCards.nth(0);
    await expect(firstCard).toBeVisible();
    await expect(firstCard.locator('.feature-title')).toContainText('Memcached Compatible');
    await expect(firstCard.locator('.feature-description')).toContainText('Drop-in replacement');

    // Verify second feature card (Persistent Storage)
    const secondCard = featureCards.nth(1);
    await expect(secondCard).toBeVisible();
    await expect(secondCard.locator('.feature-title')).toContainText('Persistent Storage');
    await expect(secondCard.locator('.feature-description')).toContainText('SSTable');

    // Verify third feature card (High Performance)
    const thirdCard = featureCards.nth(2);
    await expect(thirdCard).toBeVisible();
    await expect(thirdCard.locator('.feature-title')).toContainText('High Performance');
    await expect(thirdCard.locator('.feature-description')).toContainText('LSM tree');

    // Verify feature icons are visible (SVGs)
    const featureIcons = page.locator('.feature-icon svg');
    await expect(featureIcons).toHaveCount(3);
    for (let i = 0; i < 3; i++) {
      await expect(featureIcons.nth(i)).toBeVisible();
    }

    await context.close();
  });

  test('TC3: Code examples are readable without JavaScript', async ({ browser }) => {
    // Create a new context with JavaScript disabled
    const context = await browser.newContext({ javaScriptEnabled: false });
    const page = await context.newPage();

    await page.goto(indexPath);

    // Verify code examples section is visible
    const codeExamplesSection = page.locator('.code-examples');
    await expect(codeExamplesSection).toBeVisible();

    // Verify section title
    const sectionTitle = page.locator('.code-examples .section-title');
    await expect(sectionTitle).toBeVisible();
    await expect(sectionTitle).toHaveText('Code Examples');

    // Verify code blocks are visible
    const codeBlocks = page.locator('.code-examples .code-block');
    await expect(codeBlocks).toHaveCount(3);

    // Verify Connect example is readable
    const connectCode = page.locator('#code-connect');
    await expect(connectCode).toBeVisible();
    const connectText = await connectCode.textContent();
    expect(connectText).toContain('import memcache');
    expect(connectText).toContain('memcache.Client');
    expect(connectText).toContain('127.0.0.1:12333');

    // Verify SET example is readable
    const setCode = page.locator('#code-set');
    await expect(setCode).toBeVisible();
    const setText = await setCode.textContent();
    expect(setText).toContain('mc.set');
    expect(setText).toContain('user:1');

    // Verify GET example is readable
    const getCode = page.locator('#code-get');
    await expect(getCode).toBeVisible();
    const getText = await getCode.textContent();
    expect(getText).toContain('mc.get');
    expect(getText).toContain('John Doe');
    expect(getText).toContain('mc.delete');

    // Verify <pre> and <code> elements are rendered properly
    const preElements = page.locator('.code-examples pre');
    await expect(preElements).toHaveCount(3);
    for (let i = 0; i < 3; i++) {
      await expect(preElements.nth(i)).toBeVisible();
    }

    await context.close();
  });

  test('TC4: Navigation links are functional without JavaScript', async ({ browser }) => {
    // Create a new context with JavaScript disabled
    const context = await browser.newContext({ javaScriptEnabled: false });
    const page = await context.newPage();

    await page.goto(indexPath);

    // Verify hero CTA links are present and have correct hrefs
    const getStartedBtn = page.locator('.hero-cta a:has-text("Get Started")');
    await expect(getStartedBtn).toBeVisible();
    await expect(getStartedBtn).toHaveAttribute('href', '#getting-started');

    const githubBtn = page.locator('.hero-cta a:has-text("View on GitHub")');
    await expect(githubBtn).toBeVisible();
    await expect(githubBtn).toHaveAttribute('href', 'https://github.com/yetone/mirdb');

    // Verify footer link to GitHub
    const footerGithubLink = page.locator('.footer a:has-text("GitHub")');
    await expect(footerGithubLink).toBeVisible();
    await expect(footerGithubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');

    // Verify documentation link
    const docsLink = page.locator('.getting-started a:has-text("full documentation")');
    await expect(docsLink).toBeVisible();
    await expect(docsLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb#readme');

    // Verify internal anchor link works (Get Started -> #getting-started section)
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeVisible();

    // Click on Get Started and verify section is still accessible
    await getStartedBtn.click();
    await expect(page).toHaveURL(/#getting-started/);

    // Verify other sections are accessible via hash navigation
    const sections = ['#features', '#architecture', '#code-examples', '#configuration', '#commands'];
    for (const section of sections) {
      const sectionElement = page.locator(section);
      await expect(sectionElement).toBeVisible();
    }

    await context.close();
  });

  test('Additional: Architecture section is visible without JavaScript', async ({ browser }) => {
    // Create a new context with JavaScript disabled
    const context = await browser.newContext({ javaScriptEnabled: false });
    const page = await context.newPage();

    await page.goto(indexPath);

    // Verify architecture section
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    // Verify the SVG diagram is present and visible
    const architectureDiagram = page.locator('.architecture-diagram');
    await expect(architectureDiagram).toBeVisible();

    // Verify the diagram has alt text for accessibility
    const diagramTitle = page.locator('.architecture-diagram title');
    await expect(diagramTitle).toContainText('LSM Tree Architecture');

    // Verify architecture explanations are visible
    const writePathHeading = page.locator('.architecture-path h3:has-text("Write Path")');
    await expect(writePathHeading).toBeVisible();

    const readPathHeading = page.locator('.architecture-path h3:has-text("Read Path")');
    await expect(readPathHeading).toBeVisible();

    await context.close();
  });

  test('Additional: Getting started section is visible without JavaScript', async ({ browser }) => {
    // Create a new context with JavaScript disabled
    const context = await browser.newContext({ javaScriptEnabled: false });
    const page = await context.newPage();

    await page.goto(indexPath);

    // Verify getting started section
    const gettingStartedSection = page.locator('.getting-started');
    await expect(gettingStartedSection).toBeVisible();

    // Verify installation code block
    const installCode = page.locator('#code-install');
    await expect(installCode).toBeVisible();
    const installText = await installCode.textContent();
    expect(installText).toContain('git clone');
    expect(installText).toContain('cargo build');
    expect(installText).toContain('mirdb.toml');

    await context.close();
  });

  test('Additional: Configuration section is visible without JavaScript', async ({ browser }) => {
    // Create a new context with JavaScript disabled
    const context = await browser.newContext({ javaScriptEnabled: false });
    const page = await context.newPage();

    await page.goto(indexPath);

    // Verify configuration section
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    // Verify config tables are visible
    const configTables = page.locator('.config-table');
    await expect(configTables.first()).toBeVisible();

    // Verify configuration code example
    const configCode = page.locator('#code-config');
    await expect(configCode).toBeVisible();
    const configText = await configCode.textContent();
    expect(configText).toContain('addr');
    expect(configText).toContain('mem_table_max_size');

    await context.close();
  });

  test('Additional: Supported commands section is visible without JavaScript', async ({ browser }) => {
    // Create a new context with JavaScript disabled
    const context = await browser.newContext({ javaScriptEnabled: false });
    const page = await context.newPage();

    await page.goto(indexPath);

    // Verify commands section
    const commandsSection = page.locator('#commands');
    await expect(commandsSection).toBeVisible();

    // Verify command groups are visible
    const commandGroups = page.locator('.command-group');
    await expect(commandGroups).toHaveCount(4);

    // Verify specific commands are listed
    const setCommand = page.locator('.commands code').filter({ hasText: /^SET$/ });
    await expect(setCommand).toBeVisible();

    const getCommand = page.locator('.commands code').filter({ hasText: /^GET$/ });
    await expect(getCommand).toBeVisible();

    const deleteCommand = page.locator('.commands code').filter({ hasText: /^DELETE$/ });
    await expect(deleteCommand).toBeVisible();

    await context.close();
  });

});
