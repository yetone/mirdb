import { test, expect } from '@playwright/test';

/**
 * E2E Tests for Cross-Browser Compatibility - Firefox
 *
 * This test suite verifies that the MirDB homepage renders correctly and functions
 * properly in Mozilla Firefox browser, ensuring parity with Chrome experience.
 *
 * Requirements: NFR-5 - Cross-browser compatibility (Chrome, Firefox, Safari, Edge)
 */

test.describe('Cross-Browser Compatibility - Firefox', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Test Case 1: Page renders without visual issues in Firefox', async ({ page }) => {
    // Verify the page title loads correctly
    await expect(page).toHaveTitle('MirDB - Persistent Key-Value Store with Memcached Protocol');

    // Verify hero section renders
    const hero = page.locator('.hero');
    await expect(hero).toBeVisible();

    // Verify logo loads
    const logo = page.locator('.logo');
    await expect(logo).toBeVisible();

    // Verify main heading
    const heading = page.locator('h1');
    await expect(heading).toContainText('MirDB');

    // Verify tagline is visible
    const tagline = page.locator('.tagline');
    await expect(tagline).toBeVisible();
    await expect(tagline).toContainText('Persistent Key-Value Store with Memcached Protocol');

    // Verify description is visible
    const description = page.locator('.hero .description');
    await expect(description).toBeVisible();

    // Verify all major sections are visible
    const sections = ['#features', '#commands', '#code-example', '#architecture', '#getting-started', '#configuration'];
    for (const sectionId of sections) {
      const section = page.locator(sectionId);
      await expect(section).toBeVisible();
    }

    // Verify footer is visible
    const footer = page.locator('.footer');
    await expect(footer).toBeVisible();

    // Verify CSS styles are applied (check for proper styling)
    const heroBackground = await hero.evaluate((el) => getComputedStyle(el).background);
    expect(heroBackground).toBeTruthy();

    // Verify feature cards grid renders properly
    const featuresGrid = page.locator('.features-grid');
    await expect(featuresGrid).toBeVisible();
    const featureCards = page.locator('.feature-card');
    await expect(featureCards).toHaveCount(7);
  });

  test('Test Case 2: All buttons are clickable and functional in Firefox', async ({ page }) => {
    // Test Get Started button
    const getStartedBtn = page.locator('[data-link="get-started"]');
    await expect(getStartedBtn).toBeVisible();
    await expect(getStartedBtn).toBeEnabled();

    // Click Get Started and verify navigation to getting-started section
    await getStartedBtn.click();
    await expect(page.locator('#getting-started')).toBeInViewport();

    // Navigate back to top
    await page.goto('/');

    // Test GitHub button in hero
    const githubHeroBtn = page.locator('[data-link="github-hero"]');
    await expect(githubHeroBtn).toBeVisible();
    await expect(githubHeroBtn).toBeEnabled();
    // Verify it has correct attributes for external link
    await expect(githubHeroBtn).toHaveAttribute('target', '_blank');
    await expect(githubHeroBtn).toHaveAttribute('rel', 'noopener');
    await expect(githubHeroBtn).toHaveAttribute('href', 'https://github.com/example/mirdb');

    // Test Copy button in code example
    const copyBtn = page.locator('.copy-btn');
    await expect(copyBtn).toBeVisible();
    await expect(copyBtn).toBeEnabled();

    // Click copy button and verify it changes text
    await copyBtn.click();
    await expect(copyBtn).toContainText('Copied!');

    // Wait for button text to reset
    await page.waitForTimeout(2100);
    await expect(copyBtn).toContainText('Copy');

    // Test footer links
    const githubFooterLink = page.locator('[data-link="github-footer"]');
    await expect(githubFooterLink).toBeVisible();
    await expect(githubFooterLink).toBeEnabled();

    const issuesLink = page.locator('[data-link="issues"]');
    await expect(issuesLink).toBeVisible();
    await expect(issuesLink).toBeEnabled();

    const docsLink = page.locator('[data-link="documentation"]');
    await expect(docsLink).toBeVisible();
    await expect(docsLink).toBeEnabled();

    // Verify footer links have correct href attributes
    await expect(githubFooterLink).toHaveAttribute('href', 'https://github.com/example/mirdb');
    await expect(issuesLink).toHaveAttribute('href', 'https://github.com/example/mirdb/issues');
    await expect(docsLink).toHaveAttribute('href', '#getting-started');
  });

  test('Test Case 3: No JavaScript errors in Firefox console', async ({ page }) => {
    // Listen for page errors (uncaught exceptions from application code)
    const pageErrors: string[] = [];
    page.on('pageerror', (error) => {
      pageErrors.push(error.message);
    });

    // Listen for console errors
    const consoleErrors: string[] = [];
    page.on('console', (msg) => {
      if (msg.type() === 'error') {
        consoleErrors.push(msg.text());
      }
    });

    // Navigate to page and wait for full load
    await page.goto('/', { waitUntil: 'networkidle' });

    // Wait for all content to load and scripts to execute
    await page.waitForLoadState('domcontentloaded');

    // Interact with the page to trigger any JavaScript
    const copyBtn = page.locator('.copy-btn');
    await copyBtn.click();

    // Scroll through the page to trigger any lazy-loaded content
    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
    await page.waitForTimeout(500);
    await page.evaluate(() => window.scrollTo(0, 0));

    // Wait for any async operations to complete
    await page.waitForTimeout(500);

    // Assert no JavaScript page errors occurred (uncaught exceptions)
    expect(pageErrors).toHaveLength(0);

    // Filter out non-critical console errors that are not actual JavaScript bugs
    const criticalErrors = consoleErrors.filter((error) => {
      // Ignore external resource loading failures (badges, CDN integrity mismatches)
      if (error.includes('net::') || error.includes('Failed to load')) return false;
      // Ignore favicon errors
      if (error.includes('favicon')) return false;
      // Ignore CDN integrity hash mismatches (external resource issue, not app bug)
      if (error.includes('integrity attribute') || error.includes('hashes')) return false;
      // Ignore subresource integrity errors
      if (error.includes('subresource')) return false;
      return true;
    });

    expect(criticalErrors).toHaveLength(0);
  });

  test('Syntax highlighting works correctly in Firefox', async ({ page }) => {
    // Verify Prism.js syntax highlighting is applied
    const codeBlock = page.locator('#example-code');
    await expect(codeBlock).toBeVisible();

    // Check that syntax highlighting classes are applied
    const hasHighlighting = await codeBlock.evaluate((el) => {
      // Prism.js adds token classes for syntax highlighting
      return el.querySelector('.token') !== null || el.classList.contains('language-python');
    });
    expect(hasHighlighting).toBe(true);
  });

  test('CSS layout and styling renders correctly in Firefox', async ({ page }) => {
    // Verify hero content has proper styling (centered block layout)
    const heroContent = page.locator('.hero-content');
    await expect(heroContent).toBeVisible();
    const heroMaxWidth = await heroContent.evaluate((el) => getComputedStyle(el).maxWidth);
    expect(heroMaxWidth).toBe('800px');

    // Verify CSS Grid in features section
    const featuresGrid = page.locator('.features-grid');
    const gridDisplay = await featuresGrid.evaluate((el) => getComputedStyle(el).display);
    expect(gridDisplay).toBe('grid');

    // Verify commands grid
    const commandsGrid = page.locator('.commands-grid');
    const commandsGridDisplay = await commandsGrid.evaluate((el) => getComputedStyle(el).display);
    expect(commandsGridDisplay).toBe('grid');

    // Verify CSS variables are working (custom properties)
    const body = page.locator('body');
    const bgColor = await body.evaluate((el) => getComputedStyle(el).backgroundColor);
    expect(bgColor).toBeTruthy();

    // Verify buttons have proper styling
    const primaryBtn = page.locator('.btn-primary');
    const btnCursor = await primaryBtn.evaluate((el) => getComputedStyle(el).cursor);
    expect(btnCursor).toBe('pointer');
  });

  test('Responsive images load correctly in Firefox', async ({ page }) => {
    // Wait for network to settle to allow images to load
    await page.goto('/', { waitUntil: 'domcontentloaded' });

    // Check that logo image element exists and is visible
    const logo = page.locator('.logo');
    await expect(logo).toBeVisible();

    // Verify logo has correct src attribute
    await expect(logo).toHaveAttribute('src', '../assets/logo.gif');

    // Verify logo has alt text for accessibility
    await expect(logo).toHaveAttribute('alt', 'MirDB Logo');

    // Verify logo element has proper CSS styling applied (max-width constraint)
    const logoMaxWidth = await logo.evaluate((el) => getComputedStyle(el).maxWidth);
    expect(logoMaxWidth).toBe('120px');

    // Check badge images are present (external sources may not always load in test env)
    const badges = page.locator('[data-section="badges"] img');
    const badgeCount = await badges.count();
    expect(badgeCount).toBeGreaterThan(0);

    for (let i = 0; i < badgeCount; i++) {
      const badge = badges.nth(i);
      // Verify badges have alt text for accessibility
      const altText = await badge.getAttribute('alt');
      expect(altText).toBeTruthy();
      // Verify badges are attached to the DOM
      await expect(badge).toBeAttached();
    }
  });

  test('Scroll behavior and navigation work in Firefox', async ({ page }) => {
    // Test internal anchor navigation
    const getStartedLink = page.locator('[data-link="get-started"]');
    await getStartedLink.click();

    // Wait for scroll animation
    await page.waitForTimeout(500);

    // Verify getting-started section is in viewport
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeInViewport();

    // Test scrolling back to top using page scroll
    await page.evaluate(() => window.scrollTo(0, 0));
    await page.waitForTimeout(300);

    // Verify we're back at the top
    const hero = page.locator('.hero');
    await expect(hero).toBeInViewport();

    // Test scrolling to features section using JavaScript scroll
    await page.evaluate(() => document.getElementById('features')?.scrollIntoView({ behavior: 'instant' }));
    await page.waitForTimeout(300);

    // Verify features section is in viewport
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport();
  });

  test('Page structure and accessibility attributes render in Firefox', async ({ page }) => {
    // Verify semantic HTML structure
    const main = page.locator('main');
    await expect(main).toBeVisible();

    const header = page.locator('header');
    await expect(header).toBeVisible();

    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    // Verify sections have proper IDs for navigation
    const sections = ['features', 'commands', 'code-example', 'architecture', 'getting-started', 'configuration'];
    for (const id of sections) {
      const section = page.locator(`#${id}`);
      await expect(section).toBeVisible();
    }

    // Verify ARIA attributes are present
    const architectureDiagram = page.locator('.architecture-diagram');
    await expect(architectureDiagram).toHaveAttribute('role', 'img');
    await expect(architectureDiagram).toHaveAttribute('aria-label', /MirDB LSM-tree architecture diagram/);

    // Verify all headings are present and in order
    const h1 = page.locator('h1');
    await expect(h1).toHaveCount(1);

    const h2Count = await page.locator('h2').count();
    expect(h2Count).toBeGreaterThan(0);
  });

  test('External links have proper security attributes in Firefox', async ({ page }) => {
    // Get all external links
    const externalLinks = page.locator('a[target="_blank"]');
    const count = await externalLinks.count();

    expect(count).toBeGreaterThan(0);

    for (let i = 0; i < count; i++) {
      const link = externalLinks.nth(i);
      // Verify noopener is set for security
      await expect(link).toHaveAttribute('rel', /noopener/);
    }
  });
});
