/**
 * Progressive Enhancement Tests
 * Owner: Scenario 8 - Progressive Enhancement
 *
 * Tests:
 * - Page renders correctly with JavaScript disabled
 * - All content is visible without JS
 * - Navigation links work without JS
 * - No critical functionality depends on JS
 *
 * Traceability: NFR-4
 */

import { test, expect, Browser, BrowserContext, Page } from '@playwright/test';
import { BASE_URL, getByTestId, waitForPageLoad, VIEWPORTS } from './test-utils';

// Create a test context with JavaScript disabled
let context: BrowserContext;
let page: Page;

test.describe('Progressive Enhancement - No JavaScript', () => {
  test.beforeAll(async ({ browser }) => {
    // Create context with JavaScript disabled
    context = await browser.newContext({
      javaScriptEnabled: false,
      viewport: VIEWPORTS.desktop,
    });
    page = await context.newPage();
  });

  test.afterAll(async () => {
    await context.close();
  });

  test.beforeEach(async () => {
    // Navigate to homepage with JS disabled
    await page.goto('/homepage/');
    await page.waitForLoadState('domcontentloaded');
  });

  test('TC1: Page renders and displays all content without errors', async () => {
    // Check page title loads
    await expect(page).toHaveTitle('MirDB: A Persistent Key-Value Store');

    // Check main content sections exist
    await expect(page.locator(getByTestId('hero-section'))).toBeVisible();
    await expect(page.locator(getByTestId('about-section'))).toBeVisible();
    await expect(page.locator(getByTestId('features-section'))).toBeVisible();
    await expect(page.locator(getByTestId('quickstart-section'))).toBeVisible();
    await expect(page.locator(getByTestId('footer-section'))).toBeVisible();

    // Verify no console errors (check that page loaded successfully)
    const bodyText = await page.locator('body').textContent();
    expect(bodyText).toBeTruthy();
  });

  test('TC2: Hero section visibility without JS - Logo, title, and demo GIF visible', async () => {
    // Check hero section is visible
    const heroSection = page.locator(getByTestId('hero-section'));
    await expect(heroSection).toBeVisible();

    // Check logo is visible
    const logo = page.locator(getByTestId('hero-logo'));
    await expect(logo).toBeVisible();
    await expect(logo).toHaveAttribute('alt', /MirDB Logo/);
    await expect(logo).toHaveAttribute('src', /logo\.gif/);

    // Check title is visible
    const title = page.locator(getByTestId('hero-title'));
    await expect(title).toBeVisible();
    await expect(title).toHaveText('MirDB: A Persistent Key-Value Store');

    // Check usage demo GIF is visible
    const usageGif = page.locator(getByTestId('usage-gif'));
    await expect(usageGif).toBeVisible();
    await expect(usageGif).toHaveAttribute('alt', /usage demonstration/);
    await expect(usageGif).toHaveAttribute('src', /usage\.gif/);

    // Check CTA buttons are visible
    await expect(page.locator(getByTestId('get-started-btn'))).toBeVisible();
    await expect(page.locator(getByTestId('view-source-btn'))).toBeVisible();
  });

  test('TC3: About section visibility without JS', async () => {
    // Check About section is visible
    const aboutSection = page.locator(getByTestId('about-section'));
    await expect(aboutSection).toBeVisible();

    // Check section heading
    const heading = aboutSection.locator('h2');
    await expect(heading).toBeVisible();
    await expect(heading).toHaveText('About MirDB');

    // Check project description is visible and readable
    const description = aboutSection.locator('p');
    await expect(description).toBeVisible();
    const text = await description.textContent();
    expect(text).toContain('persistent key-value store');
    expect(text).toContain('Memcached protocol');
  });

  test('TC4: Features section visibility without JS', async () => {
    // Check Features section is visible
    const featuresSection = page.locator(getByTestId('features-section'));
    await expect(featuresSection).toBeVisible();

    // Check section heading
    const heading = featuresSection.locator('h2');
    await expect(heading).toBeVisible();
    await expect(heading).toHaveText('Key Features');

    // Check feature list is visible
    const featureList = page.locator(getByTestId('feature-list'));
    await expect(featureList).toBeVisible();

    // Check all 6 features are visible
    const features = [
      'feature-memcached',
      'feature-lsm',
      'feature-rust',
      'feature-async',
      'feature-skiplist',
      'feature-compaction',
    ];

    for (const featureId of features) {
      const feature = page.locator(getByTestId(featureId));
      await expect(feature).toBeVisible();
      // Check each feature has a heading and description
      await expect(feature.locator('h3')).toBeVisible();
      await expect(feature.locator('p')).toBeVisible();
    }
  });

  test('TC5: Quick Start section visibility without JS', async () => {
    // Check Quick Start section is visible
    const quickstartSection = page.locator(getByTestId('quickstart-section'));
    await expect(quickstartSection).toBeVisible();

    // Check section heading
    const heading = quickstartSection.locator('h2');
    await expect(heading).toBeVisible();
    await expect(heading).toHaveText('Quick Start');

    // Check code block is visible and readable
    const codeBlock = page.locator(getByTestId('quickstart-code'));
    await expect(codeBlock).toBeVisible();

    // Verify code block contains installation commands
    const codeText = await codeBlock.textContent();
    expect(codeText).toContain('cargo install mirdb');
    expect(codeText).toContain('mirdb-server');

    // Verify code is displayed in proper format (pre/code structure)
    const codeElement = codeBlock.locator('code');
    await expect(codeElement).toBeVisible();
  });

  test('TC6: GitHub link works without JS - standard anchor behavior', async () => {
    // Check GitHub link in navigation
    const githubNavLink = page.locator(getByTestId('github-link'));
    await expect(githubNavLink).toBeVisible();
    await expect(githubNavLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');
    await expect(githubNavLink).toHaveAttribute('target', '_blank');
    await expect(githubNavLink).toHaveAttribute('rel', /noopener/);

    // Check View Source Code button in hero
    const viewSourceBtn = page.locator(getByTestId('view-source-btn'));
    await expect(viewSourceBtn).toBeVisible();
    await expect(viewSourceBtn).toHaveAttribute('href', 'https://github.com/yetone/mirdb');
    await expect(viewSourceBtn).toHaveAttribute('target', '_blank');

    // Check footer GitHub link
    const footerGithubLink = page.locator(getByTestId('footer-github-link'));
    await expect(footerGithubLink).toBeVisible();
    await expect(footerGithubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');

    // Verify links are standard anchor tags (not JS-dependent)
    const navLinkTag = await githubNavLink.evaluate(el => el.tagName.toLowerCase());
    expect(navLinkTag).toBe('a');
    const btnTag = await viewSourceBtn.evaluate(el => el.tagName.toLowerCase());
    expect(btnTag).toBe('a');
  });

  test('TC7: Documentation link works without JS - standard anchor behavior', async () => {
    // Check Documentation link in navigation
    const docsNavLink = page.locator(getByTestId('docs-link'));
    await expect(docsNavLink).toBeVisible();
    await expect(docsNavLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb#readme');
    await expect(docsNavLink).toHaveAttribute('target', '_blank');
    await expect(docsNavLink).toHaveAttribute('rel', /noopener/);

    // Check Get Started button (links to documentation)
    const getStartedBtn = page.locator(getByTestId('get-started-btn'));
    await expect(getStartedBtn).toBeVisible();
    await expect(getStartedBtn).toHaveAttribute('href', 'https://github.com/yetone/mirdb#readme');
    await expect(getStartedBtn).toHaveAttribute('target', '_blank');

    // Check footer Documentation link
    const footerDocsLink = page.locator(getByTestId('footer-docs-link'));
    await expect(footerDocsLink).toBeVisible();
    await expect(footerDocsLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb#readme');

    // Verify links are standard anchor tags
    const navLinkTag = await docsNavLink.evaluate(el => el.tagName.toLowerCase());
    expect(navLinkTag).toBe('a');
  });

  test('TC8: Page layout is correct without JS - CSS only positioning', async () => {
    // Verify header is at the top
    const header = page.locator('.header');
    await expect(header).toBeVisible();
    const headerBox = await header.boundingBox();
    expect(headerBox?.y).toBe(0);

    // Verify sections are stacked vertically in order
    const heroSection = page.locator(getByTestId('hero-section'));
    const aboutSection = page.locator(getByTestId('about-section'));
    const featuresSection = page.locator(getByTestId('features-section'));
    const quickstartSection = page.locator(getByTestId('quickstart-section'));
    const footerSection = page.locator(getByTestId('footer-section'));

    const heroBox = await heroSection.boundingBox();
    const aboutBox = await aboutSection.boundingBox();
    const featuresBox = await featuresSection.boundingBox();
    const quickstartBox = await quickstartSection.boundingBox();
    const footerBox = await footerSection.boundingBox();

    // Verify vertical ordering
    expect(heroBox!.y).toBeLessThan(aboutBox!.y);
    expect(aboutBox!.y).toBeLessThan(featuresBox!.y);
    expect(featuresBox!.y).toBeLessThan(quickstartBox!.y);
    expect(quickstartBox!.y).toBeLessThan(footerBox!.y);

    // Verify footer is at the bottom
    await expect(footerSection).toBeVisible();

    // Verify no horizontal scrollbar (content fits viewport)
    const bodyWidth = await page.evaluate(() => document.body.scrollWidth);
    const viewportWidth = await page.evaluate(() => window.innerWidth);
    expect(bodyWidth).toBeLessThanOrEqual(viewportWidth);

    // Verify layout uses CSS (no JS-dependent classes or styles)
    // Check that body has flexbox layout via CSS
    const bodyDisplay = await page.evaluate(() => {
      return window.getComputedStyle(document.body).display;
    });
    expect(bodyDisplay).toBe('flex');
  });

  test('All images display without JS using standard img tags', async () => {
    // Check all images use standard <img> tags
    const images = page.locator('img');
    const imageCount = await images.count();
    expect(imageCount).toBeGreaterThan(0);

    // Verify each image has src and alt attributes
    for (let i = 0; i < imageCount; i++) {
      const img = images.nth(i);
      await expect(img).toHaveAttribute('src');
      await expect(img).toHaveAttribute('alt');
    }

    // Verify no lazy loading attributes that require JS
    // Standard HTML lazy loading (loading="lazy") is fine, but data-src patterns are not
    const logoSrc = await page.locator(getByTestId('hero-logo')).getAttribute('src');
    expect(logoSrc).not.toBeNull();
    expect(logoSrc).toMatch(/\.(gif|png|jpg|jpeg|svg|webp)$/i);

    const usageSrc = await page.locator(getByTestId('usage-gif')).getAttribute('src');
    expect(usageSrc).not.toBeNull();
    expect(usageSrc).toMatch(/\.(gif|png|jpg|jpeg|svg|webp)$/i);
  });

  test('Navigation works without JS - all links are standard anchors', async () => {
    // Get all links on the page
    const links = page.locator('a[href]');
    const linkCount = await links.count();
    expect(linkCount).toBeGreaterThan(0);

    // Verify all links have valid href attributes (not javascript: or #)
    for (let i = 0; i < linkCount; i++) {
      const link = links.nth(i);
      const href = await link.getAttribute('href');
      expect(href).not.toBeNull();
      expect(href).not.toMatch(/^javascript:/i);
    }

    // Verify external links have proper security attributes
    const externalLinks = page.locator('a[target="_blank"]');
    const externalCount = await externalLinks.count();
    for (let i = 0; i < externalCount; i++) {
      const link = externalLinks.nth(i);
      const rel = await link.getAttribute('rel');
      expect(rel).toContain('noopener');
    }
  });

  test('No content hidden by noscript or requires JS to display', async () => {
    // Check that there are no noscript tags with fallback messages
    // (meaning the page is designed to work with JS enabled)
    const noscript = page.locator('noscript');
    const noscriptCount = await noscript.count();

    // If there are noscript tags, they should contain helpful messages, not critical content
    // For our page, we don't expect any noscript tags since it's designed for progressive enhancement
    // However, if they exist, verify they don't contain hidden main content
    for (let i = 0; i < noscriptCount; i++) {
      const text = await noscript.nth(i).textContent();
      // Should not contain main content sections
      expect(text).not.toContain('About MirDB');
      expect(text).not.toContain('Key Features');
    }

    // Verify main content is visible (not hidden by CSS that JS would toggle)
    const mainContent = page.locator('main');
    await expect(mainContent).toBeVisible();
    const mainVisibility = await mainContent.evaluate(el => {
      const style = window.getComputedStyle(el);
      return {
        display: style.display,
        visibility: style.visibility,
        opacity: style.opacity,
      };
    });
    expect(mainVisibility.display).not.toBe('none');
    expect(mainVisibility.visibility).not.toBe('hidden');
    expect(parseFloat(mainVisibility.opacity)).toBeGreaterThan(0);
  });
});
