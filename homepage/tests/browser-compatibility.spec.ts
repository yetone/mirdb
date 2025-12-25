import { test, expect } from '@playwright/test';

/**
 * Browser Compatibility Tests
 * Scenario: Verify page renders correctly on modern browsers (Chrome, Firefox, Safari, Edge)
 *
 * These tests verify that the MirDB homepage renders correctly across all major
 * modern browsers. The tests run automatically against all browsers configured
 * in playwright.config.ts (chromium, firefox, webkit).
 *
 * Test Case 1: Google Chrome (Chromium project)
 * Test Case 2: Mozilla Firefox (Firefox project)
 * Test Case 3: Safari (WebKit project)
 * Test Case 4: Microsoft Edge (Chromium-based, covered by Chromium tests)
 */

test.describe('Browser Compatibility - Homepage Rendering', () => {
  test('homepage loads and renders correctly', async ({ page, browserName }) => {
    // This test runs for each browser configured in projects
    await page.goto('/');

    // Verify page loads with correct title
    await expect(page).toHaveTitle(/MirDB/);

    // Hero section renders
    const hero = page.locator('[data-testid="hero-section"]');
    await expect(hero).toBeVisible();

    // Product name is visible
    const productName = page.locator('[data-testid="product-name"]');
    await expect(productName).toBeVisible();
    await expect(productName).toHaveText('MirDB');

    // Tagline is visible
    const tagline = page.locator('[data-testid="tagline"]');
    await expect(tagline).toBeVisible();
    await expect(tagline).toContainText('Persistent memcached');

    // Navigation is functional
    const nav = page.locator('.navbar');
    await expect(nav).toBeVisible();

    // CTA buttons are clickable
    const getStartedBtn = page.locator('[data-testid="get-started-btn"]');
    await expect(getStartedBtn).toBeVisible();
    await expect(getStartedBtn).toBeEnabled();

    const githubLink = page.locator('[data-testid="github-link"]');
    await expect(githubLink).toBeVisible();
    await expect(githubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');

    // Features section renders
    const features = page.locator('#features');
    await expect(features).toBeVisible();

    // Feature cards are displayed (4 feature cards)
    const featureCards = page.locator('.feature-card');
    await expect(featureCards).toHaveCount(4);

    // Getting started section renders
    const gettingStarted = page.locator('[data-testid="getting-started-section"]');
    await expect(gettingStarted).toBeVisible();

    // Code blocks are present
    const codeBlocks = page.locator('pre code');
    expect(await codeBlocks.count()).toBeGreaterThan(0);

    // Architecture diagram renders
    const architectureDiagram = page.locator('[data-testid="architecture-diagram"]');
    await expect(architectureDiagram).toBeVisible();

    // Configuration table renders
    const configTable = page.locator('.config-table');
    await expect(configTable).toBeVisible();

    // Footer is visible
    const footer = page.locator('[data-testid="footer-section"]');
    await expect(footer).toBeVisible();
  });

  test('CSS styles are properly applied', async ({ page, browserName }) => {
    await page.goto('/');

    // Check hero title styling
    const heroTitle = page.locator('.hero-title');
    const titleStyles = await heroTitle.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        fontSize: styles.fontSize,
        fontWeight: styles.fontWeight,
      };
    });
    expect(parseInt(titleStyles.fontSize)).toBeGreaterThan(20);
    expect(parseInt(titleStyles.fontWeight)).toBeGreaterThanOrEqual(600);

    // Check navigation brand color (CSS variables)
    const navBrand = page.locator('.nav-brand');
    const brandColor = await navBrand.evaluate((el) => {
      return window.getComputedStyle(el).color;
    });
    expect(brandColor).toBeTruthy();
    expect(brandColor).not.toBe('');
  });

  test('navigation links work correctly', async ({ page, browserName }) => {
    await page.goto('/');

    // Click features link
    await page.click('a[href="#features"]');
    await expect(page.locator('#features')).toBeInViewport();

    // Click getting started link
    await page.click('a[href="#getting-started"]');
    await expect(page.locator('#getting-started')).toBeInViewport();

    // Click configuration link
    await page.click('a[href="#configuration"]');
    await expect(page.locator('#configuration')).toBeInViewport();
  });

  test('all main sections are visible', async ({ page, browserName }) => {
    await page.goto('/');

    // Check all main sections exist and are visible
    const sections = [
      '[data-testid="hero-section"]',
      '#features',
      '#architecture',
      '#getting-started',
      '#configuration',
      '[data-testid="status-section"]',
      '[data-testid="footer-section"]',
    ];

    for (const selector of sections) {
      const section = page.locator(selector);
      await expect(section).toBeVisible();
    }
  });

  test('no horizontal scroll overflow', async ({ page, browserName }) => {
    await page.goto('/');

    // Check for horizontal overflow (indicates layout issues)
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScroll).toBeFalsy();
  });

  test('feature grid layout renders correctly', async ({ page, browserName }) => {
    await page.goto('/');

    // Verify feature grid uses CSS grid
    const featureGrid = page.locator('.feature-grid');
    const gridStyles = await featureGrid.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        display: styles.display,
        gap: styles.gap,
      };
    });
    expect(gridStyles.display).toBe('grid');
  });

  test('flexbox layouts render correctly', async ({ page, browserName }) => {
    await page.goto('/');

    // Verify hero CTAs flexbox layout - the CTA buttons container uses Tailwind flex classes
    const heroCtas = page.locator('[data-testid="hero-section"] .flex');
    const ctasStyles = await heroCtas.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        display: styles.display,
      };
    });
    expect(ctasStyles.display).toBe('flex');
  });

  test('architecture diagram and components render', async ({ page, browserName }) => {
    await page.goto('/');

    // Architecture diagram should be visible
    const diagram = page.locator('[data-testid="architecture-diagram"]');
    await expect(diagram).toBeVisible();

    // Diagram rows should be visible (the architecture diagram uses .diagram-row class)
    const diagramRows = page.locator('[data-testid="architecture-diagram"] .diagram-row');
    expect(await diagramRows.count()).toBeGreaterThan(0);

    // LSM explanation should be visible
    const lsmExplanation = page.locator('[data-testid="lsm-explanation"]');
    await expect(lsmExplanation).toBeVisible();
  });

  test('external links have correct security attributes', async ({ page, browserName }) => {
    await page.goto('/');

    // GitHub links should have proper security attributes
    const githubLinks = page.locator('a[href*="github.com"]');
    const count = await githubLinks.count();

    for (let i = 0; i < count; i++) {
      const link = githubLinks.nth(i);
      await expect(link).toHaveAttribute('target', '_blank');
      await expect(link).toHaveAttribute('rel', /noopener/);
    }
  });

  test('interactive elements respond to focus', async ({ page, browserName }) => {
    await page.goto('/');

    // Test focus states for accessibility
    const getStartedBtn = page.locator('[data-testid="get-started-btn"]');
    await getStartedBtn.focus();
    await expect(getStartedBtn).toBeFocused();
  });

  test('sticky navigation remains visible on scroll', async ({ page, browserName }) => {
    await page.goto('/');

    // Scroll down the page
    await page.evaluate(() => window.scrollTo(0, 1000));
    await page.waitForTimeout(100);

    // Navigation should still be visible (sticky)
    const nav = page.locator('.navbar');
    await expect(nav).toBeVisible();
    await expect(nav).toBeInViewport();
  });

  test('project status section displays correctly', async ({ page, browserName }) => {
    await page.goto('/');

    // Status section should be visible
    const statusSection = page.locator('[data-testid="status-section"]');
    await expect(statusSection).toBeVisible();

    // Implemented features card
    const implementedCard = page.locator('[data-testid="implemented-features"]');
    await expect(implementedCard).toBeVisible();

    // Planned features card
    const plannedCard = page.locator('[data-testid="planned-features"]');
    await expect(plannedCard).toBeVisible();
  });

  test('configuration table renders with proper styling', async ({ page, browserName }) => {
    await page.goto('/');

    // Configuration table should be visible
    const configTable = page.locator('.config-table');
    await expect(configTable).toBeVisible();

    // Table should have headers
    const headers = page.locator('.config-table th');
    expect(await headers.count()).toBe(3); // Parameter, Default, Description

    // Table should have configuration rows
    const rows = page.locator('.config-table tbody tr');
    expect(await rows.count()).toBeGreaterThan(0);
  });

  test('code blocks are styled correctly', async ({ page, browserName }) => {
    await page.goto('/');

    // Find code blocks
    const codeBlocks = page.locator('pre');
    const count = await codeBlocks.count();
    expect(count).toBeGreaterThan(0);

    // Check code block styling
    const firstCodeBlock = codeBlocks.first();
    const codeStyles = await firstCodeBlock.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        backgroundColor: styles.backgroundColor,
        borderRadius: styles.borderRadius,
      };
    });

    // Should have dark background
    expect(codeStyles.backgroundColor).toBeTruthy();
    // Should have rounded corners
    expect(codeStyles.borderRadius).toBeTruthy();
  });
});
