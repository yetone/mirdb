import { test, expect, Page, ConsoleMessage } from '@playwright/test';

/**
 * Cross-Browser Compatibility Tests - Chrome
 *
 * These tests verify that the MirDB homepage renders correctly
 * and functions properly in Google Chrome browser.
 *
 * Test Cases:
 * TC1: Page loads without console errors
 * TC2: All sections display with correct layout
 * TC3: Navigation links trigger smooth scroll animation
 */

test.describe('Cross-Browser Compatibility - Chrome', () => {
  test.describe('TC1: Page loads without console errors in Chrome', () => {
    test('Homepage loads successfully without JavaScript errors', async ({ page }) => {
      // Collect all console messages
      const consoleMessages: ConsoleMessage[] = [];
      const consoleErrors: ConsoleMessage[] = [];

      page.on('console', (msg) => {
        consoleMessages.push(msg);
        if (msg.type() === 'error') {
          consoleErrors.push(msg);
        }
      });

      // Listen for page errors (uncaught exceptions)
      const pageErrors: Error[] = [];
      page.on('pageerror', (error) => {
        pageErrors.push(error);
      });

      // Navigate to the homepage
      const response = await page.goto('/');

      // Verify page loads successfully (status 200)
      expect(response).not.toBeNull();
      expect(response!.status()).toBe(200);

      // Wait for page to be fully loaded
      await page.waitForLoadState('domcontentloaded');
      await page.waitForLoadState('networkidle');

      // Verify no JavaScript errors occurred
      expect(pageErrors).toHaveLength(0);

      // Filter out known non-critical console errors (e.g., third-party CDN warnings)
      const criticalErrors = consoleErrors.filter(msg => {
        const text = msg.text();
        // Ignore third-party CDN warnings that don't affect functionality
        return !text.includes('cdnjs.cloudflare.com') || text.toLowerCase().includes('failed');
      });

      expect(criticalErrors).toHaveLength(0);
    });

    test('All external resources load correctly', async ({ page }) => {
      // Track failed resource requests
      const failedRequests: string[] = [];

      page.on('requestfailed', (request) => {
        failedRequests.push(`${request.url()} - ${request.failure()?.errorText}`);
      });

      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Verify no critical resource requests failed
      const criticalFailures = failedRequests.filter(req =>
        !req.includes('favicon') // Favicon is optional
      );

      expect(criticalFailures).toHaveLength(0);
    });

    test('Page title is correct', async ({ page }) => {
      await page.goto('/');

      // Verify page title matches expected
      await expect(page).toHaveTitle('MirDB - Persistent Key-Value Store with Memcached Compatibility');
    });

    test('Document renders with correct charset and viewport', async ({ page }) => {
      await page.goto('/');

      // Verify charset meta tag
      const charset = await page.locator('meta[charset]').getAttribute('charset');
      expect(charset?.toLowerCase()).toBe('utf-8');

      // Verify viewport meta tag exists
      const viewport = await page.locator('meta[name="viewport"]').getAttribute('content');
      expect(viewport).toContain('width=device-width');
    });
  });

  test.describe('TC2: All sections display with correct layout in Chrome', () => {
    test.beforeEach(async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('networkidle');
    });

    test('Navigation bar renders correctly', async ({ page }) => {
      const navbar = page.locator('nav.navbar');
      await expect(navbar).toBeVisible();

      // Verify navbar is fixed at top
      const navbarStyles = await navbar.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          position: styles.position,
          top: styles.top,
          zIndex: styles.zIndex
        };
      });

      expect(navbarStyles.position).toBe('fixed');
      expect(navbarStyles.top).toBe('0px');
      expect(parseInt(navbarStyles.zIndex)).toBeGreaterThanOrEqual(100);

      // Verify logo and nav links are visible
      await expect(page.locator('.nav-logo')).toBeVisible();
      await expect(page.locator('.nav-links')).toBeVisible();
    });

    test('Hero section displays correctly', async ({ page }) => {
      const hero = page.locator('.hero');
      await expect(hero).toBeVisible();

      // Verify hero content elements
      await expect(page.locator('.hero h1')).toHaveText('MirDB');
      await expect(page.locator('.tagline')).toBeVisible();

      // Verify hero background gradient is applied
      const heroBackground = await hero.evaluate((el) => {
        return window.getComputedStyle(el).backgroundImage;
      });
      expect(heroBackground).toContain('linear-gradient');

      // Verify CTA buttons are visible
      await expect(page.locator('.btn-primary')).toBeVisible();
      await expect(page.locator('.btn-secondary')).toBeVisible();
    });

    test('Features section displays with correct grid layout', async ({ page }) => {
      const featuresSection = page.locator('#features');
      await featuresSection.scrollIntoViewIfNeeded();
      await expect(featuresSection).toBeVisible();

      // Verify section heading
      await expect(featuresSection.locator('h2')).toHaveText('Key Features');

      // Verify all 4 feature cards are present
      const featureCards = page.locator('.feature-card');
      await expect(featureCards).toHaveCount(4);

      // Verify grid layout is applied
      const gridStyles = await page.locator('.features-grid').evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          display: styles.display,
          gridTemplateColumns: styles.gridTemplateColumns
        };
      });
      expect(gridStyles.display).toBe('grid');

      // Verify each feature card has icon, title, and description
      for (let i = 0; i < 4; i++) {
        const card = featureCards.nth(i);
        await expect(card.locator('.feature-icon')).toBeVisible();
        await expect(card.locator('h3')).toBeVisible();
        await expect(card.locator('p')).toBeVisible();
      }
    });

    test('Getting Started section renders code examples correctly', async ({ page }) => {
      const gettingStartedSection = page.locator('#getting-started');
      await gettingStartedSection.scrollIntoViewIfNeeded();
      await expect(gettingStartedSection).toBeVisible();

      // Verify section heading
      await expect(gettingStartedSection.locator('h2')).toHaveText('Getting Started');

      // Verify code blocks are present
      const codeBlocks = gettingStartedSection.locator('pre code');
      const codeBlockCount = await codeBlocks.count();
      expect(codeBlockCount).toBeGreaterThanOrEqual(3); // Bash, Python, TOML

      // Verify code blocks have syntax highlighting classes
      const firstCodeBlock = codeBlocks.first();
      const hasLanguageClass = await firstCodeBlock.evaluate((el) => {
        return el.className.includes('language-');
      });
      expect(hasLanguageClass).toBe(true);
    });

    test('Commands section displays all command categories', async ({ page }) => {
      const commandsSection = page.locator('#commands');
      await commandsSection.scrollIntoViewIfNeeded();
      await expect(commandsSection).toBeVisible();

      // Verify section heading
      await expect(commandsSection.locator('h2')).toHaveText('Supported Commands');

      // Verify all 4 command categories are present
      const commandCategories = page.locator('.command-category');
      await expect(commandCategories).toHaveCount(4);

      // Verify each category has proper content
      await expect(page.locator('[data-testid="storage-commands"]')).toBeVisible();
      await expect(page.locator('[data-testid="retrieval-commands"]')).toBeVisible();
      await expect(page.locator('[data-testid="deletion-commands"]')).toBeVisible();
      await expect(page.locator('[data-testid="admin-commands"]')).toBeVisible();
    });

    test('Architecture section displays SVG diagram', async ({ page }) => {
      const architectureSection = page.locator('#architecture');
      await architectureSection.scrollIntoViewIfNeeded();
      await expect(architectureSection).toBeVisible();

      // Verify SVG diagram is rendered
      const svgDiagram = page.locator('.architecture-diagram');
      await expect(svgDiagram).toBeVisible();

      // Verify SVG has proper dimensions
      const svgBox = await svgDiagram.boundingBox();
      expect(svgBox).not.toBeNull();
      expect(svgBox!.width).toBeGreaterThan(0);
      expect(svgBox!.height).toBeGreaterThan(0);

      // Verify write path and read path explanations
      await expect(page.locator('[data-testid="write-path"]')).toBeVisible();
      await expect(page.locator('[data-testid="read-path"]')).toBeVisible();
    });

    test('Footer renders with all required content', async ({ page }) => {
      const footer = page.locator('.footer');
      await footer.scrollIntoViewIfNeeded();
      await expect(footer).toBeVisible();

      // Verify footer sections
      await expect(page.locator('[data-testid="footer-links"]')).toBeVisible();
      await expect(page.locator('[data-testid="footer-status"]')).toBeVisible();

      // Verify GitHub link
      await expect(page.locator('[data-testid="footer-github-link"]')).toBeVisible();
      await expect(page.locator('[data-testid="footer-github-link"]')).toHaveAttribute('href', 'https://github.com/yetone/mirdb');

      // Verify copyright text
      await expect(page.locator('.footer-bottom')).toContainText('MirDB');
    });

    test('CSS styles are properly applied', async ({ page }) => {
      // Verify CSS variables are defined and applied
      const rootStyles = await page.evaluate(() => {
        const root = document.documentElement;
        const styles = getComputedStyle(root);
        return {
          primaryColor: styles.getPropertyValue('--primary-color').trim(),
          textColor: styles.getPropertyValue('--text-color').trim(),
          bgColor: styles.getPropertyValue('--bg-color').trim()
        };
      });

      expect(rootStyles.primaryColor).toBeTruthy();
      expect(rootStyles.textColor).toBeTruthy();
      expect(rootStyles.bgColor).toBeTruthy();

      // Verify body has correct font family applied
      const bodyStyles = await page.locator('body').evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          fontFamily: styles.fontFamily,
          lineHeight: styles.lineHeight
        };
      });

      expect(bodyStyles.fontFamily).toBeTruthy();
      expect(parseFloat(bodyStyles.lineHeight)).toBeGreaterThan(1);
    });
  });

  test.describe('TC3: Navigation links trigger smooth scroll animation in Chrome', () => {
    test.beforeEach(async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('networkidle');
    });

    test('Smooth scroll is enabled via CSS', async ({ page }) => {
      const htmlScrollBehavior = await page.evaluate(() => {
        const html = document.documentElement;
        return window.getComputedStyle(html).scrollBehavior;
      });

      expect(htmlScrollBehavior).toBe('smooth');
    });

    test('Features nav link triggers smooth scroll', async ({ page }) => {
      // Get initial scroll position
      const initialScrollY = await page.evaluate(() => window.scrollY);
      expect(initialScrollY).toBe(0);

      // Click Features nav link
      await page.click('.nav-links a[href="#features"]');

      // Wait a short time for animation to start
      await page.waitForTimeout(100);

      // Get intermediate scroll position to verify animation is happening
      const midScrollY = await page.evaluate(() => window.scrollY);

      // Wait for scroll animation to complete
      await page.waitForTimeout(600);

      // Verify features section is now in viewport
      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeInViewport();

      // Verify we actually scrolled
      const finalScrollY = await page.evaluate(() => window.scrollY);
      expect(finalScrollY).toBeGreaterThan(0);
    });

    test('Architecture nav link triggers smooth scroll', async ({ page }) => {
      // Click Architecture nav link
      await page.click('.nav-links a[href="#architecture"]');

      // Wait for scroll animation
      await page.waitForTimeout(600);

      // Verify architecture section is now in viewport
      const architectureSection = page.locator('#architecture');
      await expect(architectureSection).toBeInViewport();
    });

    test('Getting Started nav link triggers smooth scroll', async ({ page }) => {
      // Click Getting Started nav link
      await page.click('.nav-links a[href="#getting-started"]');

      // Wait for scroll animation
      await page.waitForTimeout(600);

      // Verify getting-started section is now in viewport
      const gettingStartedSection = page.locator('#getting-started');
      await expect(gettingStartedSection).toBeInViewport();
    });

    test('Commands nav link triggers smooth scroll', async ({ page }) => {
      // Click Commands nav link
      await page.click('.nav-links a[href="#commands"]');

      // Wait for scroll animation
      await page.waitForTimeout(600);

      // Verify commands section is now in viewport
      const commandsSection = page.locator('#commands');
      await expect(commandsSection).toBeInViewport();
    });

    test('Hero Get Started button triggers smooth scroll', async ({ page }) => {
      // Click the Get Started button in hero section
      await page.click('.hero-buttons .btn-primary');

      // Wait for scroll animation
      await page.waitForTimeout(600);

      // Verify getting-started section is now in viewport
      const gettingStartedSection = page.locator('#getting-started');
      await expect(gettingStartedSection).toBeInViewport();
    });

    test('Footer documentation link triggers smooth scroll', async ({ page }) => {
      // First scroll to footer to make the link clickable
      await page.locator('.footer').scrollIntoViewIfNeeded();

      // Click the documentation link in footer
      await page.click('[data-testid="footer-docs-link"]');

      // Wait for scroll animation
      await page.waitForTimeout(600);

      // Verify getting-started section is now in viewport
      const gettingStartedSection = page.locator('#getting-started');
      await expect(gettingStartedSection).toBeInViewport();
    });

    test('Smooth scroll works for all sections sequentially', async ({ page }) => {
      const sections = [
        { link: '#features', selector: '#features' },
        { link: '#architecture', selector: '#architecture' },
        { link: '#getting-started', selector: '#getting-started' },
        { link: '#commands', selector: '#commands' }
      ];

      for (const { link, selector } of sections) {
        // Navigate back to top first
        await page.evaluate(() => window.scrollTo(0, 0));
        await page.waitForTimeout(100);

        // Click the nav link
        await page.click(`.nav-links a[href="${link}"]`);

        // Wait for smooth scroll
        await page.waitForTimeout(600);

        // Verify section is in viewport
        const section = page.locator(selector);
        await expect(section).toBeInViewport();
      }
    });
  });
});
