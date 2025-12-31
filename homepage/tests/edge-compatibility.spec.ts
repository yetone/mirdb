import { test, expect, Page } from '@playwright/test';

/**
 * Cross-Browser Compatibility Tests - Microsoft Edge
 *
 * These tests verify that the MirDB homepage renders correctly and functions
 * properly in Microsoft Edge browser. They cover:
 * - Page rendering without visual defects or layout issues
 * - Navigation links working correctly
 * - Copy-to-clipboard functionality using the Clipboard API
 */

test.describe('Cross-Browser Compatibility - Edge', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test.describe('TC1: Page Rendering in Edge', () => {
    test('Homepage loads without JavaScript errors', async ({ page }) => {
      const errors: string[] = [];

      // Listen for console errors
      page.on('console', msg => {
        if (msg.type() === 'error') {
          errors.push(msg.text());
        }
      });

      // Listen for page errors
      page.on('pageerror', err => {
        errors.push(err.message);
      });

      // Reload the page to capture any errors
      await page.reload();
      await page.waitForLoadState('networkidle');

      // Filter out expected errors (like 404 for logo if it doesn't exist)
      const criticalErrors = errors.filter(err =>
        !err.includes('Failed to load resource') &&
        !err.includes('404')
      );

      expect(criticalErrors).toHaveLength(0);
    });

    test('All major sections are visible and properly rendered', async ({ page }) => {
      // Check navigation bar
      const navbar = page.locator('[data-testid="navbar"]');
      await expect(navbar).toBeVisible();

      // Check hero section
      const heroSection = page.locator('[data-testid="hero-section"]');
      await expect(heroSection).toBeVisible();

      // Check features section
      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeVisible();

      // Check architecture section
      const architectureSection = page.locator('#architecture');
      await expect(architectureSection).toBeVisible();

      // Check quick start section
      const quickStartSection = page.locator('#quick-start');
      await expect(quickStartSection).toBeVisible();

      // Check configuration section
      const configSection = page.locator('#configuration');
      await expect(configSection).toBeVisible();

      // Check project status section
      const projectStatusSection = page.locator('[data-testid="project-status-section"]');
      await expect(projectStatusSection).toBeVisible();

      // Check footer
      const footer = page.locator('[data-testid="footer-section"]');
      await expect(footer).toBeVisible();
    });

    test('Layout has no horizontal overflow or broken elements', async ({ page }) => {
      // Check for horizontal overflow
      const hasHorizontalOverflow = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });

      expect(hasHorizontalOverflow).toBe(false);
    });

    test('Typography and fonts render correctly', async ({ page }) => {
      // Check that main heading is visible and styled
      const h1 = page.locator('h1');
      await expect(h1).toBeVisible();
      await expect(h1).toContainText('MirDB');

      // Verify font is applied (not default system font)
      const fontFamily = await h1.evaluate(el => getComputedStyle(el).fontFamily);
      expect(fontFamily).toBeTruthy();

      // Check that body text is readable (font size is reasonable)
      const bodyFontSize = await page.locator('body').evaluate(el =>
        parseInt(getComputedStyle(el).fontSize)
      );
      expect(bodyFontSize).toBeGreaterThanOrEqual(14);
    });

    test('SVG architecture diagram renders correctly', async ({ page }) => {
      const diagram = page.locator('[data-testid="architecture-diagram"]');
      await expect(diagram).toBeVisible();

      // Check that SVG elements are rendered
      const svgBoxes = diagram.locator('rect');
      const boxCount = await svgBoxes.count();
      expect(boxCount).toBeGreaterThan(0);

      // Check that text labels are visible
      const svgText = diagram.locator('text');
      const textCount = await svgText.count();
      expect(textCount).toBeGreaterThan(0);
    });

    test('Feature cards display in grid layout', async ({ page }) => {
      const featureGrid = page.locator('.feature-grid');
      await expect(featureGrid).toBeVisible();

      const featureCards = page.locator('.feature-card');
      const cardCount = await featureCards.count();

      // Should have 5 feature cards as per the HTML
      expect(cardCount).toBe(5);

      // Verify all cards are visible
      for (let i = 0; i < cardCount; i++) {
        await expect(featureCards.nth(i)).toBeVisible();
      }
    });

    test('Configuration table renders properly', async ({ page }) => {
      const configTable = page.locator('.config-table');
      await expect(configTable).toBeVisible();

      // Check header row
      const headerCells = configTable.locator('thead th');
      expect(await headerCells.count()).toBe(3);

      // Check that all 6 configuration rows are present
      const bodyRows = configTable.locator('tbody tr');
      expect(await bodyRows.count()).toBe(6);
    });
  });

  test.describe('TC2: Navigation Links in Edge', () => {
    test('Navigation bar contains all expected links', async ({ page }) => {
      const navLinks = page.locator('[data-testid="nav-links"]');
      await expect(navLinks).toBeVisible();

      // Check for Features link
      const featuresLink = navLinks.locator('a[href="#features"]');
      await expect(featuresLink).toBeVisible();
      await expect(featuresLink).toHaveText('Features');

      // Check for Quick Start link
      const quickStartLink = navLinks.locator('a[href="#quick-start"]');
      await expect(quickStartLink).toBeVisible();
      await expect(quickStartLink).toHaveText('Quick Start');

      // Check for Architecture link
      const architectureLink = navLinks.locator('a[href="#architecture"]');
      await expect(architectureLink).toBeVisible();
      await expect(architectureLink).toHaveText('Architecture');

      // Check for Configuration link
      const configLink = navLinks.locator('a[href="#configuration"]');
      await expect(configLink).toBeVisible();
      await expect(configLink).toHaveText('Configuration');

      // Check for GitHub link
      const githubLink = navLinks.locator('a[href*="github"]');
      await expect(githubLink).toBeVisible();
      await expect(githubLink).toHaveText('GitHub');
    });

    test('Features navigation link scrolls to correct section', async ({ page }) => {
      const featuresLink = page.locator('[data-testid="nav-links"] a[href="#features"]');
      await featuresLink.click();

      await page.waitForTimeout(600); // Wait for smooth scroll

      await expect(page).toHaveURL(/#features/);

      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeInViewport();
    });

    test('Quick Start navigation link scrolls to correct section', async ({ page }) => {
      const quickStartLink = page.locator('[data-testid="nav-links"] a[href="#quick-start"]');
      await quickStartLink.click();

      await page.waitForTimeout(600);

      await expect(page).toHaveURL(/#quick-start/);

      const quickStartSection = page.locator('#quick-start');
      await expect(quickStartSection).toBeInViewport();
    });

    test('Architecture navigation link scrolls to correct section', async ({ page }) => {
      const architectureLink = page.locator('[data-testid="nav-links"] a[href="#architecture"]');
      await architectureLink.click();

      await page.waitForTimeout(600);

      await expect(page).toHaveURL(/#architecture/);

      const architectureSection = page.locator('#architecture');
      await expect(architectureSection).toBeInViewport();
    });

    test('Configuration navigation link scrolls to correct section', async ({ page }) => {
      const configLink = page.locator('[data-testid="nav-links"] a[href="#configuration"]');
      await configLink.click();

      await page.waitForTimeout(600);

      await expect(page).toHaveURL(/#configuration/);

      const configSection = page.locator('#configuration');
      await expect(configSection).toBeInViewport();
    });

    test('GitHub link opens in new tab with correct URL', async ({ page }) => {
      const githubLink = page.locator('[data-testid="nav-links"] a[href*="github"]');

      // Verify target="_blank" attribute
      await expect(githubLink).toHaveAttribute('target', '_blank');

      // Verify href contains github.com/yetone/mirdb
      await expect(githubLink).toHaveAttribute('href', /github\.com\/yetone\/mirdb/);

      // Verify rel attribute for security
      await expect(githubLink).toHaveAttribute('rel', /noopener/);
    });

    test('Hero CTA Get Started button navigates to Quick Start', async ({ page }) => {
      const getStartedBtn = page.locator('[data-testid="cta-get-started"]');
      await expect(getStartedBtn).toBeVisible();

      await getStartedBtn.click();
      await page.waitForTimeout(600);

      await expect(page).toHaveURL(/#quick-start/);

      const quickStartSection = page.locator('#quick-start');
      await expect(quickStartSection).toBeInViewport();
    });

    test('Hero CTA GitHub button has correct attributes', async ({ page }) => {
      const githubBtn = page.locator('[data-testid="cta-github"]');
      await expect(githubBtn).toBeVisible();

      await expect(githubBtn).toHaveAttribute('target', '_blank');
      await expect(githubBtn).toHaveAttribute('href', /github\.com\/yetone\/mirdb/);
      await expect(githubBtn).toHaveAttribute('rel', /noopener/);
    });

    test('Footer documentation link navigates correctly', async ({ page }) => {
      const footerDocsLink = page.locator('[data-testid="footer-docs-link"]');
      await expect(footerDocsLink).toBeVisible();

      await footerDocsLink.click();
      await page.waitForTimeout(600);

      await expect(page).toHaveURL(/#quick-start/);
    });

    test('Footer GitHub link has correct attributes', async ({ page }) => {
      const footerGithubLink = page.locator('[data-testid="footer-github-link"]');
      await expect(footerGithubLink).toBeVisible();

      await expect(footerGithubLink).toHaveAttribute('target', '_blank');
      await expect(footerGithubLink).toHaveAttribute('href', /github\.com\/yetone\/mirdb/);
    });
  });

  test.describe('TC3: Copy-to-Clipboard in Edge', () => {
    test.beforeEach(async ({ page, context }) => {
      // Grant clipboard permissions for Edge
      await context.grantPermissions(['clipboard-read', 'clipboard-write']);
    });

    test('Copy button is visible in code block', async ({ page }) => {
      const quickStartSection = page.locator('#quick-start');
      await expect(quickStartSection).toBeVisible();

      const copyButton = quickStartSection.locator('[data-testid="copy-button"]');
      await expect(copyButton).toBeVisible();
    });

    test('Clicking copy button copies code to clipboard using Clipboard API', async ({ page }) => {
      const quickStartSection = page.locator('#quick-start');
      const copyButton = quickStartSection.locator('[data-testid="copy-button"]');

      await copyButton.click();

      // Verify clipboard content
      const clipboardContent = await page.evaluate(() => navigator.clipboard.readText());

      // Should contain the installation command
      expect(clipboardContent).toContain('cargo install mirdb');
      expect(clipboardContent).toContain('mirdb -c /path/to/config.toml');
      expect(clipboardContent).toContain('telnet localhost 12333');
    });

    test('Copy button shows visual feedback after click', async ({ page }) => {
      const quickStartSection = page.locator('#quick-start');
      const copyButton = quickStartSection.locator('[data-testid="copy-button"]');

      // Initial state - copy text should be "Copy"
      const copyText = copyButton.locator('.copy-text');
      await expect(copyText).toHaveText('Copy');

      // Click copy button
      await copyButton.click();

      // Wait for DOM update
      await page.waitForTimeout(100);

      // Verify visual feedback
      const hasCopiedClass = await copyButton.evaluate(el => el.classList.contains('copied'));
      expect(hasCopiedClass).toBe(true);

      // Check text changed to "Copied!"
      await expect(copyText).toHaveText('Copied!');

      // Check icon is displayed (check icon should be visible when copied)
      const checkIcon = copyButton.locator('.check-icon');
      const copyIcon = copyButton.locator('.copy-icon');

      // Based on CSS, when .copied class is added, check-icon should be visible
      const checkIconDisplay = await checkIcon.evaluate(el => getComputedStyle(el).display);
      expect(checkIconDisplay).not.toBe('none');
    });

    test('Copy feedback resets after 2 seconds', async ({ page }) => {
      const quickStartSection = page.locator('#quick-start');
      const copyButton = quickStartSection.locator('[data-testid="copy-button"]');
      const copyText = copyButton.locator('.copy-text');

      // Click copy button
      await copyButton.click();

      // Verify copied state
      await expect(copyText).toHaveText('Copied!');

      // Wait for reset (2 seconds + buffer)
      await page.waitForTimeout(2500);

      // Verify reset to original state
      await expect(copyText).toHaveText('Copy');

      const hasCopiedClass = await copyButton.evaluate(el => el.classList.contains('copied'));
      expect(hasCopiedClass).toBe(false);
    });

    test('Clipboard API is supported in Edge', async ({ page }) => {
      // Verify Clipboard API is available - Edge supports the modern Clipboard API
      const clipboardSupported = await page.evaluate(() => {
        return typeof navigator.clipboard !== 'undefined' &&
               typeof navigator.clipboard.writeText === 'function' &&
               typeof navigator.clipboard.readText === 'function';
      });

      expect(clipboardSupported).toBe(true);
    });

    test('Multiple copy operations work correctly', async ({ page }) => {
      const quickStartSection = page.locator('#quick-start');
      const copyButton = quickStartSection.locator('[data-testid="copy-button"]');

      // First copy operation
      await copyButton.click();
      let clipboardContent = await page.evaluate(() => navigator.clipboard.readText());
      expect(clipboardContent).toContain('cargo install mirdb');

      // Wait for reset
      await page.waitForTimeout(2500);

      // Second copy operation
      await copyButton.click();
      clipboardContent = await page.evaluate(() => navigator.clipboard.readText());
      expect(clipboardContent).toContain('cargo install mirdb');
    });
  });
});
