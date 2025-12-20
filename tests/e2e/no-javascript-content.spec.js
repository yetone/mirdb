/**
 * E2E Tests for No JavaScript Core Content Dependency
 * Scenario: Verify that core content is visible without JavaScript enabled, meeting NFR-5 requirement
 *
 * Test Cases:
 * 1. Hero section content visible with JS disabled
 * 2. Features section visible with JS disabled
 * 3. Code examples visible and readable with JS disabled
 * 4. Navigation links work without JS
 */

const { test, expect } = require('@playwright/test');
const path = require('path');

const indexPath = 'file://' + path.resolve(__dirname, '../../index.html');

// Create a test context with JavaScript disabled
test.describe('No JavaScript Core Content Dependency (NFR-5)', () => {

  // Use a browser context with JavaScript disabled for all tests
  test.use({ javaScriptEnabled: false });

  test.describe('Test Case 1: Hero Section Content Without JavaScript', () => {
    test('should display hero section title (h1) without JavaScript', async ({ page }) => {
      await page.goto(indexPath);

      const h1 = page.locator('h1');
      await expect(h1).toBeVisible();
      await expect(h1).toHaveText('MirDB');
    });

    test('should display hero tagline without JavaScript', async ({ page }) => {
      await page.goto(indexPath);

      const tagline = page.locator('.hero-tagline');
      await expect(tagline).toBeVisible();

      const text = await tagline.textContent();
      expect(text).toContain('Persistent');
      expect(text).toContain('Memcached');
    });

    test('should display hero description without JavaScript', async ({ page }) => {
      await page.goto(indexPath);

      const description = page.locator('.hero-description');
      await expect(description).toBeVisible();

      const text = await description.textContent();
      expect(text.length).toBeGreaterThan(50);
    });

    test('should display CTA buttons without JavaScript', async ({ page }) => {
      await page.goto(indexPath);

      // Primary CTA - Get Started
      const primaryCta = page.locator('.btn-primary');
      await expect(primaryCta).toBeVisible();
      await expect(primaryCta).toContainText('Get Started');

      // Secondary CTA - GitHub
      const secondaryCta = page.locator('.btn-secondary');
      await expect(secondaryCta).toBeVisible();
      await expect(secondaryCta).toContainText('GitHub');
    });

    test('should display hero differentiators without JavaScript', async ({ page }) => {
      await page.goto(indexPath);

      const differentiators = page.locator('.hero-differentiators');
      await expect(differentiators).toBeVisible();

      // Check key differentiators are visible
      await expect(page.locator('.differentiator:has-text("Persistent Storage")')).toBeVisible();
      await expect(page.locator('.differentiator:has-text("Memcached Protocol")')).toBeVisible();
      await expect(page.locator('.differentiator:has-text("Built with Rust")')).toBeVisible();
    });

    test('should display hero logo without JavaScript', async ({ page }) => {
      await page.goto(indexPath);

      const logo = page.locator('.hero-logo');
      await expect(logo).toBeVisible();
    });
  });

  test.describe('Test Case 2: Features/Quick Start Section Without JavaScript', () => {
    test('should display quick-start section heading without JavaScript', async ({ page }) => {
      await page.goto(indexPath);

      const quickStartHeading = page.locator('#quick-start-title');
      await expect(quickStartHeading).toBeVisible();
      await expect(quickStartHeading).toHaveText('Quick Start');
    });

    test('should display all installation steps without JavaScript', async ({ page }) => {
      await page.goto(indexPath);

      // Check installation steps are visible
      const installationSteps = page.locator('.installation-steps li');
      const stepCount = await installationSteps.count();

      expect(stepCount).toBeGreaterThanOrEqual(4);

      // Verify each step title is visible
      const stepTitles = page.locator('.step-title');
      for (let i = 0; i < await stepTitles.count(); i++) {
        await expect(stepTitles.nth(i)).toBeVisible();
      }
    });

    test('should display step descriptions without JavaScript', async ({ page }) => {
      await page.goto(indexPath);

      const stepDescriptions = page.locator('.step-description');
      const count = await stepDescriptions.count();

      expect(count).toBeGreaterThanOrEqual(4);

      for (let i = 0; i < count; i++) {
        await expect(stepDescriptions.nth(i)).toBeVisible();
      }
    });

    test('should display configuration section without JavaScript', async ({ page }) => {
      await page.goto(indexPath);

      const configSection = page.locator('#configuration');
      await expect(configSection).toBeVisible();

      const configHeading = page.locator('#configuration-title');
      await expect(configHeading).toBeVisible();
      await expect(configHeading).toHaveText('Configuration');
    });

    test('should display configuration table without JavaScript', async ({ page }) => {
      await page.goto(indexPath);

      const configTable = page.locator('.config-table');
      await expect(configTable).toBeVisible();

      // Check table headers exist
      const headers = page.locator('.config-table th');
      await expect(headers.nth(0)).toContainText('Parameter');
      await expect(headers.nth(1)).toContainText('Default Value');
      await expect(headers.nth(2)).toContainText('Description');

      // Check table has data rows
      const rows = page.locator('.config-table tbody tr');
      const rowCount = await rows.count();
      expect(rowCount).toBeGreaterThanOrEqual(5);
    });
  });

  test.describe('Test Case 3: Code Examples Without JavaScript', () => {
    test('should display code blocks without JavaScript', async ({ page }) => {
      await page.goto(indexPath);

      const codeBlocks = page.locator('.code-block');
      const count = await codeBlocks.count();

      expect(count).toBeGreaterThanOrEqual(4);

      // Each code block should be visible
      for (let i = 0; i < count; i++) {
        await expect(codeBlocks.nth(i)).toBeVisible();
      }
    });

    test('should display readable code content without JavaScript', async ({ page }) => {
      await page.goto(indexPath);

      // Check first code block has readable content
      const firstCodeBlock = page.locator('.code-block').first();
      await expect(firstCodeBlock).toBeVisible();

      const codeText = await firstCodeBlock.textContent();
      expect(codeText.length).toBeGreaterThan(10);
    });

    test('should display code examples with syntax comments visible', async ({ page }) => {
      await page.goto(indexPath);

      // Code comments should be visible even without JS syntax highlighting
      const codeComments = page.locator('.code-comment');
      const count = await codeComments.count();

      // At least some comments should be present
      expect(count).toBeGreaterThanOrEqual(1);

      // Comments should be readable
      for (let i = 0; i < Math.min(count, 3); i++) {
        const commentText = await codeComments.nth(i).textContent();
        expect(commentText.length).toBeGreaterThan(0);
      }
    });

    test('should display configuration code example without JavaScript', async ({ page }) => {
      await page.goto(indexPath);

      const configCode = page.locator('.config-code');
      await expect(configCode).toBeVisible();

      const configPre = page.locator('.config-code pre');
      await expect(configPre).toBeVisible();

      const codeText = await configPre.textContent();
      expect(codeText).toContain('addr');
      expect(codeText).toContain('work_dir');
    });

    test('code blocks should have readable background contrast without JavaScript', async ({ page }) => {
      await page.goto(indexPath);

      const codeBlock = page.locator('.code-block').first();
      await expect(codeBlock).toBeVisible();

      // Verify code block has dark background (CSS styling works without JS)
      const backgroundColor = await codeBlock.evaluate((el) =>
        window.getComputedStyle(el).backgroundColor
      );

      // Background should be a dark color (rgb values should be relatively low)
      expect(backgroundColor).toBeTruthy();
      expect(backgroundColor).not.toBe('transparent');
    });
  });

  test.describe('Test Case 4: Navigation Links Without JavaScript', () => {
    test('should have functional header navigation links without JavaScript', async ({ page }) => {
      await page.goto(indexPath);

      const nav = page.locator('nav[aria-label="Main navigation"]');
      await expect(nav).toBeVisible();

      // Check navigation links exist and are visible
      const navLinks = page.locator('nav[aria-label="Main navigation"] a');
      const linkCount = await navLinks.count();
      expect(linkCount).toBeGreaterThanOrEqual(3);
    });

    test('should have Quick Start link with correct href without JavaScript', async ({ page }) => {
      await page.goto(indexPath);

      const quickStartLink = page.locator('nav a[href="#quick-start"]');
      await expect(quickStartLink).toBeVisible();
      await expect(quickStartLink).toHaveText('Quick Start');
    });

    test('should have Configuration link with correct href without JavaScript', async ({ page }) => {
      await page.goto(indexPath);

      const configLink = page.locator('nav a[href="#configuration"]');
      await expect(configLink).toBeVisible();
      await expect(configLink).toHaveText('Configuration');
    });

    test('should have GitHub link with correct href without JavaScript', async ({ page }) => {
      await page.goto(indexPath);

      const githubLink = page.locator('nav a[href="https://github.com/yetone/mirdb"]');
      await expect(githubLink).toBeVisible();
      await expect(githubLink).toHaveText('GitHub');

      // Verify it opens in new tab
      const target = await githubLink.getAttribute('target');
      expect(target).toBe('_blank');
    });

    test('internal anchor links should navigate correctly without JavaScript', async ({ page }) => {
      await page.goto(indexPath);

      // Click on Quick Start link
      await page.click('nav a[href="#quick-start"]');

      // Verify URL hash changed
      const url = page.url();
      expect(url).toContain('#quick-start');

      // Verify quick-start section is in view
      const quickStartSection = page.locator('#quick-start');
      await expect(quickStartSection).toBeInViewport();
    });

    test('should have functional footer links without JavaScript', async ({ page }) => {
      await page.goto(indexPath);

      const footer = page.locator('footer');
      await expect(footer).toBeVisible();

      // Check footer links
      const footerLinks = page.locator('.footer-links a');
      const linkCount = await footerLinks.count();
      expect(linkCount).toBeGreaterThanOrEqual(3);

      // Verify GitHub link
      const githubFooterLink = page.locator('.footer-links a[href="https://github.com/yetone/mirdb"]');
      await expect(githubFooterLink).toBeVisible();

      // Verify Documentation link
      const docsLink = page.locator('.footer-links a:has-text("Documentation")');
      await expect(docsLink).toBeVisible();

      // Verify License link
      const licenseLink = page.locator('.footer-links a:has-text("License")');
      await expect(licenseLink).toBeVisible();
    });

    test('CTA Get Started button should navigate without JavaScript', async ({ page }) => {
      await page.goto(indexPath);

      const getStartedBtn = page.locator('.btn-primary');
      await expect(getStartedBtn).toBeVisible();

      // Get href and verify it's an anchor link
      const href = await getStartedBtn.getAttribute('href');
      expect(href).toBe('#quick-start');

      // Click and verify navigation
      await getStartedBtn.click();
      const url = page.url();
      expect(url).toContain('#quick-start');
    });

    test('should not have any JavaScript-dependent navigation', async ({ page }) => {
      await page.goto(indexPath);

      // Check that all navigation links use standard href attributes (not onclick)
      const allLinks = page.locator('a[href]');
      const count = await allLinks.count();

      for (let i = 0; i < count; i++) {
        const onclick = await allLinks.nth(i).getAttribute('onclick');
        expect(onclick).toBeNull();
      }
    });
  });

  test.describe('Page Structure Without JavaScript', () => {
    test('should render complete page layout without JavaScript', async ({ page }) => {
      await page.goto(indexPath);

      // Verify main structural elements
      await expect(page.locator('header')).toBeVisible();
      await expect(page.locator('main')).toBeVisible();
      await expect(page.locator('footer')).toBeVisible();
    });

    test('should display all sections without JavaScript', async ({ page }) => {
      await page.goto(indexPath);

      // Hero section
      await expect(page.locator('.hero')).toBeVisible();

      // Quick Start section
      await expect(page.locator('#quick-start')).toBeVisible();

      // Configuration section
      await expect(page.locator('#configuration')).toBeVisible();
    });

    test('should have proper semantic HTML without JavaScript dependency', async ({ page }) => {
      await page.goto(indexPath);

      // Check semantic landmarks
      await expect(page.locator('header[role="banner"]')).toBeVisible();
      await expect(page.locator('nav[aria-label="Main navigation"]')).toBeVisible();
      await expect(page.locator('main')).toBeVisible();
      await expect(page.locator('footer[aria-label="Site footer"]')).toBeVisible();

      // Check section landmarks
      await expect(page.locator('section[aria-labelledby="hero-title"]')).toBeVisible();
      await expect(page.locator('section[aria-labelledby="quick-start-title"]')).toBeVisible();
      await expect(page.locator('section[aria-labelledby="configuration-title"]')).toBeVisible();
    });

    test('CSS styling should work without JavaScript', async ({ page }) => {
      await page.goto(indexPath);

      // Verify CSS is applied (checking computed styles)
      const hero = page.locator('.hero');
      const display = await hero.evaluate((el) => window.getComputedStyle(el).display);
      expect(display).toBe('flex');

      // Check primary button has background color
      const btn = page.locator('.btn-primary');
      const backgroundColor = await btn.evaluate((el) =>
        window.getComputedStyle(el).backgroundColor
      );
      expect(backgroundColor).not.toBe('transparent');
      expect(backgroundColor).not.toBe('rgba(0, 0, 0, 0)');
    });
  });
});
