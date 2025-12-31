import { test, expect } from '@playwright/test';
import { execSync } from 'child_process';
import * as fs from 'fs';
import * as path from 'path';

const HOMEPAGE_DIR = path.resolve(__dirname, '..');
const DIST_DIR = path.join(HOMEPAGE_DIR, 'dist');

test.describe('Static Site Build and Deployment', () => {
  // Run build before all tests
  test.beforeAll(async () => {
    // Clean and run build
    execSync('npm run build', { cwd: HOMEPAGE_DIR, stdio: 'pipe' });
  });

  test.describe('Test Case 1: Build completes without errors', () => {
    test('build command executes successfully', async () => {
      // Build was run in beforeAll, verify dist exists
      expect(fs.existsSync(DIST_DIR)).toBe(true);
    });

    test('build creates dist directory', async () => {
      const stats = fs.statSync(DIST_DIR);
      expect(stats.isDirectory()).toBe(true);
    });

    test('build generates manifest file', async () => {
      const manifestPath = path.join(DIST_DIR, 'manifest.json');
      expect(fs.existsSync(manifestPath)).toBe(true);

      const manifest = JSON.parse(fs.readFileSync(manifestPath, 'utf8'));
      expect(manifest).toHaveProperty('buildTime');
      expect(manifest).toHaveProperty('files');
      expect(Array.isArray(manifest.files)).toBe(true);
    });
  });

  test.describe('Test Case 2: Output contains index.html and assets', () => {
    test('index.html exists in build output', async () => {
      const indexPath = path.join(DIST_DIR, 'index.html');
      expect(fs.existsSync(indexPath)).toBe(true);
    });

    test('styles.css exists in build output', async () => {
      const stylesPath = path.join(DIST_DIR, 'styles.css');
      expect(fs.existsSync(stylesPath)).toBe(true);
    });

    test('404.html exists in build output', async () => {
      const notFoundPath = path.join(DIST_DIR, '404.html');
      expect(fs.existsSync(notFoundPath)).toBe(true);
    });

    test('assets directory exists with logo', async () => {
      const logoPath = path.join(DIST_DIR, 'assets', 'logo.gif');
      expect(fs.existsSync(logoPath)).toBe(true);
    });

    test('manifest contains all expected files', async () => {
      const manifestPath = path.join(DIST_DIR, 'manifest.json');
      const manifest = JSON.parse(fs.readFileSync(manifestPath, 'utf8'));

      const filePaths = manifest.files.map((f: { path: string }) => f.path);

      expect(filePaths).toContain('index.html');
      expect(filePaths).toContain('styles.css');
      expect(filePaths).toContain('404.html');
    });
  });

  test.describe('Test Case 3: Generated HTML passes validation', () => {
    test('HTML has DOCTYPE declaration', async () => {
      const indexPath = path.join(DIST_DIR, 'index.html');
      const content = fs.readFileSync(indexPath, 'utf8');
      expect(content).toContain('<!DOCTYPE html>');
    });

    test('HTML has proper structure', async () => {
      const indexPath = path.join(DIST_DIR, 'index.html');
      const content = fs.readFileSync(indexPath, 'utf8');

      expect(content).toContain('<html');
      expect(content).toContain('</html>');
      expect(content).toContain('<head>');
      expect(content).toContain('</head>');
      expect(content).toContain('<body>');
      expect(content).toContain('</body>');
    });

    test('HTML has lang attribute', async () => {
      const indexPath = path.join(DIST_DIR, 'index.html');
      const content = fs.readFileSync(indexPath, 'utf8');
      expect(content).toMatch(/lang=["']en["']/);
    });

    test('HTML has required meta tags', async () => {
      const indexPath = path.join(DIST_DIR, 'index.html');
      const content = fs.readFileSync(indexPath, 'utf8');

      expect(content).toContain('charset="UTF-8"');
      expect(content).toContain('viewport');
      expect(content).toContain('<title>');
    });

    test('HTML has accessibility features', async () => {
      const indexPath = path.join(DIST_DIR, 'index.html');
      const content = fs.readFileSync(indexPath, 'utf8');

      expect(content).toContain('skip-link');
      expect(content).toContain('aria-label');
    });

    test('HTML has SEO meta tags', async () => {
      const indexPath = path.join(DIST_DIR, 'index.html');
      const content = fs.readFileSync(indexPath, 'utf8');

      expect(content).toContain('name="description"');
      expect(content).toContain('property="og:title"');
      expect(content).toContain('rel="canonical"');
    });

    test('asset paths are correctly updated', async () => {
      const indexPath = path.join(DIST_DIR, 'index.html');
      const content = fs.readFileSync(indexPath, 'utf8');

      // Should use ./assets/ instead of ../assets/
      expect(content).toContain('./assets/logo.gif');
      expect(content).not.toContain('../assets/logo.gif');
    });
  });

  test.describe('Test Case 4: Page renders correctly from build output', () => {
    test('page loads without errors', async ({ page }) => {
      const response = await page.goto('/');
      expect(response?.status()).toBe(200);
    });

    test('page title is correct', async ({ page }) => {
      await page.goto('/');
      await expect(page).toHaveTitle(/MirDB/);
    });

    test('hero section is visible', async ({ page }) => {
      await page.goto('/');
      const hero = page.locator('[data-testid="hero-section"]');
      await expect(hero).toBeVisible();
    });

    test('navigation is functional', async ({ page }) => {
      await page.goto('/');
      const navbar = page.locator('[data-testid="navbar"]');
      await expect(navbar).toBeVisible();

      const navLinks = page.locator('[data-testid="nav-links"]');
      await expect(navLinks).toBeVisible();
    });

    test('features section renders', async ({ page }) => {
      await page.goto('/');
      const features = page.locator('#features');
      await expect(features).toBeVisible();
    });

    test('quick start section renders', async ({ page }) => {
      await page.goto('/');
      const quickStart = page.locator('#quick-start');
      await expect(quickStart).toBeVisible();
    });

    test('architecture section renders', async ({ page }) => {
      await page.goto('/');
      const architecture = page.locator('[data-testid="architecture-section"]');
      await expect(architecture).toBeVisible();
    });

    test('configuration section renders', async ({ page }) => {
      await page.goto('/');
      const config = page.locator('#configuration');
      await expect(config).toBeVisible();
    });

    test('footer section renders', async ({ page }) => {
      await page.goto('/');
      const footer = page.locator('[data-testid="footer-section"]');
      await expect(footer).toBeVisible();
    });

    test('CSS is loaded and applied', async ({ page }) => {
      await page.goto('/');

      // Check that stylesheet is linked and loaded
      const stylesheets = await page.evaluate(() => {
        return Array.from(document.styleSheets).filter(
          (sheet) => sheet.href && sheet.href.includes('styles.css')
        ).length;
      });
      expect(stylesheets).toBeGreaterThan(0);

      // Check that CSS rules are applied (hero has display properties set)
      const hero = page.locator('[data-testid="hero-section"]');
      const display = await hero.evaluate((el) => {
        return window.getComputedStyle(el).display;
      });
      expect(display).not.toBe('');
    });

    test('logo image loads correctly', async ({ page }) => {
      await page.goto('/');

      const logo = page.locator('img.logo');
      await expect(logo).toBeVisible();

      // Check image loaded successfully
      const naturalWidth = await logo.evaluate((img: HTMLImageElement) => img.naturalWidth);
      expect(naturalWidth).toBeGreaterThan(0);
    });

    test('internal navigation links work', async ({ page }) => {
      await page.goto('/');

      // Click on Features link
      await page.click('a[href="#features"]');
      await expect(page).toHaveURL(/#features$/);

      // Check features section is in view
      const features = page.locator('#features');
      await expect(features).toBeInViewport();
    });

    test('copy button functionality works', async ({ page, context }) => {
      // Grant clipboard permissions
      await context.grantPermissions(['clipboard-read', 'clipboard-write']);

      await page.goto('/');

      const copyButton = page.locator('[data-testid="copy-button"]');
      await expect(copyButton).toBeVisible();

      // Click should not cause errors
      await copyButton.click();

      // Wait for the copied class to be added
      await expect(copyButton).toHaveClass(/copied/, { timeout: 3000 });
    });
  });
});
