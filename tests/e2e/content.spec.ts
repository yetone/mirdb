/**
 * Content Sections E2E Tests
 * Owner: Scenario 3 - Content Sections
 *
 * Tests:
 * - About section with project description visible
 * - Features list displays at least 4 features
 * - Quick Start section with code block present
 * - Code block contains installation commands
 * - Code is displayed in monospace font
 *
 * Traceability: REQ-4, REQ-7, REQ-9
 */

import { test, expect } from '@playwright/test';
import { waitForPageLoad, getByTestId } from './test-utils';

test.describe('Content Sections', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await waitForPageLoad(page);
  });

  test.describe('About Section', () => {
    test('TC1: About section exists with description explaining MirDB is a persistent key-value store with Memcached protocol', async ({ page }) => {
      // Locate the about section
      const aboutSection = page.locator(getByTestId('about-section'));
      await expect(aboutSection).toBeVisible();

      // Check that the section has a heading
      const heading = aboutSection.locator('h2');
      await expect(heading).toHaveText('About MirDB');

      // Check the description content
      const description = aboutSection.locator('p');
      await expect(description).toBeVisible();

      // Verify the description mentions key concepts
      const descriptionText = await description.textContent();
      expect(descriptionText).toContain('persistent');
      expect(descriptionText).toContain('key-value');
      expect(descriptionText).toContain('Memcached');
    });
  });

  test.describe('Features Section', () => {
    test('TC2: At least 4 features are listed', async ({ page }) => {
      const featureList = page.locator(getByTestId('feature-list'));
      await expect(featureList).toBeVisible();

      const featureItems = featureList.locator('.feature-item');
      const count = await featureItems.count();
      expect(count).toBeGreaterThanOrEqual(4);
    });

    test('TC3: Feature list mentions Memcached protocol support', async ({ page }) => {
      const memcachedFeature = page.locator(getByTestId('feature-memcached'));
      await expect(memcachedFeature).toBeVisible();

      const text = await memcachedFeature.textContent();
      expect(text?.toLowerCase()).toContain('memcached');
      expect(text?.toLowerCase()).toContain('protocol');
    });

    test('TC4: Feature list mentions LSM tree or persistent storage', async ({ page }) => {
      const lsmFeature = page.locator(getByTestId('feature-lsm'));
      await expect(lsmFeature).toBeVisible();

      const text = await lsmFeature.textContent();
      const hasLSM = text?.toLowerCase().includes('lsm');
      const hasPersistent = text?.toLowerCase().includes('persistent');
      expect(hasLSM || hasPersistent).toBe(true);
    });

    test('TC5: Feature list mentions Rust for safety and performance', async ({ page }) => {
      const rustFeature = page.locator(getByTestId('feature-rust'));
      await expect(rustFeature).toBeVisible();

      const text = await rustFeature.textContent();
      expect(text).toContain('Rust');

      // Check for either safety or performance
      const hasSafety = text?.toLowerCase().includes('safety');
      const hasPerformance = text?.toLowerCase().includes('performance');
      expect(hasSafety || hasPerformance).toBe(true);
    });

    test('TC6: Feature list mentions async I/O or tokio runtime', async ({ page }) => {
      const asyncFeature = page.locator(getByTestId('feature-async'));
      await expect(asyncFeature).toBeVisible();

      const text = await asyncFeature.textContent();
      const hasAsync = text?.toLowerCase().includes('async');
      const hasTokio = text?.toLowerCase().includes('tokio');
      expect(hasAsync || hasTokio).toBe(true);
    });
  });

  test.describe('Quick Start Section', () => {
    test('TC7: Quick Start section contains a code block element', async ({ page }) => {
      const quickstartSection = page.locator(getByTestId('quickstart-section'));
      await expect(quickstartSection).toBeVisible();

      const codeBlock = page.locator(getByTestId('quickstart-code'));
      await expect(codeBlock).toBeVisible();

      // Verify it's a pre element with code
      const preElement = quickstartSection.locator('pre');
      await expect(preElement).toBeVisible();

      const codeElement = preElement.locator('code');
      await expect(codeElement).toBeVisible();
    });

    test('TC8: Code block includes cargo install mirdb or equivalent installation command', async ({ page }) => {
      const codeBlock = page.locator(getByTestId('quickstart-code'));
      const codeText = await codeBlock.textContent();

      // Check for installation command
      const hasCargoInstall = codeText?.includes('cargo install mirdb');
      const hasInstallComment = codeText?.toLowerCase().includes('install');
      expect(hasCargoInstall).toBe(true);
      expect(hasInstallComment).toBe(true);
    });

    test('TC9: Code block includes command to start the MirDB server', async ({ page }) => {
      const codeBlock = page.locator(getByTestId('quickstart-code'));
      const codeText = await codeBlock.textContent();

      // Check for server start command
      const hasServerStart = codeText?.includes('mirdb-server');
      const hasStartComment = codeText?.toLowerCase().includes('start');
      expect(hasServerStart).toBe(true);
      expect(hasStartComment).toBe(true);
    });

    test('TC10: Code block uses monospace font', async ({ page }) => {
      const codeElement = page.locator(getByTestId('quickstart-code')).locator('code');
      await expect(codeElement).toBeVisible();

      // Get the computed font-family
      const fontFamily = await codeElement.evaluate((el) => {
        return window.getComputedStyle(el).fontFamily;
      });

      // Check for monospace font family
      const isMonospace = fontFamily.toLowerCase().includes('mono') ||
                          fontFamily.toLowerCase().includes('consolas') ||
                          fontFamily.toLowerCase().includes('menlo') ||
                          fontFamily.toLowerCase().includes('courier');
      expect(isMonospace).toBe(true);
    });
  });
});
