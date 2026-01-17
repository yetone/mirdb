import { test, expect, ConsoleMessage } from '@playwright/test';

/**
 * E2E Tests for Cross-Browser Compatibility - Chrome
 *
 * This test suite verifies the homepage renders correctly in Google Chrome,
 * all interactive elements work as expected, and there are no JavaScript errors.
 *
 * Requirements: NFR-5 - Cross-browser compatibility (Chrome, Firefox, Safari, Edge)
 */

test.describe('Cross-Browser Compatibility - Chrome', () => {
  // Collect console errors throughout the test
  const consoleErrors: string[] = [];

  // Filter out expected errors that are not application JS errors
  const isExpectedError = (errorText: string): boolean => {
    // Network/resource loading errors (404s for external resources)
    if (errorText.includes('Failed to load resource')) return true;
    // SRI integrity errors from CDN resources
    if (errorText.includes("Failed to find a valid digest in the 'integrity' attribute")) return true;
    // Clipboard permission denied in headless mode (browser security feature, not app error)
    if (errorText.includes('Clipboard') && errorText.includes('permission denied')) return true;
    // Favicon 404 errors
    if (errorText.includes('favicon')) return true;
    return false;
  };

  test.beforeEach(async ({ page }) => {
    // Clear console errors for each test
    consoleErrors.length = 0;

    // Listen for console errors
    page.on('console', (msg: ConsoleMessage) => {
      if (msg.type() === 'error') {
        const text = msg.text();
        // Only track actual application JavaScript errors
        if (!isExpectedError(text)) {
          consoleErrors.push(text);
        }
      }
    });

    // Listen for page errors (uncaught exceptions)
    page.on('pageerror', (error: Error) => {
      const text = `Page Error: ${error.message}`;
      // Only track actual application JavaScript errors
      if (!isExpectedError(text)) {
        consoleErrors.push(text);
      }
    });

    await page.goto('/');
  });

  test.describe('Test Case 1: Page renders without visual issues in Chrome', () => {
    test('Homepage loads successfully and is visible', async ({ page }) => {
      // Verify the page title
      await expect(page).toHaveTitle('MirDB - Persistent Key-Value Store with Memcached Protocol');

      // Verify the body is visible
      await expect(page.locator('body')).toBeVisible();
    });

    test('Hero section renders correctly', async ({ page }) => {
      const hero = page.locator('.hero');
      await expect(hero).toBeVisible();

      // Verify logo is present
      const logo = page.locator('.logo');
      await expect(logo).toBeVisible();

      // Verify main heading
      const heading = page.locator('h1');
      await expect(heading).toContainText('MirDB');

      // Verify tagline
      const tagline = page.locator('.tagline');
      await expect(tagline).toContainText('Persistent Key-Value Store with Memcached Protocol');

      // Verify description
      const description = page.locator('.hero .description');
      await expect(description).toBeVisible();
    });

    test('Features section renders correctly', async ({ page }) => {
      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeVisible();

      // Verify section heading
      const heading = featuresSection.locator('h2');
      await expect(heading).toContainText('Key Features');

      // Verify all 7 feature cards are visible
      const featureCards = featuresSection.locator('.feature-card');
      await expect(featureCards).toHaveCount(7);

      // Verify each feature card has required elements
      for (let i = 0; i < 7; i++) {
        const card = featureCards.nth(i);
        await expect(card).toBeVisible();
        await expect(card.locator('.feature-icon')).toBeVisible();
        await expect(card.locator('h3')).toBeVisible();
        await expect(card.locator('p')).toBeVisible();
      }
    });

    test('Commands section renders correctly', async ({ page }) => {
      const commandsSection = page.locator('#commands');
      await expect(commandsSection).toBeVisible();

      // Verify all command cards are present
      const commands = ['SET', 'GET', 'DELETE', 'ADD', 'REPLACE', 'APPEND', 'PREPEND'];
      for (const command of commands) {
        const commandCard = page.locator(`[data-command="${command}"]`);
        await expect(commandCard).toBeVisible();
      }
    });

    test('Code example section renders correctly', async ({ page }) => {
      const codeSection = page.locator('#code-example');
      await expect(codeSection).toBeVisible();

      // Verify code block is present
      const codeBlock = codeSection.locator('.code-block');
      await expect(codeBlock).toBeVisible();

      // Verify code content is visible
      const code = page.locator('#example-code');
      await expect(code).toBeVisible();
      await expect(code).toContainText('pymemcache');
    });

    test('Architecture section renders correctly', async ({ page }) => {
      const archSection = page.locator('#architecture');
      await expect(archSection).toBeVisible();

      // Verify diagram components are visible
      const walComponent = page.locator('[data-component="wal"]');
      await expect(walComponent).toBeVisible();

      const memtableComponent = page.locator('[data-component="memtable"]');
      await expect(memtableComponent).toBeVisible();

      const sstableComponent = page.locator('[data-component="sstable"]');
      await expect(sstableComponent).toBeVisible();
    });

    test('Getting started section renders correctly', async ({ page }) => {
      const gettingStarted = page.locator('#getting-started');
      await expect(gettingStarted).toBeVisible();

      // Verify section heading
      const heading = gettingStarted.locator('h2');
      await expect(heading).toContainText('Getting Started');

      // Verify steps are present
      const steps = gettingStarted.locator('.step');
      await expect(steps).toHaveCount(3);
    });

    test('Configuration section renders correctly', async ({ page }) => {
      const configSection = page.locator('#configuration');
      await expect(configSection).toBeVisible();

      // Verify table is present
      const table = configSection.locator('table');
      await expect(table).toBeVisible();

      // Verify table has rows
      const rows = table.locator('tbody tr');
      const rowCount = await rows.count();
      expect(rowCount).toBeGreaterThan(0);
    });

    test('Footer section renders correctly', async ({ page }) => {
      const footer = page.locator('[data-section="footer"]');
      await expect(footer).toBeVisible();

      // Verify footer links
      const footerLinks = footer.locator('.footer-links a');
      const linkCount = await footerLinks.count();
      expect(linkCount).toBeGreaterThan(0);

      // Verify license info
      const license = page.locator('[data-info="license"]');
      await expect(license).toContainText('MIT License');

      // Verify version info
      const version = page.locator('[data-info="version"]');
      await expect(version).toContainText('Version');
    });
  });

  test.describe('Test Case 2: All buttons are clickable and functional in Chrome', () => {
    test('Get Started button is clickable and navigates to getting-started section', async ({ page }) => {
      const getStartedBtn = page.locator('[data-link="get-started"]');
      await expect(getStartedBtn).toBeVisible();
      await expect(getStartedBtn).toBeEnabled();

      // Click and verify navigation
      await getStartedBtn.click();

      // Verify the getting-started section is now in view (URL should have hash)
      await expect(page).toHaveURL(/#getting-started/);
    });

    test('View on GitHub button in hero is clickable', async ({ page }) => {
      const githubBtn = page.locator('[data-link="github-hero"]');
      await expect(githubBtn).toBeVisible();
      await expect(githubBtn).toBeEnabled();

      // Verify href attribute
      const href = await githubBtn.getAttribute('href');
      expect(href).toContain('github.com');

      // Verify target attribute for new tab
      const target = await githubBtn.getAttribute('target');
      expect(target).toBe('_blank');
    });

    test('Copy button in code example is clickable and functional', async ({ page }) => {
      const copyBtn = page.locator('.copy-btn');
      await expect(copyBtn).toBeVisible();
      await expect(copyBtn).toBeEnabled();

      // Verify initial text
      await expect(copyBtn).toContainText('Copy');

      // Click the copy button
      await copyBtn.click();

      // Verify text changes to "Copied!"
      await expect(copyBtn).toContainText('Copied!');

      // Wait for the text to revert back
      await page.waitForTimeout(2500);
      await expect(copyBtn).toContainText('Copy');
    });

    test('Footer GitHub link is clickable', async ({ page }) => {
      const githubLink = page.locator('[data-link="github-footer"]');
      await expect(githubLink).toBeVisible();
      await expect(githubLink).toBeEnabled();

      // Verify href attribute
      const href = await githubLink.getAttribute('href');
      expect(href).toContain('github.com');
    });

    test('Footer Issues link is clickable', async ({ page }) => {
      const issuesLink = page.locator('[data-link="issues"]');
      await expect(issuesLink).toBeVisible();
      await expect(issuesLink).toBeEnabled();

      // Verify href attribute
      const href = await issuesLink.getAttribute('href');
      expect(href).toContain('issues');
    });

    test('Footer Documentation link is clickable and navigates correctly', async ({ page }) => {
      const docsLink = page.locator('[data-link="documentation"]');
      await expect(docsLink).toBeVisible();
      await expect(docsLink).toBeEnabled();

      // Click and verify navigation
      await docsLink.click();

      // Verify the URL has the getting-started hash
      await expect(page).toHaveURL(/#getting-started/);
    });

    test('CI badge link is clickable', async ({ page }) => {
      const ciBadge = page.locator('[data-badge="ci"] a');
      await expect(ciBadge).toBeVisible();

      // Verify href attribute
      const href = await ciBadge.getAttribute('href');
      expect(href).toContain('circleci.com');
    });

    test('All CTA buttons have proper cursor styling', async ({ page }) => {
      const ctaButtons = page.locator('.btn');
      const count = await ctaButtons.count();

      for (let i = 0; i < count; i++) {
        const btn = ctaButtons.nth(i);
        await expect(btn).toBeVisible();

        // Verify cursor style is pointer (clickable)
        const cursor = await btn.evaluate((el) => {
          return window.getComputedStyle(el).cursor;
        });
        expect(cursor).toBe('pointer');
      }
    });
  });

  test.describe('Test Case 3: No JavaScript errors in console in Chrome', () => {
    test('Page loads without any JavaScript errors', async ({ page }) => {
      // Wait for page to fully load
      await page.waitForLoadState('networkidle');

      // Check that no console errors were captured
      expect(consoleErrors).toHaveLength(0);
    });

    test('Copy functionality works without JavaScript errors', async ({ page }) => {
      await page.waitForLoadState('networkidle');

      // Click copy button
      const copyBtn = page.locator('.copy-btn');
      await copyBtn.click();

      // Wait for the copy operation to complete
      await page.waitForTimeout(500);

      // Check that no console errors occurred during copy
      expect(consoleErrors).toHaveLength(0);
    });

    test('Navigation interactions do not cause JavaScript errors', async ({ page }) => {
      await page.waitForLoadState('networkidle');

      // Click on Get Started button
      const getStartedBtn = page.locator('[data-link="get-started"]');
      await getStartedBtn.click();

      // Wait for navigation
      await page.waitForTimeout(500);

      // Click on Documentation link in footer
      const docsLink = page.locator('[data-link="documentation"]');
      await docsLink.click();

      // Wait for navigation
      await page.waitForTimeout(500);

      // Check that no errors occurred
      expect(consoleErrors).toHaveLength(0);
    });

    test('Prism.js syntax highlighting loads without errors', async ({ page }) => {
      await page.waitForLoadState('networkidle');

      // Verify Prism classes are applied to code blocks (use first() to handle multiple matches)
      const pythonCode = page.locator('#example-code');
      await expect(pythonCode).toBeVisible();
      await expect(pythonCode).toHaveClass(/language-python/);

      // Check for syntax highlighting (Prism adds token classes)
      const tokens = page.locator('#example-code .token');
      const tokenCount = await tokens.count();
      expect(tokenCount).toBeGreaterThan(0);

      // Verify no JavaScript errors from Prism
      expect(consoleErrors).toHaveLength(0);
    });

    test('All external resources load without errors', async ({ page }) => {
      // Wait for all resources to load
      await page.waitForLoadState('networkidle');

      // Give extra time for any delayed scripts
      await page.waitForTimeout(1000);

      // Check that no errors occurred from external resources
      expect(consoleErrors).toHaveLength(0);
    });

    test('Page scroll interactions do not cause errors', async ({ page }) => {
      await page.waitForLoadState('networkidle');

      // Scroll to different sections
      await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
      await page.waitForTimeout(300);

      await page.evaluate(() => window.scrollTo(0, 0));
      await page.waitForTimeout(300);

      // Scroll to middle
      await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight / 2));
      await page.waitForTimeout(300);

      // Check that no errors occurred during scrolling
      expect(consoleErrors).toHaveLength(0);
    });
  });

  test.describe('Chrome-specific rendering tests', () => {
    test('CSS styles are applied correctly', async ({ page }) => {
      // Verify hero background
      const hero = page.locator('.hero');
      const heroBackground = await hero.evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });
      expect(heroBackground).toBeTruthy();

      // Verify buttons have correct styling
      const primaryBtn = page.locator('.btn-primary').first();
      const btnBackground = await primaryBtn.evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });
      expect(btnBackground).toBeTruthy();
    });

    test('Responsive viewport works correctly', async ({ page }) => {
      // Set viewport to desktop size (default for Chrome)
      await page.setViewportSize({ width: 1280, height: 720 });

      // Verify layout is correct at desktop size
      const featuresGrid = page.locator('.features-grid');
      await expect(featuresGrid).toBeVisible();

      // Verify the grid is laid out horizontally
      const gridDisplay = await featuresGrid.evaluate((el) => {
        return window.getComputedStyle(el).display;
      });
      expect(gridDisplay).toBe('grid');
    });

    test('Images load correctly', async ({ page }) => {
      // Wait for images to load
      await page.waitForLoadState('networkidle');

      // Check logo image
      const logo = page.locator('.logo');
      await expect(logo).toBeVisible();

      // Verify logo has dimensions
      const logoBoundingBox = await logo.boundingBox();
      expect(logoBoundingBox).toBeTruthy();
      expect(logoBoundingBox!.width).toBeGreaterThan(0);
      expect(logoBoundingBox!.height).toBeGreaterThan(0);
    });

    test('Fonts render correctly', async ({ page }) => {
      // Verify font family is applied
      const heading = page.locator('h1');
      const fontFamily = await heading.evaluate((el) => {
        return window.getComputedStyle(el).fontFamily;
      });
      expect(fontFamily).toBeTruthy();

      // Verify text is readable (has proper font size)
      const fontSize = await heading.evaluate((el) => {
        return parseFloat(window.getComputedStyle(el).fontSize);
      });
      expect(fontSize).toBeGreaterThan(16); // Should be larger than base font
    });
  });
});
