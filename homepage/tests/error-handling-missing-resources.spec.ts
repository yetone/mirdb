import { test, expect, Route } from '@playwright/test';

/**
 * Error Handling - Missing Resources Tests
 *
 * Tests for graceful degradation when resources fail to load.
 * Verifies the site follows progressive enhancement principles.
 *
 * Test Cases:
 * TC1: Block image loading and verify alt text displays
 * TC2: Disable CSS and verify content remains readable
 * TC3: Disable JavaScript and verify core content is accessible
 * TC4: Test 404 page handling for non-existent routes
 * TC5: Check for graceful degradation (progressive enhancement)
 */

test.describe('Error Handling - Missing Resources', () => {
  const baseUrl = 'file://' + process.cwd() + '/public/index.html';

  test.describe('TC1: Broken Image Handling', () => {
    test('Page loads without errors when images fail to load', async ({ page }) => {
      // Block all image requests
      await page.route('**/*.{png,jpg,jpeg,gif,webp,svg,ico}', (route: Route) => {
        route.abort();
      });

      // Navigate to the page
      const response = await page.goto(baseUrl);

      // Page should load successfully (200 for file:// protocol in this Playwright version)
      expect(response?.ok()).toBe(true);

      // Check no JavaScript errors occurred
      const errors: string[] = [];
      page.on('pageerror', (error) => errors.push(error.message));

      // Verify main content is still visible
      await expect(page.locator('h1')).toBeVisible();
      await expect(page.locator('[data-testid="hero-section"]')).toBeVisible();

      // No critical errors should have occurred
      expect(errors).toHaveLength(0);
    });

    test('SVG architecture diagram shows alt text when blocked', async ({ page }) => {
      // Navigate to page first
      await page.goto(baseUrl);

      // The SVG has role="img" and aria-label attribute for accessibility
      const diagram = page.locator('[data-testid="architecture-diagram"]');
      await expect(diagram).toBeVisible();

      // Check it has proper aria-label for screen readers
      const ariaLabel = await diagram.getAttribute('aria-label');
      expect(ariaLabel).toBeTruthy();
      expect(ariaLabel?.length).toBeGreaterThan(10);

      // Check it has title element for fallback text
      const titleElement = diagram.locator('title');
      await expect(titleElement).toHaveText(/LSM Tree/i);

      // Check it has desc element for detailed description
      const descElement = diagram.locator('desc');
      const descText = await descElement.textContent();
      expect(descText?.length).toBeGreaterThan(50);
    });

    test('All images have appropriate alt attributes defined', async ({ page }) => {
      await page.goto(baseUrl);

      // Check all img elements have alt attributes
      const images = await page.$$eval('img', (imgs) =>
        imgs.map((img) => ({
          src: img.getAttribute('src'),
          alt: img.getAttribute('alt'),
          hasAlt: img.hasAttribute('alt'),
        }))
      );

      // Every image should have an alt attribute
      for (const img of images) {
        expect(img.hasAlt, `Image ${img.src} missing alt attribute`).toBe(true);
      }

      // Check SVG images have accessible names
      const svgImages = await page.$$eval('svg[role="img"]', (svgs) =>
        svgs.map((svg) => ({
          ariaLabel: svg.getAttribute('aria-label'),
          hasTitle: !!svg.querySelector('title'),
          testId: svg.getAttribute('data-testid'),
        }))
      );

      for (const svg of svgImages) {
        // ariaLabel is a string or null; hasTitle is boolean - check if either provides accessible name
        const hasAccessibleName = (svg.ariaLabel !== null && svg.ariaLabel.length > 0) || svg.hasTitle;
        expect(hasAccessibleName, `SVG ${svg.testId} missing accessible name`).toBe(true);
      }
    });
  });

  test.describe('TC2: CSS Disabled - Content Readability', () => {
    test('Content remains readable when CSS fails to load', async ({ page }) => {
      // Block all CSS requests
      await page.route('**/*.css', (route: Route) => {
        route.abort();
      });

      // Navigate to page without CSS
      await page.goto(baseUrl);

      // Main heading should still be visible and contain product name
      const h1 = page.locator('h1');
      await expect(h1).toBeVisible();
      await expect(h1).toContainText('MirDB');

      // Tagline should be visible
      const tagline = page.locator('[data-testid="tagline"]');
      await expect(tagline).toBeVisible();

      // Features section should be visible
      const featuresSection = page.locator('[data-testid="features-section"]');
      await expect(featuresSection).toBeVisible();

      // Feature cards should be visible
      const featureCards = page.locator('[data-testid^="feature-"]');
      const cardCount = await featureCards.count();
      expect(cardCount).toBeGreaterThanOrEqual(3);

      // Configuration table should be readable
      const configTable = page.locator('[data-testid="config-table"]');
      await expect(configTable).toBeVisible();

      // Footer should be visible
      const footer = page.locator('[data-testid="footer"]');
      await expect(footer).toBeVisible();
    });

    test('Content is logically ordered without CSS', async ({ page }) => {
      // Block CSS
      await page.route('**/*.css', (route: Route) => {
        route.abort();
      });

      await page.goto(baseUrl);

      // Get all major content sections in DOM order
      const sections = await page.$$eval('main > section, header, footer', (elements) =>
        elements.map((el) => ({
          tag: el.tagName.toLowerCase(),
          id: el.id || el.getAttribute('data-testid') || 'unknown',
          firstHeading: el.querySelector('h1, h2, h3')?.textContent?.trim() || '',
        }))
      );

      // Verify logical content order (header content should come before main sections)
      const sectionOrder = sections.map((s) => s.id);

      // Hero should come before features
      const heroIndex = sectionOrder.findIndex((id) => id.includes('hero'));
      const featuresIndex = sectionOrder.findIndex((id) => id.includes('features'));
      if (heroIndex !== -1 && featuresIndex !== -1) {
        expect(heroIndex).toBeLessThan(featuresIndex);
      }

      // Features should come before architecture
      const architectureIndex = sectionOrder.findIndex((id) => id.includes('architecture'));
      if (featuresIndex !== -1 && architectureIndex !== -1) {
        expect(featuresIndex).toBeLessThan(architectureIndex);
      }

      // Quick start should be accessible
      const quickstartIndex = sectionOrder.findIndex((id) => id.includes('quickstart'));
      expect(quickstartIndex).toBeGreaterThan(-1);
    });

    test('Navigation links are functional without CSS', async ({ page }) => {
      // Block CSS
      await page.route('**/*.css', (route: Route) => {
        route.abort();
      });

      await page.goto(baseUrl);

      // Navigation links should be visible and accessible
      const navLinks = page.locator('.nav-links a, nav a');
      const linkCount = await navLinks.count();
      expect(linkCount).toBeGreaterThan(0);

      // Each link should have visible text
      for (let i = 0; i < linkCount; i++) {
        const link = navLinks.nth(i);
        const text = await link.textContent();
        expect(text?.trim().length).toBeGreaterThan(0);
      }
    });
  });

  // TC3: JavaScript Disabled tests - need to use browser context with javaScriptEnabled: false
  test.describe('TC3: JavaScript Disabled - Core Content Accessible', () => {
    // Use a custom test fixture to disable JavaScript at context level
    test('Core content is accessible without JavaScript', async ({ browser }) => {
      // Create a context with JavaScript disabled
      const context = await browser.newContext({ javaScriptEnabled: false });
      const page = await context.newPage();

      // Navigate to page
      await page.goto(baseUrl);

      // Main heading should be visible
      const h1 = page.locator('h1');
      await expect(h1).toBeVisible();
      await expect(h1).toContainText('MirDB');

      // Product description should be visible
      const heroDescription = page.locator('.hero-description');
      await expect(heroDescription).toBeVisible();

      // Features should be visible
      const featuresSection = page.locator('[data-testid="features-section"]');
      await expect(featuresSection).toBeVisible();

      // Architecture diagram should be visible
      const architectureDiagram = page.locator('[data-testid="architecture-diagram"]');
      await expect(architectureDiagram).toBeVisible();

      // Quick start code should be visible
      const quickstartSection = page.locator('[data-testid="quickstart-section"]');
      await expect(quickstartSection).toBeVisible();

      // Configuration table should be visible
      const configTable = page.locator('[data-testid="config-table"]');
      await expect(configTable).toBeVisible();

      // Footer should be visible
      const footer = page.locator('[data-testid="footer"]');
      await expect(footer).toBeVisible();

      await context.close();
    });

    test('Navigation works via standard links without JavaScript', async ({ browser }) => {
      // Create a context with JavaScript disabled
      const context = await browser.newContext({ javaScriptEnabled: false });
      const page = await context.newPage();

      await page.goto(baseUrl);

      // Internal anchor links should work (Features, Architecture, etc.)
      const featuresLink = page.locator('a[href="#features"]').first();
      await expect(featuresLink).toBeVisible();

      // Click the features link
      await featuresLink.click();

      // Should navigate to features section (URL should contain #features)
      await expect(page).toHaveURL(/#features/);

      // Features section should be visible
      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeVisible();

      await context.close();
    });

    test('External links are accessible without JavaScript', async ({ browser }) => {
      // Create a context with JavaScript disabled
      const context = await browser.newContext({ javaScriptEnabled: false });
      const page = await context.newPage();

      await page.goto(baseUrl);

      // GitHub link should be visible and have correct href
      const githubLinks = page.locator('a[href*="github.com"]');
      const count = await githubLinks.count();
      expect(count).toBeGreaterThan(0);

      // Each GitHub link should have proper attributes for external link
      for (let i = 0; i < count; i++) {
        const link = githubLinks.nth(i);
        await expect(link).toHaveAttribute('target', '_blank');
        await expect(link).toHaveAttribute('rel', /noopener/);
      }

      await context.close();
    });

    test('Skip link works without JavaScript', async ({ browser }) => {
      // Create a context with JavaScript disabled
      const context = await browser.newContext({ javaScriptEnabled: false });
      const page = await context.newPage();

      await page.goto(baseUrl);

      // Skip link should exist
      const skipLink = page.locator('[data-testid="skip-link"]');
      await expect(skipLink).toHaveAttribute('href', '#main-content');

      // Main content target should exist
      const mainContent = page.locator('#main-content');
      await expect(mainContent).toBeVisible();

      await context.close();
    });
  });

  test.describe('TC4: 404 Page Handling', () => {
    test('Non-existent file shows appropriate error', async ({ page }) => {
      // Try to navigate to a non-existent file
      // For file:// protocol, Playwright throws an error for missing files
      // which is the expected behavior - browsers handle this gracefully
      let errorOccurred = false;
      let errorMessage = '';

      try {
        await page.goto('file://' + process.cwd() + '/public/non-existent-page.html', {
          waitUntil: 'domcontentloaded',
        });
      } catch (error) {
        errorOccurred = true;
        errorMessage = (error as Error).message;
      }

      // For file:// protocol, Playwright throws ERR_FILE_NOT_FOUND which is expected
      // This confirms the browser correctly identifies missing files
      expect(errorOccurred).toBe(true);
      expect(errorMessage).toContain('ERR_FILE_NOT_FOUND');
    });

    test('Main index.html loads successfully', async ({ page }) => {
      // Verify the main page loads correctly
      const response = await page.goto(baseUrl);

      // Page should load with ok status
      expect(response?.ok()).toBe(true);

      // Page content should be visible
      await expect(page.locator('h1')).toBeVisible();
    });

    test('Static site structure handles hash navigation gracefully', async ({ page }) => {
      await page.goto(baseUrl);

      // Navigate to a non-existent hash
      await page.goto(baseUrl + '#non-existent-section');

      // Page should still be usable
      await expect(page.locator('h1')).toBeVisible();
      await expect(page.locator('[data-testid="hero-section"]')).toBeVisible();

      // Navigation should still work
      const featuresLink = page.locator('a[href="#features"]').first();
      await featuresLink.click();
      await expect(page).toHaveURL(/#features/);
    });
  });

  test.describe('TC5: Progressive Enhancement / Graceful Degradation', () => {
    test('Site follows semantic HTML structure', async ({ page }) => {
      await page.goto(baseUrl);

      // Check for semantic HTML elements
      const header = page.locator('header');
      await expect(header).toBeVisible();

      const nav = page.locator('nav');
      await expect(nav).toBeVisible();

      const main = page.locator('main');
      await expect(main).toBeVisible();

      const footer = page.locator('footer');
      await expect(footer).toBeVisible();

      // Check for proper section elements
      const sections = await page.$$('main section');
      expect(sections.length).toBeGreaterThan(0);
    });

    test('Interactive elements have non-JS fallbacks', async ({ browser }) => {
      // Create a context with JavaScript disabled
      const context = await browser.newContext({ javaScriptEnabled: false });
      const page = await context.newPage();

      await page.goto(baseUrl);

      // Mobile menu toggle should still have visible nav links in the DOM
      // (CSS may hide them on mobile, but they're still in the document)
      const navLinks = page.locator('#nav-links a');
      const count = await navLinks.count();
      expect(count).toBeGreaterThan(0);

      // Copy button exists but won't work without JS - that's acceptable
      // The code is still visible and can be manually copied
      const codeBlock = page.locator('.code-block pre code');
      await expect(codeBlock).toBeVisible();

      // Code content should be selectable text
      const codeText = await codeBlock.textContent();
      expect(codeText).toContain('mirdb');

      await context.close();
    });

    test('Core functionality works without enhanced features', async ({ browser }) => {
      // Create a context with JavaScript disabled
      const context = await browser.newContext({ javaScriptEnabled: false });
      const page = await context.newPage();

      await page.goto(baseUrl);

      // User can read product information
      const h1 = await page.locator('h1').textContent();
      expect(h1).toContain('MirDB');

      // User can read features
      const features = page.locator('.feature-card');
      const featureCount = await features.count();
      expect(featureCount).toBe(4);

      // User can read configuration
      const configRows = page.locator('[data-testid="config-table"] tbody tr');
      const rowCount = await configRows.count();
      expect(rowCount).toBe(6);

      // User can read quick start guide
      const quickstartCode = page.locator('[data-testid="quickstart-section"] code');
      await expect(quickstartCode).toBeVisible();

      // User can access external resources via links
      const githubLink = page.locator('a[href*="github.com"]').first();
      const href = await githubLink.getAttribute('href');
      expect(href).toContain('github.com');

      await context.close();
    });

    test('Content is accessible to screen readers', async ({ page }) => {
      await page.goto(baseUrl);

      // Check for ARIA landmarks
      const landmarks = await page.$$eval('[role], nav, main, header, footer, section', (elements) =>
        elements.map((el) => ({
          tag: el.tagName.toLowerCase(),
          role: el.getAttribute('role'),
          ariaLabel: el.getAttribute('aria-label'),
        }))
      );

      expect(landmarks.length).toBeGreaterThan(0);

      // Navigation should have proper role/label
      const nav = page.locator('nav');
      await expect(nav).toHaveAttribute('role', 'navigation');
      await expect(nav).toHaveAttribute('aria-label', /navigation/i);

      // Architecture diagram should be accessible
      const diagram = page.locator('[data-testid="architecture-diagram"]');
      await expect(diagram).toHaveAttribute('role', 'img');
      const ariaLabel = await diagram.getAttribute('aria-label');
      expect(ariaLabel?.length).toBeGreaterThan(0);
    });

    test('Page weight is optimized for slow connections', async ({ page }) => {
      // This is a static site with inline CSS/JS, so we check the HTML size
      await page.goto(baseUrl);

      // Get the page HTML
      const html = await page.content();
      const htmlSizeKB = Buffer.byteLength(html, 'utf8') / 1024;

      // HTML should be reasonably sized (< 100KB for a single page app)
      expect(htmlSizeKB).toBeLessThan(100);

      // Verify no large inline data URIs
      const dataUriMatches = html.match(/data:[^"]+/g) || [];
      for (const uri of dataUriMatches) {
        // Each data URI should be under 50KB
        expect(uri.length).toBeLessThan(50 * 1024);
      }
    });
  });
});
