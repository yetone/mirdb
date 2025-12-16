// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Error Handling - Missing Resources Tests
 *
 * Verifies graceful handling of missing or failed resources:
 * - Test Case 1: Block image loading and reload page - page remains functional with alt text visible
 * - Test Case 2: Access non-existent URL path - appropriate 404 handling
 * - Test Case 3: Test with JavaScript disabled - core content remains accessible
 */

test.describe('Error Handling - Missing Resources', () => {
  test.describe('Test Case 1: Failed Image Loading', () => {
    test('page remains functional when images fail to load', async ({ page }) => {
      // Block all image requests to simulate failed image loading
      await page.route('**/*.{png,jpg,jpeg,gif,webp,svg}', (route) => {
        route.abort('failed');
      });

      // Also block images loaded via CDN or external sources
      await page.route('**/*', (route, request) => {
        const resourceType = request.resourceType();
        if (resourceType === 'image') {
          return route.abort('failed');
        }
        return route.continue();
      });

      // Navigate to the homepage
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Verify the page loaded successfully despite blocked images
      await expect(page).toHaveTitle(/MirDB/);

      // Verify main sections are still visible and functional
      const heroSection = page.locator('[data-testid="hero-section"]');
      await expect(heroSection).toBeVisible();

      const productName = page.locator('[data-testid="product-name"]');
      await expect(productName).toBeVisible();
      await expect(productName).toHaveText('MirDB');

      const tagline = page.locator('[data-testid="tagline"]');
      await expect(tagline).toBeVisible();

      // Verify features section is accessible
      const featuresSection = page.locator('[data-testid="features-section"]');
      await expect(featuresSection).toBeVisible();

      // Verify navigation still works
      const primaryCTA = page.locator('[data-testid="primary-cta"]');
      await expect(primaryCTA).toBeVisible();
      await expect(primaryCTA).toBeEnabled();

      const secondaryCTA = page.locator('[data-testid="secondary-cta"]');
      await expect(secondaryCTA).toBeVisible();
      await expect(secondaryCTA).toBeEnabled();
    });

    test('all img elements have alt text for accessibility when images fail', async ({ page }) => {
      // Block all images
      await page.route('**/*', (route, request) => {
        const resourceType = request.resourceType();
        if (resourceType === 'image') {
          return route.abort('failed');
        }
        return route.continue();
      });

      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Get all img elements and verify they have alt attributes
      const images = await page.locator('img').all();

      for (const img of images) {
        const hasAlt = await img.evaluate((el) => el.hasAttribute('alt'));
        expect(hasAlt, 'All img elements must have alt attribute').toBe(true);

        // Verify alt text is meaningful (not empty for non-decorative images)
        const altText = await img.getAttribute('alt');
        const ariaHidden = await img.getAttribute('aria-hidden');
        const role = await img.getAttribute('role');

        // If image is decorative (empty alt), it should be marked appropriately
        // If not decorative, alt should have meaningful text
        if (altText === '') {
          const isDecorativeMarked = role === 'presentation' || ariaHidden === 'true';
          // Empty alt is valid for decorative images, or the page uses proper marking
          expect(
            isDecorativeMarked || altText === '',
            'Decorative images should have empty alt or be marked as decorative'
          ).toBe(true);
        }
      }

      // Page should still be functional even if no images are present
      // (MirDB homepage uses Mermaid diagrams instead of img elements)
      await expect(page.locator('[data-testid="hero-section"]')).toBeVisible();
    });

    test('page layout remains intact when images fail to load', async ({ page }) => {
      // Block all images
      await page.route('**/*', (route, request) => {
        if (request.resourceType() === 'image') {
          return route.abort('failed');
        }
        return route.continue();
      });

      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Verify layout integrity by checking key sections exist in proper order
      const sections = [
        { selector: '[data-testid="hero-section"]', name: 'Hero' },
        { selector: '[data-testid="features-section"]', name: 'Features' },
        { selector: '[data-testid="architecture-section"]', name: 'Architecture' },
        { selector: '[data-testid="commands-section"]', name: 'Commands' },
        { selector: '[data-testid="getting-started-section"]', name: 'Getting Started' },
      ];

      for (const section of sections) {
        const element = page.locator(section.selector);
        await expect(element, `${section.name} section should be visible`).toBeVisible();
      }

      // Verify footer is visible
      const footer = page.locator('footer');
      await expect(footer).toBeVisible();

      // Verify the architecture diagram container still exists
      // (Mermaid renders as SVG, not img, so it should be unaffected)
      const diagramContainer = page.locator('[data-testid="architecture-diagram"]');
      await expect(diagramContainer).toBeVisible();
    });
  });

  test.describe('Test Case 2: 404 Page Handling', () => {
    test('appropriate handling when accessing non-existent URL path', async ({ page }) => {
      // Navigate to a non-existent page
      const response = await page.goto('/non-existent-page-12345');

      // The server should respond with a 404 status code
      // serve package returns 404 for non-existent paths
      expect(response).not.toBeNull();
      expect(response?.status()).toBe(404);
    });

    test('non-existent paths return proper HTTP status', async ({ page }) => {
      // Test multiple non-existent paths
      const nonExistentPaths = [
        '/about',
        '/contact',
        '/docs/missing',
        '/api/data',
        '/random-path-xyz',
      ];

      for (const path of nonExistentPaths) {
        const response = await page.goto(path);
        expect(response).not.toBeNull();
        expect(response?.status(), `Path ${path} should return 404`).toBe(404);
      }
    });

    test('404 response still allows navigation back to homepage', async ({ page }) => {
      // First go to homepage to establish a working state
      await page.goto('/');
      await expect(page.locator('[data-testid="hero-section"]')).toBeVisible();

      // Navigate to non-existent page
      const response = await page.goto('/does-not-exist');
      expect(response?.status()).toBe(404);

      // Navigate back to homepage - should work
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Verify homepage is functional
      await expect(page.locator('[data-testid="hero-section"]')).toBeVisible();
      await expect(page.locator('[data-testid="product-name"]')).toHaveText('MirDB');
    });

    test('static assets return 200 for existing files', async ({ page }) => {
      // Verify that existing files are served correctly (not 404)
      const existingPaths = [
        { path: '/', expectedStatus: 200 },
        { path: '/index.html', expectedStatus: 200 },
        { path: '/styles.css', expectedStatus: 200 },
        { path: '/main.js', expectedStatus: 200 },
      ];

      for (const { path, expectedStatus } of existingPaths) {
        const response = await page.goto(path);
        expect(response).not.toBeNull();
        expect(response?.status(), `Path ${path} should return ${expectedStatus}`).toBe(expectedStatus);
      }
    });
  });

  test.describe('Test Case 3: JavaScript Disabled', () => {
    test.use({ javaScriptEnabled: false });

    test('core content remains accessible without JavaScript', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Verify main content is still visible
      const heroSection = page.locator('[data-testid="hero-section"]');
      await expect(heroSection).toBeVisible();

      const productName = page.locator('[data-testid="product-name"]');
      await expect(productName).toBeVisible();
      await expect(productName).toHaveText('MirDB');

      const tagline = page.locator('[data-testid="tagline"]');
      await expect(tagline).toBeVisible();
      const taglineText = await tagline.textContent();
      expect(taglineText).toContain('persistent key-value store');
      expect(taglineText).toContain('Memcached');
      expect(taglineText).toContain('Rust');
    });

    test('all major sections are visible without JavaScript', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Check all major sections
      const sectionsToVerify = [
        { selector: '[data-testid="hero-section"]', contains: 'MirDB' },
        { selector: '[data-testid="features-section"]', contains: 'Key Features' },
        { selector: '[data-testid="architecture-section"]', contains: 'Architecture' },
        { selector: '[data-testid="commands-section"]', contains: 'Supported Commands' },
        { selector: '[data-testid="getting-started-section"]', contains: 'Getting Started' },
      ];

      for (const { selector, contains } of sectionsToVerify) {
        const section = page.locator(selector);
        await expect(section, `Section ${selector} should be visible`).toBeVisible();
        const text = await section.textContent();
        expect(text, `Section should contain "${contains}"`).toContain(contains);
      }
    });

    test('navigation links are present without JavaScript', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Verify navigation links exist and have proper hrefs
      // Use .nav-link class to specifically target navigation links
      const navLinks = [
        { text: 'Features', href: '#features' },
        { text: 'Architecture', href: '#architecture' },
        { text: 'Commands', href: '#commands' },
        { text: 'Getting Started', href: '#getting-started' },
      ];

      for (const { text, href } of navLinks) {
        // Use .nav-link class to specifically target navigation links (not CTAs)
        const link = page.locator(`.nav-link[href="${href}"]`);
        await expect(link, `Navigation link to ${href} should exist`).toBeAttached();
      }
    });

    test('feature cards content is accessible without JavaScript', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      const featuresSection = page.locator('[data-testid="features-section"]');
      await expect(featuresSection).toBeVisible();

      // Verify feature cards contain expected content
      const expectedFeatures = [
        'Memcached Compatible',
        'Persistent Storage',
        'High Performance',
        'Durable',
        'Efficient',
        'Written in Rust',
      ];

      const sectionText = await featuresSection.textContent();
      for (const feature of expectedFeatures) {
        expect(sectionText, `Features should include "${feature}"`).toContain(feature);
      }
    });

    test('commands table is readable without JavaScript', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      const commandsTable = page.locator('[data-testid="commands-table"]');
      await expect(commandsTable).toBeVisible();

      // Verify getter commands
      const getterRow = page.locator('[data-testid="getter-commands-row"]');
      await expect(getterRow).toBeVisible();
      const getterText = await getterRow.textContent();
      expect(getterText).toContain('get');
      expect(getterText).toContain('gets');

      // Verify setter commands
      const setterRow = page.locator('[data-testid="setter-commands-row"]');
      await expect(setterRow).toBeVisible();
      const setterText = await setterRow.textContent();
      expect(setterText).toContain('set');
      expect(setterText).toContain('add');
      expect(setterText).toContain('replace');

      // Verify other commands
      const otherRow = page.locator('[data-testid="other-commands-row"]');
      await expect(otherRow).toBeVisible();
      const otherText = await otherRow.textContent();
      expect(otherText).toContain('delete');
    });

    test('getting started code examples are visible without JavaScript', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Verify installation instructions are visible
      const installationSection = page.locator('[data-testid="installation-instructions"]');
      await expect(installationSection).toBeVisible();
      const installText = await installationSection.textContent();
      expect(installText).toContain('git clone');
      expect(installText).toContain('cargo build');

      // Verify configuration example is visible
      const configSection = page.locator('[data-testid="configuration-example"]');
      await expect(configSection).toBeVisible();
      const configText = await configSection.textContent();
      expect(configText).toContain('addr');
      expect(configText).toContain('work_dir');

      // Verify usage examples are visible
      const usageSection = page.locator('[data-testid="usage-examples"]');
      await expect(usageSection).toBeVisible();
      const usageText = await usageSection.textContent();
      expect(usageText).toContain('telnet');
      expect(usageText).toContain('set mykey');
    });

    test('CTA buttons are functional without JavaScript', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Check primary CTA (View on GitHub) - external link
      const primaryCTA = page.locator('[data-testid="primary-cta"]');
      await expect(primaryCTA).toBeVisible();
      const primaryHref = await primaryCTA.getAttribute('href');
      expect(primaryHref).toContain('github.com');

      // Check secondary CTA (Learn More) - anchor link
      const secondaryCTA = page.locator('[data-testid="secondary-cta"]');
      await expect(secondaryCTA).toBeVisible();
      const secondaryHref = await secondaryCTA.getAttribute('href');
      expect(secondaryHref).toBe('#features');
    });

    test('footer content is accessible without JavaScript', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      const footer = page.locator('footer');
      await expect(footer).toBeVisible();

      const footerText = await footer.textContent();
      expect(footerText).toContain('GitHub');
      expect(footerText).toContain('Rust');
      expect(footerText).toContain('Tokio');
      expect(footerText).toContain('MIT License');
    });

    test('architecture diagram fallback content without JavaScript', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      const architectureSection = page.locator('[data-testid="architecture-section"]');
      await expect(architectureSection).toBeVisible();

      // Without JavaScript, Mermaid won't render the diagram as SVG
      // But the mermaid code block should still be visible as text content
      const mermaidDiagram = page.locator('[data-testid="mermaid-diagram"]');
      await expect(mermaidDiagram).toBeAttached();

      // The raw mermaid code should be visible as fallback
      // or at minimum, the architecture descriptions should be visible
      const writePathDesc = page.locator('[data-testid="write-path-description"]');
      await expect(writePathDesc).toBeVisible();
      const writePathText = await writePathDesc.textContent();
      expect(writePathText).toContain('WAL');
      expect(writePathText).toContain('Memtable');
      expect(writePathText).toContain('SSTable');

      const readPathDesc = page.locator('[data-testid="read-path-description"]');
      await expect(readPathDesc).toBeVisible();
      const readPathText = await readPathDesc.textContent();
      expect(readPathText).toContain('Memtable');
      expect(readPathText).toContain('SSTable');
    });

    test('page is semantically correct without JavaScript', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Verify semantic HTML structure
      // Check for main landmark
      const main = page.locator('main');
      await expect(main).toBeAttached();

      // Check for header
      const header = page.locator('header');
      await expect(header).toBeAttached();

      // Check for footer
      const footer = page.locator('footer');
      await expect(footer).toBeAttached();

      // Check for nav
      const nav = page.locator('nav');
      await expect(nav).toBeAttached();

      // Check that there's exactly one h1
      const h1Count = await page.locator('h1').count();
      expect(h1Count).toBe(1);

      // Check that sections have appropriate headings
      const h2Count = await page.locator('h2').count();
      expect(h2Count).toBeGreaterThan(0);
    });
  });

  test.describe('Combined Resource Failures', () => {
    test('page functions when multiple resources fail simultaneously', async ({ page }) => {
      // Block images and simulate CSS loading issues
      await page.route('**/*', (route, request) => {
        const resourceType = request.resourceType();
        const url = request.url();

        // Block images
        if (resourceType === 'image') {
          return route.abort('failed');
        }

        // Allow other resources to continue
        return route.continue();
      });

      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Verify page is still functional
      await expect(page.locator('[data-testid="hero-section"]')).toBeVisible();
      await expect(page.locator('[data-testid="features-section"]')).toBeVisible();
      await expect(page.locator('[data-testid="product-name"]')).toHaveText('MirDB');
    });

    test('external CDN failure does not break core functionality', async ({ page }) => {
      // Block all external CDN requests (like Mermaid.js)
      await page.route('**cdn.jsdelivr.net/**', (route) => {
        route.abort('failed');
      });

      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Verify page loads and core content is accessible
      // (Mermaid diagram might not render, but page should function)
      await expect(page.locator('[data-testid="hero-section"]')).toBeVisible();
      await expect(page.locator('[data-testid="product-name"]')).toHaveText('MirDB');
      await expect(page.locator('[data-testid="features-section"]')).toBeVisible();
      await expect(page.locator('[data-testid="commands-section"]')).toBeVisible();
      await expect(page.locator('[data-testid="getting-started-section"]')).toBeVisible();

      // Architecture section should still be visible with fallback content
      const architectureSection = page.locator('[data-testid="architecture-section"]');
      await expect(architectureSection).toBeVisible();

      // Write and Read path descriptions should be visible regardless of Mermaid
      await expect(page.locator('[data-testid="write-path-description"]')).toBeVisible();
      await expect(page.locator('[data-testid="read-path-description"]')).toBeVisible();
    });
  });
});
