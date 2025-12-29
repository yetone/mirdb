import { test, expect } from '@playwright/test';

/**
 * E2E Tests for Error Handling - Missing Assets
 *
 * This test suite verifies the page handles missing or failed asset loads gracefully.
 * It implements progressive enhancement principles ensuring core content remains
 * accessible even when CSS, JavaScript, or images fail to load.
 *
 * Test Cases:
 * 1. Content remains readable when CSS is disabled/blocked
 * 2. Core content and navigation work when JavaScript is disabled
 * 3. Alt text displays for all images when images are blocked
 */

test.describe('Error Handling - Missing Assets', () => {
  test.describe('TC1: Page with CSS Disabled', () => {
    test('Content remains readable and accessible without styling', async ({ page }) => {
      // Block all CSS files (both local and CDN)
      await page.route('**/*.css', (route) => route.abort());
      await page.route('**/prism*.css', (route) => route.abort());

      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Verify the page title is still present
      const pageTitle = await page.title();
      expect(pageTitle).toContain('MirDB');

      // Hero section content should be visible and readable
      const heroHeading = page.locator('.hero h1');
      await expect(heroHeading).toBeVisible();
      await expect(heroHeading).toContainText('MirDB');

      // Tagline should be visible
      const tagline = page.locator('.tagline');
      await expect(tagline).toBeVisible();
      await expect(tagline).toContainText('Memcached Protocol');

      // Description text should be readable
      const description = page.locator('.hero .description');
      await expect(description).toBeVisible();
      const descriptionText = await description.textContent();
      expect(descriptionText?.length).toBeGreaterThan(50);

      // CTA buttons should be functional (links work without CSS)
      const getStartedButton = page.locator('a[href="#getting-started"]');
      await expect(getStartedButton).toBeVisible();
      await expect(getStartedButton).toHaveAttribute('href', '#getting-started');

      const githubButton = page.locator('a[href="https://github.com/yetone/mirdb"]').first();
      await expect(githubButton).toBeVisible();

      // Features section content is accessible
      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeVisible();

      // Feature card text is readable
      const featureCards = page.locator('.feature-card');
      const cardCount = await featureCards.count();
      expect(cardCount).toBeGreaterThanOrEqual(4);

      // Check first feature card content
      const firstCard = featureCards.first();
      await expect(firstCard.locator('h3')).toBeVisible();
      await expect(firstCard.locator('p')).toBeVisible();

      // Code examples section is accessible
      const codeSection = page.locator('#code-examples');
      await expect(codeSection).toBeVisible();

      // Code blocks should display their content (plain text without syntax highlighting)
      const codeBlocks = page.locator('.code-block pre code');
      const codeBlockCount = await codeBlocks.count();
      expect(codeBlockCount).toBeGreaterThan(0);

      // Verify code content is still present
      const firstCodeBlock = codeBlocks.first();
      const codeContent = await firstCodeBlock.textContent();
      expect(codeContent).toContain('mirdb');

      // Getting started section is accessible
      const gettingStartedSection = page.locator('#getting-started');
      await expect(gettingStartedSection).toBeVisible();

      // Architecture section is accessible
      const architectureSection = page.locator('#architecture');
      await expect(architectureSection).toBeVisible();

      // Footer is accessible
      const footer = page.locator('.footer');
      await expect(footer).toBeVisible();

      // Footer links are functional
      const footerLinks = footer.locator('a');
      const footerLinkCount = await footerLinks.count();
      expect(footerLinkCount).toBeGreaterThan(0);

      // Verify navigation still works (scroll to section)
      await getStartedButton.click();
      await page.waitForTimeout(500); // Allow scroll animation

      // Getting started section should be in view after click
      await expect(gettingStartedSection).toBeInViewport();
    });

    test('All text content is accessible without CSS', async ({ page }) => {
      // Block CSS
      await page.route('**/*.css', (route) => route.abort());

      await page.goto('/');

      // Get all visible text on the page
      const bodyText = await page.locator('body').textContent();

      // Key content should be present
      expect(bodyText).toContain('MirDB');
      expect(bodyText).toContain('Memcached');
      expect(bodyText).toContain('Persistent');
      expect(bodyText).toContain('LSM-Tree');
      expect(bodyText).toContain('GET');
      expect(bodyText).toContain('SET');
      expect(bodyText).toContain('DELETE');
      expect(bodyText).toContain('Getting Started');
      expect(bodyText).toContain('Architecture');
    });

    test('Links remain clickable without CSS', async ({ page }) => {
      // Block CSS
      await page.route('**/*.css', (route) => route.abort());

      await page.goto('/');

      // All internal anchor links should work
      const anchorLinks = page.locator('a[href^="#"]');
      const anchorCount = await anchorLinks.count();
      expect(anchorCount).toBeGreaterThan(0);

      for (let i = 0; i < anchorCount; i++) {
        const link = anchorLinks.nth(i);
        await expect(link).toBeEnabled();
      }

      // External links should have proper attributes
      const externalLinks = page.locator('a[target="_blank"]');
      const externalCount = await externalLinks.count();

      for (let i = 0; i < externalCount; i++) {
        const link = externalLinks.nth(i);
        await expect(link).toHaveAttribute('rel', /noopener/);
      }
    });
  });

  test.describe('TC2: Page with JavaScript Disabled', () => {
    test('Core content and navigation work without JS', async ({ page }) => {
      // Block all JavaScript files
      await page.route('**/*.js', (route) => route.abort());
      await page.route('**/prism*.js', (route) => route.abort());

      await page.goto('/');

      // Core content should still be visible
      const heroHeading = page.locator('.hero h1');
      await expect(heroHeading).toBeVisible();
      await expect(heroHeading).toContainText('MirDB');

      // Features section should render
      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeVisible();

      // Feature cards should be visible
      const featureCards = page.locator('.feature-card');
      const cardCount = await featureCards.count();
      expect(cardCount).toBeGreaterThanOrEqual(4);

      // Navigation links should work (anchor links don't need JS)
      const getStartedLink = page.locator('a[href="#getting-started"]');
      await expect(getStartedLink).toBeVisible();
      await expect(getStartedLink).toHaveAttribute('href', '#getting-started');

      // Getting started section should be visible
      const gettingStartedSection = page.locator('#getting-started');
      await expect(gettingStartedSection).toBeVisible();

      // Architecture section should be visible
      const architectureSection = page.locator('#architecture');
      await expect(architectureSection).toBeVisible();

      // Architecture diagram (SVG) should still render
      const archDiagram = page.locator('.architecture-diagram svg');
      await expect(archDiagram).toBeVisible();

      // Footer should be visible
      const footer = page.locator('.footer');
      await expect(footer).toBeVisible();

      // Footer navigation links should work
      const footerNav = page.locator('.footer-nav');
      await expect(footerNav).toBeVisible();
      const footerLinks = footerNav.locator('a');
      expect(await footerLinks.count()).toBeGreaterThan(0);
    });

    test('Code blocks display content without syntax highlighting', async ({ page }) => {
      // Block JavaScript
      await page.route('**/*.js', (route) => route.abort());

      await page.goto('/');

      // Code blocks should still have content
      const codeBlocks = page.locator('pre code');
      const codeBlockCount = await codeBlocks.count();
      expect(codeBlockCount).toBeGreaterThan(0);

      // Verify code content is present (just won't be syntax highlighted)
      for (let i = 0; i < Math.min(codeBlockCount, 3); i++) {
        const codeBlock = codeBlocks.nth(i);
        await expect(codeBlock).toBeVisible();
        const content = await codeBlock.textContent();
        expect(content?.length).toBeGreaterThan(0);
      }

      // The code should contain expected content
      const allCodeContent = await page.locator('pre code').allTextContents();
      const combinedContent = allCodeContent.join(' ');
      expect(combinedContent).toContain('mirdb');
    });

    test('Table renders correctly without JavaScript', async ({ page }) => {
      // Block JavaScript
      await page.route('**/*.js', (route) => route.abort());

      await page.goto('/');

      // Defaults table should be visible and readable
      const table = page.locator('.defaults-table');
      await expect(table).toBeVisible();

      // Table headers should be present
      const headers = table.locator('th');
      expect(await headers.count()).toBe(2);

      // Table rows with data should be present
      const rows = table.locator('tbody tr');
      expect(await rows.count()).toBeGreaterThan(0);

      // Check specific table content
      const tableText = await table.textContent();
      expect(tableText).toContain('Listen Address');
      expect(tableText).toContain('12333');
    });

    test('SVG architecture diagram renders without JavaScript', async ({ page }) => {
      // Block JavaScript
      await page.route('**/*.js', (route) => route.abort());

      await page.goto('/');

      // SVG diagram should render
      const archSvg = page.locator('.architecture-diagram svg');
      await expect(archSvg).toBeVisible();

      // Check SVG has proper accessibility attributes (title/desc are hidden metadata elements)
      const svgTitle = archSvg.locator('title');
      expect(await svgTitle.count()).toBeGreaterThan(0);
      const titleText = await svgTitle.textContent();
      expect(titleText).toBeTruthy();
      expect(titleText!.length).toBeGreaterThan(10);

      const svgDesc = archSvg.locator('desc');
      expect(await svgDesc.count()).toBeGreaterThan(0);
      const descText = await svgDesc.textContent();
      expect(descText).toBeTruthy();
      expect(descText!.length).toBeGreaterThan(30);

      // Verify SVG content (boxes and text)
      const svgRects = archSvg.locator('rect');
      expect(await svgRects.count()).toBeGreaterThan(3);

      const svgText = archSvg.locator('text');
      expect(await svgText.count()).toBeGreaterThan(5);

      // Key architecture terms should be in the SVG
      const svgContent = await archSvg.textContent();
      expect(svgContent).toContain('Write');
      expect(svgContent).toContain('WAL');
      expect(svgContent).toContain('Memtable');
      expect(svgContent).toContain('SSTable');
    });
  });

  test.describe('TC3: Page with Images Blocked', () => {
    test('Alt text displays for all images, layout remains intact', async ({ page }) => {
      // Block all image requests
      await page.route('**/*.{png,jpg,jpeg,gif,webp,ico,svg}', (route) => {
        // Only block external image requests, not inline SVGs
        const url = route.request().url();
        if (url.startsWith('http') && !url.includes('localhost')) {
          return route.abort();
        }
        return route.continue();
      });

      // Also block data: images if any
      await page.route('**/*', (route) => {
        const url = route.request().url();
        if (url.includes('.png') || url.includes('.jpg') || url.includes('.gif')) {
          return route.abort();
        }
        return route.continue();
      });

      await page.goto('/');

      // Verify page loads
      await expect(page.locator('.hero h1')).toBeVisible();

      // Check all img elements have alt attributes
      const images = page.locator('img');
      const imageCount = await images.count();

      for (let i = 0; i < imageCount; i++) {
        const img = images.nth(i);
        const altText = await img.getAttribute('alt');

        // Every image must have an alt attribute
        expect(altText).not.toBeNull();

        // Non-decorative images should have meaningful alt text
        if (altText !== '') {
          expect(altText!.length).toBeGreaterThan(3);
        }
      }

      // Layout should remain intact - verify key sections are still positioned correctly
      const hero = page.locator('.hero');
      await expect(hero).toBeVisible();

      const features = page.locator('#features');
      await expect(features).toBeVisible();

      const codeExamples = page.locator('#code-examples');
      await expect(codeExamples).toBeVisible();

      const gettingStarted = page.locator('#getting-started');
      await expect(gettingStarted).toBeVisible();

      const architecture = page.locator('#architecture');
      await expect(architecture).toBeVisible();

      const footer = page.locator('.footer');
      await expect(footer).toBeVisible();
    });

    test('SVG elements provide accessible alternatives when images blocked', async ({ page }) => {
      await page.goto('/');

      // Check all SVG elements have appropriate accessibility
      const svgElements = page.locator('svg');
      const svgCount = await svgElements.count();

      // SVGs in feature cards are decorative (inside cards with text)
      const decorativeSvgs = page.locator('.feature-icon svg');
      const decorativeCount = await decorativeSvgs.count();

      // Architecture diagram SVG should have title and desc (hidden metadata elements)
      const archSvg = page.locator('.architecture-diagram svg');
      if (await archSvg.count() > 0) {
        const title = archSvg.locator('title');
        const desc = archSvg.locator('desc');

        // title and desc elements exist (they are hidden metadata, not visible)
        expect(await title.count()).toBeGreaterThan(0);
        expect(await desc.count()).toBeGreaterThan(0);

        const titleText = await title.textContent();
        const descText = await desc.textContent();

        // Title and desc should be meaningful
        expect(titleText?.length).toBeGreaterThan(10);
        expect(descText?.length).toBeGreaterThan(30);
      }

      // The container should have role="img" and aria-label
      const archContainer = page.locator('.architecture-diagram');
      const role = await archContainer.getAttribute('role');
      const ariaLabel = await archContainer.getAttribute('aria-label');

      expect(role).toBe('img');
      expect(ariaLabel).toContain('architecture');
    });

    test('Page layout does not break when images fail to load', async ({ page }) => {
      // Simulate network failure for images
      await page.route('**/*.{png,jpg,jpeg,gif,webp}', (route) => route.abort());

      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Check that main layout sections maintain their structure
      const container = page.locator('.container').first();
      const containerBox = await container.boundingBox();
      expect(containerBox).not.toBeNull();
      expect(containerBox!.width).toBeGreaterThan(0);

      // Features grid should still have proper layout
      const featuresGrid = page.locator('.features-grid');
      await expect(featuresGrid).toBeVisible();

      const featureCards = page.locator('.feature-card');
      const cardCount = await featureCards.count();
      expect(cardCount).toBeGreaterThanOrEqual(4);

      // Check cards still have dimensions (layout intact)
      for (let i = 0; i < Math.min(cardCount, 2); i++) {
        const card = featureCards.nth(i);
        const cardBox = await card.boundingBox();
        expect(cardBox).not.toBeNull();
        expect(cardBox!.width).toBeGreaterThan(100);
        expect(cardBox!.height).toBeGreaterThan(50);
      }

      // Getting started content maintains layout
      const gettingStartedContent = page.locator('.getting-started-content');
      await expect(gettingStartedContent).toBeVisible();

      // Steps should still be visible
      const steps = page.locator('.step');
      expect(await steps.count()).toBeGreaterThanOrEqual(3);
    });

    test('Inline SVG icons remain functional when external images blocked', async ({ page }) => {
      // Block all external images
      await page.route('**/*.{png,jpg,jpeg,gif,webp,ico}', (route) => route.abort());

      await page.goto('/');

      // Feature icons (inline SVGs) should still render
      const featureIcons = page.locator('.feature-icon svg');
      const iconCount = await featureIcons.count();
      expect(iconCount).toBe(4);

      // Each icon should be visible
      for (let i = 0; i < iconCount; i++) {
        const icon = featureIcons.nth(i);
        await expect(icon).toBeVisible();

        // SVG should have valid structure
        const paths = icon.locator('path, line, polyline, circle, rect');
        expect(await paths.count()).toBeGreaterThan(0);
      }

      // Architecture SVG should still render
      const archSvg = page.locator('.arch-svg');
      await expect(archSvg).toBeVisible();
    });
  });

  test.describe('Combined Degradation Scenarios', () => {
    test('Page remains usable with both CSS and JS blocked', async ({ page }) => {
      // Block both CSS and JS
      await page.route('**/*.css', (route) => route.abort());
      await page.route('**/*.js', (route) => route.abort());

      await page.goto('/');

      // Core content should be accessible
      const bodyText = await page.locator('body').textContent();

      // Essential information present
      expect(bodyText).toContain('MirDB');
      expect(bodyText).toContain('Memcached');
      expect(bodyText).toContain('Key Features');
      expect(bodyText).toContain('Getting Started');
      expect(bodyText).toContain('Architecture');

      // Navigation links should work
      const navLinks = page.locator('a[href^="#"]');
      expect(await navLinks.count()).toBeGreaterThan(0);

      // External links accessible
      const githubLinks = page.locator('a[href*="github.com"]');
      expect(await githubLinks.count()).toBeGreaterThan(0);

      // Footer visible
      const footer = page.locator('.footer');
      await expect(footer).toBeVisible();
    });

    test('No console errors for blocked resources', async ({ page }) => {
      const consoleErrors: string[] = [];

      page.on('console', (msg) => {
        if (msg.type() === 'error') {
          consoleErrors.push(msg.text());
        }
      });

      // Block CSS (which is optional - page should handle gracefully)
      await page.route('**/styles.css', (route) => route.abort());

      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Filter out network errors for intentionally blocked resources
      const unexpectedErrors = consoleErrors.filter(
        (err) =>
          !err.includes('net::ERR_FAILED') &&
          !err.includes('net::ERR_BLOCKED') &&
          !err.includes('styles.css') &&
          !err.includes('Failed to load resource')
      );

      // Should be no unexpected JavaScript errors
      expect(unexpectedErrors.length).toBe(0);
    });
  });
});
