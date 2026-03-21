/**
 * No-JavaScript Fallback E2E Tests
 * Owner: Scenario 10 - No-JavaScript Fallback
 *
 * Tests:
 * - Content visibility without JS
 * - Navigation functionality
 * - Code block readability
 * - CSS-only theme preference
 */

const { test, expect } = require('@playwright/test');

// Test suite for no-JavaScript functionality
test.describe('No-JavaScript Fallback', () => {
  // Create a context with JavaScript disabled
  test.use({ javaScriptEnabled: false });

  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');
  });

  test('TC1: All content sections are visible without JavaScript', async ({ page }) => {
    // Verify Hero section is visible
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // Verify Hero content is readable
    const heroTitle = page.locator('.hero__title');
    await expect(heroTitle).toBeVisible();
    await expect(heroTitle).toHaveText('MirDB');

    const heroTagline = page.locator('.hero__tagline');
    await expect(heroTagline).toBeVisible();
    await expect(heroTagline).toContainText('A Persistent Key-Value Store');

    // Verify Features section is visible
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Verify feature cards are rendered
    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();
    expect(cardCount).toBeGreaterThanOrEqual(1);

    // Verify at least one feature title is visible
    const featureTitle = page.locator('.feature-card h3').first();
    await expect(featureTitle).toBeVisible();

    // Verify Quick Start section is visible
    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeVisible();

    // Verify Usage section is visible
    const usageSection = page.locator('#usage');
    await expect(usageSection).toBeVisible();

    // Verify usage content is accessible
    const usageTitle = page.locator('.usage__title');
    await expect(usageTitle).toBeVisible();
    await expect(usageTitle).toHaveText('Usage');

    // Verify Status section is visible
    const statusSection = page.locator('#status');
    await expect(statusSection).toBeVisible();

    // Verify status checklist is visible
    const statusChecklist = page.locator('.status-checklist');
    await expect(statusChecklist).toBeVisible();

    // Verify Footer is visible
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();
  });

  test('TC2: Navigation links work without JavaScript using native anchor behavior', async ({ page }) => {
    // Verify navigation menu exists
    const navMenu = page.locator('.nav-menu');
    await expect(navMenu).toBeVisible();

    // Verify all navigation links in nav menu exist with proper href attributes
    // Use .nav-link class to specifically target navigation menu links
    const featuresLink = page.locator('.nav-menu a[href="#features"]');
    await expect(featuresLink).toBeVisible();
    await expect(featuresLink).toHaveAttribute('href', '#features');

    const quickStartLink = page.locator('.nav-menu a[href="#quick-start"]');
    await expect(quickStartLink).toBeVisible();
    await expect(quickStartLink).toHaveAttribute('href', '#quick-start');

    const usageLink = page.locator('.nav-menu a[href="#usage"]');
    await expect(usageLink).toBeVisible();
    await expect(usageLink).toHaveAttribute('href', '#usage');

    const statusLink = page.locator('.nav-menu a[href="#status"]');
    await expect(statusLink).toBeVisible();
    await expect(statusLink).toHaveAttribute('href', '#status');

    // Verify that target elements with corresponding IDs exist
    // This ensures anchor navigation will work natively
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeAttached();

    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeAttached();

    const usageSection = page.locator('#usage');
    await expect(usageSection).toBeAttached();

    const statusSection = page.locator('#status');
    await expect(statusSection).toBeAttached();

    // Test direct anchor navigation via URL
    // Navigate directly to a section via hash URL
    await page.goto('/#quick-start');
    await page.waitForLoadState('domcontentloaded');

    // Verify the URL has the correct hash
    expect(page.url()).toContain('#quick-start');

    // The Quick Start section should exist and be part of the page
    await expect(page.locator('#quick-start')).toBeAttached();

    // Verify Get Started button (primary CTA) also has anchor link
    const getStartedButton = page.locator('a.hero__cta--primary');
    await expect(getStartedButton).toHaveAttribute('href', '#quick-start');
  });

  test('TC3: Installation command code block is readable without Prism.js syntax highlighting', async ({ page }) => {
    // Navigate to quick start section
    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeVisible();

    // Verify code block exists and contains installation command
    const codeBlock = page.locator('.code-block code');
    await expect(codeBlock).toBeVisible();

    // Verify the installation command text is readable
    const codeText = await codeBlock.textContent();
    expect(codeText).toBeTruthy();
    expect(codeText).toContain('git clone');
    expect(codeText).toContain('mirdb');

    // Verify the code has an identifiable ID for the copy functionality
    const installCommand = page.locator('#install-command');
    await expect(installCommand).toBeVisible();

    // Verify the pre element displays correctly
    const preElement = page.locator('.code-block pre');
    await expect(preElement).toBeVisible();

    // Verify code is readable (has text content that includes the command)
    const preText = await preElement.textContent();
    expect(preText).toContain('cargo run');

    // Also verify the usage section code blocks are readable
    const usageCodeBlock = page.locator('.usage__code-block code');
    await expect(usageCodeBlock).toBeVisible();

    const usageCodeText = await usageCodeBlock.textContent();
    expect(usageCodeText).toContain('telnet');
    expect(usageCodeText).toContain('set');
    expect(usageCodeText).toContain('get');
  });

  test('TC4: Page respects system color scheme preference via CSS media query', async ({ page }) => {
    // Verify CSS file is loaded
    const stylesheet = page.locator('link[rel="stylesheet"][href*="styles.css"]');
    await expect(stylesheet).toHaveCount(1);

    // Verify that CSS variables are being used (page has styles)
    const bodyBgColor = await page.evaluate(() => {
      const style = getComputedStyle(document.body);
      return style.backgroundColor;
    });

    // Body should have a background color applied via CSS
    expect(bodyBgColor).toBeTruthy();
    expect(bodyBgColor).not.toBe('');

    // Verify content is readable (text has color)
    const textColor = await page.evaluate(() => {
      const heading = document.querySelector('.hero__title');
      if (!heading) return '';
      return getComputedStyle(heading).color;
    });
    expect(textColor).toBeTruthy();

    // Test with prefers-color-scheme: dark emulation
    // First test light mode
    await page.emulateMedia({ colorScheme: 'light' });
    await page.goto('/');

    const lightBgColor = await page.evaluate(() => {
      return getComputedStyle(document.body).backgroundColor;
    });

    // Then test dark mode
    await page.emulateMedia({ colorScheme: 'dark' });
    await page.goto('/');

    const darkBgColor = await page.evaluate(() => {
      return getComputedStyle(document.body).backgroundColor;
    });

    // Colors should be different between light and dark mode
    // This confirms the prefers-color-scheme media query is working
    expect(lightBgColor).not.toBe(darkBgColor);

    // Verify in dark mode that text is still readable (contrast)
    const darkTextColor = await page.evaluate(() => {
      const heading = document.querySelector('.hero__title');
      if (!heading) return '';
      return getComputedStyle(heading).color;
    });
    expect(darkTextColor).toBeTruthy();

    // Verify navigation is visible in dark mode
    const navVisible = await page.locator('#main-nav').isVisible();
    expect(navVisible).toBeTruthy();
  });

  test('Theme toggle button is present even without JS (for progressive enhancement)', async ({ page }) => {
    // The theme toggle should exist in the DOM for progressive enhancement
    const themeToggle = page.locator('#theme-toggle');
    await expect(themeToggle).toBeVisible();

    // Verify it has proper accessibility attributes
    await expect(themeToggle).toHaveAttribute('aria-label', 'Toggle dark mode');

    // The button won't work without JS, but it should be present for when JS loads
  });

  test('All headings are visible without JavaScript', async ({ page }) => {
    // h1 - Main heading
    const h1 = page.locator('h1');
    await expect(h1).toBeVisible();
    await expect(h1).toHaveText('MirDB');

    // h2 headings for sections
    const h2Elements = page.locator('h2');
    const h2Count = await h2Elements.count();
    expect(h2Count).toBeGreaterThanOrEqual(4); // Features, Quick Start, Usage, Status

    // Verify specific section headings
    const featuresHeading = page.locator('#features h2');
    await expect(featuresHeading).toBeVisible();
    await expect(featuresHeading).toHaveText('Features');

    const statusHeading = page.locator('#status h2');
    await expect(statusHeading).toBeVisible();
    await expect(statusHeading).toContainText('Status');
  });

  test('External links are accessible without JavaScript', async ({ page }) => {
    // View Source link should work
    const viewSourceLink = page.locator('a.hero__cta--secondary');
    await expect(viewSourceLink).toBeVisible();

    const href = await viewSourceLink.getAttribute('href');
    expect(href).toContain('github.com');

    // Link should open in new tab
    await expect(viewSourceLink).toHaveAttribute('target', '_blank');
    await expect(viewSourceLink).toHaveAttribute('rel', /noopener/);

    // Documentation link in quick start
    const docsLink = page.locator('.quickstart-docs-link a');
    await expect(docsLink).toBeVisible();

    const docsHref = await docsLink.getAttribute('href');
    expect(docsHref).toContain('github.com');
  });

  test('Images are visible without JavaScript', async ({ page }) => {
    // Usage demo image/GIF should be visible
    const usageImage = page.locator('.usage__gif');
    await expect(usageImage).toBeVisible();

    // Should have alt text for accessibility
    const altText = await usageImage.getAttribute('alt');
    expect(altText).toBeTruthy();
    expect(altText.length).toBeGreaterThan(0);
  });

  test('Copy button exists but no feedback shown without JS', async ({ page }) => {
    // Copy button should exist in DOM
    const copyButton = page.locator('.copy-button');
    await expect(copyButton).toBeVisible();

    // The button should have a data attribute for the copy target
    await expect(copyButton).toHaveAttribute('data-copy-target', '#install-command');

    // Without JS, clicking won't copy, but the button should be there
    // for progressive enhancement
    await expect(copyButton).toHaveText('Copy');
  });
});
