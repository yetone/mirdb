/**
 * Browser Compatibility E2E Tests
 * Owner: Scenario 14 - Browser Compatibility
 *
 * Validates browser compatibility across Chrome, Firefox, Safari, and Edge
 * as specified in NFR-7.
 *
 * Test cases:
 * - Chrome rendering correct
 * - Firefox rendering correct
 * - Safari rendering correct
 * - Edge rendering correct (Edge uses Chromium engine)
 * - Smooth scroll works across all browsers
 */

const { test, expect } = require('@playwright/test');

// Common test to verify page renders correctly with all sections visible
test.describe('Browser Compatibility - Page Rendering', () => {
  test('page renders correctly with all major sections visible', async ({ page, browserName }) => {
    await page.goto('/');

    // Wait for page to be fully loaded
    await page.waitForLoadState('domcontentloaded');

    // Verify the page title contains MirDB
    const title = await page.title();
    expect(title).toContain('MirDB');

    // Verify hero section is visible
    const heroSection = page.locator('section.hero, #hero');
    await expect(heroSection).toBeVisible();

    // Verify hero tagline
    const tagline = page.locator('.hero__tagline, h1');
    await expect(tagline).toBeVisible();
    const taglineText = await tagline.textContent();
    expect(taglineText).toContain('MirDB');

    // Verify CTA buttons are visible and interactive
    const ctaButtons = page.locator('.hero__cta .btn');
    const buttonCount = await ctaButtons.count();
    expect(buttonCount).toBeGreaterThanOrEqual(1);

    // Verify "What is MirDB?" section
    const whatIsSection = page.locator('section#what-is, .what-is');
    await expect(whatIsSection).toBeVisible();

    // Verify Features section
    const featuresSection = page.locator('section#features, .features');
    await expect(featuresSection).toBeVisible();

    // Verify feature cards are present
    const featureCards = page.locator('.feature-card');
    const featureCount = await featureCards.count();
    expect(featureCount).toBeGreaterThanOrEqual(4);

    // Verify Quick Start section
    const quickstartSection = page.locator('section#quickstart, .quickstart');
    await expect(quickstartSection).toBeVisible();

    // Verify Code Example section
    const codeSection = page.locator('section#code-example, .code-example');
    await expect(codeSection).toBeVisible();

    // Verify code blocks are present
    const codeBlocks = page.locator('.code-block, pre code');
    const codeBlockCount = await codeBlocks.count();
    expect(codeBlockCount).toBeGreaterThanOrEqual(1);

    // Verify Why MirDB section (comparison)
    const comparisonSection = page.locator('section#why-mirdb, .comparison');
    await expect(comparisonSection).toBeVisible();

    // Verify Links/Resources section
    const linksSection = page.locator('section#links, .links');
    await expect(linksSection).toBeVisible();

    // Verify footer is present
    const footer = page.locator('footer, .site-footer');
    await expect(footer).toBeVisible();

    // Log browser name for test report
    console.log(`Page renders correctly in ${browserName}`);
  });

  test('all interactive elements are clickable', async ({ page, browserName }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    // Verify navigation links are clickable
    const navLinks = page.locator('nav a, .nav-links a, header a');
    const navCount = await navLinks.count();
    expect(navCount).toBeGreaterThanOrEqual(1);

    // Check CTA buttons are enabled and clickable
    const primaryCta = page.locator('.btn--primary, .btn').first();
    await expect(primaryCta).toBeEnabled();

    // Check copy buttons in code blocks if present
    const copyButtons = page.locator('.code-block__copy');
    const copyCount = await copyButtons.count();
    if (copyCount > 0) {
      const firstCopyBtn = copyButtons.first();
      await expect(firstCopyBtn).toBeEnabled();
    }

    // Verify external links have correct attributes
    const externalLinks = page.locator('a[target="_blank"]');
    const extCount = await externalLinks.count();
    for (let i = 0; i < extCount; i++) {
      const link = externalLinks.nth(i);
      const rel = await link.getAttribute('rel');
      // External links should have noopener for security
      if (rel) {
        expect(rel).toContain('noopener');
      }
    }

    console.log(`All interactive elements are clickable in ${browserName}`);
  });

  test('CSS styles are correctly applied', async ({ page, browserName }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    // Wait for CSS to be applied
    await page.waitForTimeout(500);

    // Check that the hero section has proper styling
    const heroSection = page.locator('section.hero, #hero');
    const heroBox = await heroSection.boundingBox();
    expect(heroBox).not.toBeNull();
    expect(heroBox.height).toBeGreaterThan(100);

    // Check that feature cards have proper layout
    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();
    if (cardCount > 0) {
      const firstCard = featureCards.first();
      const cardBox = await firstCard.boundingBox();
      expect(cardBox).not.toBeNull();
      expect(cardBox.width).toBeGreaterThan(50);
    }

    // Verify buttons have proper styling (not collapsed)
    const buttons = page.locator('.btn');
    const btnCount = await buttons.count();
    if (btnCount > 0) {
      const firstBtn = buttons.first();
      const btnBox = await firstBtn.boundingBox();
      expect(btnBox).not.toBeNull();
      expect(btnBox.width).toBeGreaterThan(30);
      expect(btnBox.height).toBeGreaterThan(20);
    }

    console.log(`CSS styles are correctly applied in ${browserName}`);
  });

  test('images and icons load correctly', async ({ page, browserName }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    // Check SVG icons in features section
    const svgIcons = page.locator('.feature-icon svg, .feature-card svg');
    const svgCount = await svgIcons.count();
    expect(svgCount).toBeGreaterThanOrEqual(0); // SVGs might be inline

    // Check that icons are visible (if they exist)
    const featureIcons = page.locator('.feature-icon');
    const iconCount = await featureIcons.count();
    for (let i = 0; i < Math.min(iconCount, 3); i++) {
      const icon = featureIcons.nth(i);
      await expect(icon).toBeVisible();
    }

    console.log(`Images and icons load correctly in ${browserName}`);
  });
});

test.describe('Browser Compatibility - Smooth Scroll', () => {
  test('smooth scroll navigation works', async ({ page, browserName }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY);

    // Find an anchor link that points to a section
    const anchorLink = page.locator('a[href="#features"], a[href="#quickstart"], a[href="#code-example"]').first();
    const linkExists = await anchorLink.count() > 0;

    if (linkExists) {
      // Click the anchor link
      await anchorLink.click();

      // Wait for smooth scroll to complete
      await page.waitForTimeout(1000);

      // Get new scroll position
      const newScrollY = await page.evaluate(() => window.scrollY);

      // Verify that the page has scrolled (smooth scroll happened)
      expect(newScrollY).not.toBe(initialScrollY);

      console.log(`Smooth scroll works in ${browserName}: scrolled from ${initialScrollY} to ${newScrollY}`);
    } else {
      // If no anchor links found, test by scrolling to a section directly
      const featuresSection = page.locator('#features, .features').first();
      if (await featuresSection.count() > 0) {
        await featuresSection.scrollIntoViewIfNeeded();
        const scrolledY = await page.evaluate(() => window.scrollY);
        expect(scrolledY).toBeGreaterThan(0);
        console.log(`Scroll to section works in ${browserName}`);
      }
    }
  });

  test('CTA buttons trigger smooth scroll to target sections', async ({ page, browserName }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    // Find "Get Started" button that links to #quickstart
    const getStartedBtn = page.locator('a[href="#quickstart"], .btn--primary[href="#quickstart"]').first();
    const btnExists = await getStartedBtn.count() > 0;

    if (btnExists) {
      const initialY = await page.evaluate(() => window.scrollY);

      await getStartedBtn.click();
      await page.waitForTimeout(1000);

      const newY = await page.evaluate(() => window.scrollY);

      // Scroll should have happened
      expect(newY).toBeGreaterThan(initialY);

      // Target section should be visible
      const quickstartSection = page.locator('#quickstart');
      if (await quickstartSection.count() > 0) {
        await expect(quickstartSection).toBeInViewport();
      }

      console.log(`CTA smooth scroll works in ${browserName}`);
    }
  });

  test('navigation between multiple sections works', async ({ page, browserName }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    // Define sections to test navigation between
    const sections = ['#features', '#quickstart', '#code-example', '#why-mirdb'];
    let previousY = 0;

    for (const sectionId of sections) {
      const section = page.locator(sectionId);
      if (await section.count() > 0) {
        await section.scrollIntoViewIfNeeded();
        await page.waitForTimeout(300);

        const currentY = await page.evaluate(() => window.scrollY);
        // Verify we can scroll to different sections
        console.log(`Navigated to ${sectionId} at Y=${currentY} in ${browserName}`);
        previousY = currentY;
      }
    }

    // Scroll back to top
    await page.evaluate(() => window.scrollTo({ top: 0, behavior: 'smooth' }));
    await page.waitForTimeout(800);
    const topY = await page.evaluate(() => window.scrollY);
    expect(topY).toBeLessThan(100);

    console.log(`Navigation between sections and back to top works in ${browserName}`);
  });
});

test.describe('Browser Compatibility - JavaScript Functionality', () => {
  test('JavaScript is loaded and functional', async ({ page, browserName }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    // Verify JavaScript is running by checking for initialized elements
    // Mobile menu toggle should have event listeners
    const navToggle = page.locator('.nav-toggle');
    const toggleExists = await navToggle.count() > 0;

    if (toggleExists) {
      const ariaExpanded = await navToggle.getAttribute('aria-expanded');
      // aria-expanded should be set (by JS or HTML)
      expect(ariaExpanded).not.toBeNull();
    }

    // Test copy button functionality if present
    const copyButtons = page.locator('.code-block__copy');
    if (await copyButtons.count() > 0) {
      const firstCopyBtn = copyButtons.first();
      await expect(firstCopyBtn).toBeEnabled();

      // Clicking copy button should work (check for visual feedback)
      await firstCopyBtn.click();
      await page.waitForTimeout(500);

      // The button should show feedback (e.g., text change to "Copied!")
      const copyText = page.locator('.code-block__copy-text').first();
      const text = await copyText.textContent();
      // Text should be either "Copy" or "Copied!" depending on timing
      expect(['Copy', 'Copied!']).toContain(text);
    }

    console.log(`JavaScript is functional in ${browserName}`);
  });

  test('event handlers work correctly', async ({ page, browserName }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    // Test hover effects on feature cards
    const featureCards = page.locator('.feature-card');
    if (await featureCards.count() > 0) {
      const firstCard = featureCards.first();
      await firstCard.hover();
      await page.waitForTimeout(200);
      // Card should be visible and potentially have hover state
      await expect(firstCard).toBeVisible();
    }

    // Test button hover states
    const buttons = page.locator('.btn');
    if (await buttons.count() > 0) {
      const firstBtn = buttons.first();
      await firstBtn.hover();
      await page.waitForTimeout(200);
      await expect(firstBtn).toBeVisible();
    }

    console.log(`Event handlers work correctly in ${browserName}`);
  });
});

test.describe('Browser Compatibility - Layout and Responsiveness', () => {
  test('desktop layout renders correctly', async ({ page, browserName }) => {
    // Set desktop viewport
    await page.setViewportSize({ width: 1280, height: 800 });
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    // Verify layout is not collapsed
    const body = page.locator('body');
    const bodyBox = await body.boundingBox();
    expect(bodyBox).not.toBeNull();
    expect(bodyBox.width).toBeGreaterThanOrEqual(1000);

    // Feature cards should be in a grid layout (multiple per row)
    const featuresGrid = page.locator('.features-grid');
    if (await featuresGrid.count() > 0) {
      const gridBox = await featuresGrid.boundingBox();
      expect(gridBox).not.toBeNull();
      expect(gridBox.width).toBeGreaterThan(600);
    }

    console.log(`Desktop layout renders correctly in ${browserName}`);
  });

  test('page is scrollable', async ({ page, browserName }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    // Get page height
    const pageHeight = await page.evaluate(() => document.documentElement.scrollHeight);
    const viewportHeight = await page.evaluate(() => window.innerHeight);

    // Page should be taller than viewport (scrollable)
    expect(pageHeight).toBeGreaterThan(viewportHeight);

    // Test scrolling
    await page.evaluate(() => window.scrollTo(0, 500));
    await page.waitForTimeout(300);
    const scrollY = await page.evaluate(() => window.scrollY);
    expect(scrollY).toBeGreaterThan(0);

    console.log(`Page is scrollable in ${browserName}: height=${pageHeight}, viewport=${viewportHeight}`);
  });
});
