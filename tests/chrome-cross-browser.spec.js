// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Chrome Cross-Browser Compatibility Test Suite
 * Scenario: Cross-Browser Compatibility - Chrome
 *
 * This test suite verifies that the MirDB homepage renders correctly
 * and functions properly in Google Chrome browser.
 *
 * Test Cases:
 * 1. Load homepage in Chrome (latest version) - All sections render correctly
 * 2. Test navigation in Chrome - All links and buttons function correctly
 * 3. Load homepage in Chrome (latest - 1 version) - All sections render correctly
 */

// Test Case 1: Load homepage in Chrome (latest version) - All sections render correctly
test.describe('TC1: Chrome Latest - Visual Rendering', () => {
  test('Homepage loads successfully and all main sections are visible', async ({ page, browserName }) => {
    // This test runs in Chrome/Chromium as configured in playwright.config.js
    await page.goto('/');

    // Verify the page loaded successfully
    await expect(page).toHaveTitle(/MirDB/);

    // Verify header/navigation is visible
    const header = page.locator('header.header');
    await expect(header).toBeVisible();

    // Verify logo is visible
    const logo = page.locator('.logo');
    await expect(logo).toBeVisible();

    // Verify navigation links are visible
    const navLinks = page.locator('.nav-links');
    await expect(navLinks).toBeVisible();
  });

  test('Hero section renders correctly without visual issues', async ({ page }) => {
    await page.goto('/');

    // Verify hero section exists and is visible
    const heroSection = page.locator('section.hero');
    await expect(heroSection).toBeVisible();

    // Verify hero title (h1) is visible and contains MirDB
    const heroTitle = page.locator('.hero h1');
    await expect(heroTitle).toBeVisible();
    await expect(heroTitle).toContainText('MirDB');

    // Verify tagline is visible
    const tagline = page.locator('.tagline');
    await expect(tagline).toBeVisible();
    await expect(tagline).toContainText('Persistent Key-Value Store');

    // Verify description is visible
    const description = page.locator('.hero .description');
    await expect(description).toBeVisible();

    // Verify CTA buttons are visible
    const primaryCTA = page.locator('[data-testid="primary-cta"]');
    const secondaryCTA = page.locator('[data-testid="secondary-cta"]');
    await expect(primaryCTA).toBeVisible();
    await expect(secondaryCTA).toBeVisible();
  });

  test('Features section renders correctly without visual issues', async ({ page }) => {
    await page.goto('/');

    // Verify features section exists and is visible
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Verify features title
    const featuresTitle = page.locator('#features-title');
    await expect(featuresTitle).toBeVisible();
    await expect(featuresTitle).toContainText('Key Features');

    // Verify features grid is visible
    const featuresGrid = page.locator('[data-testid="features-grid"]');
    await expect(featuresGrid).toBeVisible();

    // Verify all 4 feature cards are visible
    const memcachedCard = page.locator('[data-testid="feature-card-memcached"]');
    const persistenceCard = page.locator('[data-testid="feature-card-persistence"]');
    const lsmCard = page.locator('[data-testid="feature-card-lsm"]');
    const asyncCard = page.locator('[data-testid="feature-card-async"]');

    await expect(memcachedCard).toBeVisible();
    await expect(persistenceCard).toBeVisible();
    await expect(lsmCard).toBeVisible();
    await expect(asyncCard).toBeVisible();

    // Verify each card has a heading
    await expect(memcachedCard.locator('h3')).toContainText('Memcached Protocol');
    await expect(persistenceCard.locator('h3')).toContainText('Persistent Storage');
    await expect(lsmCard.locator('h3')).toContainText('LSM Tree');
    await expect(asyncCard.locator('h3')).toContainText('Async Networking');
  });

  test('Quick Start section renders correctly without visual issues', async ({ page }) => {
    await page.goto('/');

    // Verify quick-start section exists and is visible
    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeVisible();

    // Verify quick-start title
    const quickStartTitle = page.locator('#quickstart-title');
    await expect(quickStartTitle).toBeVisible();
    await expect(quickStartTitle).toContainText('Quick Start');

    // Verify installation step
    const installationStep = page.locator('[data-testid="installation-step"]');
    await expect(installationStep).toBeVisible();

    // Verify code blocks are visible
    const installationCode = page.locator('[data-testid="installation-code"]');
    await expect(installationCode).toBeVisible();

    // Verify configuration step
    const configStep = page.locator('[data-testid="configuration-step"]');
    await expect(configStep).toBeVisible();

    // Verify usage step
    const usageStep = page.locator('[data-testid="usage-step"]');
    await expect(usageStep).toBeVisible();

    // Verify commands step
    const commandsStep = page.locator('[data-testid="commands-step"]');
    await expect(commandsStep).toBeVisible();
  });

  test('Footer renders correctly without visual issues', async ({ page }) => {
    await page.goto('/');

    // Verify footer exists and is visible
    const footer = page.locator('[data-testid="footer"]');
    await expect(footer).toBeVisible();

    // Verify footer links
    const footerLinks = page.locator('[data-testid="footer-links"]');
    await expect(footerLinks).toBeVisible();

    // Verify project status
    const projectStatus = page.locator('[data-testid="project-status"]');
    await expect(projectStatus).toBeVisible();

    // Verify GitHub link
    const githubLink = page.locator('[data-testid="footer-github-link"]');
    await expect(githubLink).toBeVisible();

    // Verify docs link
    const docsLink = page.locator('[data-testid="footer-docs-link"]');
    await expect(docsLink).toBeVisible();

    // Verify license link
    const licenseLink = page.locator('[data-testid="footer-license-link"]');
    await expect(licenseLink).toBeVisible();
  });

  test('Page layout renders with proper CSS grid and flexbox', async ({ page }) => {
    await page.goto('/');

    // Verify features grid uses CSS grid layout
    const featuresGrid = page.locator('[data-testid="features-grid"]');
    const gridDisplay = await featuresGrid.evaluate(el =>
      window.getComputedStyle(el).display
    );
    expect(gridDisplay).toBe('grid');

    // Verify nav uses flexbox
    const nav = page.locator('.nav');
    const navDisplay = await nav.evaluate(el =>
      window.getComputedStyle(el).display
    );
    expect(navDisplay).toBe('flex');

    // Verify CTA buttons container uses flexbox
    const ctaButtons = page.locator('.cta-buttons');
    const ctaDisplay = await ctaButtons.evaluate(el =>
      window.getComputedStyle(el).display
    );
    expect(ctaDisplay).toBe('flex');
  });

  test('CSS custom properties (variables) are properly applied', async ({ page }) => {
    await page.goto('/');

    // Verify CSS custom properties are applied correctly in Chrome
    const body = page.locator('body');
    const fontFamily = await body.evaluate(el =>
      window.getComputedStyle(el).fontFamily
    );
    // Should contain system font stack
    expect(fontFamily).toMatch(/(system-ui|-apple-system|BlinkMacSystemFont|Segoe UI|Roboto)/i);

    // Verify primary button has correct background color
    const primaryBtn = page.locator('[data-testid="primary-cta"]');
    const bgColor = await primaryBtn.evaluate(el =>
      window.getComputedStyle(el).backgroundColor
    );
    // Should be the primary blue color (rgb(37, 99, 235) = #2563eb)
    expect(bgColor).toMatch(/rgb\(37,\s*99,\s*235\)/);
  });

  test('SVG icons render correctly in Chrome', async ({ page }) => {
    await page.goto('/');

    // Verify feature card icons are visible
    const featureIcons = page.locator('.feature-icon svg');
    const iconCount = await featureIcons.count();
    expect(iconCount).toBe(4);

    // Verify each SVG is visible
    for (let i = 0; i < iconCount; i++) {
      await expect(featureIcons.nth(i)).toBeVisible();
    }
  });
});

// Test Case 2: Test navigation in Chrome - All links and buttons function correctly
test.describe('TC2: Chrome - Navigation Functionality', () => {
  test('Header navigation links are clickable and functional', async ({ page }) => {
    await page.goto('/');

    // Test Features nav link - should scroll to features section
    const featuresLink = page.locator('.nav-links a[href="#features"]');
    await expect(featuresLink).toBeVisible();
    await featuresLink.click();

    // Verify the features section is in view
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport();

    // Test Quick Start nav link - should scroll to quick-start section
    const quickStartLink = page.locator('.nav-links a[href="#quick-start"]');
    await expect(quickStartLink).toBeVisible();
    await quickStartLink.click();

    // Verify the quick-start section is in view
    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeInViewport();
  });

  test('GitHub external link in navigation has correct attributes', async ({ page }) => {
    await page.goto('/');

    const githubNavLink = page.locator('.nav-links a[href*="github.com"]');
    await expect(githubNavLink).toBeVisible();

    // Verify external link attributes for security
    const target = await githubNavLink.getAttribute('target');
    const rel = await githubNavLink.getAttribute('rel');
    const href = await githubNavLink.getAttribute('href');

    expect(target).toBe('_blank');
    expect(rel).toContain('noopener');
    expect(rel).toContain('noreferrer');
    expect(href).toContain('github.com/yetone/mirdb');
  });

  test('Primary CTA button is clickable and has correct href', async ({ page }) => {
    await page.goto('/');

    const primaryCTA = page.locator('[data-testid="primary-cta"]');
    await expect(primaryCTA).toBeVisible();

    // Get button attributes
    const href = await primaryCTA.getAttribute('href');
    const text = await primaryCTA.textContent();

    // Primary CTA links to quick-start section
    expect(href).toBe('#quick-start');
    expect(text?.toLowerCase()).toContain('get started');

    // Click and verify navigation
    await primaryCTA.click();
    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeInViewport();
  });

  test('Secondary CTA button (GitHub) is clickable and has correct attributes', async ({ page }) => {
    await page.goto('/');

    const secondaryCTA = page.locator('[data-testid="secondary-cta"]');
    await expect(secondaryCTA).toBeVisible();

    // Get button attributes
    const href = await secondaryCTA.getAttribute('href');
    const target = await secondaryCTA.getAttribute('target');
    const rel = await secondaryCTA.getAttribute('rel');
    const text = await secondaryCTA.textContent();

    expect(href).toContain('github.com/yetone/mirdb');
    expect(target).toBe('_blank');
    expect(rel).toContain('noopener');
    expect(text?.toLowerCase()).toContain('github');
  });

  test('Footer navigation links are clickable and have correct attributes', async ({ page }) => {
    await page.goto('/');

    // Test GitHub link
    const githubLink = page.locator('[data-testid="footer-github-link"]');
    await expect(githubLink).toBeVisible();
    const githubHref = await githubLink.getAttribute('href');
    expect(githubHref).toContain('github.com/yetone/mirdb');

    // Test Documentation link
    const docsLink = page.locator('[data-testid="footer-docs-link"]');
    await expect(docsLink).toBeVisible();
    const docsHref = await docsLink.getAttribute('href');
    expect(docsHref).toContain('github.com/yetone/mirdb');

    // Test License link
    const licenseLink = page.locator('[data-testid="footer-license-link"]');
    await expect(licenseLink).toBeVisible();
    const licenseHref = await licenseLink.getAttribute('href');
    expect(licenseHref).toContain('LICENSE');
  });

  test('Skip link is functional for keyboard navigation', async ({ page }) => {
    await page.goto('/');

    // The skip link should be initially not in viewport
    const skipLink = page.locator('[data-testid="skip-link"]');
    await expect(skipLink).toBeAttached();

    // Tab to the skip link
    await page.keyboard.press('Tab');

    // Skip link should now be visible when focused
    await expect(skipLink).toBeFocused();

    // Activate the skip link
    await page.keyboard.press('Enter');

    // Main content should now have focus
    const mainContent = page.locator('#main-content');
    await expect(mainContent).toBeFocused();
  });

  test('Smooth scroll behavior works for anchor links', async ({ page }) => {
    await page.goto('/');

    // Click on features link
    const featuresLink = page.locator('.nav-links a[href="#features"]');
    await featuresLink.click();

    // Wait a bit for smooth scroll
    await page.waitForTimeout(500);

    // Verify features section is in viewport
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport();
  });

  test('All interactive elements have visible focus states', async ({ page }) => {
    await page.goto('/');

    // Tab through interactive elements and verify focus is visible
    const primaryCTA = page.locator('[data-testid="primary-cta"]');
    await primaryCTA.focus();

    // Verify focus outline is present
    const outlineStyle = await primaryCTA.evaluate(el =>
      window.getComputedStyle(el).outlineStyle
    );
    // Focus outline should be applied (not none)
    expect(outlineStyle).not.toBe('none');
  });

  test('Button hover states work correctly', async ({ page }) => {
    await page.goto('/');

    const primaryCTA = page.locator('[data-testid="primary-cta"]');

    // Get initial background color
    const initialBgColor = await primaryCTA.evaluate(el =>
      window.getComputedStyle(el).backgroundColor
    );

    // Hover over the button
    await primaryCTA.hover();

    // Wait for transition
    await page.waitForTimeout(250);

    // Get hover background color
    const hoverBgColor = await primaryCTA.evaluate(el =>
      window.getComputedStyle(el).backgroundColor
    );

    // Background color should change on hover (darker)
    expect(hoverBgColor).not.toBe(initialBgColor);
  });
});

// Test Case 3: Load homepage in Chrome (latest - 1 version) - All sections render correctly
// Note: Playwright runs against the stable Chromium channel which covers latest Chrome
// This test ensures backward compatibility with CSS features used
test.describe('TC3: Chrome Backward Compatibility', () => {
  test('CSS Grid layout is supported and renders correctly', async ({ page }) => {
    await page.goto('/');

    // Verify CSS Grid is supported and working
    const featuresGrid = page.locator('[data-testid="features-grid"]');

    const gridStyles = await featuresGrid.evaluate(el => {
      const styles = window.getComputedStyle(el);
      return {
        display: styles.display,
        gridTemplateColumns: styles.gridTemplateColumns
      };
    });

    expect(gridStyles.display).toBe('grid');
    // Grid should have multiple columns defined
    expect(gridStyles.gridTemplateColumns).not.toBe('none');
  });

  test('CSS Custom Properties (CSS Variables) are supported', async ({ page }) => {
    await page.goto('/');

    // Verify CSS custom properties work in Chrome
    const root = page.locator(':root');
    const primaryColor = await root.evaluate(() => {
      return getComputedStyle(document.documentElement)
        .getPropertyValue('--color-primary').trim();
    });

    // Verify the CSS variable has a value
    expect(primaryColor).toBeTruthy();
    expect(primaryColor).toBe('#2563eb');
  });

  test('Flexbox layout is supported and renders correctly', async ({ page }) => {
    await page.goto('/');

    // Verify Flexbox is working in Chrome
    const nav = page.locator('.nav');
    const flexStyles = await nav.evaluate(el => {
      const styles = window.getComputedStyle(el);
      return {
        display: styles.display,
        justifyContent: styles.justifyContent,
        alignItems: styles.alignItems
      };
    });

    expect(flexStyles.display).toBe('flex');
    expect(flexStyles.justifyContent).toBe('space-between');
    expect(flexStyles.alignItems).toBe('center');
  });

  test('CSS animations work correctly (status indicator pulse)', async ({ page }) => {
    await page.goto('/');

    // Verify CSS animation is applied
    const statusIndicator = page.locator('.status-indicator');
    await expect(statusIndicator).toBeVisible();

    const animationName = await statusIndicator.evaluate(el =>
      window.getComputedStyle(el).animationName
    );

    // Animation should be 'pulse'
    expect(animationName).toBe('pulse');
  });

  test('Scroll behavior smooth is supported', async ({ page }) => {
    await page.goto('/');

    // Verify smooth scroll is applied
    const html = page.locator('html');
    const scrollBehavior = await html.evaluate(el =>
      window.getComputedStyle(el).scrollBehavior
    );

    expect(scrollBehavior).toBe('smooth');
  });

  test('Box-sizing border-box is applied correctly', async ({ page }) => {
    await page.goto('/');

    // Verify box-sizing is border-box for all elements
    const featureCard = page.locator('.feature-card').first();
    const boxSizing = await featureCard.evaluate(el =>
      window.getComputedStyle(el).boxSizing
    );

    expect(boxSizing).toBe('border-box');
  });

  test('Linear gradients render correctly in hero section', async ({ page }) => {
    await page.goto('/');

    const heroSection = page.locator('section.hero');
    const backgroundImage = await heroSection.evaluate(el =>
      window.getComputedStyle(el).backgroundImage
    );

    // Should have a linear gradient
    expect(backgroundImage).toContain('linear-gradient');
  });

  test('Border radius is applied correctly', async ({ page }) => {
    await page.goto('/');

    const primaryCTA = page.locator('[data-testid="primary-cta"]');
    const borderRadius = await primaryCTA.evaluate(el =>
      window.getComputedStyle(el).borderRadius
    );

    // Should have border radius applied (8px from CSS variable)
    expect(borderRadius).toBe('8px');
  });

  test('Box shadows are rendered correctly', async ({ page }) => {
    await page.goto('/');

    // Hover over a feature card to trigger shadow
    const featureCard = page.locator('.feature-card').first();
    await featureCard.hover();

    // Wait for transition
    await page.waitForTimeout(250);

    const boxShadow = await featureCard.evaluate(el =>
      window.getComputedStyle(el).boxShadow
    );

    // Should have a box shadow on hover
    expect(boxShadow).not.toBe('none');
  });

  test('Sticky positioning works for header', async ({ page }) => {
    await page.goto('/');

    const header = page.locator('header.header');
    const position = await header.evaluate(el =>
      window.getComputedStyle(el).position
    );

    expect(position).toBe('sticky');

    // Scroll down the page
    await page.evaluate(() => window.scrollTo(0, 500));

    // Header should still be visible at top
    await expect(header).toBeInViewport();
  });

  test('Transform translateY works on hover', async ({ page }) => {
    await page.goto('/');

    const featureCard = page.locator('.feature-card').first();

    // Get initial transform
    const initialTransform = await featureCard.evaluate(el =>
      window.getComputedStyle(el).transform
    );

    // Hover to trigger transform
    await featureCard.hover();
    await page.waitForTimeout(250);

    const hoverTransform = await featureCard.evaluate(el =>
      window.getComputedStyle(el).transform
    );

    // Transform should change on hover
    expect(hoverTransform).not.toBe(initialTransform);
  });

  test('Media query responsive breakpoints work', async ({ page }) => {
    // Test at mobile viewport
    await page.setViewportSize({ width: 375, height: 667 });
    await page.goto('/');

    const nav = page.locator('.nav');
    const navDirection = await nav.evaluate(el =>
      window.getComputedStyle(el).flexDirection
    );

    // On mobile, nav should be column
    expect(navDirection).toBe('column');

    // Test at desktop viewport
    await page.setViewportSize({ width: 1280, height: 800 });

    const navDirectionDesktop = await nav.evaluate(el =>
      window.getComputedStyle(el).flexDirection
    );

    // On desktop, nav should be row
    expect(navDirectionDesktop).toBe('row');
  });

  test('Prefers-reduced-motion is respected when set', async ({ page }) => {
    // Emulate reduced motion preference
    await page.emulateMedia({ reducedMotion: 'reduce' });
    await page.goto('/');

    const statusIndicator = page.locator('.status-indicator');
    const animationDuration = await statusIndicator.evaluate(el =>
      window.getComputedStyle(el).animationDuration
    );

    // With reduced motion, animation duration should be very short
    // Chrome may report this as '0.01ms' or '1e-05s' (scientific notation)
    const durationMs = parseFloat(animationDuration) *
      (animationDuration.includes('ms') ? 1 : 1000);
    expect(durationMs).toBeLessThan(1); // Less than 1ms
  });
});
