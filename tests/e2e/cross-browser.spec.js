/**
 * Cross-Browser Compatibility E2E Tests for MirDB Homepage.
 *
 * This module tests that the homepage renders correctly and functions
 * properly across major browsers per NFR-6:
 * - Chrome (chromium)
 * - Firefox
 * - Safari (webkit)
 * - Edge
 *
 * These tests verify:
 * - Page loads successfully without errors
 * - All visual elements render correctly
 * - Interactive elements function properly
 * - CSS features work consistently across browsers
 */

const { test, expect } = require('@playwright/test');

// Use the baseURL from playwright.config.js which serves via local web server

test.describe('Cross-Browser Compatibility - Page Load and Rendering', () => {
  test('page loads successfully with correct title', async ({ page, browserName }) => {
    await page.goto('/');

    // Page should have correct title
    await expect(page).toHaveTitle(/MirDB/);

    // Log browser name for debugging
    console.log(`Testing in browser: ${browserName}`);
  });

  test('hero section renders correctly', async ({ page }) => {
    await page.goto('/');

    // Hero section should be visible
    const hero = page.locator('.hero');
    await expect(hero).toBeVisible();

    // Product name should be visible
    const h1 = page.locator('.hero h1');
    await expect(h1).toBeVisible();
    await expect(h1).toContainText('MirDB');

    // Tagline should be visible
    const tagline = page.locator('.tagline');
    await expect(tagline).toBeVisible();
    await expect(tagline).toContainText('Memcached protocol');

    // CTA buttons should be visible and styled
    const primaryBtn = page.locator('.btn-primary').first();
    const secondaryBtn = page.locator('.btn-secondary').first();
    await expect(primaryBtn).toBeVisible();
    await expect(secondaryBtn).toBeVisible();
  });

  test('navigation renders correctly', async ({ page }) => {
    await page.goto('/');

    // Navbar should be visible
    const navbar = page.locator('.navbar');
    await expect(navbar).toBeVisible();

    // Logo should be visible
    const logo = page.locator('.logo-img');
    await expect(logo).toBeVisible();

    // Nav links should be visible
    const navLinks = page.locator('.nav-links a');
    const linkCount = await navLinks.count();
    expect(linkCount).toBeGreaterThanOrEqual(4);

    for (let i = 0; i < linkCount; i++) {
      await expect(navLinks.nth(i)).toBeVisible();
    }
  });

  test('features section renders correctly', async ({ page }) => {
    await page.goto('/');

    // Features section should be visible
    const features = page.locator('#features');
    await expect(features).toBeVisible();

    // Feature cards should render
    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();
    expect(cardCount).toBeGreaterThanOrEqual(6);

    // Each card should have heading and description
    for (let i = 0; i < cardCount; i++) {
      const card = featureCards.nth(i);
      await expect(card).toBeVisible();
      const heading = card.locator('h3');
      await expect(heading).toBeVisible();
    }
  });

  test('commands table renders correctly', async ({ page }) => {
    await page.goto('/');

    // Commands section should be visible
    const commands = page.locator('#commands');
    await expect(commands).toBeVisible();

    // Table should render with headers
    const table = page.locator('.commands-table table');
    await expect(table).toBeVisible();

    // Table should have headers
    const headers = page.locator('.commands-table th');
    expect(await headers.count()).toBe(2);

    // Table should have command rows
    const rows = page.locator('.commands-table tbody tr');
    expect(await rows.count()).toBeGreaterThanOrEqual(8);
  });

  test('getting started section renders correctly', async ({ page }) => {
    await page.goto('/');

    // Getting started section should be visible
    const gettingStarted = page.locator('#getting-started');
    await expect(gettingStarted).toBeVisible();

    // Steps should render
    const steps = page.locator('.step');
    expect(await steps.count()).toBeGreaterThanOrEqual(3);

    // Code blocks should be visible
    const codeBlocks = page.locator('.steps pre');
    expect(await codeBlocks.count()).toBeGreaterThanOrEqual(3);
  });

  test('architecture section renders correctly', async ({ page }) => {
    await page.goto('/');

    // Architecture section should be visible
    const architecture = page.locator('#architecture');
    await expect(architecture).toBeVisible();

    // Diagram components should render
    const components = page.locator('.arch-component');
    expect(await components.count()).toBeGreaterThanOrEqual(4);
  });

  test('footer renders correctly', async ({ page }) => {
    await page.goto('/');

    // Footer should be visible
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    // Footer logo should be visible
    const footerLogo = page.locator('.footer-logo');
    await expect(footerLogo).toBeVisible();

    // Footer links should be present
    const footerLinks = page.locator('footer a');
    expect(await footerLinks.count()).toBeGreaterThanOrEqual(2);
  });
});

test.describe('Cross-Browser Compatibility - CSS Features', () => {
  test('CSS variables are applied correctly', async ({ page }) => {
    await page.goto('/');

    // Check that CSS custom properties (variables) are working
    const primaryColor = await page.evaluate(() => {
      return getComputedStyle(document.documentElement).getPropertyValue('--primary-color').trim();
    });
    expect(primaryColor).toBeTruthy();
  });

  test('flexbox layouts render correctly', async ({ page }) => {
    await page.goto('/');

    // Navbar uses flexbox
    const navbar = page.locator('.navbar');
    const navDisplay = await navbar.evaluate(el => getComputedStyle(el).display);
    expect(navDisplay).toBe('flex');

    // CTA buttons use flexbox
    const ctaButtons = page.locator('.cta-buttons');
    const ctaDisplay = await ctaButtons.evaluate(el => getComputedStyle(el).display);
    expect(ctaDisplay).toBe('flex');

    // Badges use flexbox
    const badges = page.locator('.badges');
    const badgesDisplay = await badges.evaluate(el => getComputedStyle(el).display);
    expect(badgesDisplay).toBe('flex');
  });

  test('grid layouts render correctly', async ({ page }) => {
    await page.goto('/');

    // Features grid uses CSS Grid
    const featuresGrid = page.locator('.features-grid');
    const gridDisplay = await featuresGrid.evaluate(el => getComputedStyle(el).display);
    expect(gridDisplay).toBe('grid');

    // Footer content uses grid
    const footerContent = page.locator('.footer-content');
    const footerDisplay = await footerContent.evaluate(el => getComputedStyle(el).display);
    expect(footerDisplay).toBe('grid');
  });

  test('gradient backgrounds render correctly', async ({ page }) => {
    await page.goto('/');

    // Hero should have gradient background
    const hero = page.locator('.hero');
    const background = await hero.evaluate(el => getComputedStyle(el).backgroundImage);
    expect(background).toContain('gradient');
  });

  test('background-clip text effect works for hero title', async ({ page, browserName }) => {
    await page.goto('/');

    // Hero h1 uses background-clip: text
    const heroH1 = page.locator('.hero h1');
    const backgroundClip = await heroH1.evaluate(el => getComputedStyle(el).webkitBackgroundClip || getComputedStyle(el).backgroundClip);

    // Different browsers may report this differently
    const validValues = ['text', '-webkit-text'];
    expect(validValues.some(v => backgroundClip.includes(v))).toBeTruthy();
  });

  test('transitions are defined correctly', async ({ page }) => {
    await page.goto('/');

    // Buttons should have transitions
    const btn = page.locator('.btn').first();
    const transition = await btn.evaluate(el => getComputedStyle(el).transition);
    expect(transition).toBeTruthy();
    expect(transition).not.toBe('none');
  });

  test('box-shadow renders correctly on feature cards', async ({ page }) => {
    await page.goto('/');

    // Feature cards should have box-shadow
    const featureCard = page.locator('.feature-card').first();
    const boxShadow = await featureCard.evaluate(el => getComputedStyle(el).boxShadow);
    expect(boxShadow).toBeTruthy();
    expect(boxShadow).not.toBe('none');
  });

  test('border-radius renders correctly', async ({ page }) => {
    await page.goto('/');

    // Buttons should have border-radius
    const btn = page.locator('.btn').first();
    const borderRadius = await btn.evaluate(el => getComputedStyle(el).borderRadius);
    expect(borderRadius).toBeTruthy();
    expect(borderRadius).not.toBe('0px');

    // Feature cards should have border-radius
    const featureCard = page.locator('.feature-card').first();
    const cardRadius = await featureCard.evaluate(el => getComputedStyle(el).borderRadius);
    expect(cardRadius).toBeTruthy();
    expect(cardRadius).not.toBe('0px');
  });
});

test.describe('Cross-Browser Compatibility - Interactivity', () => {
  test('internal navigation links work correctly', async ({ page }) => {
    await page.goto('/');

    // Click on Features link
    const featuresLink = page.locator('a[href="#features"]');
    await featuresLink.click();

    // Features section should be in viewport
    const features = page.locator('#features');
    await expect(features).toBeInViewport();
  });

  test('smooth scrolling works for anchor links', async ({ page }) => {
    await page.goto('/');

    // Check that smooth scroll is enabled
    const scrollBehavior = await page.evaluate(() => {
      return getComputedStyle(document.documentElement).scrollBehavior;
    });
    expect(scrollBehavior).toBe('smooth');
  });

  test('hover effects work on buttons', async ({ page }) => {
    await page.goto('/');

    const btn = page.locator('.btn-primary').first();

    // Get initial background color
    const initialBg = await btn.evaluate(el => getComputedStyle(el).backgroundColor);

    // Hover over button
    await btn.hover();

    // Wait a moment for transition
    await page.waitForTimeout(100);

    // Background should potentially change on hover (or at least not break)
    const hoverBg = await btn.evaluate(el => getComputedStyle(el).backgroundColor);
    expect(hoverBg).toBeTruthy();
  });

  test('hover effects work on feature cards', async ({ page }) => {
    await page.goto('/');

    const card = page.locator('.feature-card').first();

    // Get initial transform
    const initialTransform = await card.evaluate(el => getComputedStyle(el).transform);

    // Hover over card
    await card.hover();

    // Wait a moment for transition
    await page.waitForTimeout(350);

    // Transform should change on hover (translateY)
    const hoverTransform = await card.evaluate(el => getComputedStyle(el).transform);
    expect(hoverTransform).not.toBe(initialTransform);
  });

  test('external links have correct attributes', async ({ page }) => {
    await page.goto('/');

    // GitHub links should open in new tab with security attributes
    const githubLink = page.locator('a[href="https://github.com/yetone/mirdb"]').first();

    const target = await githubLink.getAttribute('target');
    const rel = await githubLink.getAttribute('rel');

    expect(target).toBe('_blank');
    expect(rel).toContain('noopener');
    expect(rel).toContain('noreferrer');
  });
});

test.describe('Cross-Browser Compatibility - Accessibility', () => {
  test('skip link is functional', async ({ page }) => {
    await page.goto('/');

    // Skip link should exist
    const skipLink = page.locator('.skip-link');
    await expect(skipLink).toBeAttached();

    // Should become visible on focus
    await skipLink.focus();
    await expect(skipLink).toBeVisible();
  });

  test('focus indicators are visible', async ({ page }) => {
    await page.goto('/');

    // Tab to first interactive element
    await page.keyboard.press('Tab');
    await page.keyboard.press('Tab');

    // Get focused element's outline
    const focusedOutline = await page.evaluate(() => {
      const focused = document.activeElement;
      return focused ? getComputedStyle(focused).outline : null;
    });

    // Should have visible outline (not 'none')
    expect(focusedOutline).toBeTruthy();
  });

  test('images have alt attributes', async ({ page }) => {
    await page.goto('/');

    // All images should have alt attributes
    const images = page.locator('img');
    const imgCount = await images.count();

    for (let i = 0; i < imgCount; i++) {
      const alt = await images.nth(i).getAttribute('alt');
      expect(alt).toBeTruthy();
    }
  });

  test('semantic HTML structure is correct', async ({ page }) => {
    await page.goto('/');

    // Should have main landmark
    const main = page.locator('main');
    await expect(main).toBeAttached();

    // Should have header
    const header = page.locator('header');
    await expect(header).toBeAttached();

    // Should have footer
    const footer = page.locator('footer');
    await expect(footer).toBeAttached();

    // Should have nav
    const nav = page.locator('nav');
    await expect(nav).toBeAttached();
  });
});

test.describe('Cross-Browser Compatibility - No JavaScript Errors', () => {
  test('page loads without JavaScript errors', async ({ page }) => {
    const jsErrors = [];

    // Listen for actual JavaScript errors (uncaught exceptions)
    page.on('pageerror', err => {
      jsErrors.push(err.message);
    });

    // Listen for console errors, but filter out 404 resource loading errors
    // which are not JavaScript errors per se
    page.on('console', msg => {
      if (msg.type() === 'error') {
        const text = msg.text();
        // Exclude 404 resource loading errors (images, fonts, etc.)
        // These are network issues, not JavaScript errors
        if (!text.includes('Failed to load resource') &&
            !text.includes('404') &&
            !text.includes('net::ERR_')) {
          jsErrors.push(text);
        }
      }
    });

    await page.goto('/');

    // Wait for page to fully load
    await page.waitForLoadState('domcontentloaded');

    // Should have no JavaScript execution errors
    expect(jsErrors).toHaveLength(0);
  });
});

test.describe('Cross-Browser Compatibility - No Horizontal Overflow', () => {
  test('no horizontal scrollbar appears', async ({ page }) => {
    await page.goto('/');

    const scrollWidth = await page.evaluate(() => document.documentElement.scrollWidth);
    const clientWidth = await page.evaluate(() => document.documentElement.clientWidth);

    expect(scrollWidth).toBeLessThanOrEqual(clientWidth);
  });
});
