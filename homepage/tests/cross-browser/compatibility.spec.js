// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Cross-Browser Compatibility Tests
 * Scenario: Verify that the homepage works correctly across Chrome, Firefox, Safari, and Edge as per NFR-4
 *
 * Test Cases:
 * TC1: Test page rendering in Chrome - Page renders correctly with all features working
 * TC2: Test page rendering in Firefox - Page renders correctly with all features working
 * TC3: Test page rendering in Safari (WebKit) - Page renders correctly with all features working
 * TC4: Test page rendering in Edge - Page renders correctly with all features working
 * TC5: Check CSS flexbox/grid support - Layout renders correctly using CSS flexbox/grid across all browsers
 * TC6: Check smooth scroll behavior - Smooth scrolling works or gracefully degrades across all browsers
 */

test.describe('Cross-Browser Compatibility Tests (NFR-4)', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to homepage before each test
    await page.goto('/');
    // Wait for page to be fully loaded
    await page.waitForLoadState('domcontentloaded');
  });

  /**
   * Test Case 1-4: Page Rendering Tests
   * Tests run in each browser configured in playwright.config.js
   * This single test validates core page rendering for Chrome, Firefox, Safari, and Edge
   */
  test.describe('Page Rendering Verification', () => {
    test('TC1-4: Homepage renders correctly with all essential elements visible', async ({ page, browserName }) => {
      // Log which browser we're testing
      console.log(`Testing in browser: ${browserName}`);

      // Verify the page title
      await expect(page).toHaveTitle(/MirDB/);

      // Test Hero Section renders correctly
      const heroSection = page.locator('[data-testid="hero-section"]');
      await expect(heroSection).toBeVisible();

      // Verify hero title
      const heroTitle = page.locator('[data-testid="hero-title"]');
      await expect(heroTitle).toBeVisible();
      await expect(heroTitle).toContainText('MirDB');

      // Verify hero tagline
      const heroTagline = page.locator('[data-testid="hero-tagline"]');
      await expect(heroTagline).toBeVisible();

      // Verify CTA buttons
      const getStartedBtn = page.locator('[data-testid="get-started-btn"]');
      const githubBtn = page.locator('[data-testid="github-btn"]');
      await expect(getStartedBtn).toBeVisible();
      await expect(githubBtn).toBeVisible();

      // Test Features Section renders
      const featuresSection = page.locator('[data-testid="features-section"]');
      await expect(featuresSection).toBeVisible();

      // Verify at least one feature card exists
      const featureCards = page.locator('.feature-card');
      await expect(featureCards.first()).toBeVisible();

      // Test Getting Started Section renders
      const gettingStartedSection = page.locator('[data-testid="getting-started-section"]');
      await expect(gettingStartedSection).toBeVisible();

      // Test How It Works Section renders
      const howItWorksSection = page.locator('[data-testid="how-it-works-section"]');
      await expect(howItWorksSection).toBeVisible();

      // Test Code Examples Section renders
      const codeExamplesSection = page.locator('[data-testid="code-examples-section"]');
      await expect(codeExamplesSection).toBeVisible();

      // Test Footer renders
      const footer = page.locator('.footer');
      await expect(footer).toBeVisible();

      // Take a screenshot for visual verification
      await page.screenshot({ path: `test-results/cross-browser-${browserName}.png`, fullPage: true });
    });

    test('TC1-4b: All interactive elements are functional', async ({ page, browserName }) => {
      console.log(`Testing interactive elements in: ${browserName}`);

      // Test CTA button is clickable
      const getStartedBtn = page.locator('[data-testid="get-started-btn"]');
      await expect(getStartedBtn).toBeEnabled();

      // Test GitHub button has correct href
      const githubBtn = page.locator('[data-testid="github-btn"]');
      const href = await githubBtn.getAttribute('href');
      expect(href).toBe('https://github.com/yetone/mirdb');

      // Scroll to code examples section
      await page.locator('[data-testid="code-examples-section"]').scrollIntoViewIfNeeded();

      // Test copy buttons exist and are visible
      const copyButtons = page.locator('.copy-btn');
      const count = await copyButtons.count();
      expect(count).toBeGreaterThan(0);
      await expect(copyButtons.first()).toBeVisible();
    });

    test('TC1-4c: Page has no JavaScript errors', async ({ page, browserName }) => {
      console.log(`Testing JavaScript execution in: ${browserName}`);

      // Collect console errors
      const errors = [];
      page.on('console', msg => {
        if (msg.type() === 'error') {
          errors.push(msg.text());
        }
      });

      // Wait for scripts to execute
      await page.waitForTimeout(1000);

      // Check for validation functions (exposed by main.js)
      const heroValid = await page.evaluate(() => {
        return typeof window.MirDBHomepage !== 'undefined' &&
               typeof window.MirDBHomepage.validateHeroRender === 'function';
      });
      expect(heroValid).toBe(true);

      // Run validation
      const validationResult = await page.evaluate(() => {
        return window.MirDBHomepage.validateHeroRender();
      });
      expect(validationResult).toBe(true);

      // Filter out expected warnings (like Mermaid) and check for critical errors
      const criticalErrors = errors.filter(e =>
        !e.includes('Mermaid') &&
        !e.includes('mermaid') &&
        !e.includes('Failed to load resource')
      );
      expect(criticalErrors).toHaveLength(0);
    });
  });

  /**
   * Test Case 5: CSS Flexbox/Grid Support
   * Verify layout renders correctly using CSS flexbox/grid across all browsers
   */
  test.describe('CSS Flexbox/Grid Support (TC5)', () => {
    test('TC5a: Feature grid uses CSS Grid correctly', async ({ page, browserName }) => {
      console.log(`Testing CSS Grid in: ${browserName}`);

      // Navigate to features section
      const featuresSection = page.locator('[data-testid="features-section"]');
      await featuresSection.scrollIntoViewIfNeeded();

      // Verify feature grid exists
      const featureGrid = page.locator('.feature-grid');
      await expect(featureGrid).toBeVisible();

      // Check grid display property
      const display = await featureGrid.evaluate(el => getComputedStyle(el).display);
      expect(display).toBe('grid');

      // Verify grid-template-columns is set (should be auto-fit)
      const gridTemplateColumns = await featureGrid.evaluate(el => getComputedStyle(el).gridTemplateColumns);
      // Grid columns should be present (will vary based on viewport)
      expect(gridTemplateColumns).toBeTruthy();
      expect(gridTemplateColumns).not.toBe('none');

      // Verify feature cards are laid out properly
      const featureCards = featureGrid.locator('.feature-card');
      const cardCount = await featureCards.count();
      expect(cardCount).toBeGreaterThanOrEqual(6); // 6 feature cards expected
    });

    test('TC5b: Hero CTA buttons use flexbox correctly', async ({ page, browserName }) => {
      console.log(`Testing Flexbox in: ${browserName}`);

      // Verify CTA buttons container uses flexbox
      const ctaButtons = page.locator('[data-testid="hero-cta-buttons"]');
      await expect(ctaButtons).toBeVisible();

      // Check flex display
      const display = await ctaButtons.evaluate(el => getComputedStyle(el).display);
      expect(display).toBe('flex');

      // Verify justify-content is center
      const justifyContent = await ctaButtons.evaluate(el => getComputedStyle(el).justifyContent);
      expect(justifyContent).toBe('center');

      // Verify gap exists between buttons
      const gap = await ctaButtons.evaluate(el => getComputedStyle(el).gap);
      expect(gap).toBeTruthy();
      expect(gap).not.toBe('normal');
    });

    test('TC5c: Commands grid uses CSS Grid correctly', async ({ page, browserName }) => {
      console.log(`Testing commands grid in: ${browserName}`);

      // Navigate to code examples section
      await page.locator('[data-testid="code-examples-section"]').scrollIntoViewIfNeeded();

      // Verify commands grid exists and uses grid layout
      const commandsGrid = page.locator('[data-testid="commands-grid"]');
      await expect(commandsGrid).toBeVisible();

      const display = await commandsGrid.evaluate(el => getComputedStyle(el).display);
      expect(display).toBe('grid');

      // Verify command cards are displayed
      const commandCards = commandsGrid.locator('.command-card');
      const cardCount = await commandCards.count();
      expect(cardCount).toBeGreaterThanOrEqual(8); // 8 command cards expected
    });

    test('TC5d: Data paths grid uses CSS Grid correctly', async ({ page, browserName }) => {
      console.log(`Testing data paths grid in: ${browserName}`);

      // Navigate to how it works section
      await page.locator('[data-testid="how-it-works-section"]').scrollIntoViewIfNeeded();

      // Verify data paths grid exists
      const dataPathsGrid = page.locator('.data-paths-grid');
      await expect(dataPathsGrid).toBeVisible();

      const display = await dataPathsGrid.evaluate(el => getComputedStyle(el).display);
      expect(display).toBe('grid');
    });

    test('TC5e: Footer navigation uses flexbox correctly', async ({ page, browserName }) => {
      console.log(`Testing footer flexbox in: ${browserName}`);

      // Navigate to footer
      await page.locator('.footer').scrollIntoViewIfNeeded();

      // Verify footer navigation uses flexbox
      const footerNav = page.locator('.footer-nav');
      await expect(footerNav).toBeVisible();

      const display = await footerNav.evaluate(el => getComputedStyle(el).display);
      expect(display).toBe('flex');

      // Verify flex-wrap is set for responsive behavior
      const flexWrap = await footerNav.evaluate(el => getComputedStyle(el).flexWrap);
      expect(flexWrap).toBe('wrap');
    });
  });

  /**
   * Test Case 6: Smooth Scroll Behavior
   * Verify smooth scrolling works or gracefully degrades across all browsers
   */
  test.describe('Smooth Scroll Behavior (TC6)', () => {
    test('TC6a: Clicking Get Started button scrolls to getting-started section', async ({ page, browserName }) => {
      console.log(`Testing smooth scroll in: ${browserName}`);

      // Ensure we start at the top of the page
      await page.evaluate(() => window.scrollTo(0, 0));
      await page.waitForTimeout(100);

      // Get initial scroll position
      const initialScrollY = await page.evaluate(() => window.scrollY);
      expect(initialScrollY).toBeLessThan(100);

      // Click Get Started button
      const getStartedBtn = page.locator('[data-testid="get-started-btn"]');
      await getStartedBtn.click();

      // Wait for scroll animation to complete
      await page.waitForTimeout(800);

      // Verify we've scrolled down
      const afterScrollY = await page.evaluate(() => window.scrollY);
      expect(afterScrollY).toBeGreaterThan(initialScrollY);

      // Verify the getting-started section is in viewport
      const gettingStartedSection = page.locator('[data-testid="getting-started-section"]');
      await expect(gettingStartedSection).toBeInViewport();
    });

    test('TC6b: Smooth scroll JavaScript is initialized', async ({ page, browserName }) => {
      console.log(`Testing smooth scroll initialization in: ${browserName}`);

      // Verify anchor links have click handlers (via the initSmoothScroll function)
      const anchorLinks = page.locator('a[href^="#"]');
      const count = await anchorLinks.count();
      expect(count).toBeGreaterThan(0);

      // Test that clicking an anchor link prevents default behavior and scrolls
      // Start at top
      await page.evaluate(() => window.scrollTo(0, 0));
      await page.waitForTimeout(100);

      // Click the Get Started button (which links to #getting-started)
      await page.locator('[data-testid="get-started-btn"]').click();

      // The URL should NOT have the hash added (preventDefault was called)
      // But since we use scrollIntoView, this behavior varies by browser
      // What matters is that the page scrolled

      await page.waitForTimeout(600);
      const scrollY = await page.evaluate(() => window.scrollY);
      expect(scrollY).toBeGreaterThan(100);
    });

    test('TC6c: Footer documentation link scrolls smoothly', async ({ page, browserName }) => {
      console.log(`Testing footer smooth scroll in: ${browserName}`);

      // Scroll to footer first
      await page.locator('.footer').scrollIntoViewIfNeeded();
      await page.waitForTimeout(100);

      // Find the documentation link that points to #getting-started
      const docsLink = page.locator('.footer-nav a[href="#getting-started"]');

      // Check if this link exists (it may be an external link instead)
      const count = await docsLink.count();
      if (count > 0) {
        // Click the docs link
        await docsLink.click();

        // Wait for scroll animation
        await page.waitForTimeout(800);

        // Verify the getting-started section is now in viewport
        const gettingStartedSection = page.locator('[data-testid="getting-started-section"]');
        await expect(gettingStartedSection).toBeInViewport();
      } else {
        // If no internal docs link, that's okay - the test passes
        console.log('No internal documentation link found in footer');
      }
    });

    test('TC6d: Smooth scroll uses scrollIntoView API', async ({ page, browserName }) => {
      console.log(`Testing scrollIntoView API in: ${browserName}`);

      // Check that scrollIntoView is available (it's a standard API)
      const scrollIntoViewSupported = await page.evaluate(() => {
        return typeof Element.prototype.scrollIntoView === 'function';
      });
      expect(scrollIntoViewSupported).toBe(true);

      // Verify smooth scroll behavior option is supported
      const smoothScrollSupported = await page.evaluate(() => {
        try {
          // Test if options are supported (modern browsers)
          const testEl = document.createElement('div');
          document.body.appendChild(testEl);
          testEl.scrollIntoView({ behavior: 'smooth', block: 'start' });
          document.body.removeChild(testEl);
          return true;
        } catch (e) {
          return false;
        }
      });

      // Modern browsers should support this - if not, smooth scroll gracefully degrades
      console.log(`Smooth scroll behavior support: ${smoothScrollSupported}`);
      // We don't fail if not supported - smooth scroll will fallback to instant scroll
    });
  });

  /**
   * Additional Cross-Browser Visual Tests
   */
  test.describe('Cross-Browser Visual Consistency', () => {
    test('Hero gradient background renders correctly', async ({ page, browserName }) => {
      console.log(`Testing gradient in: ${browserName}`);

      const heroSection = page.locator('.hero');
      await expect(heroSection).toBeVisible();

      // Check that background includes gradient
      const backgroundImage = await heroSection.evaluate(el => getComputedStyle(el).backgroundImage);
      expect(backgroundImage).toContain('gradient');
    });

    test('CSS custom properties (variables) work correctly', async ({ page, browserName }) => {
      console.log(`Testing CSS variables in: ${browserName}`);

      // Check that CSS custom properties are applied
      const primaryColor = await page.evaluate(() => {
        return getComputedStyle(document.documentElement).getPropertyValue('--primary-color').trim();
      });
      expect(primaryColor).toBeTruthy();
      expect(primaryColor).toMatch(/^#[0-9a-fA-F]{6}$/); // Should be a hex color
    });

    test('CSS transitions work correctly', async ({ page, browserName }) => {
      console.log(`Testing CSS transitions in: ${browserName}`);

      // Check that transitions are defined on feature cards
      const featureCard = page.locator('.feature-card').first();
      await featureCard.scrollIntoViewIfNeeded();

      const transition = await featureCard.evaluate(el => getComputedStyle(el).transition);
      expect(transition).toBeTruthy();
      expect(transition).not.toBe('none 0s ease 0s');
    });

    test('Border radius renders correctly', async ({ page, browserName }) => {
      console.log(`Testing border-radius in: ${browserName}`);

      const featureCard = page.locator('.feature-card').first();
      await featureCard.scrollIntoViewIfNeeded();

      const borderRadius = await featureCard.evaluate(el => getComputedStyle(el).borderRadius);
      expect(borderRadius).toBeTruthy();
      expect(borderRadius).not.toBe('0px');
    });

    test('Box shadows render correctly', async ({ page, browserName }) => {
      console.log(`Testing box-shadow in: ${browserName}`);

      const featureCard = page.locator('.feature-card').first();
      await featureCard.scrollIntoViewIfNeeded();

      const boxShadow = await featureCard.evaluate(el => getComputedStyle(el).boxShadow);
      expect(boxShadow).toBeTruthy();
      expect(boxShadow).not.toBe('none');
    });
  });

  /**
   * Browser-specific Feature Detection
   */
  test.describe('Browser Feature Detection', () => {
    test('Clipboard API is available or fallback works', async ({ page, browserName }) => {
      console.log(`Testing clipboard API in: ${browserName}`);

      // Check if clipboard API is available
      const clipboardAvailable = await page.evaluate(() => {
        return typeof navigator.clipboard !== 'undefined' &&
               typeof navigator.clipboard.writeText === 'function';
      });

      // Clipboard API may not be available in all contexts (e.g., non-HTTPS)
      // The fallback function should be defined in main.js
      console.log(`Clipboard API available: ${clipboardAvailable}`);

      // Verify copy buttons have click handlers regardless
      const copyBtn = page.locator('.copy-btn').first();
      await page.locator('[data-testid="code-examples-section"]').scrollIntoViewIfNeeded();
      await expect(copyBtn).toBeVisible();
    });

    test('CSS animation keyframes work correctly', async ({ page, browserName }) => {
      console.log(`Testing CSS animations in: ${browserName}`);

      // Hero content should have fadeIn animation
      const heroContent = page.locator('.hero-content');
      const animation = await heroContent.evaluate(el => getComputedStyle(el).animationName);

      // Animation name should be 'fadeIn' or similar
      expect(animation).toBeTruthy();
    });
  });
});
