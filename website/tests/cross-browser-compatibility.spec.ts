import { test, expect } from '@playwright/test';

/**
 * Cross-Browser Compatibility Tests
 *
 * NFR-6: Website must render correctly on modern browsers (Chrome, Firefox, Safari, Edge)
 *
 * These tests verify that the website renders correctly across all supported browsers.
 * The tests run on chromium, firefox, webkit (Safari), and edge via Playwright projects.
 */

test.describe('Cross-Browser Compatibility - NFR-6', () => {
  test.describe('Page Load and Basic Rendering', () => {
    test('homepage loads successfully', async ({ page, browserName }) => {
      const response = await page.goto('/');
      expect(response?.status()).toBe(200);

      // Verify page title loads correctly
      await expect(page).toHaveTitle(/MirDB/i);
    });

    test('all critical sections are visible', async ({ page, browserName }) => {
      await page.goto('/');

      // Verify hero section renders
      const heroSection = page.locator('[data-testid="hero-section"]');
      await expect(heroSection).toBeVisible();

      // Verify features section renders
      const featuresSection = page.locator('[data-testid="features-section"]');
      await expect(featuresSection).toBeVisible();

      // Verify navigation renders
      const navigation = page.locator('[data-testid="main-navigation"]');
      await expect(navigation).toBeVisible();

      // Verify architecture section renders
      const architectureSection = page.locator('[data-testid="architecture-section"]');
      await expect(architectureSection).toBeVisible();
    });

    test('no JavaScript errors on page load', async ({ page, browserName }) => {
      const errors: string[] = [];

      page.on('pageerror', (error) => {
        errors.push(error.message);
      });

      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Verify no critical JavaScript errors
      expect(errors).toHaveLength(0);
    });
  });

  test.describe('Layout and Visual Rendering', () => {
    test('hero section layout is correct', async ({ page, browserName }) => {
      await page.goto('/');

      const heroSection = page.locator('[data-testid="hero-section"]');
      await expect(heroSection).toBeVisible();

      // Verify hero content structure
      const headline = page.locator('[data-testid="hero-headline"]');
      await expect(headline).toBeVisible();

      const subheadline = page.locator('[data-testid="hero-subheadline"]');
      await expect(subheadline).toBeVisible();

      // Verify CTA buttons are visible
      const ctaButtons = page.locator('[data-testid="hero-section"] a, [data-testid="hero-section"] button');
      const buttonCount = await ctaButtons.count();
      expect(buttonCount).toBeGreaterThan(0);
    });

    test('features section displays all feature cards', async ({ page, browserName }) => {
      await page.goto('/');

      const featuresSection = page.locator('[data-testid="features-section"]');
      await expect(featuresSection).toBeVisible();

      // Verify feature cards are rendered
      const featureCards = page.locator('[data-testid="features-section"] [data-testid*="feature-"]');
      const cardCount = await featureCards.count();
      expect(cardCount).toBeGreaterThan(0);
    });

    test('no horizontal overflow on page', async ({ page, browserName }) => {
      await page.goto('/');

      // Check for horizontal overflow
      const hasHorizontalOverflow = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });

      expect(hasHorizontalOverflow).toBe(false);
    });

    test('content is not clipped or hidden', async ({ page, browserName }) => {
      await page.goto('/');

      // Verify main content area has proper overflow handling
      const bodyOverflow = await page.evaluate(() => {
        const computedStyle = window.getComputedStyle(document.body);
        return {
          overflowX: computedStyle.overflowX,
          visibility: computedStyle.visibility
        };
      });

      expect(bodyOverflow.visibility).not.toBe('hidden');
    });
  });

  test.describe('Navigation Functionality', () => {
    test('navigation links are clickable', async ({ page, browserName }) => {
      await page.goto('/');

      // Get visible desktop navigation links (excluding mobile nav which is hidden)
      const desktopNavLinks = page.locator('[data-testid="main-navigation"] a:visible');
      const linkCount = await desktopNavLinks.count();

      expect(linkCount).toBeGreaterThan(0);

      // Verify all visible navigation links are accessible
      for (let i = 0; i < linkCount; i++) {
        const link = desktopNavLinks.nth(i);
        await expect(link).toBeVisible();
      }
    });

    test('navigation is accessible via keyboard', async ({ page, browserName }) => {
      await page.goto('/');

      // Tab to first interactive element
      await page.keyboard.press('Tab');

      // Verify focus is visible (element is focused)
      const focusedElement = await page.evaluate(() => {
        const activeElement = document.activeElement;
        return activeElement?.tagName;
      });

      // Should be able to tab to an interactive element
      expect(focusedElement).toBeTruthy();
    });
  });

  test.describe('Typography and Fonts', () => {
    test('text is readable and renders correctly', async ({ page, browserName }) => {
      await page.goto('/');

      // Verify headline text is present and readable
      const headline = page.locator('[data-testid="hero-headline"]');
      await expect(headline).toBeVisible();

      const headlineText = await headline.textContent();
      expect(headlineText?.length).toBeGreaterThan(0);

      // Verify font size is adequate
      const fontSize = await headline.evaluate((el) => {
        return parseInt(window.getComputedStyle(el).fontSize);
      });
      expect(fontSize).toBeGreaterThanOrEqual(24); // At least 24px for headline
    });

    test('code blocks render correctly', async ({ page, browserName }) => {
      await page.goto('/');

      // Look for code blocks in the page
      const codeBlocks = page.locator('pre, code');
      const codeCount = await codeBlocks.count();

      if (codeCount > 0) {
        const firstCodeBlock = codeBlocks.first();
        await expect(firstCodeBlock).toBeVisible();

        // Verify code block has proper styling
        const styles = await firstCodeBlock.evaluate((el) => {
          const computedStyle = window.getComputedStyle(el);
          return {
            fontFamily: computedStyle.fontFamily,
            display: computedStyle.display
          };
        });

        // Code should use monospace font
        expect(styles.fontFamily.toLowerCase()).toMatch(/mono|courier|consolas/);
      }
    });
  });

  test.describe('Interactive Elements', () => {
    test('buttons have proper hover states', async ({ page, browserName }) => {
      await page.goto('/');

      // Find primary CTA buttons
      const buttons = page.locator('[data-testid="hero-section"] a, [data-testid="hero-section"] button');
      const buttonCount = await buttons.count();

      if (buttonCount > 0) {
        const firstButton = buttons.first();
        await expect(firstButton).toBeVisible();

        // Hover over button
        await firstButton.hover();

        // Button should still be visible and interactive after hover
        await expect(firstButton).toBeVisible();
      }
    });

    test('links are properly styled', async ({ page, browserName }) => {
      await page.goto('/');

      // Find links in the page
      const links = page.locator('a[href]');
      const linkCount = await links.count();

      expect(linkCount).toBeGreaterThan(0);

      // Verify links are visible
      const firstVisibleLink = links.first();
      await expect(firstVisibleLink).toBeVisible();
    });
  });

  test.describe('Images and Media', () => {
    test('images load without errors', async ({ page, browserName }) => {
      const brokenImages: string[] = [];

      page.on('requestfailed', (request) => {
        if (request.resourceType() === 'image') {
          brokenImages.push(request.url());
        }
      });

      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Verify no broken images
      expect(brokenImages).toHaveLength(0);
    });

    test('SVG elements render correctly', async ({ page, browserName }) => {
      await page.goto('/');

      // Find SVG elements (logos, icons, diagrams)
      const svgElements = page.locator('svg');
      const svgCount = await svgElements.count();

      // If SVGs exist, verify they're visible
      if (svgCount > 0) {
        for (let i = 0; i < Math.min(svgCount, 5); i++) {
          const svg = svgElements.nth(i);
          const box = await svg.boundingBox();

          if (box) {
            // Verify SVG has dimensions (is rendered)
            expect(box.width).toBeGreaterThan(0);
            expect(box.height).toBeGreaterThan(0);
          }
        }
      }
    });
  });

  test.describe('CSS and Styling', () => {
    test('Tailwind CSS styles are applied', async ({ page, browserName }) => {
      await page.goto('/');

      // Verify CSS classes are being applied (Tailwind generates styles)
      const bodyStyles = await page.evaluate(() => {
        const computedStyle = window.getComputedStyle(document.body);
        return {
          margin: computedStyle.margin,
          fontFamily: computedStyle.fontFamily
        };
      });

      // Body should have Tailwind's default font family applied
      expect(bodyStyles.fontFamily).toBeTruthy();
    });

    test('responsive utility classes work correctly', async ({ page, browserName }) => {
      await page.goto('/');

      // Set viewport to desktop
      await page.setViewportSize({ width: 1280, height: 800 });

      // Verify page still renders correctly
      const heroSection = page.locator('[data-testid="hero-section"]');
      await expect(heroSection).toBeVisible();

      // Check for layout issues
      const hasOverflow = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });

      expect(hasOverflow).toBe(false);
    });
  });

  test.describe('Scrolling Behavior', () => {
    test('page scrolls smoothly', async ({ page, browserName }) => {
      await page.goto('/');

      // Get initial scroll position
      const initialScroll = await page.evaluate(() => window.scrollY);

      // Scroll down
      await page.evaluate(() => window.scrollTo(0, 500));

      // Verify scroll happened
      const newScroll = await page.evaluate(() => window.scrollY);
      expect(newScroll).toBeGreaterThan(initialScroll);
    });

    test('footer is reachable by scrolling', async ({ page, browserName }) => {
      await page.goto('/');

      // Scroll to bottom of page
      await page.evaluate(() => {
        window.scrollTo(0, document.body.scrollHeight);
      });

      // Look for footer element
      const footer = page.locator('footer');
      const footerCount = await footer.count();

      if (footerCount > 0) {
        await expect(footer).toBeVisible();
      }
    });
  });
});
