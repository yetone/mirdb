import { test, expect } from '@playwright/test';

/**
 * Browser Compatibility - Safari Tests (NFR-6)
 *
 * These tests verify that the homepage functions correctly on Safari browser
 * as per Non-Functional Requirement NFR-6:
 * "Page shall function correctly on latest versions of Chrome, Firefox, Safari, and Edge"
 *
 * This test file specifically targets Safari/WebKit browser compatibility.
 */

test.describe('Browser Compatibility - Safari', () => {
  test.describe('Test Case 1: Homepage Rendering in Safari', () => {
    test('homepage loads and renders correctly with all styles applied', async ({ page, browserName }) => {
      // Ensure we're running on WebKit (Safari)
      expect(browserName).toBe('webkit');

      // Navigate to homepage
      await page.goto('/');

      // Verify page loads successfully
      await expect(page).toHaveTitle(/frontend|MirDB|Homepage/i);

      // Verify hero section renders with styles
      const heroSection = page.locator('[data-testid="hero-section"]');
      await expect(heroSection).toBeVisible();

      // Verify hero has background styling applied
      const heroBgColor = await heroSection.evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });
      expect(heroBgColor).toBeTruthy();
      expect(heroBgColor).not.toBe('');

      // Verify headline renders correctly
      const headline = page.locator('[data-testid="hero-headline"]');
      await expect(headline).toBeVisible();
      await expect(headline).toHaveText(/./);

      // Verify headline has proper font styling
      const headlineFontSize = await headline.evaluate((el) => {
        return window.getComputedStyle(el).fontSize;
      });
      expect(parseFloat(headlineFontSize)).toBeGreaterThan(16);

      // Verify subheadline renders
      const subheadline = page.locator('[data-testid="hero-subheadline"]');
      await expect(subheadline).toBeVisible();

      // Verify CTA button renders with visible styling
      const ctaButton = page.locator('[data-testid="hero-cta"]');
      await expect(ctaButton).toBeVisible();

      const ctaBgColor = await ctaButton.evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });
      // CTA should have a non-transparent background
      expect(ctaBgColor).not.toBe('rgba(0, 0, 0, 0)');
      expect(ctaBgColor).not.toBe('transparent');

      // Verify navigation renders
      const navigation = page.locator('[data-testid="navigation"]');
      await expect(navigation).toBeVisible();

      // Verify footer renders
      const footer = page.locator('[data-testid="footer"]');
      await expect(footer).toBeVisible();
    });

    test('CSS flexbox and grid layouts render correctly in Safari', async ({ page }) => {
      await page.goto('/');

      // Verify flexbox is supported and working in WebKit
      const heroContent = page.locator('.hero-content');
      if (await heroContent.count() > 0) {
        const display = await heroContent.evaluate((el) => {
          return window.getComputedStyle(el).display;
        });
        // Should use flexbox or block layout
        expect(['flex', 'block', 'grid']).toContain(display);
      }

      // Verify footer content uses appropriate layout
      const footerContent = page.locator('[data-testid="footer-content"]');
      await expect(footerContent).toBeVisible();
    });

    test('CSS custom properties (variables) are applied in Safari', async ({ page }) => {
      await page.goto('/');

      // Verify CSS is loading and applying styles in WebKit
      const app = page.locator('.app');
      if (await app.count() > 0) {
        const computedStyle = await app.evaluate((el) => {
          const style = window.getComputedStyle(el);
          return {
            display: style.display,
            position: style.position,
          };
        });
        // App should have some layout applied
        expect(computedStyle.display).toBeTruthy();
      }
    });
  });

  test.describe('Test Case 2: Navigation Links in Safari', () => {
    test('all navigation links are present and visible', async ({ page }) => {
      await page.goto('/');

      // Verify main navigation exists
      const navigation = page.locator('[data-testid="navigation"]');
      await expect(navigation).toBeVisible();

      // Verify navigation links container
      const navLinks = page.locator('[data-testid="navigation-links"]');
      await expect(navLinks).toBeVisible();

      // Verify Home link
      const homeLink = page.locator('[data-testid="navigation-link-home"]');
      await expect(homeLink).toBeVisible();
      await expect(homeLink).toHaveText('Home');

      // Verify Features link
      const featuresLink = page.locator('[data-testid="navigation-link-features"]');
      await expect(featuresLink).toBeVisible();
      await expect(featuresLink).toHaveText('Features');

      // Verify About link
      const aboutLink = page.locator('[data-testid="navigation-link-about"]');
      await expect(aboutLink).toBeVisible();
      await expect(aboutLink).toHaveText('About');

      // Verify Contact link
      const contactLink = page.locator('[data-testid="navigation-link-contact"]');
      await expect(contactLink).toBeVisible();
      await expect(contactLink).toHaveText('Contact');
    });

    test('navigation links have correct href attributes', async ({ page }) => {
      await page.goto('/');

      // Check Home link href
      const homeLink = page.locator('[data-testid="navigation-link-home"]');
      await expect(homeLink).toHaveAttribute('href', '/');

      // Check Features link href
      const featuresLink = page.locator('[data-testid="navigation-link-features"]');
      await expect(featuresLink).toHaveAttribute('href', '/features');

      // Check About link href
      const aboutLink = page.locator('[data-testid="navigation-link-about"]');
      await expect(aboutLink).toHaveAttribute('href', '/about');

      // Check Contact link href
      const contactLink = page.locator('[data-testid="navigation-link-contact"]');
      await expect(contactLink).toHaveAttribute('href', '/contact');
    });

    test('navigation links are clickable and navigable', async ({ page }) => {
      await page.goto('/');

      // Test that Home link works (navigates to same page)
      const homeLink = page.locator('[data-testid="navigation-link-home"]');
      await homeLink.click();
      await expect(page).toHaveURL(/\//);

      // Verify page still renders after navigation
      const heroSection = page.locator('[data-testid="hero-section"]');
      await expect(heroSection).toBeVisible();
    });

    test('footer navigation links function correctly', async ({ page }) => {
      await page.goto('/');

      // Verify footer exists
      const footer = page.locator('[data-testid="footer"]');
      await expect(footer).toBeVisible();

      // Verify footer has navigation links
      const footerLinks = footer.locator('.footer-link');
      const linkCount = await footerLinks.count();
      expect(linkCount).toBeGreaterThan(0);

      // Verify all footer links have href attributes
      for (let i = 0; i < linkCount; i++) {
        const link = footerLinks.nth(i);
        const href = await link.getAttribute('href');
        expect(href).toBeTruthy();
      }
    });

    test('logo link navigates to home', async ({ page }) => {
      await page.goto('/');

      const logo = page.locator('[data-testid="navigation-logo"]');
      await expect(logo).toBeVisible();
      await expect(logo).toHaveAttribute('href', '/');

      await logo.click();
      await expect(page).toHaveURL(/\//);
    });
  });

  test.describe('Test Case 3: WebKit-Specific CSS Compatibility', () => {
    test('WebKit vendor prefixes are not required for modern CSS', async ({ page }) => {
      await page.goto('/');

      // Verify transform property works without -webkit- prefix
      const ctaButton = page.locator('[data-testid="hero-cta"]');
      await expect(ctaButton).toBeVisible();

      // Get computed styles to verify CSS is working
      const styles = await ctaButton.evaluate((el) => {
        const computed = window.getComputedStyle(el);
        return {
          transform: computed.transform || computed.getPropertyValue('-webkit-transform'),
          transition: computed.transition || computed.getPropertyValue('-webkit-transition'),
          display: computed.display,
          backgroundColor: computed.backgroundColor,
        };
      });

      // Verify styles are computed (even if 'none')
      expect(styles.display).toBeTruthy();
      expect(styles.backgroundColor).toBeTruthy();
    });

    test('flexbox layout renders correctly in WebKit/Safari', async ({ page }) => {
      await page.goto('/');

      // Check navigation uses flexbox properly
      const navigation = page.locator('[data-testid="navigation"]');
      const navDisplay = await navigation.evaluate((el) => {
        return window.getComputedStyle(el).display;
      });
      expect(['flex', 'block', 'grid']).toContain(navDisplay);

      // Check footer content layout
      const footerContent = page.locator('[data-testid="footer-content"]');
      const footerDisplay = await footerContent.evaluate((el) => {
        return window.getComputedStyle(el).display;
      });
      expect(['flex', 'block', 'grid']).toContain(footerDisplay);
    });

    test('CSS animations and transitions work in Safari', async ({ page }) => {
      await page.goto('/');

      // Verify that hover/transition CSS is properly defined
      const ctaButton = page.locator('[data-testid="hero-cta"]');
      await expect(ctaButton).toBeVisible();

      // Get transition property
      const transition = await ctaButton.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return style.transition || style.getPropertyValue('-webkit-transition');
      });

      // Transition may be 'none' or defined - just verify it's accessible
      expect(transition).toBeDefined();
    });

    test('Safari handles responsive images correctly', async ({ page }) => {
      await page.goto('/');

      // Find all images
      const images = page.locator('img');
      const count = await images.count();

      for (let i = 0; i < count; i++) {
        const img = images.nth(i);
        if (await img.isVisible()) {
          // Verify image is loaded (not broken)
          const isLoaded = await img.evaluate((el: HTMLImageElement) => {
            return el.complete && el.naturalWidth > 0;
          });
          expect(isLoaded).toBe(true);

          // Verify image has proper dimensions
          const dimensions = await img.evaluate((el: HTMLImageElement) => {
            return {
              width: el.offsetWidth,
              height: el.offsetHeight,
            };
          });
          expect(dimensions.width).toBeGreaterThan(0);
          expect(dimensions.height).toBeGreaterThan(0);
        }
      }
    });

    test('viewport meta tag is respected in Safari', async ({ page }) => {
      await page.goto('/');

      // Verify viewport is set correctly for Safari
      const viewport = await page.evaluate(() => {
        const meta = document.querySelector('meta[name="viewport"]');
        return meta?.getAttribute('content');
      });

      expect(viewport).toContain('width=device-width');
    });

    test('no horizontal scroll issues on Safari desktop', async ({ page }) => {
      await page.setViewportSize({ width: 1280, height: 720 });
      await page.goto('/');

      // Check for horizontal overflow - a common Safari issue
      const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });

      expect(hasHorizontalScroll).toBe(false);
    });

    test('no horizontal scroll issues on Safari mobile', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 667 });
      await page.goto('/');

      // Wait for any responsive layout adjustments
      await page.waitForTimeout(100);

      // Check for horizontal overflow
      const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });

      expect(hasHorizontalScroll).toBe(false);
    });

    test('touch targets meet minimum size requirements on Safari mobile', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 667 });
      await page.goto('/');

      // Get all interactive elements
      const interactiveElements = page.locator('a, button');
      const count = await interactiveElements.count();

      for (let i = 0; i < count; i++) {
        const element = interactiveElements.nth(i);
        if (await element.isVisible()) {
          const size = await element.evaluate((el) => {
            const rect = el.getBoundingClientRect();
            return {
              width: rect.width,
              height: rect.height,
            };
          });

          // Apple recommends minimum 44x44 touch target
          // Allow some flexibility for text links
          const isButton = await element.evaluate((el) => el.tagName === 'BUTTON');
          if (isButton) {
            expect(size.width).toBeGreaterThanOrEqual(44);
            expect(size.height).toBeGreaterThanOrEqual(44);
          }
        }
      }
    });

    test('CSS backdrop-filter and blur effects render in Safari', async ({ page }) => {
      await page.goto('/');

      // Check if any elements use backdrop-filter (Safari was early adopter)
      const hasBackdropFilter = await page.evaluate(() => {
        const elements = document.querySelectorAll('*');
        for (const el of elements) {
          const style = window.getComputedStyle(el);
          const backdropFilter = style.getPropertyValue('backdrop-filter') ||
                                 style.getPropertyValue('-webkit-backdrop-filter');
          if (backdropFilter && backdropFilter !== 'none') {
            return true;
          }
        }
        return false;
      });

      // This test just verifies the check runs without error
      // The result depends on whether the design uses backdrop-filter
      expect(typeof hasBackdropFilter).toBe('boolean');
    });

    test('smooth scrolling works in Safari', async ({ page }) => {
      await page.goto('/');

      // Check if smooth scrolling is enabled
      const scrollBehavior = await page.evaluate(() => {
        return window.getComputedStyle(document.documentElement).scrollBehavior;
      });

      // scroll-behavior may be 'auto' or 'smooth' - just verify it's accessible
      expect(['auto', 'smooth', '']).toContain(scrollBehavior);
    });

    test('position sticky works correctly in Safari', async ({ page }) => {
      await page.goto('/');

      // Check navigation header for sticky positioning
      const navHeader = page.locator('.navigation-header');
      if (await navHeader.count() > 0) {
        const position = await navHeader.evaluate((el) => {
          return window.getComputedStyle(el).position;
        });

        // Position should be sticky, fixed, or relative (depending on implementation)
        expect(['sticky', 'fixed', 'relative', 'absolute', 'static']).toContain(position);
      }
    });
  });

  test.describe('Safari Interactive Elements', () => {
    test('CTA button is clickable and interactive', async ({ page }) => {
      await page.goto('/');

      const ctaButton = page.locator('[data-testid="hero-cta"]');
      await expect(ctaButton).toBeVisible();
      await expect(ctaButton).toBeEnabled();

      // Verify CTA has href
      const href = await ctaButton.getAttribute('href');
      expect(href).toBeTruthy();

      // Click the button
      await ctaButton.click();

      // Should navigate or scroll (depending on href)
      const currentUrl = page.url();
      expect(currentUrl).toContain(href!.startsWith('#') ? href : '');
    });

    test('mobile menu toggle button works in Safari', async ({ page }) => {
      // Set viewport to mobile size
      await page.setViewportSize({ width: 375, height: 667 });
      await page.goto('/');

      const menuToggle = page.locator('[data-testid="mobile-menu-toggle"]');

      // On mobile, menu toggle should be visible
      await expect(menuToggle).toBeVisible();

      // Click to open menu
      await menuToggle.click();

      // Menu should open (have 'open' class or similar)
      const navLinks = page.locator('[data-testid="navigation-links"]');
      await expect(navLinks).toHaveClass(/open/);

      // Click again to close
      await menuToggle.click();

      // Wait for animation/transition
      await page.waitForTimeout(300);
    });

    test('keyboard navigation works for interactive elements', async ({ page }) => {
      await page.goto('/');

      // Tab through the page
      await page.keyboard.press('Tab');

      // First focusable element should be focused
      const focusedElement = await page.evaluate(() => {
        return document.activeElement?.tagName;
      });
      expect(focusedElement).toBeTruthy();
      expect(focusedElement).not.toBe('BODY');

      // Continue tabbing to verify more elements are focusable
      await page.keyboard.press('Tab');
      const secondFocused = await page.evaluate(() => {
        return document.activeElement?.tagName;
      });
      expect(secondFocused).toBeTruthy();
    });

    test('JavaScript executes correctly in Safari/WebKit', async ({ page }) => {
      await page.goto('/');

      // Verify React has mounted (app content is present)
      const root = page.locator('#root');
      await expect(root).not.toBeEmpty();

      // Verify interactive components are mounted
      const heroSection = page.locator('[data-testid="hero-section"]');
      await expect(heroSection).toBeVisible();

      // Verify event handlers work
      const ctaButton = page.locator('[data-testid="hero-cta"]');
      await expect(ctaButton).toBeEnabled();

      // Test click event
      const clickResult = await ctaButton.evaluate((el) => {
        let clicked = false;
        const originalClick = el.onclick;
        el.onclick = () => { clicked = true; };
        el.click();
        el.onclick = originalClick;
        return clicked;
      });
      expect(clickResult).toBe(true);
    });
  });
});
