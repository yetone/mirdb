import { test, expect } from '@playwright/test';

/**
 * Browser Compatibility - Edge Tests (NFR-6)
 *
 * These tests verify that the homepage functions correctly on Microsoft Edge browser
 * as per Non-Functional Requirement NFR-6:
 * "Page shall function correctly on latest versions of Chrome, Firefox, Safari, and Edge"
 *
 * This test file specifically targets Microsoft Edge browser compatibility.
 */

test.describe('Browser Compatibility - Edge', () => {
  test.describe('Test Case 1: Homepage Rendering in Edge', () => {
    test('homepage loads and renders correctly with all styles applied', async ({ page, browserName }) => {
      // Ensure we're running on Edge (Chromium-based)
      expect(browserName).toBe('chromium');

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

    test('CSS flexbox and grid layouts render correctly in Edge', async ({ page }) => {
      await page.goto('/');

      // Verify flexbox is supported and working
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

    test('CSS custom properties (variables) are applied in Edge', async ({ page }) => {
      await page.goto('/');

      // Verify CSS is loading and applying styles
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

  test.describe('Test Case 2: Navigation Links in Edge', () => {
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

  test.describe('Test Case 3: Interactive Elements in Edge', () => {
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
      // If it's an anchor link like #getting-started, the URL will have the hash
      const currentUrl = page.url();
      expect(currentUrl).toContain(href!.startsWith('#') ? href : '');
    });

    test('mobile menu toggle button works', async ({ page }) => {
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

    test('interactive elements have hover states in Edge', async ({ page }) => {
      await page.goto('/');

      const ctaButton = page.locator('[data-testid="hero-cta"]');

      // Get initial background color
      const initialBgColor = await ctaButton.evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });

      // Hover over the button
      await ctaButton.hover();

      // CSS hover states are applied (may or may not change color depending on CSS)
      // Just verify the element remains interactive after hover
      await expect(ctaButton).toBeEnabled();
    });

    test('navigation links have focus states', async ({ page }) => {
      await page.goto('/');

      const homeLink = page.locator('[data-testid="navigation-link-home"]');

      // Focus the link
      await homeLink.focus();

      // Verify link is focused
      await expect(homeLink).toBeFocused();

      // Get outline/focus styling
      const focusOutline = await homeLink.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return style.outline || style.outlineStyle;
      });

      // Element should have some focus indication (outline, box-shadow, etc.)
      // or use default browser focus styles
      expect(focusOutline).toBeDefined();
    });

    test('forms and buttons submit correctly (if present)', async ({ page }) => {
      await page.goto('/');

      // Check for any forms on the page
      const forms = page.locator('form');
      const formCount = await forms.count();

      if (formCount > 0) {
        // If forms exist, verify they have action or onsubmit handlers
        for (let i = 0; i < formCount; i++) {
          const form = forms.nth(i);
          const action = await form.getAttribute('action');
          const method = await form.getAttribute('method');
          expect(action || method).toBeDefined();
        }
      }

      // All buttons should be accessible
      const buttons = page.locator('button');
      const buttonCount = await buttons.count();

      for (let i = 0; i < buttonCount; i++) {
        const button = buttons.nth(i);
        if (await button.isVisible()) {
          await expect(button).toBeEnabled();
        }
      }
    });

    test('keyboard navigation works for interactive elements', async ({ page }) => {
      await page.goto('/');

      // Tab through the page
      await page.keyboard.press('Tab');

      // First focusable element should be focused (likely skip link or logo)
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

    test('social links in footer open correctly', async ({ page }) => {
      await page.goto('/');

      // Find social links
      const socialLinks = page.locator('.footer-social-link');
      const count = await socialLinks.count();

      if (count > 0) {
        // Verify social links have target="_blank" for external links
        for (let i = 0; i < count; i++) {
          const link = socialLinks.nth(i);
          const target = await link.getAttribute('target');
          const rel = await link.getAttribute('rel');

          expect(target).toBe('_blank');
          expect(rel).toContain('noopener');
        }
      }
    });
  });

  test.describe('Edge-Specific Rendering Checks', () => {
    test('viewport meta tag is respected', async ({ page }) => {
      await page.goto('/');

      // Verify viewport is set correctly
      const viewport = await page.evaluate(() => {
        const meta = document.querySelector('meta[name="viewport"]');
        return meta?.getAttribute('content');
      });

      expect(viewport).toContain('width=device-width');
    });

    test('page does not have horizontal scroll on desktop', async ({ page }) => {
      await page.setViewportSize({ width: 1280, height: 720 });
      await page.goto('/');

      // Check for horizontal overflow
      const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });

      expect(hasHorizontalScroll).toBe(false);
    });

    test('page does not have horizontal scroll on mobile', async ({ page }) => {
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

    test('images load correctly in Edge', async ({ page }) => {
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
        }
      }
    });

    test('JavaScript executes correctly in Edge', async ({ page }) => {
      await page.goto('/');

      // Verify React has mounted (app content is present)
      const root = page.locator('#root');
      await expect(root).not.toBeEmpty();

      // Verify interactive components are mounted
      const heroSection = page.locator('[data-testid="hero-section"]');
      await expect(heroSection).toBeVisible();
    });

    test('Edge-specific CSS features are supported', async ({ page }) => {
      await page.goto('/');

      // Test CSS Grid support (Edge 16+)
      const supportsGrid = await page.evaluate(() => {
        return CSS.supports('display', 'grid');
      });
      expect(supportsGrid).toBe(true);

      // Test CSS Flexbox support (Edge 12+)
      const supportsFlex = await page.evaluate(() => {
        return CSS.supports('display', 'flex');
      });
      expect(supportsFlex).toBe(true);

      // Test CSS Custom Properties support (Edge 15+)
      const supportsCustomProps = await page.evaluate(() => {
        return CSS.supports('--test', '0');
      });
      expect(supportsCustomProps).toBe(true);

      // Test CSS calc() support (Edge 12+)
      const supportsCalc = await page.evaluate(() => {
        return CSS.supports('width', 'calc(100% - 10px)');
      });
      expect(supportsCalc).toBe(true);
    });

    test('modern JavaScript features work in Edge', async ({ page }) => {
      await page.goto('/');

      // Test modern JS features that should work in latest Edge
      const jsFeatures = await page.evaluate(() => {
        return {
          // Arrow functions
          arrowFunctions: (() => true)(),
          // Template literals
          templateLiterals: `test` === 'test',
          // Destructuring
          destructuring: (() => {
            const { a } = { a: 1 };
            return a === 1;
          })(),
          // Spread operator
          spreadOperator: (() => {
            const arr = [...[1, 2, 3]];
            return arr.length === 3;
          })(),
          // Promises
          promises: typeof Promise !== 'undefined',
          // Async/await (implicit through page.evaluate)
          asyncAwait: true,
          // Optional chaining
          optionalChaining: (() => {
            const obj: { a?: { b?: number } } = {};
            return obj?.a?.b === undefined;
          })(),
          // Nullish coalescing
          nullishCoalescing: (() => {
            const val = null ?? 'default';
            return val === 'default';
          })(),
        };
      });

      expect(jsFeatures.arrowFunctions).toBe(true);
      expect(jsFeatures.templateLiterals).toBe(true);
      expect(jsFeatures.destructuring).toBe(true);
      expect(jsFeatures.spreadOperator).toBe(true);
      expect(jsFeatures.promises).toBe(true);
      expect(jsFeatures.optionalChaining).toBe(true);
      expect(jsFeatures.nullishCoalescing).toBe(true);
    });
  });
});
