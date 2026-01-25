/**
 * E2E tests for Browser Compatibility - Chrome
 * Scenario 20 - Browser Compatibility Testing
 *
 * Tests that the homepage works correctly in Google Chrome browser:
 * - All sections render correctly with proper styling
 * - All links and buttons function correctly
 * - Theme switching works with smooth transitions
 */

import { test, expect, waitForHeroSection, viewports } from './fixtures';

// Browser compatibility tests use the chromium project defined in playwright.config.ts
// This file focuses on Chrome-specific compatibility testing

test.describe('Browser Compatibility - Chrome', () => {
  test.describe('Test Case 1: All sections render correctly with proper styling', () => {
    test('should render Hero section correctly in Chrome', async ({ page }) => {
      await page.goto('/');
      await waitForHeroSection(page);

      // Verify hero section is visible
      const heroSection = page.locator('.hero');
      await expect(heroSection).toBeVisible();

      // Verify headline is present and styled
      const headline = page.locator('h1');
      await expect(headline).toBeVisible();
      await expect(headline).toContainText('Shorten URLs. Track Every Click.');

      // Verify headline has proper text color (not transparent/invisible)
      const headlineStyles = await headline.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          color: styles.color,
          display: styles.display,
          visibility: styles.visibility,
        };
      });
      expect(headlineStyles.visibility).toBe('visible');
      expect(headlineStyles.display).not.toBe('none');

      // Verify subheadline is visible
      const subheadline = page.locator('text=Transform long, unwieldy URLs');
      await expect(subheadline).toBeVisible();

      // Verify CTA button is visible and properly styled
      const ctaButton = page.getByTestId('hero-cta');
      await expect(ctaButton).toBeVisible();
      const ctaStyles = await ctaButton.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          cursor: styles.cursor,
          display: styles.display,
        };
      });
      expect(ctaStyles.cursor).toBe('pointer');
    });

    test('should render Features section correctly in Chrome', async ({ page }) => {
      await page.goto('/');
      await waitForHeroSection(page);

      // Scroll to features section
      const featuresSection = page.getByTestId('features-section');
      await featuresSection.scrollIntoViewIfNeeded();
      await expect(featuresSection).toBeVisible();

      // Verify section heading
      const featuresHeading = page.locator('h2:has-text("Features")');
      await expect(featuresHeading).toBeVisible();

      // Verify all feature cards are present
      await expect(page.getByTestId('feature-card-url-shortening')).toBeVisible();
      await expect(page.getByTestId('feature-card-click-analytics')).toBeVisible();
      await expect(page.getByTestId('feature-card-geographic-insights')).toBeVisible();
      await expect(page.getByTestId('feature-card-shareable-stats')).toBeVisible();

      // Verify cards have proper layout (grid)
      const gridStyles = await featuresSection.evaluate((el) => {
        const grid = el.querySelector('.grid');
        if (!grid) return null;
        const styles = window.getComputedStyle(grid);
        return {
          display: styles.display,
        };
      });
      expect(gridStyles?.display).toBe('grid');
    });

    test('should render How It Works section correctly in Chrome', async ({ page }) => {
      await page.goto('/');
      await waitForHeroSection(page);

      // Scroll to How It Works section
      const howItWorksSection = page.getByTestId('how-it-works-section');
      await howItWorksSection.scrollIntoViewIfNeeded();
      await expect(howItWorksSection).toBeVisible();

      // Verify section heading
      const howItWorksHeading = page.locator('h2:has-text("How It Works")');
      await expect(howItWorksHeading).toBeVisible();

      // Verify all steps are present
      await expect(page.getByTestId('step-1')).toBeVisible();
      await expect(page.getByTestId('step-2')).toBeVisible();
      await expect(page.getByTestId('step-3')).toBeVisible();

      // Verify steps have proper content
      const step1 = page.getByTestId('step-1');
      await expect(step1).toContainText('Paste');

      const step2 = page.getByTestId('step-2');
      await expect(step2).toContainText('Share');

      const step3 = page.getByTestId('step-3');
      await expect(step3).toContainText(/Analytics|Watch/i);
    });

    test('should render Dashboard Preview section correctly in Chrome', async ({ page }) => {
      await page.goto('/');
      await waitForHeroSection(page);

      // Scroll to Dashboard Preview section
      const dashboardPreview = page.getByTestId('dashboard-preview-section');
      await dashboardPreview.scrollIntoViewIfNeeded();
      await expect(dashboardPreview).toBeVisible();

      // Verify preview image or mockup is visible
      const previewImage = dashboardPreview.locator('img, svg');
      await expect(previewImage.first()).toBeVisible();

      // Verify CTA is present
      const dashboardCta = page.getByTestId('dashboard-preview-cta');
      await expect(dashboardCta).toBeVisible();
    });

    test('should render Footer section correctly in Chrome', async ({ page }) => {
      await page.goto('/');
      await waitForHeroSection(page);

      // Scroll to footer
      const footer = page.getByTestId('footer');
      await footer.scrollIntoViewIfNeeded();
      await expect(footer).toBeVisible();

      // Verify copyright is present
      const copyright = page.getByTestId('footer-copyright');
      await expect(copyright).toBeVisible();
      await expect(copyright).toContainText(/\d{4}/); // Contains year

      // Verify footer has proper structure
      const footerTag = page.locator('footer');
      await expect(footerTag).toBeVisible();
    });

    test('should render Navbar correctly in Chrome', async ({ page }) => {
      await page.goto('/');
      await waitForHeroSection(page);

      // Verify main navbar is visible (the fixed top nav bar, not the footer nav)
      const navbar = page.locator('nav.navbar');
      await expect(navbar).toBeVisible();

      // Verify theme toggle is present
      const themeToggle = page.getByRole('button', { name: /switch to (dark|light) mode/i });
      await expect(themeToggle).toBeVisible();

      // Verify login/register buttons or links are present in the main navbar
      const authElements = navbar.locator('a, button');
      const count = await authElements.count();
      expect(count).toBeGreaterThan(0);
    });
  });

  test.describe('Test Case 2: All links and buttons function correctly', () => {
    test('should navigate to registration page when clicking Get Started CTA in Chrome', async ({
      page,
    }) => {
      await page.goto('/');
      await waitForHeroSection(page);

      // Click the primary CTA
      const ctaButton = page.getByTestId('hero-cta');
      await expect(ctaButton).toBeVisible();
      await ctaButton.click();

      // Verify navigation to register page
      await expect(page).toHaveURL(/\/register/);
    });

    test('should navigate to login page when clicking login link in Chrome', async ({ page }) => {
      await page.goto('/');
      await waitForHeroSection(page);

      // Click the login link in hero section
      const loginLink = page.getByTestId('hero-login-link');
      await expect(loginLink).toBeVisible();
      await loginLink.click();

      // Verify navigation to login page
      await expect(page).toHaveURL(/\/login/);
    });

    test('should navigate to registration from Dashboard Preview CTA in Chrome', async ({
      page,
    }) => {
      await page.goto('/');
      await waitForHeroSection(page);

      // Scroll to Dashboard Preview section
      const dashboardPreview = page.getByTestId('dashboard-preview-section');
      await dashboardPreview.scrollIntoViewIfNeeded();

      // Click the CTA
      const dashboardCta = page.getByTestId('dashboard-preview-cta');
      await expect(dashboardCta).toBeVisible();
      await dashboardCta.click();

      // Verify navigation to register page
      await expect(page).toHaveURL(/\/register/);
    });

    test('should have all buttons clickable and responsive in Chrome', async ({ page }) => {
      await page.goto('/');
      await waitForHeroSection(page);

      // Test hero CTA button hover state
      const ctaButton = page.getByTestId('hero-cta');
      await ctaButton.hover();

      // Verify button has pointer cursor on hover
      const cursorStyle = await ctaButton.evaluate((el) => {
        return window.getComputedStyle(el).cursor;
      });
      expect(cursorStyle).toBe('pointer');

      // Verify button is enabled and clickable
      await expect(ctaButton).toBeEnabled();
    });

    test('should scroll page smoothly when navigating to sections in Chrome', async ({ page }) => {
      await page.goto('/');
      await waitForHeroSection(page);

      // Get initial scroll position
      const initialScrollY = await page.evaluate(() => window.scrollY);

      // Scroll to features section
      const featuresSection = page.getByTestId('features-section');
      await featuresSection.scrollIntoViewIfNeeded();

      // Wait a moment for scroll animation
      await page.waitForTimeout(500);

      // Verify scroll position changed
      const newScrollY = await page.evaluate(() => window.scrollY);
      expect(newScrollY).toBeGreaterThan(initialScrollY);
    });
  });

  test.describe('Test Case 3: Theme switching works with smooth transitions', () => {
    test('should toggle from light to dark theme in Chrome', async ({ page }) => {
      // Set initial light theme
      await page.goto('/');
      await page.evaluate(() => {
        localStorage.setItem('theme', 'light');
      });
      await page.reload();
      await waitForHeroSection(page);

      // Verify initial theme is light
      const initialTheme = await page.evaluate(() =>
        document.documentElement.getAttribute('data-theme')
      );
      expect(initialTheme).toBe('light');

      // Find and click the theme toggle
      const themeToggle = page.getByRole('button', { name: /switch to dark mode/i });
      await expect(themeToggle).toBeVisible();
      await themeToggle.click();

      // Verify theme changed to dark
      const newTheme = await page.evaluate(() =>
        document.documentElement.getAttribute('data-theme')
      );
      expect(newTheme).toBe('dark');

      // Verify localStorage was updated
      const storedTheme = await page.evaluate(() => localStorage.getItem('theme'));
      expect(storedTheme).toBe('dark');
    });

    test('should toggle from dark to light theme in Chrome', async ({ page }) => {
      // Set initial dark theme
      await page.goto('/');
      await page.evaluate(() => {
        localStorage.setItem('theme', 'dark');
      });
      await page.reload();
      await waitForHeroSection(page);

      // Verify initial theme is dark
      const initialTheme = await page.evaluate(() =>
        document.documentElement.getAttribute('data-theme')
      );
      expect(initialTheme).toBe('dark');

      // Find and click the theme toggle
      const themeToggle = page.getByRole('button', { name: /switch to light mode/i });
      await expect(themeToggle).toBeVisible();
      await themeToggle.click();

      // Verify theme changed to light
      const newTheme = await page.evaluate(() =>
        document.documentElement.getAttribute('data-theme')
      );
      expect(newTheme).toBe('light');
    });

    test('should have CSS transition properties on theme change in Chrome', async ({ page }) => {
      await page.goto('/');
      await page.evaluate(() => {
        localStorage.setItem('theme', 'light');
      });
      await page.reload();
      await waitForHeroSection(page);

      // Check for transition property on body or root element
      const hasTransition = await page.evaluate(() => {
        const body = document.body;
        const styles = window.getComputedStyle(body);
        const transitionDuration = styles.transitionDuration;
        // Transitions can be on any element, check if any transition is defined
        return transitionDuration !== '0s' || document.querySelector('[class*="transition"]') !== null;
      });

      // Either has explicit transition or uses Tailwind transition classes
      const hasTransitionClass = await page.evaluate(() => {
        return document.querySelector('[class*="transition"]') !== null;
      });

      expect(hasTransition || hasTransitionClass).toBe(true);
    });

    test('should maintain all section visibility after theme toggle in Chrome', async ({
      page,
    }) => {
      await page.goto('/');
      await page.evaluate(() => {
        localStorage.setItem('theme', 'light');
      });
      await page.reload();
      await waitForHeroSection(page);

      // Verify all sections visible in light mode
      await expect(page.locator('.hero')).toBeVisible();
      await expect(page.getByTestId('features-section')).toBeVisible();
      await expect(page.getByTestId('how-it-works-section')).toBeVisible();
      await expect(page.getByTestId('footer')).toBeVisible();

      // Toggle to dark mode
      const themeToggle = page.getByRole('button', { name: /switch to dark mode/i });
      await themeToggle.click();

      // Verify all sections still visible in dark mode
      await expect(page.locator('.hero')).toBeVisible();
      await expect(page.getByTestId('features-section')).toBeVisible();
      await expect(page.getByTestId('how-it-works-section')).toBeVisible();
      await expect(page.getByTestId('footer')).toBeVisible();
    });

    test('should persist theme preference across page reloads in Chrome', async ({ page }) => {
      await page.goto('/');
      await page.evaluate(() => {
        localStorage.setItem('theme', 'light');
      });
      await page.reload();
      await waitForHeroSection(page);

      // Toggle to dark mode
      const themeToggle = page.getByRole('button', { name: /switch to dark mode/i });
      await themeToggle.click();

      // Reload the page
      await page.reload();
      await waitForHeroSection(page);

      // Verify dark theme persisted
      const theme = await page.evaluate(() =>
        document.documentElement.getAttribute('data-theme')
      );
      expect(theme).toBe('dark');
    });

    test('should show correct toggle icon based on current theme in Chrome', async ({ page }) => {
      // Start with light theme
      await page.goto('/');
      await page.evaluate(() => {
        localStorage.setItem('theme', 'light');
      });
      await page.reload();
      await waitForHeroSection(page);

      // In light mode, should have toggle to switch to dark (moon icon expected)
      let themeToggle = page.getByRole('button', { name: /switch to dark mode/i });
      await expect(themeToggle).toBeVisible();
      let svg = themeToggle.locator('svg');
      await expect(svg).toBeVisible();

      // Toggle to dark
      await themeToggle.click();

      // In dark mode, should have toggle to switch to light (sun icon expected)
      themeToggle = page.getByRole('button', { name: /switch to light mode/i });
      await expect(themeToggle).toBeVisible();
      svg = themeToggle.locator('svg');
      await expect(svg).toBeVisible();
    });
  });

  test.describe('Additional Chrome-specific tests', () => {
    test('should not have any console errors on page load in Chrome', async ({ page }) => {
      const consoleErrors: string[] = [];
      page.on('console', (msg) => {
        if (msg.type() === 'error') {
          consoleErrors.push(msg.text());
        }
      });

      await page.goto('/');
      await waitForHeroSection(page);

      // Allow some time for any async errors
      await page.waitForTimeout(500);

      // Filter out known non-critical errors (like third-party script issues)
      const criticalErrors = consoleErrors.filter(
        (error) =>
          !error.includes('favicon') &&
          !error.includes('net::ERR_BLOCKED_BY_CLIENT') &&
          !error.includes('Failed to load resource')
      );

      expect(criticalErrors).toHaveLength(0);
    });

    test('should render images without errors in Chrome', async ({ page }) => {
      await page.goto('/');
      await waitForHeroSection(page);

      // Find all images on the page
      const images = page.locator('img');
      const imageCount = await images.count();

      // Check each image loaded successfully
      for (let i = 0; i < imageCount; i++) {
        const img = images.nth(i);
        const isVisible = await img.isVisible();
        if (isVisible) {
          const naturalWidth = await img.evaluate((el: HTMLImageElement) => el.naturalWidth);
          // Image should have loaded (naturalWidth > 0) or be an SVG placeholder
          const src = await img.getAttribute('src');
          if (src && !src.includes('data:')) {
            expect(naturalWidth).toBeGreaterThan(0);
          }
        }
      }
    });

    test('should have proper viewport meta tag for Chrome mobile in Chrome', async ({ page }) => {
      await page.goto('/');

      // Check for viewport meta tag
      const viewportMeta = await page.locator('meta[name="viewport"]').getAttribute('content');
      expect(viewportMeta).toContain('width=device-width');
    });

    test('should support CSS Grid and Flexbox layouts in Chrome', async ({ page }) => {
      await page.goto('/');
      await waitForHeroSection(page);

      // Verify grid layout in features section
      const featuresSection = page.getByTestId('features-section');
      await featuresSection.scrollIntoViewIfNeeded();

      const gridDisplay = await featuresSection.evaluate((el) => {
        const grid = el.querySelector('.grid');
        return grid ? window.getComputedStyle(grid).display : null;
      });
      expect(gridDisplay).toBe('grid');

      // Verify flexbox is supported in hero section
      const heroFlex = await page.evaluate(() => {
        const flexContainers = document.querySelectorAll('.flex');
        if (flexContainers.length === 0) return null;
        return window.getComputedStyle(flexContainers[0]).display;
      });
      expect(heroFlex).toBe('flex');
    });
  });
});
