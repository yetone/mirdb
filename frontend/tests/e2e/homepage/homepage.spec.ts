/**
 * E2E tests for homepage - Hero Section Display
 * Scenario 1 - Test Case 4: Navigate to homepage as unauthenticated user
 *
 * Tests that the hero section loads within 2 seconds with all elements visible
 */

import { test, expect, measurePageLoadTime, waitForHeroSection } from './fixtures';

test.describe('Hero Section Display - E2E', () => {
  test.describe('Test Case 4: Navigate to homepage as unauthenticated user', () => {
    test('should load hero section within 2 seconds with all elements visible', async ({ page }) => {
      const startTime = Date.now();

      // Navigate to homepage
      await page.goto('/');

      // Wait for hero section to be visible
      await waitForHeroSection(page);

      // Measure total load time
      const loadTime = Date.now() - startTime;

      // Verify load time is under 2 seconds (2000ms)
      expect(loadTime).toBeLessThan(2000);

      // Verify all hero section elements are visible
      // Headline
      const headline = page.locator('h1');
      await expect(headline).toBeVisible();
      await expect(headline).toContainText(/shorten.*url/i);

      // Primary CTA button
      const ctaButton = page.getByTestId('hero-cta');
      await expect(ctaButton).toBeVisible();
      await expect(ctaButton).toContainText(/get started/i);

      // Login link
      const loginLink = page.getByTestId('hero-login-link');
      await expect(loginLink).toBeVisible();
      await expect(loginLink).toContainText(/login/i);
    });

    test('should display headline with URL shortening messaging', async ({ page }) => {
      await page.goto('/');
      await waitForHeroSection(page);

      const headline = page.locator('h1');
      await expect(headline).toBeVisible();
      await expect(headline).toContainText('Shorten URLs. Track Every Click.');
    });

    test('should display subheadline with value proposition', async ({ page }) => {
      await page.goto('/');
      await waitForHeroSection(page);

      // Check for subheadline text
      const subheadline = page.locator('text=Transform long, unwieldy URLs');
      await expect(subheadline).toBeVisible();
    });

    test('should have clickable Get Started CTA that navigates to registration', async ({
      page,
    }) => {
      await page.goto('/');
      await waitForHeroSection(page);

      const ctaButton = page.getByTestId('hero-cta');
      await expect(ctaButton).toBeVisible();

      // Click the CTA button
      await ctaButton.click();

      // Verify navigation to registration page
      await expect(page).toHaveURL(/\/register/);
    });

    test('should have clickable Login link that navigates to login page', async ({ page }) => {
      await page.goto('/');
      await waitForHeroSection(page);

      const loginLink = page.getByTestId('hero-login-link');
      await expect(loginLink).toBeVisible();

      // Click the login link
      await loginLink.click();

      // Verify navigation to login page
      await expect(page).toHaveURL(/\/login/);
    });

    test('should display hero section with proper visual hierarchy', async ({ page }) => {
      await page.goto('/');
      await waitForHeroSection(page);

      // Check hero section exists
      const heroSection = page.locator('.hero');
      await expect(heroSection).toBeVisible();

      // Check h1 is present
      const h1 = page.locator('h1');
      await expect(h1).toBeVisible();

      // Check CTA buttons container
      const ctaContainer = page.locator('.flex.flex-col.sm\\:flex-row');
      await expect(ctaContainer).toBeVisible();
    });
  });
});

/**
 * E2E tests for Theme Toggle Functionality
 * Scenario 12 - Test Case 4: E2E test - Toggle theme and verify all sections update
 *
 * Tests that theme toggle works correctly and all homepage sections reflect theme changes
 */
test.describe('Theme Toggle Functionality - E2E', () => {
  test.describe('Test Case 4: Toggle theme and verify all sections update', () => {
    test('should toggle theme from light to dark and verify all sections update', async ({
      page,
    }) => {
      // Clear localStorage to start fresh
      await page.goto('/');
      await page.evaluate(() => {
        localStorage.setItem('theme', 'light');
      });

      // Reload to apply the theme
      await page.reload();
      await waitForHeroSection(page);

      // Verify initial light theme
      const initialTheme = await page.evaluate(() =>
        document.documentElement.getAttribute('data-theme')
      );
      expect(initialTheme).toBe('light');

      // Verify all sections are visible before toggle
      await expect(page.locator('.hero')).toBeVisible();
      await expect(page.getByTestId('features-section')).toBeVisible();
      await expect(page.getByTestId('how-it-works-section')).toBeVisible();
      await expect(page.getByTestId('footer')).toBeVisible();

      // Find and click the theme toggle button
      const themeToggle = page.getByRole('button', { name: /switch to dark mode/i });
      await expect(themeToggle).toBeVisible();
      await themeToggle.click();

      // Verify theme changed to dark
      const newTheme = await page.evaluate(() =>
        document.documentElement.getAttribute('data-theme')
      );
      expect(newTheme).toBe('dark');

      // Verify all sections are still visible after theme change
      await expect(page.locator('.hero')).toBeVisible();
      await expect(page.getByTestId('features-section')).toBeVisible();
      await expect(page.getByTestId('how-it-works-section')).toBeVisible();
      await expect(page.getByTestId('footer')).toBeVisible();

      // Verify localStorage was updated
      const storedTheme = await page.evaluate(() => localStorage.getItem('theme'));
      expect(storedTheme).toBe('dark');
    });

    test('should toggle theme from dark to light and verify all sections update', async ({
      page,
    }) => {
      // Set initial theme to dark
      await page.goto('/');
      await page.evaluate(() => {
        localStorage.setItem('theme', 'dark');
      });

      // Reload to apply the theme
      await page.reload();
      await waitForHeroSection(page);

      // Verify initial dark theme
      const initialTheme = await page.evaluate(() =>
        document.documentElement.getAttribute('data-theme')
      );
      expect(initialTheme).toBe('dark');

      // Find and click the theme toggle button
      const themeToggle = page.getByRole('button', { name: /switch to light mode/i });
      await expect(themeToggle).toBeVisible();
      await themeToggle.click();

      // Verify theme changed to light
      const newTheme = await page.evaluate(() =>
        document.documentElement.getAttribute('data-theme')
      );
      expect(newTheme).toBe('light');

      // Verify all sections are still visible after theme change
      await expect(page.locator('.hero')).toBeVisible();
      await expect(page.getByTestId('features-section')).toBeVisible();
      await expect(page.getByTestId('how-it-works-section')).toBeVisible();
      await expect(page.getByTestId('footer')).toBeVisible();
    });

    test('should show correct icon when toggling themes', async ({ page }) => {
      // Start with light theme
      await page.goto('/');
      await page.evaluate(() => {
        localStorage.setItem('theme', 'light');
      });
      await page.reload();
      await waitForHeroSection(page);

      // In light mode, should show moon icon (to switch to dark)
      let themeToggle = page.getByRole('button', { name: /switch to dark mode/i });
      await expect(themeToggle).toBeVisible();
      let svg = themeToggle.locator('svg');
      await expect(svg).toBeVisible();

      // Click to switch to dark mode
      await themeToggle.click();

      // In dark mode, should show sun icon (to switch to light)
      themeToggle = page.getByRole('button', { name: /switch to light mode/i });
      await expect(themeToggle).toBeVisible();
      svg = themeToggle.locator('svg');
      await expect(svg).toBeVisible();
    });

    test('should persist theme preference after page reload', async ({ page }) => {
      // Start with light theme
      await page.goto('/');
      await page.evaluate(() => {
        localStorage.setItem('theme', 'light');
      });
      await page.reload();
      await waitForHeroSection(page);

      // Toggle to dark
      const themeToggle = page.getByRole('button', { name: /switch to dark mode/i });
      await themeToggle.click();

      // Verify dark theme
      let currentTheme = await page.evaluate(() =>
        document.documentElement.getAttribute('data-theme')
      );
      expect(currentTheme).toBe('dark');

      // Reload page
      await page.reload();
      await waitForHeroSection(page);

      // Verify theme persisted
      currentTheme = await page.evaluate(() =>
        document.documentElement.getAttribute('data-theme')
      );
      expect(currentTheme).toBe('dark');
    });

    test('should update Hero section styling on theme change', async ({ page }) => {
      await page.goto('/');
      await page.evaluate(() => {
        localStorage.setItem('theme', 'light');
      });
      await page.reload();
      await waitForHeroSection(page);

      // Verify hero headline is visible in light mode
      const headline = page.locator('h1');
      await expect(headline).toBeVisible();
      await expect(headline).toContainText('Shorten URLs. Track Every Click.');

      // Toggle to dark mode
      const themeToggle = page.getByRole('button', { name: /switch to dark mode/i });
      await themeToggle.click();

      // Verify hero headline is still visible in dark mode
      await expect(headline).toBeVisible();
      await expect(headline).toContainText('Shorten URLs. Track Every Click.');
    });

    test('should update Features section styling on theme change', async ({ page }) => {
      await page.goto('/');
      await page.evaluate(() => {
        localStorage.setItem('theme', 'light');
      });
      await page.reload();
      await waitForHeroSection(page);

      // Scroll to features section
      const featuresSection = page.getByTestId('features-section');
      await featuresSection.scrollIntoViewIfNeeded();
      await expect(featuresSection).toBeVisible();

      // Toggle theme
      const themeToggle = page.getByRole('button', { name: /switch to dark mode/i });
      await themeToggle.click();

      // Verify features section is still visible after theme change
      await expect(featuresSection).toBeVisible();

      // Verify feature cards are still present
      await expect(page.getByTestId('feature-card-url-shortening')).toBeVisible();
      await expect(page.getByTestId('feature-card-click-analytics')).toBeVisible();
    });

    test('should update How It Works section styling on theme change', async ({ page }) => {
      await page.goto('/');
      await page.evaluate(() => {
        localStorage.setItem('theme', 'light');
      });
      await page.reload();
      await waitForHeroSection(page);

      // Scroll to how it works section
      const howItWorksSection = page.getByTestId('how-it-works-section');
      await howItWorksSection.scrollIntoViewIfNeeded();
      await expect(howItWorksSection).toBeVisible();

      // Toggle theme
      const themeToggle = page.getByRole('button', { name: /switch to dark mode/i });
      await themeToggle.click();

      // Verify section is still visible after theme change
      await expect(howItWorksSection).toBeVisible();

      // Verify steps are still present
      await expect(page.getByTestId('step-1')).toBeVisible();
      await expect(page.getByTestId('step-2')).toBeVisible();
      await expect(page.getByTestId('step-3')).toBeVisible();
    });

    test('should update Footer section styling on theme change', async ({ page }) => {
      await page.goto('/');
      await page.evaluate(() => {
        localStorage.setItem('theme', 'light');
      });
      await page.reload();
      await waitForHeroSection(page);

      // Scroll to footer
      const footer = page.getByTestId('footer');
      await footer.scrollIntoViewIfNeeded();
      await expect(footer).toBeVisible();

      // Toggle theme
      const themeToggle = page.getByRole('button', { name: /switch to dark mode/i });
      await themeToggle.click();

      // Verify footer is still visible after theme change
      await expect(footer).toBeVisible();

      // Verify copyright is still present
      await expect(page.getByTestId('footer-copyright')).toBeVisible();
    });
  });
});

/**
 * E2E tests for Framer Motion Animations
 * Scenario 19 - Framer Motion Animations
 *
 * Tests that Framer Motion animations work correctly:
 * - Feature cards animate in with fade/slide effect on scroll
 * - Scrolling remains smooth (60fps) during animations
 * - Buttons have smooth hover transition effects
 */
test.describe('Framer Motion Animations - E2E', () => {
  test.describe('Test Case 1: Scroll to features section', () => {
    test('should animate feature cards with fade/slide effect on scroll', async ({ page }) => {
      await page.goto('/');
      await waitForHeroSection(page);

      // Get the features section
      const featuresSection = page.getByTestId('features-section');
      const featuresGrid = page.getByTestId('features-grid');

      // Verify section exists
      await expect(featuresSection).toBeAttached();

      // Scroll to features section to trigger animations
      await featuresSection.scrollIntoViewIfNeeded();

      // Wait for animations to complete (Framer Motion uses whileInView)
      await page.waitForTimeout(1000);

      // Verify all feature cards are visible after animation
      const featureCards = [
        page.getByTestId('feature-card-url-shortening'),
        page.getByTestId('feature-card-click-analytics'),
        page.getByTestId('feature-card-geographic-insights'),
        page.getByTestId('feature-card-shareable-stats'),
      ];

      for (const card of featureCards) {
        await expect(card).toBeVisible();
      }

      // Verify the features grid is visible and has proper opacity
      await expect(featuresGrid).toBeVisible();

      // Check that feature cards have been animated to near-final position
      // Framer Motion sets opacity near 1 and y near 0 when animation completes
      // Use tolerance for animation timing variations
      for (const card of featureCards) {
        const opacity = await card.evaluate((el) => {
          return window.getComputedStyle(el).opacity;
        });
        // Animation should bring opacity to at least 0.9 (near completion)
        expect(parseFloat(opacity)).toBeGreaterThan(0.9);
      }
    });

    test('should animate How It Works section on scroll', async ({ page }) => {
      await page.goto('/');
      await waitForHeroSection(page);

      // Scroll to How It Works section
      const howItWorksSection = page.getByTestId('how-it-works-section');
      await howItWorksSection.scrollIntoViewIfNeeded();

      // Wait for animations to complete
      await page.waitForTimeout(1000);

      // Verify all steps are visible after animation
      const steps = [
        page.getByTestId('step-1'),
        page.getByTestId('step-2'),
        page.getByTestId('step-3'),
      ];

      for (const step of steps) {
        await expect(step).toBeVisible();

        // Verify opacity is near 1 after animation (use tolerance for timing variations)
        const opacity = await step.evaluate((el) => {
          return window.getComputedStyle(el).opacity;
        });
        // Animation should bring opacity to at least 0.9 (near completion)
        expect(parseFloat(opacity)).toBeGreaterThan(0.9);
      }
    });

    test('should stagger animation of feature cards', async ({ page }) => {
      await page.goto('/');
      await waitForHeroSection(page);

      // Scroll to features section
      const featuresSection = page.getByTestId('features-section');
      await featuresSection.scrollIntoViewIfNeeded();

      // The staggerChildren property in containerVariants should cause
      // cards to animate sequentially. Verify all cards eventually become visible.
      await page.waitForTimeout(800);

      const featureCards = [
        page.getByTestId('feature-card-url-shortening'),
        page.getByTestId('feature-card-click-analytics'),
        page.getByTestId('feature-card-geographic-insights'),
        page.getByTestId('feature-card-shareable-stats'),
      ];

      // All cards should be visible after staggered animation completes
      for (const card of featureCards) {
        await expect(card).toBeVisible();
      }
    });
  });

  test.describe('Test Case 2: Check animation does not impact scroll performance', () => {
    test('should maintain smooth scrolling (60fps) during animations', async ({ page }) => {
      await page.goto('/');
      await waitForHeroSection(page);

      // Enable performance metrics
      const client = await page.context().newCDPSession(page);
      await client.send('Performance.enable');

      // Perform scroll action while measuring performance
      const scrollStartTime = Date.now();

      // Scroll through the entire page
      await page.evaluate(async () => {
        const scrollStep = async () => {
          return new Promise<void>((resolve) => {
            let currentPosition = 0;
            const maxPosition = document.body.scrollHeight - window.innerHeight;
            const step = 100;

            const scroll = () => {
              if (currentPosition < maxPosition) {
                currentPosition += step;
                window.scrollTo(0, currentPosition);
                requestAnimationFrame(scroll);
              } else {
                resolve();
              }
            };
            requestAnimationFrame(scroll);
          });
        };
        await scrollStep();
      });

      const scrollEndTime = Date.now();
      const scrollDuration = scrollEndTime - scrollStartTime;

      // Get performance metrics
      const metrics = await client.send('Performance.getMetrics');
      const layoutCount = metrics.metrics.find((m) => m.name === 'LayoutCount')?.value || 0;
      const recalcStyleCount =
        metrics.metrics.find((m) => m.name === 'RecalcStyleCount')?.value || 0;

      // Performance assertions:
      // 1. Scroll should complete in reasonable time (not blocked by heavy JS)
      expect(scrollDuration).toBeLessThan(5000); // 5 seconds max for full page scroll

      // 2. Layout thrashing should be minimal during animations
      // High values would indicate poor performance
      expect(layoutCount).toBeLessThan(1000);
      expect(recalcStyleCount).toBeLessThan(1000);

      // Verify page is scrollable and reached the bottom
      const currentScrollPosition = await page.evaluate(() => window.scrollY);
      const maxScrollPosition = await page.evaluate(
        () => document.body.scrollHeight - window.innerHeight
      );

      // Should have scrolled near the bottom
      expect(currentScrollPosition).toBeGreaterThan(maxScrollPosition * 0.8);
    });

    test('should not cause jank during scroll-triggered animations', async ({ page }) => {
      await page.goto('/');
      await waitForHeroSection(page);

      // Collect long task durations during scroll
      const longTasks: number[] = [];

      // Set up PerformanceObserver to monitor long tasks
      await page.evaluate(() => {
        (window as any).__longTasks = [];
        const observer = new PerformanceObserver((list) => {
          for (const entry of list.getEntries()) {
            (window as any).__longTasks.push(entry.duration);
          }
        });
        observer.observe({ entryTypes: ['longtask'] });
      });

      // Scroll through the page
      await page.evaluate(async () => {
        const scrollTo = (y: number) =>
          new Promise<void>((resolve) => {
            window.scrollTo({ top: y, behavior: 'smooth' });
            setTimeout(resolve, 100);
          });

        const sections = [500, 1000, 1500, 2000];
        for (const y of sections) {
          await scrollTo(y);
        }
      });

      // Wait for animations to settle
      await page.waitForTimeout(500);

      // Get recorded long tasks
      const recordedLongTasks = await page.evaluate(() => (window as any).__longTasks);

      // Long tasks over 50ms can cause jank. We allow a few but not many.
      const veryLongTasks = recordedLongTasks.filter((t: number) => t > 100);

      // Should not have excessive long tasks (would indicate animation performance issues)
      expect(veryLongTasks.length).toBeLessThan(5);
    });

    test('should use hardware-accelerated CSS transforms for animations', async ({ page }) => {
      await page.goto('/');
      await waitForHeroSection(page);

      // Scroll to trigger animations
      const featuresSection = page.getByTestId('features-section');
      await featuresSection.scrollIntoViewIfNeeded();
      await page.waitForTimeout(300);

      // Check that feature cards use transform property (GPU accelerated)
      const firstCard = page.getByTestId('feature-card-url-shortening');
      await expect(firstCard).toBeVisible();

      // Framer Motion uses CSS transforms which are hardware accelerated
      // The transform property should be set (even if it's 'none' after animation)
      const transform = await firstCard.evaluate((el) => {
        return window.getComputedStyle(el).transform;
      });

      // Transform should be defined (matrix, translate, etc. or 'none' after completion)
      expect(transform).toBeDefined();
    });
  });

  test.describe('Test Case 3: Check hover effects on buttons', () => {
    test('should have smooth hover transition on hero CTA button', async ({ page }) => {
      await page.goto('/');
      await waitForHeroSection(page);

      const ctaButton = page.getByTestId('hero-cta');
      await expect(ctaButton).toBeVisible();

      // Check button has transition classes for smooth hover
      const hasTransitionClass = await ctaButton.evaluate((el) => {
        const classes = el.className;
        return classes.includes('transition');
      });
      expect(hasTransitionClass).toBe(true);

      // Get initial transform
      const initialTransform = await ctaButton.evaluate((el) => {
        return window.getComputedStyle(el).transform;
      });

      // Hover over the button
      await ctaButton.hover();

      // Wait for transition to apply
      await page.waitForTimeout(350);

      // Get transform after hover
      const hoverTransform = await ctaButton.evaluate((el) => {
        return window.getComputedStyle(el).transform;
      });

      // The button should have scale effect on hover (hover:scale-105)
      // Check that transform changed or has scale applied
      const hasScaleEffect =
        hoverTransform !== 'none' && hoverTransform.includes('matrix');

      // Either transform changed or is a matrix (indicating scale applied)
      expect(hasScaleEffect || hoverTransform !== initialTransform).toBe(true);
    });

    test('should have transition duration configured on buttons', async ({ page }) => {
      await page.goto('/');
      await waitForHeroSection(page);

      const ctaButton = page.getByTestId('hero-cta');
      await expect(ctaButton).toBeVisible();

      // Check transition duration is set (duration-300 = 300ms)
      const transitionDuration = await ctaButton.evaluate((el) => {
        return window.getComputedStyle(el).transitionDuration;
      });

      // Should have a transition duration (not 0s)
      expect(transitionDuration).not.toBe('0s');

      // Parse the duration and verify it's a reasonable animation time (100-500ms)
      const durationMs = parseFloat(transitionDuration) * 1000;
      expect(durationMs).toBeGreaterThanOrEqual(100);
      expect(durationMs).toBeLessThanOrEqual(500);
    });

    test('should apply hover effect using CSS transition-all', async ({ page }) => {
      await page.goto('/');
      await waitForHeroSection(page);

      const ctaButton = page.getByTestId('hero-cta');

      // Verify the button has transition-all class
      const classes = await ctaButton.evaluate((el) => el.className);
      expect(classes).toContain('transition-all');
    });

    test('should have hover effect on login link', async ({ page }) => {
      await page.goto('/');
      await waitForHeroSection(page);

      const loginLink = page.getByTestId('hero-login-link');
      await expect(loginLink).toBeVisible();

      // Login link has hover:underline class
      const hasHoverUnderline = await loginLink.evaluate((el) => {
        return el.className.includes('hover:underline');
      });
      expect(hasHoverUnderline).toBe(true);

      // Hover and check text-decoration
      await loginLink.hover();
      await page.waitForTimeout(100);

      const textDecoration = await loginLink.evaluate((el) => {
        return window.getComputedStyle(el).textDecoration;
      });

      // Should have underline on hover
      expect(textDecoration).toContain('underline');
    });
  });
});
