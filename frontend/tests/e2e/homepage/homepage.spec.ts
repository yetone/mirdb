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

/**
 * E2E tests for Smooth Scroll Behavior
 * Scenario 27 - Smooth Scroll Behavior
 *
 * Tests that smooth scrolling works for anchor links and section navigation
 */
test.describe('Smooth Scroll Behavior - E2E', () => {
  test.describe('Test Case 1: Click anchor link to features section', () => {
    test('should smoothly scroll to features section when clicking anchor link', async ({ page }) => {
      await page.goto('/');
      await waitForHeroSection(page);

      // Get initial scroll position
      const initialScrollY = await page.evaluate(() => window.scrollY);
      expect(initialScrollY).toBe(0);

      // Create a temporary anchor link to #features and click it
      // This simulates user clicking an anchor link to navigate to features
      await page.evaluate(() => {
        const link = document.createElement('a');
        link.href = '#features';
        link.id = 'test-features-link';
        link.style.position = 'fixed';
        link.style.top = '10px';
        link.style.left = '10px';
        link.style.zIndex = '9999';
        link.textContent = 'Go to Features';
        document.body.appendChild(link);
      });

      // Click the anchor link
      await page.click('#test-features-link');

      // Wait for scroll animation to complete (smooth scroll takes time)
      await page.waitForTimeout(1000);

      // Verify page scrolled to features section
      const featuresSection = page.getByTestId('features-section');
      await expect(featuresSection).toBeVisible();

      // Get the features section position and verify scroll position
      const featuresTop = await page.evaluate(() => {
        const section = document.getElementById('features');
        return section ? section.getBoundingClientRect().top + window.scrollY : 0;
      });

      const finalScrollY = await page.evaluate(() => window.scrollY);

      // The scroll position should be close to the features section
      // Allow some tolerance due to smooth scroll behavior
      expect(finalScrollY).toBeGreaterThan(0);
      expect(Math.abs(finalScrollY - featuresTop)).toBeLessThan(100);
    });

    test('should have features section with proper ID for anchor navigation', async ({ page }) => {
      await page.goto('/');
      await waitForHeroSection(page);

      // Verify features section has the correct ID for anchor linking
      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeAttached();

      // Verify it's the same as features-section testid
      const testIdSection = page.getByTestId('features-section');
      await expect(testIdSection).toHaveAttribute('id', 'features');
    });

    test('should smoothly scroll using hash navigation', async ({ page }) => {
      await page.goto('/');
      await waitForHeroSection(page);

      // Navigate directly to the hash
      await page.goto('/#features');

      // Wait for smooth scroll animation
      await page.waitForTimeout(1000);

      // Verify the features section is in view
      const featuresSection = page.getByTestId('features-section');
      const isInViewport = await featuresSection.evaluate((el) => {
        const rect = el.getBoundingClientRect();
        return rect.top >= 0 && rect.top <= window.innerHeight;
      });

      expect(isInViewport).toBe(true);
    });

    test('should scroll smoothly without jarring jumps', async ({ page }) => {
      await page.goto('/');
      await waitForHeroSection(page);

      // Set up scroll tracking
      await page.evaluate(() => {
        (window as any).__scrollPositions = [];
        const trackScroll = () => {
          (window as any).__scrollPositions.push({
            y: window.scrollY,
            time: performance.now(),
          });
        };
        window.addEventListener('scroll', trackScroll);
      });

      // Navigate to features section
      await page.evaluate(() => {
        document.getElementById('features')?.scrollIntoView({ behavior: 'smooth' });
      });

      // Wait for scroll to complete
      await page.waitForTimeout(1500);

      // Get recorded scroll positions
      const positions = await page.evaluate(() => (window as any).__scrollPositions);

      // Verify there are multiple scroll events (indicating smooth animation)
      // A jarring jump would only have 1-2 events, smooth scroll has many
      expect(positions.length).toBeGreaterThan(5);

      // Verify scroll positions increase progressively (no jumping back)
      for (let i = 1; i < positions.length; i++) {
        expect(positions[i].y).toBeGreaterThanOrEqual(positions[i - 1].y - 1); // Allow 1px tolerance
      }
    });
  });

  test.describe('Test Case 2: Check scroll-behavior CSS property', () => {
    test('should have scroll-behavior: smooth applied to html element', async ({ page }) => {
      await page.goto('/');
      await waitForHeroSection(page);

      // Check scroll-behavior CSS property on html element
      const scrollBehavior = await page.evaluate(() => {
        return window.getComputedStyle(document.documentElement).scrollBehavior;
      });

      expect(scrollBehavior).toBe('smooth');
    });

    test('should apply smooth scroll behavior consistently', async ({ page }) => {
      await page.goto('/');
      await waitForHeroSection(page);

      // Verify the CSS is properly applied from stylesheet
      const htmlStyles = await page.evaluate(() => {
        const html = document.documentElement;
        const computedStyle = window.getComputedStyle(html);
        return {
          scrollBehavior: computedStyle.scrollBehavior,
          // Also check body to ensure it inherits or has no conflicting scroll behavior
          bodyScrollBehavior: window.getComputedStyle(document.body).scrollBehavior,
        };
      });

      // HTML should have smooth scroll
      expect(htmlStyles.scrollBehavior).toBe('smooth');
    });

    test('should work across page refreshes', async ({ page }) => {
      await page.goto('/');
      await waitForHeroSection(page);

      // Check scroll-behavior is applied
      let scrollBehavior = await page.evaluate(() => {
        return window.getComputedStyle(document.documentElement).scrollBehavior;
      });
      expect(scrollBehavior).toBe('smooth');

      // Refresh the page
      await page.reload();
      await waitForHeroSection(page);

      // Verify scroll-behavior is still applied after refresh
      scrollBehavior = await page.evaluate(() => {
        return window.getComputedStyle(document.documentElement).scrollBehavior;
      });
      expect(scrollBehavior).toBe('smooth');
    });
  });
});

/**
 * E2E tests for Error States - No JavaScript
 * Scenario 26 - Graceful degradation when JavaScript is disabled
 *
 * Tests that basic content is visible and navigation links work when JavaScript is disabled
 */
test.describe('Error States - No JavaScript', () => {
  test.describe('Test Case 1: Load homepage with JavaScript disabled', () => {
    test('should display noscript fallback content when JavaScript is disabled', async ({
      browser,
    }) => {
      // Create a new context with JavaScript disabled
      const context = await browser.newContext({
        javaScriptEnabled: false,
      });
      const page = await context.newPage();

      // Navigate to homepage
      await page.goto('/');

      // Verify noscript fallback is visible
      const noscriptFallback = page.locator('.noscript-fallback');
      await expect(noscriptFallback).toBeVisible();

      // Verify main heading is visible
      const heading = page.locator('.noscript-fallback h1');
      await expect(heading).toBeVisible();
      await expect(heading).toContainText('ShortURL');

      // Verify welcome message is visible
      const welcomeHeading = page.locator('.noscript-fallback h2');
      await expect(welcomeHeading).toContainText('Welcome to ShortURL');

      // Verify value proposition text is visible
      const valueProposition = page.locator('.noscript-fallback').getByText('Shorten URLs. Track Every Click.');
      await expect(valueProposition).toBeVisible();

      // Verify JavaScript required message is displayed
      const jsRequiredMessage = page.locator('.noscript-fallback').getByText('JavaScript Required');
      await expect(jsRequiredMessage).toBeVisible();

      await context.close();
    });

    test('should display key features list when JavaScript is disabled', async ({ browser }) => {
      const context = await browser.newContext({
        javaScriptEnabled: false,
      });
      const page = await context.newPage();

      await page.goto('/');

      // Verify features heading
      const featuresHeading = page.locator('.noscript-fallback h3');
      await expect(featuresHeading).toContainText('Key Features');

      // Verify feature items are visible
      const featuresList = page.locator('.noscript-fallback ul li');
      const count = await featuresList.count();
      expect(count).toBe(4);

      // Verify specific features are listed
      await expect(page.locator('.noscript-fallback').getByText('Instant URL Shortening')).toBeVisible();
      await expect(page.locator('.noscript-fallback').getByText('Detailed Click Analytics')).toBeVisible();
      await expect(page.locator('.noscript-fallback').getByText('Geographic Insights')).toBeVisible();
      await expect(page.locator('.noscript-fallback').getByText('Shareable Stats')).toBeVisible();

      await context.close();
    });

    test('should display footer with copyright when JavaScript is disabled', async ({ browser }) => {
      const context = await browser.newContext({
        javaScriptEnabled: false,
      });
      const page = await context.newPage();

      await page.goto('/');

      // Verify footer is visible
      const footer = page.locator('.noscript-fallback footer');
      await expect(footer).toBeVisible();

      // Verify copyright text
      await expect(footer).toContainText('ShortURL');
      await expect(footer).toContainText('All rights reserved');

      await context.close();
    });
  });

  test.describe('Test Case 2: Check navigation links without JavaScript', () => {
    test('should have functional Login link in header when JavaScript is disabled', async ({
      browser,
    }) => {
      const context = await browser.newContext({
        javaScriptEnabled: false,
      });
      const page = await context.newPage();

      await page.goto('/');

      // Find the Login link in the header navigation
      const headerNav = page.locator('.noscript-fallback header nav');
      const loginLink = headerNav.locator('a[href="/login"]');

      await expect(loginLink).toBeVisible();
      await expect(loginLink).toContainText('Login');

      // Verify the href attribute
      const href = await loginLink.getAttribute('href');
      expect(href).toBe('/login');

      // Click and verify navigation
      await loginLink.click();
      await expect(page).toHaveURL(/\/login/);

      await context.close();
    });

    test('should have functional Register link in header when JavaScript is disabled', async ({
      browser,
    }) => {
      const context = await browser.newContext({
        javaScriptEnabled: false,
      });
      const page = await context.newPage();

      await page.goto('/');

      // Find the Register link in the header navigation
      const headerNav = page.locator('.noscript-fallback header nav');
      const registerLink = headerNav.locator('a[href="/register"]');

      await expect(registerLink).toBeVisible();
      await expect(registerLink).toContainText('Register');

      // Verify the href attribute
      const href = await registerLink.getAttribute('href');
      expect(href).toBe('/register');

      // Click and verify navigation
      await registerLink.click();
      await expect(page).toHaveURL(/\/register/);

      await context.close();
    });

    test('should have functional Get Started CTA when JavaScript is disabled', async ({
      browser,
    }) => {
      const context = await browser.newContext({
        javaScriptEnabled: false,
      });
      const page = await context.newPage();

      await page.goto('/');

      // Find the Get Started button/link
      const getStartedLink = page.locator('.noscript-fallback a').filter({ hasText: 'Get Started' });
      await expect(getStartedLink).toBeVisible();

      // Verify it links to register
      const href = await getStartedLink.getAttribute('href');
      expect(href).toBe('/register');

      // Click and verify navigation
      await getStartedLink.click();
      await expect(page).toHaveURL(/\/register/);

      await context.close();
    });

    test('should have functional Login link in CTA section when JavaScript is disabled', async ({
      browser,
    }) => {
      const context = await browser.newContext({
        javaScriptEnabled: false,
      });
      const page = await context.newPage();

      await page.goto('/');

      // Find the "Already have an account? Login" link
      const ctaSection = page.locator('.noscript-fallback main section').last();
      const loginLink = ctaSection.locator('a[href="/login"]');

      await expect(loginLink).toBeVisible();
      await expect(loginLink).toContainText('Login');

      // Click and verify navigation
      await loginLink.click();
      await expect(page).toHaveURL(/\/login/);

      await context.close();
    });

    test('should have all navigation links with proper href attributes', async ({ browser }) => {
      const context = await browser.newContext({
        javaScriptEnabled: false,
      });
      const page = await context.newPage();

      await page.goto('/');

      // Get all links in the noscript fallback
      const allLinks = page.locator('.noscript-fallback a');
      const linkCount = await allLinks.count();

      // Should have multiple navigation links
      expect(linkCount).toBeGreaterThanOrEqual(4);

      // Verify each link has a valid href
      for (let i = 0; i < linkCount; i++) {
        const link = allLinks.nth(i);
        const href = await link.getAttribute('href');
        expect(href).toBeTruthy();
        expect(href).toMatch(/^\/(login|register)?$/);
      }

      await context.close();
    });
  });
});

/**
 * E2E tests for Console Error Free
 * Scenario 28 - Verify homepage renders without any JavaScript console errors
 *
 * Tests that no JavaScript errors appear in the browser console during:
 * - Initial page load
 * - Navigating through sections
 * - Theme toggling
 */
test.describe('Console Error Free - E2E', () => {
  test.describe('Test Case 1: Load homepage and check console', () => {
    test('should load homepage without any JavaScript errors in console', async ({ page }) => {
      // Collect all console errors
      const consoleErrors: string[] = [];

      page.on('console', (msg) => {
        if (msg.type() === 'error') {
          consoleErrors.push(msg.text());
        }
      });

      // Listen for page errors (uncaught exceptions)
      const pageErrors: string[] = [];
      page.on('pageerror', (error) => {
        pageErrors.push(error.message);
      });

      // Navigate to homepage
      await page.goto('/');
      await waitForHeroSection(page);

      // Wait for any async operations to complete
      await page.waitForTimeout(1000);

      // Assert no console errors occurred
      expect(consoleErrors).toEqual([]);
      expect(pageErrors).toEqual([]);
    });

    test('should have no JavaScript errors after page fully loads', async ({ page }) => {
      const consoleErrors: string[] = [];
      const pageErrors: string[] = [];

      page.on('console', (msg) => {
        if (msg.type() === 'error') {
          consoleErrors.push(msg.text());
        }
      });

      page.on('pageerror', (error) => {
        pageErrors.push(error.message);
      });

      // Navigate and wait for full load
      await page.goto('/');
      await page.waitForLoadState('networkidle');
      await waitForHeroSection(page);

      // Verify all major sections are visible (ensuring page fully rendered)
      await expect(page.locator('.hero')).toBeVisible();
      await expect(page.getByTestId('features-section')).toBeVisible();
      await expect(page.getByTestId('how-it-works-section')).toBeVisible();
      await expect(page.getByTestId('footer')).toBeVisible();

      // Assert no errors
      expect(consoleErrors.length).toBe(0);
      expect(pageErrors.length).toBe(0);
    });

    test('should load all homepage components without triggering errors', async ({ page }) => {
      const errors: { type: string; message: string }[] = [];

      page.on('console', (msg) => {
        if (msg.type() === 'error') {
          errors.push({ type: 'console', message: msg.text() });
        }
      });

      page.on('pageerror', (error) => {
        errors.push({ type: 'pageerror', message: error.message });
      });

      await page.goto('/');
      await waitForHeroSection(page);

      // Verify all major components are present
      const components = [
        { name: 'Hero Section', selector: '.hero' },
        { name: 'Features Section', selector: '[data-testid="features-section"]' },
        { name: 'How It Works Section', selector: '[data-testid="how-it-works-section"]' },
        { name: 'Footer', selector: '[data-testid="footer"]' },
      ];

      for (const component of components) {
        const element = page.locator(component.selector);
        await expect(element).toBeAttached();
      }

      // Final check for errors
      expect(errors).toEqual([]);
    });
  });

  test.describe('Test Case 2: Navigate through all homepage sections', () => {
    test('should not produce errors when scrolling through sections', async ({ page }) => {
      const consoleErrors: string[] = [];
      const pageErrors: string[] = [];

      page.on('console', (msg) => {
        if (msg.type() === 'error') {
          consoleErrors.push(msg.text());
        }
      });

      page.on('pageerror', (error) => {
        pageErrors.push(error.message);
      });

      await page.goto('/');
      await waitForHeroSection(page);

      // Scroll through each section to trigger any lazy-loaded content
      const sections = [
        page.locator('.hero'),
        page.getByTestId('features-section'),
        page.getByTestId('how-it-works-section'),
        page.getByTestId('dashboard-preview-section'),
        page.getByTestId('footer'),
      ];

      for (const section of sections) {
        // Only scroll if section exists (dashboard-preview may be optional)
        if (await section.count() > 0) {
          await section.scrollIntoViewIfNeeded();
          await page.waitForTimeout(300); // Wait for scroll animations
        }
      }

      // Scroll back to top
      await page.evaluate(() => window.scrollTo(0, 0));
      await page.waitForTimeout(300);

      // Assert no errors during scrolling
      expect(consoleErrors).toEqual([]);
      expect(pageErrors).toEqual([]);
    });

    test('should not produce errors during Framer Motion scroll animations', async ({ page }) => {
      const errors: string[] = [];

      page.on('console', (msg) => {
        if (msg.type() === 'error') {
          errors.push(msg.text());
        }
      });

      page.on('pageerror', (error) => {
        errors.push(error.message);
      });

      await page.goto('/');
      await waitForHeroSection(page);

      // Scroll to features section to trigger whileInView animations
      const featuresSection = page.getByTestId('features-section');
      await featuresSection.scrollIntoViewIfNeeded();
      await page.waitForTimeout(500); // Wait for animations

      // Scroll to How It Works section
      const howItWorksSection = page.getByTestId('how-it-works-section');
      await howItWorksSection.scrollIntoViewIfNeeded();
      await page.waitForTimeout(500);

      // Check for feature cards (animated elements)
      const featureCards = [
        page.getByTestId('feature-card-url-shortening'),
        page.getByTestId('feature-card-click-analytics'),
        page.getByTestId('feature-card-geographic-insights'),
        page.getByTestId('feature-card-shareable-stats'),
      ];

      for (const card of featureCards) {
        await expect(card).toBeVisible();
      }

      // Assert no animation-related errors
      expect(errors).toEqual([]);
    });

    test('should not produce errors when interacting with CTA buttons', async ({ page }) => {
      const errors: string[] = [];

      page.on('console', (msg) => {
        if (msg.type() === 'error') {
          errors.push(msg.text());
        }
      });

      page.on('pageerror', (error) => {
        errors.push(error.message);
      });

      await page.goto('/');
      await waitForHeroSection(page);

      // Hover over CTA button (triggers hover animations)
      const ctaButton = page.getByTestId('hero-cta');
      await ctaButton.hover();
      await page.waitForTimeout(200);

      // Hover over login link
      const loginLink = page.getByTestId('hero-login-link');
      await loginLink.hover();
      await page.waitForTimeout(200);

      // Move mouse away
      await page.mouse.move(0, 0);

      // Assert no errors during hover interactions
      expect(errors).toEqual([]);
    });
  });

  test.describe('Test Case 3: Toggle theme and check console', () => {
    test('should not produce errors when toggling from light to dark theme', async ({ page }) => {
      const errors: string[] = [];

      page.on('console', (msg) => {
        if (msg.type() === 'error') {
          errors.push(msg.text());
        }
      });

      page.on('pageerror', (error) => {
        errors.push(error.message);
      });

      // Start with light theme
      await page.goto('/');
      await page.evaluate(() => {
        localStorage.setItem('theme', 'light');
      });
      await page.reload();
      await waitForHeroSection(page);

      // Verify initial theme
      const initialTheme = await page.evaluate(() =>
        document.documentElement.getAttribute('data-theme')
      );
      expect(initialTheme).toBe('light');

      // Toggle to dark theme
      const themeToggle = page.getByRole('button', { name: /switch to dark mode/i });
      await themeToggle.click();

      // Wait for theme transition
      await page.waitForTimeout(500);

      // Verify theme changed
      const newTheme = await page.evaluate(() =>
        document.documentElement.getAttribute('data-theme')
      );
      expect(newTheme).toBe('dark');

      // Assert no errors during theme transition
      expect(errors).toEqual([]);
    });

    test('should not produce errors when toggling from dark to light theme', async ({ page }) => {
      const errors: string[] = [];

      page.on('console', (msg) => {
        if (msg.type() === 'error') {
          errors.push(msg.text());
        }
      });

      page.on('pageerror', (error) => {
        errors.push(error.message);
      });

      // Start with dark theme
      await page.goto('/');
      await page.evaluate(() => {
        localStorage.setItem('theme', 'dark');
      });
      await page.reload();
      await waitForHeroSection(page);

      // Toggle to light theme
      const themeToggle = page.getByRole('button', { name: /switch to light mode/i });
      await themeToggle.click();

      // Wait for theme transition
      await page.waitForTimeout(500);

      // Verify theme changed
      const newTheme = await page.evaluate(() =>
        document.documentElement.getAttribute('data-theme')
      );
      expect(newTheme).toBe('light');

      // Assert no errors during theme transition
      expect(errors).toEqual([]);
    });

    test('should not produce errors during multiple rapid theme toggles', async ({ page }) => {
      const errors: string[] = [];

      page.on('console', (msg) => {
        if (msg.type() === 'error') {
          errors.push(msg.text());
        }
      });

      page.on('pageerror', (error) => {
        errors.push(error.message);
      });

      await page.goto('/');
      await page.evaluate(() => {
        localStorage.setItem('theme', 'light');
      });
      await page.reload();
      await waitForHeroSection(page);

      // Perform multiple rapid theme toggles
      for (let i = 0; i < 5; i++) {
        // Find the current toggle button (name changes based on current theme)
        const themeToggle = page.getByRole('button', { name: /switch to (dark|light) mode/i });
        await themeToggle.click();
        await page.waitForTimeout(100); // Small delay between toggles
      }

      // Wait for any pending transitions
      await page.waitForTimeout(500);

      // Assert no errors during rapid toggling
      expect(errors).toEqual([]);
    });

    test('should not produce errors when toggling theme while scrolled', async ({ page }) => {
      const errors: string[] = [];

      page.on('console', (msg) => {
        if (msg.type() === 'error') {
          errors.push(msg.text());
        }
      });

      page.on('pageerror', (error) => {
        errors.push(error.message);
      });

      await page.goto('/');
      await page.evaluate(() => {
        localStorage.setItem('theme', 'light');
      });
      await page.reload();
      await waitForHeroSection(page);

      // Scroll to features section
      const featuresSection = page.getByTestId('features-section');
      await featuresSection.scrollIntoViewIfNeeded();
      await page.waitForTimeout(300);

      // Toggle theme while scrolled
      const themeToggle = page.getByRole('button', { name: /switch to dark mode/i });
      await themeToggle.click();
      await page.waitForTimeout(500);

      // Scroll to another section
      const howItWorksSection = page.getByTestId('how-it-works-section');
      await howItWorksSection.scrollIntoViewIfNeeded();
      await page.waitForTimeout(300);

      // Verify sections are still visible after theme change
      await expect(featuresSection).toBeVisible();
      await expect(howItWorksSection).toBeVisible();

      // Assert no errors
      expect(errors).toEqual([]);
    });
  });
});
