/**
 * E2E tests for Interactive UI Elements.
 * Owner: Scenario 16 - Interactive UI Elements
 *
 * Tests hover effects, animations, and visual feedback on interactive elements.
 */
import { test, expect } from '@playwright/test';

test.describe('Interactive UI Elements', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Wait for the page to fully load
    await page.waitForLoadState('networkidle');
  });

  test('TC1: Get Started button shows hover state with visual change', async ({ page }) => {
    // Test case 1: Hover over Get Started button
    const ctaButton = page.locator('[data-testid="hero-cta"]');
    await expect(ctaButton).toBeVisible();

    // Get initial styles
    const initialStyles = await ctaButton.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        transform: styles.transform,
        backgroundColor: styles.backgroundColor,
      };
    });

    // Hover over the button
    await ctaButton.hover();

    // Wait for transition to complete
    await page.waitForTimeout(200);

    // Get hover styles
    const hoverStyles = await ctaButton.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        transform: styles.transform,
        backgroundColor: styles.backgroundColor,
      };
    });

    // Verify visual change occurred (either transform or color change)
    const hasVisualChange =
      initialStyles.transform !== hoverStyles.transform ||
      initialStyles.backgroundColor !== hoverStyles.backgroundColor;

    expect(hasVisualChange).toBe(true);
  });

  test('TC2: Feature card shows hover state with lift and shadow', async ({ page }) => {
    // Test case 2: Hover over feature card
    // Scroll to features section first
    await page.locator('#features').scrollIntoViewIfNeeded();

    const featureCard = page.locator('[data-testid="feature-card"]').first();
    await expect(featureCard).toBeVisible();

    // Get initial styles
    const initialStyles = await featureCard.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        transform: styles.transform,
        boxShadow: styles.boxShadow,
      };
    });

    // Hover over the card
    await featureCard.hover();

    // Wait for transition to complete
    await page.waitForTimeout(250);

    // Get hover styles
    const hoverStyles = await featureCard.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        transform: styles.transform,
        boxShadow: styles.boxShadow,
      };
    });

    // Verify hover effect (transform lift or shadow change)
    const hasHoverEffect =
      initialStyles.transform !== hoverStyles.transform ||
      initialStyles.boxShadow !== hoverStyles.boxShadow;

    expect(hasHoverEffect).toBe(true);
  });

  test('TC3: Navigation links show hover state with color change', async ({ page }) => {
    // Test case 3: Hover over navigation links
    // Get the first visible nav link (desktop only)
    const navLink = page.locator('[data-testid^="nav-link-"]').first();

    // Skip test on mobile where nav links are hidden
    const isVisible = await navLink.isVisible();
    if (!isVisible) {
      test.skip();
      return;
    }

    // Get initial color
    const initialColor = await navLink.evaluate((el) => {
      return window.getComputedStyle(el).color;
    });

    // Hover over the link
    await navLink.hover();

    // Wait for transition to complete
    await page.waitForTimeout(200);

    // Get hover color
    const hoverColor = await navLink.evaluate((el) => {
      return window.getComputedStyle(el).color;
    });

    // Verify color change on hover
    expect(initialColor).not.toBe(hoverColor);
  });

  test('TC4: Feature cards animate into view on scroll', async ({ page }) => {
    // Test case 4: Scroll to Features section to verify reveal animation
    // First, ensure we're at the top
    await page.evaluate(() => window.scrollTo(0, 0));
    await page.waitForTimeout(100);

    // Get features section
    const featuresSection = page.locator('#features');

    // Verify section exists
    await expect(featuresSection).toBeAttached();

    // Scroll to features section
    await featuresSection.scrollIntoViewIfNeeded();

    // Wait for animation
    await page.waitForTimeout(500);

    // Verify feature cards are visible and have proper opacity
    const featureCards = page.locator('[data-testid="feature-card"]');
    const cardCount = await featureCards.count();

    expect(cardCount).toBeGreaterThan(0);

    // Check that all cards are visible after scroll
    for (let i = 0; i < cardCount; i++) {
      const card = featureCards.nth(i);
      await expect(card).toBeVisible();

      // Verify the card has completed any reveal animation (opacity should be 1)
      const opacity = await card.evaluate((el) => {
        return window.getComputedStyle(el).opacity;
      });
      expect(parseFloat(opacity)).toBe(1);
    }
  });

  test('TC5: Button shows visible focus indicator for accessibility', async ({ page }) => {
    // Test case 5: Focus on button with keyboard
    const ctaButton = page.locator('[data-testid="hero-cta"]');
    await expect(ctaButton).toBeVisible();

    // Focus the button using keyboard navigation
    await ctaButton.focus();

    // Get focus styles
    const focusStyles = await ctaButton.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        outline: styles.outline,
        outlineWidth: styles.outlineWidth,
        outlineStyle: styles.outlineStyle,
        outlineColor: styles.outlineColor,
        boxShadow: styles.boxShadow,
      };
    });

    // Verify focus indicator exists (either outline or box-shadow)
    const hasOutline =
      focusStyles.outlineStyle !== 'none' && focusStyles.outlineWidth !== '0px';
    const hasBoxShadow = focusStyles.boxShadow !== 'none';

    expect(hasOutline || hasBoxShadow).toBe(true);
  });

  test('Multiple buttons have consistent hover behavior', async ({ page }) => {
    // Additional test: Verify all buttons on the page have hover transitions
    const buttons = page.locator('a[class*="button"], button[class*="button"]');
    const buttonCount = await buttons.count();

    if (buttonCount === 0) {
      // Skip if no buttons found with expected classes
      return;
    }

    for (let i = 0; i < Math.min(buttonCount, 3); i++) {
      const button = buttons.nth(i);
      const isVisible = await button.isVisible();

      if (!isVisible) continue;

      // Check that button has transition property
      const hasTransition = await button.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return (
          styles.transition !== 'none' &&
          styles.transition !== '' &&
          styles.transition !== 'all 0s ease 0s'
        );
      });

      expect(hasTransition).toBe(true);
    }
  });

  test('Interactive elements respect reduced motion preference', async ({ page }) => {
    // Emulate reduced motion preference
    await page.emulateMedia({ reducedMotion: 'reduce' });
    await page.goto('/');

    // Check that animations are disabled/reduced
    const featureCard = page.locator('[data-testid="feature-card"]').first();

    if (await featureCard.isVisible()) {
      const transitionDuration = await featureCard.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        // Parse transition duration
        const duration = styles.transitionDuration;
        // Convert to ms for comparison (0.01ms is essentially instant)
        return parseFloat(duration) * (duration.includes('ms') ? 1 : 1000);
      });

      // With reduced motion, transitions should be very fast or instant
      expect(transitionDuration).toBeLessThanOrEqual(10);
    }
  });

  test('Theme toggle button has interactive states', async ({ page }) => {
    // Find theme toggle button
    const themeToggle = page.locator('[data-testid="theme-toggle"]');

    if (await themeToggle.isVisible()) {
      // Get initial state
      const initialStyles = await themeToggle.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          transform: styles.transform,
          backgroundColor: styles.backgroundColor,
        };
      });

      // Hover over the toggle
      await themeToggle.hover();
      await page.waitForTimeout(200);

      // Get hover state
      const hoverStyles = await themeToggle.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          transform: styles.transform,
          backgroundColor: styles.backgroundColor,
        };
      });

      // Either transform or background should change on hover
      const hasHoverState =
        initialStyles.transform !== hoverStyles.transform ||
        initialStyles.backgroundColor !== hoverStyles.backgroundColor;

      // Theme toggle should have hover state or transition property
      const hasTransition = await themeToggle.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return styles.transition !== 'none' && styles.transition !== '';
      });

      expect(hasHoverState || hasTransition).toBe(true);
    }
  });
});
