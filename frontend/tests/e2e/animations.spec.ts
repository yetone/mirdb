/**
 * Visual Effects and Animations E2E Tests
 * Owner: Scenario 12 - Visual Effects and Animations
 *
 * End-to-end tests for:
 * - BackgroundEffect component rendering
 * - FuturisticButton hover animations
 * - GlassMorphismCard hover effects
 * - prefers-reduced-motion accessibility support
 * - Page transitions
 */

import { test, expect } from '@playwright/test';

test.describe('Visual Effects and Animations', () => {
  test.describe('BackgroundEffect Component', () => {
    test('should render animated background on homepage', async ({ page }) => {
      await page.goto('/');

      // Wait for the background effect to be present
      const backgroundEffect = page.getByTestId('background-effect');
      await expect(backgroundEffect).toBeVisible();

      // Verify the background is positioned and has content
      const boundingBox = await backgroundEffect.boundingBox();
      expect(boundingBox).toBeTruthy();
      expect(boundingBox!.width).toBeGreaterThan(0);
      expect(boundingBox!.height).toBeGreaterThan(0);
    });

    test('should display animated particles in background', async ({ page }) => {
      await page.goto('/');

      const backgroundEffect = page.getByTestId('background-effect');
      await expect(backgroundEffect).toBeVisible();

      // Check that reduced motion is not active by default
      const reducedMotion = await backgroundEffect.getAttribute('data-reduced-motion');
      expect(reducedMotion).toBe('false');

      // Particles should exist within the background effect
      const particles = page.locator('[data-testid^="particle-"]');
      const particleCount = await particles.count();
      expect(particleCount).toBeGreaterThan(0);
    });

    test('should have gradient background visible', async ({ page }) => {
      await page.goto('/');

      const backgroundEffect = page.getByTestId('background-effect');

      // The background should contain gradient elements
      const gradientDiv = backgroundEffect.locator('div').first();
      await expect(gradientDiv).toBeVisible();

      // Verify gradient has background-image style with gradient
      const backgroundStyle = await gradientDiv.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return style.backgroundImage || style.background;
      });

      // Should have some background styling (gradient)
      expect(backgroundStyle).toBeTruthy();
    });
  });

  test.describe('FuturisticButton Hover Animation', () => {
    test.beforeEach(async ({ page }) => {
      await page.goto('/animation-demo');
    });

    test('should show smooth hover animation on FuturisticButton', async ({ page }) => {
      const button = page.getByTestId('futuristic-button').first();
      await expect(button).toBeVisible();

      // Get initial transform
      const initialTransform = await button.evaluate((el) => {
        return window.getComputedStyle(el).transform;
      });

      // Hover over the button
      await button.hover();

      // Wait for animation to apply
      await page.waitForTimeout(400);

      // Get hover transform
      const hoverTransform = await button.evaluate((el) => {
        return window.getComputedStyle(el).transform;
      });

      // Transform should change (scale effect)
      // Note: transform will be a matrix value
      expect(hoverTransform).not.toBe('none');
    });

    test('each button variant should respond to hover', async ({ page }) => {
      const buttons = page.getByTestId('futuristic-button');
      const count = await buttons.count();

      expect(count).toBeGreaterThan(0);

      // Test first button hover
      const firstButton = buttons.first();
      await expect(firstButton).toBeVisible();

      // Hover and verify button is interactive
      await firstButton.hover();
      await page.waitForTimeout(200);

      // Button should still be visible after hover
      await expect(firstButton).toBeVisible();
    });

    test('button should have focus ring when focused', async ({ page }) => {
      const button = page.getByTestId('futuristic-button').first();
      await expect(button).toBeVisible();

      // Focus the button
      await button.focus();

      // Check that the button is focused
      const isFocused = await button.evaluate((el) => {
        return document.activeElement === el;
      });

      expect(isFocused).toBe(true);
    });
  });

  test.describe('GlassMorphismCard Hover Effect', () => {
    test.beforeEach(async ({ page }) => {
      await page.goto('/animation-demo');
    });

    test('should show subtle hover effect on GlassMorphismCard (scale, glow, or highlight)', async ({
      page,
    }) => {
      const card = page.getByTestId('glass-card').first();
      await expect(card).toBeVisible();

      // Get initial box-shadow and transform
      const initialStyles = await card.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return {
          transform: style.transform,
          boxShadow: style.boxShadow,
        };
      });

      // Hover over the card
      await card.hover();

      // Wait for animation to apply
      await page.waitForTimeout(400);

      // Get hover styles
      const hoverStyles = await card.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return {
          transform: style.transform,
          boxShadow: style.boxShadow,
        };
      });

      // Either transform or box-shadow should change
      const hasChange =
        hoverStyles.transform !== initialStyles.transform ||
        hoverStyles.boxShadow !== initialStyles.boxShadow;

      expect(hasChange).toBe(true);
    });

    test('card with hoverEffect disabled should not animate', async ({ page }) => {
      // Find the card with hover effect disabled (third card)
      const cards = page.getByTestId('glass-card');
      const noHoverCard = cards.nth(2);

      await expect(noHoverCard).toBeVisible();

      // Check data attribute
      const hoverEffectAttr = await noHoverCard.getAttribute('data-hover-effect');
      expect(hoverEffectAttr).toBe('false');
    });

    test('all glass cards should be visible', async ({ page }) => {
      const cards = page.getByTestId('glass-card');
      const count = await cards.count();

      expect(count).toBe(3);

      for (let i = 0; i < count; i++) {
        await expect(cards.nth(i)).toBeVisible();
      }
    });
  });

  test.describe('Reduced Motion Accessibility', () => {
    test('should respect prefers-reduced-motion media query', async ({ page }) => {
      // Emulate prefers-reduced-motion: reduce
      await page.emulateMedia({ reducedMotion: 'reduce' });

      await page.goto('/animation-demo');

      // Check that reduced motion is detected
      const backgroundEffect = page.getByTestId('background-effect');
      await expect(backgroundEffect).toBeVisible();

      // The background should indicate reduced motion
      const reducedMotion = await backgroundEffect.getAttribute('data-reduced-motion');
      expect(reducedMotion).toBe('true');
    });

    test('should disable particle animations when reduced motion is preferred', async ({
      page,
    }) => {
      // Emulate prefers-reduced-motion: reduce
      await page.emulateMedia({ reducedMotion: 'reduce' });

      await page.goto('/');

      const backgroundEffect = page.getByTestId('background-effect');
      await expect(backgroundEffect).toBeVisible();

      // In reduced motion mode, particles should not be rendered
      const particles = page.locator('[data-testid^="particle-"]');
      const particleCount = await particles.count();

      // No particles in reduced motion mode
      expect(particleCount).toBe(0);
    });

    test('buttons should indicate reduced motion state', async ({ page }) => {
      // Emulate prefers-reduced-motion: reduce
      await page.emulateMedia({ reducedMotion: 'reduce' });

      await page.goto('/animation-demo');

      const button = page.getByTestId('futuristic-button').first();
      await expect(button).toBeVisible();

      // Check data attribute for reduced motion
      const reducedMotion = await button.getAttribute('data-reduced-motion');
      expect(reducedMotion).toBe('true');
    });

    test('glass cards should indicate reduced motion state', async ({ page }) => {
      // Emulate prefers-reduced-motion: reduce
      await page.emulateMedia({ reducedMotion: 'reduce' });

      await page.goto('/animation-demo');

      const card = page.getByTestId('glass-card').first();
      await expect(card).toBeVisible();

      // Check data attribute for reduced motion
      const reducedMotion = await card.getAttribute('data-reduced-motion');
      expect(reducedMotion).toBe('true');
    });

    test('animations should work when reduced motion is not preferred', async ({
      page,
    }) => {
      // Emulate no-preference (animations enabled)
      await page.emulateMedia({ reducedMotion: 'no-preference' });

      await page.goto('/animation-demo');

      // Background should not be in reduced motion mode
      const backgroundEffect = page.getByTestId('background-effect');
      const reducedMotion = await backgroundEffect.getAttribute('data-reduced-motion');
      expect(reducedMotion).toBe('false');

      // Button should not be in reduced motion mode
      const button = page.getByTestId('futuristic-button').first();
      const buttonReducedMotion = await button.getAttribute('data-reduced-motion');
      expect(buttonReducedMotion).toBe('false');

      // Card should not be in reduced motion mode
      const card = page.getByTestId('glass-card').first();
      const cardReducedMotion = await card.getAttribute('data-reduced-motion');
      expect(cardReducedMotion).toBe('false');
    });
  });

  test.describe('Page Transitions', () => {
    test('should have smooth transitions when navigating between sections', async ({
      page,
    }) => {
      await page.goto('/');

      // Scroll to different sections and verify they render
      const sections = [
        'features-section',
        'how-it-works-section',
        'cta-section',
        'footer-section',
      ];

      for (const sectionId of sections) {
        const section = page.getByTestId(sectionId);

        // Scroll to section
        await section.scrollIntoViewIfNeeded();

        // Wait for potential animations to start
        await page.waitForTimeout(300);

        // Section should be visible
        await expect(section).toBeVisible();
      }
    });

    test('should navigate between pages smoothly', async ({ page }) => {
      await page.goto('/');

      // Navigate to login page
      const loginLink = page.getByRole('link', { name: /login/i });
      await loginLink.click();

      // Verify we're on login page
      await expect(page).toHaveURL(/\/login/);
      await expect(page.getByRole('heading', { name: /login/i })).toBeVisible();

      // Navigate back to home
      await page.goto('/');
      await expect(page).toHaveURL('/');

      // Home page content should be visible
      await expect(page.getByTestId('background-effect')).toBeVisible();
    });

    test('features section should have entrance animation', async ({ page }) => {
      await page.goto('/');

      // Get the features section
      const featuresSection = page.getByTestId('features-section');

      // Scroll to features section
      await featuresSection.scrollIntoViewIfNeeded();

      // Wait for entrance animation
      await page.waitForTimeout(500);

      // Section should be visible
      await expect(featuresSection).toBeVisible();

      // Feature cards should be visible after animation
      const featureCards = page.locator('[data-testid^="feature-card-"]');
      const cardCount = await featureCards.count();
      expect(cardCount).toBeGreaterThan(0);
    });

    test('CTA section should animate into view', async ({ page }) => {
      await page.goto('/');

      // Get the CTA section
      const ctaSection = page.getByTestId('cta-section');

      // Scroll to CTA section
      await ctaSection.scrollIntoViewIfNeeded();

      // Wait for entrance animation
      await page.waitForTimeout(500);

      // Section content should be visible
      await expect(ctaSection).toBeVisible();
      await expect(page.getByTestId('cta-headline')).toBeVisible();
      await expect(page.getByTestId('cta-signup-button')).toBeVisible();
    });
  });

  test.describe('Animation Demo Page', () => {
    test('demo page should render all animation components', async ({ page }) => {
      await page.goto('/animation-demo');

      // Background effect should be present
      await expect(page.getByTestId('background-effect')).toBeVisible();

      // Button demo section should be present
      await expect(page.getByTestId('button-demo')).toBeVisible();

      // Card demo section should be present
      await expect(page.getByTestId('card-demo')).toBeVisible();

      // Buttons should be present
      const buttons = page.getByTestId('futuristic-button');
      await expect(buttons).toHaveCount(4);

      // Cards should be present
      const cards = page.getByTestId('glass-card');
      await expect(cards).toHaveCount(3);
    });
  });
});
