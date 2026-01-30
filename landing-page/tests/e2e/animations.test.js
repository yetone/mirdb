/**
 * MirDB Landing Page - Animation and Motion E2E Tests
 * Owner: Scenario 18 - Animation and Motion
 *
 * Tests for:
 * - Hero section entrance animations
 * - Feature card hover effects
 * - Prefers-reduced-motion support
 * - Smooth scroll navigation behavior
 */

const { test, expect } = require('@playwright/test');
const { setupPage, waitForAnimations } = require('../helpers/test-utils');

test.describe('Animation and Motion - Scenario 18', () => {
  test.beforeEach(async ({ page }) => {
    await setupPage(page);
  });

  test('Test Case 1: Hero elements have smooth entrance animations', async ({ page }) => {
    // Check that hero elements have CSS animations defined
    const heroLogo = page.locator('.hero-logo');
    const heroTitle = page.locator('.hero-title');
    const heroTagline = page.locator('.hero-tagline');
    const heroValueProp = page.locator('.hero-value-proposition');
    const heroCta = page.locator('.hero-cta');

    // Verify hero logo has animation
    const logoAnimation = await heroLogo.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return style.animationName;
    });
    expect(logoAnimation).not.toBe('none');
    expect(logoAnimation).toContain('scaleIn');

    // Verify hero title has animation
    const titleAnimation = await heroTitle.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return style.animationName;
    });
    expect(titleAnimation).not.toBe('none');
    expect(titleAnimation).toContain('fadeInUp');

    // Verify hero tagline has animation
    const taglineAnimation = await heroTagline.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return style.animationName;
    });
    expect(taglineAnimation).not.toBe('none');
    expect(taglineAnimation).toContain('fadeInUp');

    // Verify hero value proposition has animation
    const valuePropAnimation = await heroValueProp.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return style.animationName;
    });
    expect(valuePropAnimation).not.toBe('none');
    expect(valuePropAnimation).toContain('fadeInUp');

    // Verify hero CTA has animation
    const ctaAnimation = await heroCta.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return style.animationName;
    });
    expect(ctaAnimation).not.toBe('none');
    expect(ctaAnimation).toContain('fadeInUp');

    // Wait for animations to complete and verify elements are visible
    await waitForAnimations(page, 1500);

    // All elements should be visible after animations complete
    await expect(heroLogo).toBeVisible();
    await expect(heroTitle).toBeVisible();
    await expect(heroTagline).toBeVisible();
    await expect(heroValueProp).toBeVisible();
    await expect(heroCta).toBeVisible();
  });

  test('Test Case 2: Feature cards have hover effects', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();

    // Get the first feature card
    const featureCard = page.locator('.feature-card').first();
    await expect(featureCard).toBeVisible();

    // Get initial transform and box-shadow styles
    const initialStyles = await featureCard.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        transform: style.transform,
        boxShadow: style.boxShadow,
        borderColor: style.borderColor
      };
    });

    // Hover over the card
    await featureCard.hover();

    // Wait for transition
    await page.waitForTimeout(300);

    // Get hover styles
    const hoverStyles = await featureCard.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        transform: style.transform,
        boxShadow: style.boxShadow,
        borderColor: style.borderColor
      };
    });

    // Verify transform change (translateY)
    expect(hoverStyles.transform).not.toBe(initialStyles.transform);
    // The transform should include a Y translation
    expect(hoverStyles.transform).toMatch(/matrix|translateY/);

    // Verify box-shadow change
    expect(hoverStyles.boxShadow).not.toBe('none');

    // Verify feature card has transition property for smooth animation
    const hasTransition = await featureCard.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return style.transitionProperty !== 'none' && style.transitionDuration !== '0s';
    });
    expect(hasTransition).toBe(true);
  });

  test('Test Case 3: Animations are disabled with prefers-reduced-motion', async ({ page }) => {
    // Emulate prefers-reduced-motion: reduce
    await page.emulateMedia({ reducedMotion: 'reduce' });

    // Reload page to apply media preference
    await page.reload();
    await page.waitForLoadState('domcontentloaded');

    // Check that hero elements either have no animations or very short duration
    const heroLogo = page.locator('.hero-logo');
    const heroTitle = page.locator('.hero-title');

    // Verify animations are disabled or have minimal duration
    const logoAnimationData = await heroLogo.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        animationDuration: style.animationDuration,
        animationName: style.animationName,
        transitionDuration: style.transitionDuration
      };
    });

    // Animation should be disabled or have minimal duration (0.01ms as per CSS)
    const durationMs = parseFloat(logoAnimationData.animationDuration) * 1000;
    expect(durationMs).toBeLessThanOrEqual(10); // Should be 0.01ms or 0

    // Verify smooth scroll behavior is disabled
    const scrollBehavior = await page.evaluate(() => {
      return window.getComputedStyle(document.documentElement).scrollBehavior;
    });
    expect(scrollBehavior).toBe('auto');

    // Feature cards should have no hover transform animation
    const featureCard = page.locator('.feature-card').first();
    await featureCard.scrollIntoViewIfNeeded();

    const cardTransitionDuration = await featureCard.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return style.transitionDuration;
    });

    // Transition duration should be minimal with reduced motion
    const cardDurationMs = parseFloat(cardTransitionDuration) * 1000;
    expect(cardDurationMs).toBeLessThanOrEqual(10);
  });

  test('Test Case 4: Navigation triggers smooth scroll to sections', async ({ page }) => {
    // First verify scroll behavior is smooth (without reduced motion)
    const scrollBehavior = await page.evaluate(() => {
      return window.getComputedStyle(document.documentElement).scrollBehavior;
    });
    expect(scrollBehavior).toBe('smooth');

    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY);

    // Click on the features navigation link
    const featuresLink = page.locator('.nav-links a[href="#features"]');
    await featuresLink.click();

    // Wait for scroll animation
    await page.waitForTimeout(500);

    // Verify page scrolled
    const newScrollY = await page.evaluate(() => window.scrollY);
    expect(newScrollY).toBeGreaterThan(initialScrollY);

    // Verify the features section is now in view
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport({ ratio: 0.5 });

    // Test scrolling to another section
    const architectureLink = page.locator('.nav-links a[href="#architecture"]');
    await architectureLink.click();

    // Wait for smooth scroll to complete
    await page.waitForTimeout(1000);

    const architectureSection = page.locator('#architecture');
    // The section should be at least partially visible in the viewport
    await expect(architectureSection).toBeInViewport({ ratio: 0.3 });
  });

  test('Animations JavaScript module initializes correctly', async ({ page }) => {
    // Wait for page to fully load
    await page.waitForLoadState('load');

    // Check that animations module sets the data attribute
    const hasAnimationsReady = await page.evaluate(() => {
      return document.body.getAttribute('data-animations-ready') === 'true';
    });
    expect(hasAnimationsReady).toBe(true);
  });

  test('Hero elements become fully visible after animation completes', async ({ page }) => {
    // Wait for all animations to complete (staggered, so wait for the longest)
    await waitForAnimations(page, 2000);

    // Check that all hero elements have opacity 1 (fully visible)
    const heroElements = ['.hero-logo', '.hero-title', '.hero-tagline', '.hero-value-proposition', '.hero-cta'];

    for (const selector of heroElements) {
      const element = page.locator(selector);
      const opacity = await element.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return parseFloat(style.opacity);
      });
      expect(opacity).toBe(1);
    }
  });

  test('Feature card transitions are smooth', async ({ page }) => {
    const featureCard = page.locator('.feature-card').first();
    await featureCard.scrollIntoViewIfNeeded();

    // Check transition properties
    const transitionProps = await featureCard.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        property: style.transitionProperty,
        duration: style.transitionDuration,
        timingFunction: style.transitionTimingFunction
      };
    });

    // Should have transitions defined
    expect(transitionProps.property).toBeTruthy();
    expect(transitionProps.duration).not.toBe('0s');
    // Should have an easing function
    expect(transitionProps.timingFunction).toMatch(/ease|cubic-bezier/);
  });
});
