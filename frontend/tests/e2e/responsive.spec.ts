/**
 * Responsive Design E2E Tests
 * Owner: Scenario 7 - Responsive Design
 *
 * End-to-end tests for responsive layouts:
 * - Mobile viewport (320px)
 * - iPhone viewport (428px)
 * - Tablet viewport (768px)
 * - Desktop viewport (1024px)
 * - Large desktop viewport (1920px)
 * - Verify no horizontal scrolling at any viewport
 * - Verify responsive classes and structure
 */

import { test, expect } from '@playwright/test';

test.describe('Responsive Design - Mobile (320px)', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize({ width: 320, height: 568 });
    await page.goto('/');
    await page.waitForLoadState('networkidle');
  });

  test('TC1: Hero section displays in single column with no horizontal scroll', async ({ page }) => {
    // Verify hero section is visible
    const heroHeadline = page.locator('#hero-headline');
    await expect(heroHeadline).toBeVisible();

    // Verify content fits within viewport (no horizontal overflow)
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScroll).toBe(false);

    // Verify the form exists and has proper responsive classes
    const form = page.locator('form[role="form"]');
    await expect(form).toBeVisible();

    // Verify form container uses flex-col for mobile (single column layout)
    const formFlexCol = page.locator('form[role="form"] .flex-col');
    await expect(formFlexCol.first()).toBeVisible();
  });

  test('TC3: URL input field is accessible with proper structure on mobile', async ({ page }) => {
    // Verify input field is visible
    const urlInput = page.getByTestId('url-input');
    await expect(urlInput).toBeVisible();

    // Verify input has proper accessibility attributes
    await expect(urlInput).toHaveAttribute('aria-label', 'URL to shorten');

    // Verify shorten button exists and is accessible
    const shortenButton = page.getByTestId('shorten-button');
    await expect(shortenButton).toBeVisible();
    await expect(shortenButton).toHaveAttribute('aria-label');

    // Verify input has the input classes for styling
    const inputClasses = await urlInput.getAttribute('class');
    expect(inputClasses).toContain('input');

    // Verify button has the btn classes for styling
    const buttonClasses = await shortenButton.getAttribute('class');
    expect(buttonClasses).toContain('btn');
  });

  test('TC4: Navigation is accessible on mobile viewport', async ({ page }) => {
    // Verify navbar is visible
    const navbar = page.getByTestId('navbar');
    await expect(navbar).toBeVisible();

    // Verify navigation has proper accessibility attributes
    await expect(navbar).toHaveAttribute('role', 'navigation');
    await expect(navbar).toHaveAttribute('aria-label', 'Main navigation');

    // Verify theme toggle is accessible
    const themeToggle = page.getByTestId('theme-toggle');
    await expect(themeToggle).toBeVisible();
    await expect(themeToggle).toHaveAttribute('aria-label');

    // Check that navigation links exist
    const loginLink = page.getByTestId('navbar-login');
    const registerLink = page.getByTestId('navbar-register');

    // At least navigation options should be in the DOM
    await expect(loginLink).toBeAttached();
    await expect(registerLink).toBeAttached();
  });
});

test.describe('Responsive Design - iPhone (428px)', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize({ width: 428, height: 926 });
    await page.goto('/');
    await page.waitForLoadState('networkidle');
  });

  test('TC2: All content is readable with proper structure', async ({ page }) => {
    // Verify hero headline is visible
    const heroHeadline = page.locator('#hero-headline');
    await expect(heroHeadline).toBeVisible();

    // Verify content fits within viewport (no horizontal overflow)
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScroll).toBe(false);

    // Verify all interactive elements are present
    const interactiveElements = [
      page.getByTestId('url-input'),
      page.getByTestId('shorten-button'),
      page.getByTestId('theme-toggle'),
    ];

    for (const element of interactiveElements) {
      await expect(element).toBeVisible();
    }

    // Verify headline has responsive text classes
    const headlineClasses = await heroHeadline.getAttribute('class');
    expect(headlineClasses).toContain('text-4xl');
    expect(headlineClasses).toContain('sm:text-5xl');
    expect(headlineClasses).toContain('lg:text-6xl');
  });
});

test.describe('Responsive Design - Tablet (768px)', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize({ width: 768, height: 1024 });
    await page.goto('/');
    await page.waitForLoadState('networkidle');
  });

  test('Layout adjusts properly for tablet viewport', async ({ page }) => {
    // Verify no horizontal scrolling
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScroll).toBe(false);

    // Verify hero section is visible
    const heroHeadline = page.locator('#hero-headline');
    await expect(heroHeadline).toBeVisible();

    // Verify form has sm: responsive layout classes
    const formInputContainer = page.locator('form[role="form"] .sm\\:flex-row');
    await expect(formInputContainer).toBeAttached();

    // Verify navigation items are visible
    const loginLink = page.getByTestId('navbar-login');
    await expect(loginLink).toBeVisible();
  });
});

test.describe('Responsive Design - Desktop (1024px)', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize({ width: 1024, height: 768 });
    await page.goto('/');
    await page.waitForLoadState('networkidle');
  });

  test('Layout adjusts properly for desktop viewport', async ({ page }) => {
    // Verify no horizontal scrolling
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScroll).toBe(false);

    // Verify hero section displays properly
    const heroHeadline = page.locator('#hero-headline');
    await expect(heroHeadline).toBeVisible();

    // Verify form elements are present
    const form = page.locator('form[role="form"]');
    await expect(form).toBeVisible();

    // Verify navigation is visible and properly laid out
    const navbar = page.getByTestId('navbar');
    await expect(navbar).toBeVisible();

    const loginLink = page.getByTestId('navbar-login');
    const registerLink = page.getByTestId('navbar-register');
    await expect(loginLink).toBeVisible();
    await expect(registerLink).toBeVisible();

    // Verify "How It Works" section has lg: layout classes
    const stepsContainer = page.getByTestId('steps-container');
    await expect(stepsContainer).toBeVisible();
    const stepsClasses = await stepsContainer.getAttribute('class');
    expect(stepsClasses).toContain('lg:flex-row');
  });
});

test.describe('Responsive Design - Large Desktop (1920px)', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize({ width: 1920, height: 1080 });
    await page.goto('/');
    await page.waitForLoadState('networkidle');
  });

  test('TC5: Content has max-width constraints and is centered', async ({ page }) => {
    // Verify no horizontal scrolling
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScroll).toBe(false);

    // Verify hero section is visible
    const heroSection = page.locator('section[aria-labelledby="hero-headline"]');
    await expect(heroSection).toBeVisible();

    // Verify text container has max-width class for constraining content
    const textContainer = page.locator('section[aria-labelledby="hero-headline"] .max-w-4xl');
    await expect(textContainer).toBeAttached();
    const containerClasses = await textContainer.getAttribute('class');
    expect(containerClasses).toContain('max-w-4xl');
    expect(containerClasses).toContain('mx-auto'); // centered

    // Verify form container has max-width class
    const formContainer = page.locator('form[role="form"]');
    const formClasses = await formContainer.getAttribute('class');
    expect(formClasses).toContain('max-w-2xl');
    expect(formClasses).toContain('mx-auto'); // centered
  });
});

test.describe('Responsive Design - Text Readability', () => {
  const viewports = [
    { width: 320, height: 568, name: 'mobile-320' },
    { width: 768, height: 1024, name: 'tablet-768' },
    { width: 1024, height: 768, name: 'desktop-1024' },
    { width: 1920, height: 1080, name: 'large-desktop-1920' },
  ];

  for (const viewport of viewports) {
    test(`TC6: Text content has responsive typography at ${viewport.name}`, async ({ page }) => {
      await page.setViewportSize({ width: viewport.width, height: viewport.height });
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Verify headline has responsive text size classes
      const headline = page.locator('#hero-headline');
      await expect(headline).toBeVisible();

      const headlineClasses = await headline.getAttribute('class');
      // Should have base mobile size (text-4xl)
      expect(headlineClasses).toContain('text-4xl');
      // Should have responsive sizes for larger screens
      expect(headlineClasses).toContain('sm:text-5xl');
      expect(headlineClasses).toContain('lg:text-6xl');

      // Verify tagline has responsive text size classes
      const tagline = page.locator('section[aria-labelledby="hero-headline"] p');
      await expect(tagline.first()).toBeVisible();

      const taglineClasses = await tagline.first().getAttribute('class');
      expect(taglineClasses).toContain('text-lg');
      expect(taglineClasses).toContain('sm:text-xl');
      expect(taglineClasses).toContain('lg:text-2xl');

      // Verify content container is constrained with max-width
      const contentContainer = page.locator('section[aria-labelledby="hero-headline"] .max-w-4xl');
      await expect(contentContainer).toBeAttached();
    });
  }
});

test.describe('Responsive Design - No Horizontal Scroll', () => {
  const viewports = [
    { width: 320, height: 568 },
    { width: 375, height: 667 },
    { width: 428, height: 926 },
    { width: 768, height: 1024 },
    { width: 1024, height: 768 },
    { width: 1280, height: 720 },
    { width: 1920, height: 1080 },
  ];

  for (const viewport of viewports) {
    test(`No horizontal scrolling at ${viewport.width}px width`, async ({ page }) => {
      await page.setViewportSize(viewport);
      await page.goto('/');

      // Wait for page to fully load
      await page.waitForLoadState('networkidle');

      // Check for horizontal overflow
      const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });

      expect(hasHorizontalScroll).toBe(false);
    });
  }
});
