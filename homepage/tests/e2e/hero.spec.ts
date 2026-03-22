/**
 * Hero Section E2E Tests
 * Owners: Scenario 1 (Hero), Scenario 2 (Badges), Scenario 18 (Theme)
 *
 * Test groups:
 * - Hero section visibility and content
 * - Logo display and animation
 * - Value proposition text validation
 * - Primary CTA presence and functionality
 * - Status badges display and links
 * - Theme toggle functionality
 */

import { test, expect } from '@playwright/test';
import { waitForLoad, countWords } from './utils';

test.describe('Hero Section Display (Scenario 1)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await waitForLoad(page);
  });

  test('TC1: Hero section contains logo image with logo.gif source', async ({ page }) => {
    // Test Case 1: Load homepage and inspect hero section DOM
    // Expected: Hero section contains img element with src pointing to logo.gif or logo asset

    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    const logo = page.locator('#hero-logo');
    await expect(logo).toBeVisible();

    const logoSrc = await logo.getAttribute('src');
    expect(logoSrc).toContain('logo.gif');
  });

  test('TC2: Headline text is present and contains 15 words or fewer', async ({ page }) => {
    // Test Case 2: Extract headline text from hero section
    // Expected: Headline text is present, non-empty, and contains 15 words or fewer

    const headline = page.locator('#hero-headline');
    await expect(headline).toBeVisible();

    const headlineText = await headline.textContent();
    expect(headlineText).toBeTruthy();
    expect(headlineText!.trim().length).toBeGreaterThan(0);

    const wordCount = countWords(headlineText!);
    expect(wordCount).toBeLessThanOrEqual(15);
  });

  test('TC3: Value proposition contains relevant keywords', async ({ page }) => {
    // Test Case 3: Check value proposition content
    // Expected: Text includes keywords like 'persistent', 'key-value', or 'Memcached'

    const headline = page.locator('#hero-headline');
    const headlineText = await headline.textContent();

    const lowerText = headlineText!.toLowerCase();
    const hasRelevantKeyword =
      lowerText.includes('persistent') ||
      lowerText.includes('key-value') ||
      lowerText.includes('memcached');

    expect(hasRelevantKeyword).toBe(true);
  });

  test('TC4: Primary CTA button exists and is clickable', async ({ page }) => {
    // Test Case 4: Locate and click primary CTA button
    // Expected: CTA button exists with visible text and is clickable

    const primaryCta = page.locator('#primary-cta');
    await expect(primaryCta).toBeVisible();

    const ctaText = await primaryCta.textContent();
    expect(ctaText).toBeTruthy();
    expect(ctaText!.trim().length).toBeGreaterThan(0);

    // Check that the button is enabled and clickable
    await expect(primaryCta).toBeEnabled();

    // Verify it has an href attribute (links somewhere)
    const href = await primaryCta.getAttribute('href');
    expect(href).toBeTruthy();
  });
});

test.describe('Status Badges Display (Scenario 2)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await waitForLoad(page);
  });

  test('TC1: At least one status badge is present on the page', async ({ page }) => {
    // Find the badges section
    const badgesSection = page.locator('#badges');
    await expect(badgesSection).toBeVisible();

    // Find all badge images/elements
    const badgeImages = badgesSection.locator('img');
    const badgeCount = await badgeImages.count();

    // Verify at least one badge is present
    expect(badgeCount).toBeGreaterThanOrEqual(1);

    // Verify badges are visible
    const firstBadge = badgeImages.first();
    await expect(firstBadge).toBeVisible();
  });

  test('TC2: Badge links to CircleCI or relevant CI/CD service', async ({ page }) => {
    // Find the badges section
    const badgesSection = page.locator('#badges');

    // Extract hrefs from badge parent anchor elements
    const badgeLinks = badgesSection.locator('a');
    const linkCount = await badgeLinks.count();

    // Verify at least one link exists
    expect(linkCount).toBeGreaterThanOrEqual(1);

    // Check that at least one badge links to CircleCI
    const hrefs: string[] = [];
    for (let i = 0; i < linkCount; i++) {
      const href = await badgeLinks.nth(i).getAttribute('href');
      if (href) {
        hrefs.push(href);
      }
    }

    // Verify CircleCI link is present
    const hasCircleCILink = hrefs.some(href =>
      href.includes('circleci.com') || href.includes('github.com')
    );
    expect(hasCircleCILink).toBe(true);

    // Verify the specific CircleCI pipeline link
    const circleCILink = hrefs.find(href => href.includes('circleci.com/gh/yetone/mirdb'));
    expect(circleCILink).toBeDefined();
  });

  test('TC3: Badge images have valid src attributes', async ({ page }) => {
    // Find badge images
    const badgesSection = page.locator('#badges');
    const badgeImages = badgesSection.locator('img');
    const imageCount = await badgeImages.count();

    expect(imageCount).toBeGreaterThanOrEqual(1);

    // Verify each badge image has a valid src
    for (let i = 0; i < imageCount; i++) {
      const src = await badgeImages.nth(i).getAttribute('src');
      expect(src).toBeTruthy();
      expect(src).toMatch(/^https?:\/\//); // Should be a URL
    }
  });

  test('TC3-integration: Badge image loads successfully with 200 status', async ({ page, request }) => {
    // Find badge images
    const badgesSection = page.locator('#badges');
    const badgeImages = badgesSection.locator('img');
    const imageCount = await badgeImages.count();

    expect(imageCount).toBeGreaterThanOrEqual(1);

    // Find a reliable badge to test (license badge from shields.io is always reliable)
    let testedSuccessfully = false;
    for (let i = 0; i < imageCount; i++) {
      const src = await badgeImages.nth(i).getAttribute('src');
      if (src && src.includes('img.shields.io/badge/license')) {
        // Test the license badge which is a static badge and always returns 200
        const response = await request.get(src);
        expect(response.status()).toBe(200);

        // Verify the response is an image
        const contentType = response.headers()['content-type'];
        expect(contentType).toMatch(/image\//);
        testedSuccessfully = true;
        break;
      }
    }

    // Ensure we tested at least one badge
    expect(testedSuccessfully).toBe(true);
  });

  test('Badges section has proper accessibility attributes', async ({ page }) => {
    const badgesSection = page.locator('#badges');

    // Check for aria-label on section
    const ariaLabel = await badgesSection.getAttribute('aria-label');
    expect(ariaLabel).toBeTruthy();

    // Check that badge links have aria-labels
    const badgeLinks = badgesSection.locator('a');
    const linkCount = await badgeLinks.count();

    for (let i = 0; i < linkCount; i++) {
      const linkAriaLabel = await badgeLinks.nth(i).getAttribute('aria-label');
      expect(linkAriaLabel).toBeTruthy();
    }

    // Check that badge images have alt text
    const badgeImages = badgesSection.locator('img');
    const imageCount = await badgeImages.count();

    for (let i = 0; i < imageCount; i++) {
      const alt = await badgeImages.nth(i).getAttribute('alt');
      expect(alt).toBeTruthy();
    }
  });

  test('Badge links open in new tab with security attributes', async ({ page }) => {
    const badgesSection = page.locator('#badges');
    const badgeLinks = badgesSection.locator('a');
    const linkCount = await badgeLinks.count();

    for (let i = 0; i < linkCount; i++) {
      const link = badgeLinks.nth(i);

      // Check target="_blank"
      const target = await link.getAttribute('target');
      expect(target).toBe('_blank');

      // Check rel="noopener noreferrer"
      const rel = await link.getAttribute('rel');
      expect(rel).toContain('noopener');
    }
  });
});

test.describe('Theme Toggle - Dark/Light Mode (Scenario 18)', () => {
  test.beforeEach(async ({ page }) => {
    // Clear localStorage before each test
    await page.goto('/');
    await page.evaluate(() => localStorage.clear());
    await page.reload();
    await waitForLoad(page);
  });

  test('TC1: Theme toggle button exists and is visible', async ({ page }) => {
    // Test Case 1: Locate theme toggle button on page
    // Expected: Theme toggle control exists and is visible

    const themeToggle = page.locator('#theme-toggle');
    await expect(themeToggle).toBeVisible();

    // Verify it's a button
    const tagName = await themeToggle.evaluate(el => el.tagName.toLowerCase());
    expect(tagName).toBe('button');

    // Verify it has an accessible label
    const ariaLabel = await themeToggle.getAttribute('aria-label');
    expect(ariaLabel).toBeTruthy();
    expect(ariaLabel).toMatch(/switch to (dark|light) mode/i);
  });

  test('TC2: Click theme toggle and check background color change', async ({ page }) => {
    // Test Case 2: Click theme toggle and check background color change
    // Expected: Background color changes between dark and light values

    const themeToggle = page.locator('#theme-toggle');
    await expect(themeToggle).toBeVisible();

    // Get initial background color
    const initialBgColor = await page.evaluate(() => {
      return window.getComputedStyle(document.body).backgroundColor;
    });

    // Click the toggle
    await themeToggle.click();

    // Wait for transition
    await page.waitForTimeout(400);

    // Get new background color
    const newBgColor = await page.evaluate(() => {
      return window.getComputedStyle(document.body).backgroundColor;
    });

    // Background color should have changed
    expect(newBgColor).not.toBe(initialBgColor);

    // Verify the theme class was applied
    const hasThemeClass = await page.evaluate(() => {
      return document.documentElement.classList.contains('dark-theme') ||
             document.documentElement.classList.contains('light-theme');
    });
    expect(hasThemeClass).toBe(true);
  });

  test('TC2-extended: Toggle changes aria-label appropriately', async ({ page }) => {
    const themeToggle = page.locator('#theme-toggle');
    await expect(themeToggle).toBeVisible();

    // Get initial aria-label
    const initialAriaLabel = await themeToggle.getAttribute('aria-label');

    // Click the toggle
    await themeToggle.click();
    await page.waitForTimeout(100);

    // Get new aria-label
    const newAriaLabel = await themeToggle.getAttribute('aria-label');

    // Aria-label should have changed to indicate the opposite action
    expect(newAriaLabel).not.toBe(initialAriaLabel);
    expect(newAriaLabel).toMatch(/switch to (dark|light) mode/i);
  });

  test('TC3: Toggle theme, reload page, check persisted theme', async ({ page }) => {
    // Test Case 3: Toggle theme, reload page, check persisted theme
    // Expected: Theme preference persists after page reload

    const themeToggle = page.locator('#theme-toggle');
    await expect(themeToggle).toBeVisible();

    // Get initial theme state
    const initialHasDarkTheme = await page.evaluate(() => {
      return document.documentElement.classList.contains('dark-theme');
    });

    // Click the toggle to change theme
    await themeToggle.click();
    await page.waitForTimeout(400);

    // Verify theme changed
    const afterToggleHasDarkTheme = await page.evaluate(() => {
      return document.documentElement.classList.contains('dark-theme');
    });
    expect(afterToggleHasDarkTheme).not.toBe(initialHasDarkTheme);

    // Verify localStorage was updated
    const savedTheme = await page.evaluate(() => {
      return localStorage.getItem('mirdb-theme');
    });
    expect(savedTheme).toBeTruthy();
    expect(['dark', 'light']).toContain(savedTheme);

    // Reload the page
    await page.reload();
    await waitForLoad(page);

    // Verify theme persisted after reload
    const afterReloadHasDarkTheme = await page.evaluate(() => {
      return document.documentElement.classList.contains('dark-theme');
    });
    expect(afterReloadHasDarkTheme).toBe(afterToggleHasDarkTheme);

    // Verify aria-label matches the persisted theme
    const themeToggleAfterReload = page.locator('#theme-toggle');
    const ariaLabel = await themeToggleAfterReload.getAttribute('aria-label');
    if (afterReloadHasDarkTheme) {
      expect(ariaLabel).toMatch(/switch to light mode/i);
    } else {
      expect(ariaLabel).toMatch(/switch to dark mode/i);
    }
  });

  test('TC4: Check system preference detection', async ({ page }) => {
    // Test Case 4: Check system preference detection
    // Expected: Page respects prefers-color-scheme media query on initial load

    // First, ensure no saved preference
    await page.evaluate(() => localStorage.clear());

    // Emulate dark color scheme preference
    await page.emulateMedia({ colorScheme: 'dark' });

    // Reload to apply the emulated preference
    await page.reload();
    await waitForLoad(page);

    // Verify dark theme is applied (either via class or CSS variables)
    const hasDarkStyles = await page.evaluate(() => {
      const root = document.documentElement;
      const bgColor = window.getComputedStyle(document.body).backgroundColor;
      // Dark background should have low RGB values
      const rgb = bgColor.match(/\d+/g);
      if (rgb && rgb.length >= 3) {
        const [r, g, b] = rgb.map(Number);
        // Dark colors typically have low luminance
        return (r + g + b) / 3 < 128;
      }
      // Fallback: check if dark-theme class is applied
      return root.classList.contains('dark-theme');
    });
    expect(hasDarkStyles).toBe(true);

    // Now emulate light color scheme preference
    await page.evaluate(() => localStorage.clear());
    await page.emulateMedia({ colorScheme: 'light' });

    // Reload to apply the emulated preference
    await page.reload();
    await waitForLoad(page);

    // Verify light theme is applied
    const hasLightStyles = await page.evaluate(() => {
      const root = document.documentElement;
      const bgColor = window.getComputedStyle(document.body).backgroundColor;
      // Light background should have high RGB values
      const rgb = bgColor.match(/\d+/g);
      if (rgb && rgb.length >= 3) {
        const [r, g, b] = rgb.map(Number);
        // Light colors typically have high luminance
        return (r + g + b) / 3 >= 128;
      }
      // Fallback: check if light-theme class is applied
      return root.classList.contains('light-theme');
    });
    expect(hasLightStyles).toBe(true);
  });

  test('Theme toggle is keyboard accessible', async ({ page }) => {
    const themeToggle = page.locator('#theme-toggle');
    await expect(themeToggle).toBeVisible();

    // Tab to the toggle (it should be focusable)
    await themeToggle.focus();

    // Verify it received focus
    const isFocused = await page.evaluate(() => {
      return document.activeElement?.id === 'theme-toggle';
    });
    expect(isFocused).toBe(true);

    // Get initial theme state
    const initialHasDarkTheme = await page.evaluate(() => {
      return document.documentElement.classList.contains('dark-theme');
    });

    // Press Enter to toggle
    await page.keyboard.press('Enter');
    await page.waitForTimeout(100);

    // Verify theme changed
    const afterEnterHasDarkTheme = await page.evaluate(() => {
      return document.documentElement.classList.contains('dark-theme');
    });
    expect(afterEnterHasDarkTheme).not.toBe(initialHasDarkTheme);

    // Press Space to toggle back
    await page.keyboard.press('Space');
    await page.waitForTimeout(100);

    // Verify theme changed back
    const afterSpaceHasDarkTheme = await page.evaluate(() => {
      return document.documentElement.classList.contains('dark-theme');
    });
    expect(afterSpaceHasDarkTheme).toBe(initialHasDarkTheme);
  });

  test('Theme toggle icons update correctly', async ({ page }) => {
    const themeToggle = page.locator('#theme-toggle');
    const sunIcon = page.locator('.theme-icon-sun');
    const moonIcon = page.locator('.theme-icon-moon');

    await expect(themeToggle).toBeVisible();

    // Get initial icon visibility
    const initialSunDisplay = await sunIcon.evaluate(el => window.getComputedStyle(el).display);
    const initialMoonDisplay = await moonIcon.evaluate(el => window.getComputedStyle(el).display);

    // Click to toggle theme
    await themeToggle.click();
    await page.waitForTimeout(100);

    // Get new icon visibility
    const newSunDisplay = await sunIcon.evaluate(el => window.getComputedStyle(el).display);
    const newMoonDisplay = await moonIcon.evaluate(el => window.getComputedStyle(el).display);

    // Icons should have swapped visibility
    expect(newSunDisplay).not.toBe(initialSunDisplay);
    expect(newMoonDisplay).not.toBe(initialMoonDisplay);
  });
});
