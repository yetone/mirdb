import { test, expect } from '@playwright/test';

test.describe('Navigation Functionality', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Navigation menu or links are visible on the page', async ({ page }) => {
    // Check for navigation bar presence
    const navbar = page.locator('[data-testid="navbar"]');
    await expect(navbar).toBeVisible();

    // Verify navigation links are present
    const navLinks = page.locator('[data-testid="nav-links"]');
    await expect(navLinks).toBeVisible();

    // Check that navigation contains expected links
    const featuresLink = navLinks.locator('a[href="#features"]');
    const quickStartLink = navLinks.locator('a[href="#quick-start"]');

    await expect(featuresLink).toBeVisible();
    await expect(quickStartLink).toBeVisible();
  });

  test('TC2: Click navigation link to Features section', async ({ page }) => {
    // Get initial scroll position
    const initialScroll = await page.evaluate(() => window.scrollY);

    // Click on Features navigation link
    const featuresLink = page.locator('[data-testid="nav-links"] a[href="#features"]');
    await expect(featuresLink).toBeVisible();
    await featuresLink.click();

    // Wait for smooth scrolling to complete
    await page.waitForTimeout(500);

    // Verify URL has updated with hash
    await expect(page).toHaveURL(/#features/);

    // Verify the Features section is in view
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport();
  });

  test('TC3: Click navigation link to Quick Start section', async ({ page }) => {
    // Click on Quick Start navigation link
    const quickStartLink = page.locator('[data-testid="nav-links"] a[href="#quick-start"]');
    await expect(quickStartLink).toBeVisible();
    await quickStartLink.click();

    // Wait for smooth scrolling to complete
    await page.waitForTimeout(500);

    // Verify URL has updated with hash
    await expect(page).toHaveURL(/#quick-start/);

    // Verify the Quick Start section is in view
    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeInViewport();
  });

  test('TC4: Click navigation link to Documentation', async ({ page }) => {
    // Documentation link can be either:
    // 1. An internal anchor link (e.g., #quick-start)
    // 2. An external link to full documentation

    const docsLink = page.locator('[data-testid="nav-links"] a').filter({ hasText: /documentation|docs/i });

    // If there's a dedicated docs link
    const docsLinkCount = await docsLink.count();
    if (docsLinkCount > 0) {
      await expect(docsLink.first()).toBeVisible();

      // Check if it's an external link
      const href = await docsLink.first().getAttribute('href');

      if (href && href.startsWith('http')) {
        // External documentation link - verify it has target="_blank"
        await expect(docsLink.first()).toHaveAttribute('target', '_blank');
        await expect(docsLink.first()).toHaveAttribute('href', /github\.com|docs/);
      } else {
        // Internal documentation link - click and verify navigation
        await docsLink.first().click();
        await page.waitForTimeout(500);
        // Should navigate to a documentation section
      }
    } else {
      // Fallback: Use GitHub link as documentation access
      const githubLink = page.locator('[data-testid="nav-links"] a[href*="github"]');
      await expect(githubLink).toBeVisible();
      await expect(githubLink).toHaveAttribute('target', '_blank');
    }
  });

  test('TC5: Verify all critical info accessible within 2 clicks', async ({ page }) => {
    // Test 1: Features accessible from navigation (1 click)
    const featuresLink = page.locator('[data-testid="nav-links"] a[href="#features"]');
    await expect(featuresLink).toBeVisible();
    await featuresLink.click();
    await page.waitForTimeout(500);

    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport();
    await expect(featuresSection.locator('h2')).toContainText(/features/i);

    // Go back to top for next test
    await page.goto('/');

    // Test 2: Installation/Quick Start accessible from navigation (1 click)
    const quickStartLink = page.locator('[data-testid="nav-links"] a[href="#quick-start"]');
    await expect(quickStartLink).toBeVisible();
    await quickStartLink.click();
    await page.waitForTimeout(500);

    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeInViewport();

    // Verify installation instructions are present
    const installCode = quickStartSection.locator('code');
    await expect(installCode.first()).toBeVisible();

    // Go back to top for next test
    await page.goto('/');

    // Test 3: Usage examples accessible (within Quick Start, so 1 click)
    await quickStartLink.click();
    await page.waitForTimeout(500);

    // Quick Start section should contain usage examples (SET/GET commands)
    const codeBlocks = quickStartSection.locator('pre code');
    await expect(codeBlocks.first()).toBeVisible();
  });

  test('Navigation uses smooth scrolling', async ({ page }) => {
    // Start at the top
    await page.evaluate(() => window.scrollTo(0, 0));

    // Click on a navigation link
    const featuresLink = page.locator('[data-testid="nav-links"] a[href="#features"]');
    await expect(featuresLink).toBeVisible();

    // Record scroll positions over time
    const scrollPositions: number[] = [];

    // Start monitoring scroll
    await page.evaluate(() => {
      (window as any).__scrollPositions = [];
      const recordScroll = () => {
        (window as any).__scrollPositions.push(window.scrollY);
      };
      window.addEventListener('scroll', recordScroll);
    });

    await featuresLink.click();

    // Wait for scrolling to complete
    await page.waitForTimeout(600);

    // Get recorded positions
    const positions = await page.evaluate(() => (window as any).__scrollPositions);

    // Verify smooth scrolling CSS is applied
    const htmlElement = page.locator('html');
    const scrollBehavior = await htmlElement.evaluate((el) => getComputedStyle(el).scrollBehavior);
    expect(scrollBehavior).toBe('smooth');
  });
});
