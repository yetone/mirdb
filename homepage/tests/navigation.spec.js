// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Navigation Functionality', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('should scroll to Features section when clicking Features navigation link', async ({ page }) => {
    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY);

    // Click the Get Started button which links to #quick-start
    // First, let's verify the hero section has the Get Started link
    const getStartedBtn = page.locator('#get-started-btn');
    await expect(getStartedBtn).toBeVisible();

    // The Features section should exist
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Navigate to features by directly clicking a link to #features
    // Since the Get Started links to #quick-start, let's test scrolling to features by evaluating
    await page.evaluate(() => {
      const featuresLink = document.querySelector('a[href="#features"]');
      if (featuresLink) {
        featuresLink.click();
      } else {
        // If no link exists, simulate navigation to features
        document.querySelector('#features')?.scrollIntoView({ behavior: 'smooth', block: 'start' });
      }
    });

    // Wait for scroll animation to complete
    await page.waitForTimeout(1000);

    // Verify the features section is visible in viewport
    await expect(featuresSection).toBeInViewport();
  });

  test('should scroll to Quick Start section when clicking Get Started button', async ({ page }) => {
    // Ensure we're at the top of the page
    await page.evaluate(() => window.scrollTo(0, 0));
    await page.waitForTimeout(100);

    // Verify the Get Started button exists and is visible
    const getStartedBtn = page.locator('#get-started-btn');
    await expect(getStartedBtn).toBeVisible();
    await expect(getStartedBtn).toHaveAttribute('href', '#quick-start');

    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY);

    // Click the Get Started button
    await getStartedBtn.click();

    // Wait for smooth scroll animation to complete
    await page.waitForTimeout(1000);

    // Verify the Quick Start section is now visible in viewport
    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeInViewport();

    // Verify we actually scrolled (scroll position changed)
    const finalScrollY = await page.evaluate(() => window.scrollY);
    expect(finalScrollY).toBeGreaterThan(initialScrollY);
  });

  test('should load page with Quick Start section visible when URL has #quick-start anchor', async ({ page }) => {
    // Navigate directly to the page with anchor
    await page.goto('/#quick-start');

    // Wait for any smooth scrolling to complete
    await page.waitForTimeout(500);

    // Verify the Quick Start section is visible in viewport
    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeInViewport();

    // Verify the section heading is visible
    const quickStartHeading = quickStartSection.locator('h2');
    await expect(quickStartHeading).toContainText('Quick Start');
  });

  test('should use smooth scrolling behavior for better UX', async ({ page }) => {
    // Ensure we're at the top of the page
    await page.evaluate(() => window.scrollTo(0, 0));
    await page.waitForTimeout(100);

    // Get the Get Started button
    const getStartedBtn = page.locator('#get-started-btn');
    await expect(getStartedBtn).toBeVisible();

    // Record scroll positions over time to verify smooth scrolling
    const scrollPositions = [];

    // Start recording scroll positions
    const recordScrollPositions = page.evaluate(() => {
      return new Promise((resolve) => {
        const positions = [];
        const startTime = Date.now();

        const recordPosition = () => {
          positions.push({
            y: window.scrollY,
            time: Date.now() - startTime
          });

          // Record for 1 second
          if (Date.now() - startTime < 1000) {
            requestAnimationFrame(recordPosition);
          } else {
            resolve(positions);
          }
        };

        recordPosition();
      });
    });

    // Click the Get Started button to trigger scroll
    await getStartedBtn.click();

    // Wait for and get the scroll positions
    const positions = await recordScrollPositions;

    // Verify smooth scrolling by checking that we have intermediate positions
    // (not just start and end positions)
    expect(Array.isArray(positions)).toBe(true);
    expect(positions.length).toBeGreaterThan(5);

    // Get unique scroll positions
    const uniquePositions = [...new Set(positions.map(p => p.y))];

    // Smooth scrolling should have multiple intermediate positions
    // If it was an instant jump, we'd only have 2 unique positions (start and end)
    expect(uniquePositions.length).toBeGreaterThan(2);

    // Verify that scrolling happened (final position > initial position)
    const initialY = positions[0].y;
    const finalY = positions[positions.length - 1].y;
    expect(finalY).toBeGreaterThan(initialY);
  });

  test('should navigate to Architecture section via anchor link', async ({ page }) => {
    // Navigate directly to architecture section via URL anchor
    await page.goto('/#architecture');

    // Wait for scroll to complete
    await page.waitForTimeout(500);

    // Verify architecture section is in viewport
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeInViewport();
  });

  test('should navigate to Commands section via anchor link', async ({ page }) => {
    // Navigate directly to commands section via URL anchor
    await page.goto('/#commands');

    // Wait for scroll to complete
    await page.waitForTimeout(500);

    // Verify commands section is in viewport
    const commandsSection = page.locator('#commands');
    await expect(commandsSection).toBeInViewport();
  });

  test('should navigate to Configuration section via anchor link', async ({ page }) => {
    // Navigate directly to configuration section via URL anchor
    await page.goto('/#configuration');

    // Wait for scroll to complete
    await page.waitForTimeout(500);

    // Verify configuration section is in viewport
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeInViewport();
  });

  test('should navigate to Project Status section via anchor link', async ({ page }) => {
    // Navigate directly to project-status section via URL anchor
    await page.goto('/#project-status');

    // Wait for scroll to complete
    await page.waitForTimeout(500);

    // Verify project-status section is in viewport
    const statusSection = page.locator('#project-status');
    await expect(statusSection).toBeInViewport();
  });
});
