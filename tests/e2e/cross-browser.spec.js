/**
 * Cross-Browser Compatibility E2E Tests
 * Owner: Scenario 18 - Cross-Browser Compatibility
 *
 * Tests:
 * - Verify homepage renders correctly across Chrome, Firefox, Safari, Edge
 * - Check all major sections are visible and properly laid out
 * - Ensure no layout issues or visual regressions
 */
import { test, expect } from '@playwright/test';

test.describe('Cross-Browser Compatibility', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('homepage renders correctly with all sections visible', async ({ page, browserName }) => {
    // Verify page title
    await expect(page).toHaveTitle(/MirDB/);

    // Verify header is visible and properly rendered
    const header = page.locator('#header');
    await expect(header).toBeVisible();

    // Verify logo is visible
    const logo = page.locator('#header img[alt="MirDB Logo"]');
    await expect(logo).toBeVisible();

    // Verify navigation links are present (desktop nav, not mobile menu)
    const desktopNav = page.locator('#header .hidden.md\\:flex');
    const featuresLink = desktopNav.locator('a[href="#features"]');
    const quickStartLink = desktopNav.locator('a[href="#quick-start"]');
    await expect(featuresLink).toBeVisible();
    await expect(quickStartLink).toBeVisible();

    // Verify Hero section
    const hero = page.locator('#hero');
    await expect(hero).toBeVisible();
    await expect(hero.locator('h1')).toContainText('MirDB');

    // Verify Demo section
    const demo = page.locator('#demo');
    await expect(demo).toBeVisible();
    await expect(demo.locator('h2')).toContainText('See It In Action');

    // Verify Features section
    const features = page.locator('#features');
    await expect(features).toBeVisible();
    await expect(features.locator('h2')).toContainText('Features');
    // Check feature cards are rendered
    const featureCards = features.locator('.bg-white.p-6.rounded-lg.shadow-md');
    await expect(featureCards).toHaveCount(6);

    // Verify Quick Start section
    const quickStart = page.locator('#quick-start');
    await expect(quickStart).toBeVisible();
    await expect(quickStart.locator('h2')).toContainText('Quick Start');

    // Verify Architecture section
    const architecture = page.locator('#architecture');
    await expect(architecture).toBeVisible();
    await expect(architecture.locator('h2')).toContainText('Architecture');

    // Verify Comparison section
    const comparison = page.locator('#comparison');
    await expect(comparison).toBeVisible();
    await expect(comparison.locator('h2')).toContainText('MirDB vs Memcached');

    // Verify Commands section
    const commands = page.locator('#commands');
    await expect(commands).toBeVisible();
    await expect(commands.locator('h2')).toContainText('Supported Commands');

    // Verify Status section
    const status = page.locator('#status');
    await expect(status).toBeVisible();
    await expect(status.locator('h2')).toContainText('Project Status');

    // Verify Footer
    const footer = page.locator('#footer');
    await expect(footer).toBeVisible();

    // Log browser name for reporting
    console.log(`Cross-browser test passed for: ${browserName}`);
  });

  test('page layout has no horizontal overflow', async ({ page, browserName }) => {
    // Check that body doesn't have horizontal overflow
    const bodyWidth = await page.evaluate(() => document.body.scrollWidth);
    const viewportWidth = await page.evaluate(() => window.innerWidth);

    // Allow small tolerance for scrollbars
    expect(bodyWidth).toBeLessThanOrEqual(viewportWidth + 20);
    console.log(`No horizontal overflow in: ${browserName} (body: ${bodyWidth}px, viewport: ${viewportWidth}px)`);
  });

  test('images load correctly', async ({ page, browserName }) => {
    // Wait for network to be idle
    await page.waitForLoadState('networkidle');

    // Check logo image
    const logo = page.locator('img[alt="MirDB Logo"]');
    await expect(logo).toBeVisible();

    // Check usage demo image
    const usageDemo = page.locator('img[alt*="MirDB usage demonstration"]');
    await expect(usageDemo).toBeVisible();

    // Verify images have natural dimensions (loaded successfully)
    const logoLoaded = await logo.evaluate((img) => img.naturalWidth > 0);
    const demoLoaded = await usageDemo.evaluate((img) => img.naturalWidth > 0);

    expect(logoLoaded).toBeTruthy();
    expect(demoLoaded).toBeTruthy();
    console.log(`Images loaded correctly in: ${browserName}`);
  });

  test('navigation links are clickable and work', async ({ page, browserName }) => {
    // Use desktop nav links (not mobile menu)
    const desktopNav = page.locator('#header .hidden.md\\:flex');

    // Test clicking on Features link scrolls to section
    await desktopNav.locator('a[href="#features"]').click();
    await page.waitForTimeout(500); // Wait for scroll

    const features = page.locator('#features');
    await expect(features).toBeInViewport();

    // Test Quick Start link
    await desktopNav.locator('a[href="#quick-start"]').click();
    await page.waitForTimeout(500);

    const quickStart = page.locator('#quick-start');
    await expect(quickStart).toBeInViewport();

    console.log(`Navigation works correctly in: ${browserName}`);
  });

  test('text content renders properly', async ({ page, browserName }) => {
    // Check key text content is visible and readable
    const heroText = page.locator('#hero p').first();
    await expect(heroText).toContainText('persistent key-value store');

    // Check feature descriptions are present
    const featureTexts = page.locator('#features .bg-white p.text-gray-600');
    await expect(featureTexts.first()).toBeVisible();

    // Check code blocks in quick start
    const codeBlocks = page.locator('#quick-start pre code');
    await expect(codeBlocks.first()).toBeVisible();

    console.log(`Text content renders properly in: ${browserName}`);
  });

  test('comparison table renders correctly', async ({ page, browserName }) => {
    // Navigate to comparison section
    const comparison = page.locator('#comparison');
    await comparison.scrollIntoViewIfNeeded();

    // Check table is visible
    const table = page.locator('#comparison table');
    await expect(table).toBeVisible();

    // Check table headers
    const headers = page.locator('#comparison table th');
    await expect(headers).toHaveCount(3);

    // Check table rows contain expected features
    const persistenceRow = page.locator('#comparison [data-feature="persistence"]');
    await expect(persistenceRow).toBeVisible();

    console.log(`Comparison table renders correctly in: ${browserName}`);
  });

  test('footer links are present and visible', async ({ page, browserName }) => {
    const footer = page.locator('#footer');
    await footer.scrollIntoViewIfNeeded();

    // Check GitHub link
    const githubLink = footer.locator('a[href*="github.com/yetone/mirdb"]').first();
    await expect(githubLink).toBeVisible();

    // Check license link
    const licenseLink = footer.locator('a[href*="LICENSE"]').first();
    await expect(licenseLink).toBeVisible();

    // Check author attribution
    const authorLink = footer.locator('a[data-author="yetone"]');
    await expect(authorLink).toBeVisible();

    console.log(`Footer links present in: ${browserName}`);
  });
});
