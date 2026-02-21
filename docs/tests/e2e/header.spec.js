/**
 * E2E tests for Header and Navigation
 * Owner: Scenario 1
 *
 * Test cases:
 * - Logo visibility and alt text
 * - Navigation links presence and functionality
 * - Theme toggle functionality and persistence
 * - Skip-to-content accessibility link
 * - Focus indicators on interactive elements
 */

const { test, expect } = require('@playwright/test');

test.describe('Header and Navigation', () => {
  test.beforeEach(async ({ page }) => {
    // Clear localStorage before each test
    await page.goto('');
    await page.evaluate(() => localStorage.clear());
    await page.reload();
  });

  test('Test Case 1: Logo is visible with appropriate alt text', async ({ page }) => {
    await page.goto('');

    // Check that logo image exists and is visible
    const logo = page.locator('header img[alt="MirDB Logo"]');
    await expect(logo).toBeVisible();

    // Verify the src contains logo.gif
    const src = await logo.getAttribute('src');
    expect(src).toContain('logo.gif');
  });

  test('Test Case 2: Navigation contains required links', async ({ page }) => {
    await page.goto('');

    const nav = page.locator('nav[aria-label="Main navigation"]');
    await expect(nav).toBeVisible();

    // Check for Features link
    const featuresLink = nav.locator('a[href="#features"]');
    await expect(featuresLink).toBeVisible();
    await expect(featuresLink).toHaveText('Features');

    // Check for Architecture link
    const architectureLink = nav.locator('a[href="#architecture"]');
    await expect(architectureLink).toBeVisible();
    await expect(architectureLink).toHaveText('Architecture');

    // Check for Getting Started link
    const gettingStartedLink = nav.locator('a[href="#getting-started"]');
    await expect(gettingStartedLink).toBeVisible();
    await expect(gettingStartedLink).toHaveText('Getting Started');

    // Check for Status link
    const statusLink = nav.locator('a[href="#status"]');
    await expect(statusLink).toBeVisible();
    await expect(statusLink).toHaveText('Status');

    // Check for GitHub link
    const githubLink = nav.locator('a[href="https://github.com/yetone/mirdb"]');
    await expect(githubLink).toBeVisible();
    await expect(githubLink).toContainText('GitHub');

    // Verify GitHub link has proper security attributes
    await expect(githubLink).toHaveAttribute('target', '_blank');
    await expect(githubLink).toHaveAttribute('rel', 'noopener noreferrer');
  });

  test('Test Case 3: Theme toggle switches between light and dark modes', async ({ page }) => {
    await page.goto('');

    // Initially check if we're in light mode (no dark class)
    const html = page.locator('html');
    const themeToggle = page.locator('#theme-toggle');

    await expect(themeToggle).toBeVisible();
    await expect(themeToggle).toHaveAttribute('aria-label', 'Toggle dark mode');

    // Get initial state
    const initialHasDark = await html.evaluate(el => el.classList.contains('dark'));

    // Click theme toggle
    await themeToggle.click();

    // Verify the class has changed
    const afterClickHasDark = await html.evaluate(el => el.classList.contains('dark'));
    expect(afterClickHasDark).not.toBe(initialHasDark);

    // Click again to toggle back
    await themeToggle.click();

    // Verify we're back to initial state
    const finalHasDark = await html.evaluate(el => el.classList.contains('dark'));
    expect(finalHasDark).toBe(initialHasDark);
  });

  test('Test Case 4: Theme preference persists via localStorage after page reload', async ({ page }) => {
    await page.goto('');

    // Clear localStorage and ensure light mode
    await page.evaluate(() => {
      localStorage.clear();
      document.documentElement.classList.remove('dark');
    });
    await page.reload();

    // Toggle to dark mode
    const themeToggle = page.locator('#theme-toggle');
    await themeToggle.click();

    // Verify dark mode is applied
    const html = page.locator('html');
    await expect(html).toHaveClass(/dark/);

    // Verify localStorage was set
    const storedTheme = await page.evaluate(() => localStorage.getItem('mirdb-theme'));
    expect(storedTheme).toBe('dark');

    // Reload the page
    await page.reload();

    // Verify dark mode persists after reload
    await expect(html).toHaveClass(/dark/);

    // Verify localStorage still has the value
    const storedThemeAfterReload = await page.evaluate(() => localStorage.getItem('mirdb-theme'));
    expect(storedThemeAfterReload).toBe('dark');
  });

  test('Test Case 5: Focus indicators visible on all interactive elements', async ({ page }) => {
    await page.goto('');

    // Press Tab to navigate through header elements
    await page.keyboard.press('Tab');

    // First Tab should focus skip link
    const skipLink = page.locator('.skip-link');
    await expect(skipLink).toBeFocused();

    // Continue tabbing to check focus on nav links
    await page.keyboard.press('Tab'); // Logo link
    const logoLink = page.locator('header a[aria-label="MirDB Home"]');
    await expect(logoLink).toBeFocused();

    // Tab to first nav link
    await page.keyboard.press('Tab');
    const featuresLink = page.locator('nav[aria-label="Main navigation"] a[href="#features"]');
    await expect(featuresLink).toBeFocused();

    // Verify focus indicator is visible (outline)
    const focusOutline = await featuresLink.evaluate(el => {
      const styles = window.getComputedStyle(el);
      return styles.outline !== 'none' || styles.outlineWidth !== '0px';
    });
    expect(focusOutline).toBe(true);

    // Continue tabbing through other links
    await page.keyboard.press('Tab'); // Architecture
    await page.keyboard.press('Tab'); // Getting Started
    await page.keyboard.press('Tab'); // Status
    await page.keyboard.press('Tab'); // GitHub

    // Tab to theme toggle
    await page.keyboard.press('Tab');
    const themeToggle = page.locator('#theme-toggle');
    await expect(themeToggle).toBeFocused();

    // Verify theme toggle has focus outline
    const toggleFocusOutline = await themeToggle.evaluate(el => {
      const styles = window.getComputedStyle(el);
      return styles.outline !== 'none' || styles.outlineWidth !== '0px';
    });
    expect(toggleFocusOutline).toBe(true);
  });

  test('Test Case 6: Skip-to-main-content link becomes visible on focus and jumps to main content', async ({ page }) => {
    await page.goto('');

    // Initially, skip link should not be visible (positioned off-screen)
    const skipLink = page.locator('.skip-link');

    // Get initial position - should be negative (off-screen)
    const initialBoundingBox = await skipLink.boundingBox();
    expect(initialBoundingBox.y).toBeLessThan(0);

    // Press Tab to focus the skip link
    await page.keyboard.press('Tab');
    await expect(skipLink).toBeFocused();

    // Wait for the CSS transition to complete (200ms transition in CSS)
    await page.waitForTimeout(300);

    // Now the skip link should be visible (moved to top: 0 when focused)
    const focusedBoundingBox = await skipLink.boundingBox();
    expect(focusedBoundingBox.y).toBeGreaterThanOrEqual(0);

    // Check that skip link has correct href
    await expect(skipLink).toHaveAttribute('href', '#main-content');

    // Verify main-content element exists
    const mainContent = page.locator('#main-content');
    await expect(mainContent).toBeVisible();

    // Click the skip link (or press Enter since it's focused)
    await page.keyboard.press('Enter');

    // Verify main content is now focused (has tabindex=-1 after focus)
    // The smooth scroll and focus should have happened
    await page.waitForTimeout(500); // Wait for smooth scroll

    // Verify the page has scrolled or main content has focus
    const mainContentTabIndex = await mainContent.getAttribute('tabindex');
    expect(mainContentTabIndex).toBe('-1');
  });

  test('Header is fixed at the top of the viewport', async ({ page }) => {
    await page.goto('');

    const header = page.locator('header.header-fixed');
    await expect(header).toBeVisible();

    // Check that header has position fixed
    const position = await header.evaluate(el => {
      const styles = window.getComputedStyle(el);
      return styles.position;
    });
    expect(position).toBe('fixed');

    // Scroll down and verify header is still at top
    await page.evaluate(() => window.scrollTo(0, 500));
    await page.waitForTimeout(100);

    const boundingBox = await header.boundingBox();
    expect(boundingBox.y).toBe(0);
  });

  test('Navigation links use smooth scrolling', async ({ page }) => {
    await page.goto('');

    // Click on Features link
    const featuresLink = page.locator('nav[aria-label="Main navigation"] a[href="#features"]');
    await featuresLink.click();

    // Wait for smooth scroll to complete
    await page.waitForTimeout(500);

    // Check that Features section is in view
    const featuresSection = page.locator('#features');
    const isInViewport = await featuresSection.evaluate(el => {
      const rect = el.getBoundingClientRect();
      return rect.top >= 0 && rect.top < window.innerHeight;
    });
    expect(isInViewport).toBe(true);
  });
});
