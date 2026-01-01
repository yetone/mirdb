const { test, expect } = require('@playwright/test');

test.describe('Navigation and Links', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Fixed navigation header is present with correct positioning', async ({ page }) => {
    // Query for navigation header element
    const navHeader = page.locator('[data-testid="navigation-header"]');

    // Verify navigation header exists and is visible
    await expect(navHeader).toBeVisible();

    // Verify nav element has fixed positioning
    const position = await navHeader.evaluate(el => getComputedStyle(el).position);
    expect(position).toBe('fixed');

    // Verify nav is at the top of the page
    const top = await navHeader.evaluate(el => getComputedStyle(el).top);
    expect(top).toBe('0px');

    // Verify nav has full width
    const width = await navHeader.evaluate(el => getComputedStyle(el).width);
    const viewportWidth = await page.viewportSize();
    expect(parseInt(width)).toBeGreaterThanOrEqual(viewportWidth.width - 20); // Allow for scrollbar

    // Verify nav has a high z-index to stay on top
    const zIndex = await navHeader.evaluate(el => getComputedStyle(el).zIndex);
    expect(parseInt(zIndex)).toBeGreaterThanOrEqual(100);

    // Verify nav contains navigation links
    const navLinks = page.locator('[data-testid="nav-links"]');
    await expect(navLinks).toBeVisible();

    // Verify nav logo is present
    const navLogo = page.locator('[data-testid="nav-logo"]');
    await expect(navLogo).toBeVisible();
    await expect(navLogo).toHaveText('MirDB');
  });

  test('TC2: GitHub link points to MirDB repository URL', async ({ page }) => {
    // Query for GitHub link and check href attribute
    const githubLink = page.locator('[data-testid="nav-link-github"]');

    // Verify GitHub link exists and is visible
    await expect(githubLink).toBeVisible();

    // Verify link text
    await expect(githubLink).toHaveText('GitHub');

    // Verify href points to MirDB repository
    const href = await githubLink.getAttribute('href');
    expect(href).toBe('https://github.com/yetone/mirdb');

    // Verify link opens in new tab
    const target = await githubLink.getAttribute('target');
    expect(target).toBe('_blank');

    // Verify noopener for security
    const rel = await githubLink.getAttribute('rel');
    expect(rel).toContain('noopener');
  });

  test('TC3: Documentation link is present and has valid href', async ({ page }) => {
    // Query for documentation link
    const docsLink = page.locator('[data-testid="nav-link-docs"]');

    // Verify documentation link exists and is visible
    await expect(docsLink).toBeVisible();

    // Verify link text
    await expect(docsLink).toHaveText('Docs');

    // Verify href has a valid documentation URL
    const href = await docsLink.getAttribute('href');
    expect(href).toBeTruthy();
    expect(href).toContain('github.com/yetone/mirdb');

    // Verify link opens in new tab
    const target = await docsLink.getAttribute('target');
    expect(target).toBe('_blank');

    // Verify noopener for security
    const rel = await docsLink.getAttribute('rel');
    expect(rel).toContain('noopener');
  });

  test('TC4: Clicking navigation links smoothly scrolls to corresponding sections', async ({ page }) => {
    // First, scroll down to ensure we're not at the top
    await page.evaluate(() => window.scrollTo(0, 500));
    await page.waitForTimeout(100);

    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY);
    expect(initialScrollY).toBeGreaterThan(0);

    // Click on Features link
    const featuresLink = page.locator('[data-testid="nav-link-features"]');
    await expect(featuresLink).toBeVisible();
    await featuresLink.click();

    // Wait for scroll to complete
    await page.waitForTimeout(500);

    // Verify features section is in viewport
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport({ ratio: 0.5 });

    // Now click on Quick Start link
    const quickstartLink = page.locator('[data-testid="nav-link-quickstart"]');
    await expect(quickstartLink).toBeVisible();
    await quickstartLink.click();

    // Wait for scroll to complete
    await page.waitForTimeout(500);

    // Verify quickstart section is in viewport
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeInViewport({ ratio: 0.5 });

    // Click logo to scroll back to top
    const navLogo = page.locator('[data-testid="nav-logo"]');
    await navLogo.click();

    // Wait for scroll to complete
    await page.waitForTimeout(500);

    // Verify we're back at the top (or near top considering fixed nav)
    const finalScrollY = await page.evaluate(() => window.scrollY);
    expect(finalScrollY).toBeLessThan(100);
  });

  test('TC5: All navigation links are keyboard accessible', async ({ page }) => {
    // Start from the beginning of the page
    await page.keyboard.press('Tab');

    // Tab through navigation - first link should be the logo
    let focusedElement = await page.evaluate(() => document.activeElement?.getAttribute('data-testid'));

    // Keep tabbing until we find navigation elements
    let foundNavElements = [];
    const navTestIds = ['nav-logo', 'nav-link-features', 'nav-link-quickstart', 'nav-link-github', 'nav-link-docs'];

    // Tab through the document and collect focused nav elements
    for (let i = 0; i < 20; i++) {
      focusedElement = await page.evaluate(() => document.activeElement?.getAttribute('data-testid'));
      if (focusedElement && navTestIds.includes(focusedElement) && !foundNavElements.includes(focusedElement)) {
        foundNavElements.push(focusedElement);

        // Verify the element is visible and interactive
        const element = page.locator(`[data-testid="${focusedElement}"]`);
        await expect(element).toBeVisible();

        // Verify element can receive focus (is an anchor tag)
        const tagName = await element.evaluate(el => el.tagName.toLowerCase());
        expect(tagName).toBe('a');
      }
      await page.keyboard.press('Tab');
    }

    // Verify all nav links were found and are keyboard accessible
    expect(foundNavElements).toContain('nav-logo');
    expect(foundNavElements).toContain('nav-link-features');
    expect(foundNavElements).toContain('nav-link-quickstart');
    expect(foundNavElements).toContain('nav-link-github');
    expect(foundNavElements).toContain('nav-link-docs');

    // Test that Enter key activates a navigation link
    // Reset to beginning
    await page.goto('/');

    // Tab to features link
    for (let i = 0; i < 10; i++) {
      await page.keyboard.press('Tab');
      focusedElement = await page.evaluate(() => document.activeElement?.getAttribute('data-testid'));
      if (focusedElement === 'nav-link-features') break;
    }

    // Press Enter to activate the link
    const initialScroll = await page.evaluate(() => window.scrollY);
    await page.keyboard.press('Enter');
    await page.waitForTimeout(500);

    // Verify scroll happened (indicating link was activated)
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport({ ratio: 0.3 });
  });
});
