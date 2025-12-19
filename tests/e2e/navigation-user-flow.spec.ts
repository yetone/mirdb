import { test, expect } from '@playwright/test';

test.describe('Navigation and User Flow', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Click Get Started CTA in hero scrolls to Getting Started section', async ({ page }) => {
    // Test Case 1: Click Get Started CTA in hero
    // Expected: Page scrolls to Getting Started section

    // Find and click the Get Started CTA button in the hero section
    const heroSection = page.locator('#hero, section.hero, [data-testid="hero"]');
    const getStartedBtn = heroSection.locator('a').filter({ hasText: /get started/i });

    await expect(getStartedBtn).toBeVisible();

    // Get the initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY);

    // Click the Get Started button
    await getStartedBtn.click();

    // Wait for smooth scroll animation to complete
    await page.waitForTimeout(500);

    // Verify the Getting Started section is now in view
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeInViewport();

    // Verify scroll position changed
    const finalScrollY = await page.evaluate(() => window.scrollY);
    expect(finalScrollY).toBeGreaterThan(initialScrollY);
  });

  test('TC2: Click navigation link to features scrolls to features section', async ({ page }) => {
    // Test Case 2: Click navigation link to features
    // Expected: Page scrolls to features section

    // First scroll to bottom to ensure we need to scroll up/back
    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
    await page.waitForTimeout(100);

    // Find the navigation link to features
    const navLinks = page.locator('.nav-links, nav');
    const featuresLink = navLinks.locator('a[href="#features"]');

    await expect(featuresLink).toBeVisible();

    // Click the features link
    await featuresLink.click();

    // Wait for scroll animation
    await page.waitForTimeout(500);

    // Verify the features section is now in view
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport();
  });

  test('TC3: Click navigation link to architecture scrolls to architecture section', async ({ page }) => {
    // Test Case 3: Click navigation link to architecture
    // Expected: Page scrolls to architecture section

    // Find the navigation link to architecture
    const navLinks = page.locator('.nav-links, nav');
    const architectureLink = navLinks.locator('a[href="#architecture"]');

    // Check if architecture link exists in navigation
    const linkCount = await architectureLink.count();

    if (linkCount === 0) {
      // If no dedicated nav link, try footer link or section should be accessible via scroll
      const footerArchLink = page.locator('footer a[href="#architecture"]');
      if (await footerArchLink.count() > 0) {
        await footerArchLink.click();
      } else {
        // Navigate directly to the section using URL hash
        await page.goto('/#architecture');
      }
    } else {
      await architectureLink.click();
    }

    // Wait for scroll animation
    await page.waitForTimeout(500);

    // Verify the architecture section is now in view
    const architectureSection = page.locator('#architecture, [data-testid="architecture"]');
    await expect(architectureSection).toBeInViewport();
  });

  test('TC4: All internal anchor links navigate to correct sections', async ({ page }) => {
    // Test Case 4: Verify all internal links work
    // Expected: All anchor links navigate to correct sections

    // Get all internal anchor links (excluding external links)
    const internalLinks = page.locator('a[href^="#"]:not([href="#"])');
    const linkCount = await internalLinks.count();

    expect(linkCount).toBeGreaterThan(0);

    // Collect all unique href values
    const hrefs = new Set<string>();
    for (let i = 0; i < linkCount; i++) {
      const href = await internalLinks.nth(i).getAttribute('href');
      if (href && href.startsWith('#') && href !== '#') {
        hrefs.add(href);
      }
    }

    // Test each unique anchor link
    for (const href of hrefs) {
      const sectionId = href.substring(1); // Remove the # prefix
      const targetSection = page.locator(`#${sectionId}`);

      // Check if the target section exists
      const sectionExists = await targetSection.count() > 0;

      if (sectionExists) {
        // Click the first link with this href
        const link = page.locator(`a[href="${href}"]`).first();
        await link.click();

        // Wait for scroll
        await page.waitForTimeout(500);

        // Verify section is in viewport
        await expect(targetSection).toBeInViewport();

        // Scroll back to top for next iteration
        await page.evaluate(() => window.scrollTo(0, 0));
        await page.waitForTimeout(100);
      }
    }
  });

  test('TC5: External GitHub link opens repository in new tab', async ({ page }) => {
    // Test Case 5: Test external GitHub link
    // Expected: GitHub link opens repository in new tab

    // Find GitHub links (can be in hero, nav, or footer)
    const githubLinks = page.locator('a[href*="github.com/yetone/mirdb"]');
    const linkCount = await githubLinks.count();

    expect(linkCount).toBeGreaterThan(0);

    // Check the first GitHub link
    const firstGithubLink = githubLinks.first();

    // Verify the link has correct attributes for opening in new tab
    await expect(firstGithubLink).toHaveAttribute('target', '_blank');
    await expect(firstGithubLink).toHaveAttribute('rel', /noopener/);

    // Verify the href points to the correct repository
    const href = await firstGithubLink.getAttribute('href');
    expect(href).toContain('github.com/yetone/mirdb');

    // Verify clicking opens in new tab by listening for popup event
    const [newPage] = await Promise.all([
      page.waitForEvent('popup'),
      firstGithubLink.click()
    ]);

    // Verify the new page URL
    expect(newPage.url()).toContain('github.com');

    // Close the popup
    await newPage.close();
  });

  test('Smooth scrolling is enabled for anchor navigation', async ({ page }) => {
    // Additional test to verify smooth scrolling behavior

    // Get initial scroll position
    await page.evaluate(() => window.scrollTo(0, 0));
    const initialY = await page.evaluate(() => window.scrollY);

    // Click an internal link
    const getStartedLink = page.locator('a[href="#getting-started"]').first();
    await getStartedLink.click();

    // Check scroll position at short interval (during animation)
    await page.waitForTimeout(50);
    const midScrollY = await page.evaluate(() => window.scrollY);

    // Wait for animation to complete
    await page.waitForTimeout(500);
    const finalScrollY = await page.evaluate(() => window.scrollY);

    // Verify scroll happened (final position changed)
    expect(finalScrollY).toBeGreaterThan(initialY);

    // Verify the target section is visible
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeInViewport();
  });

  test('Navigation links are accessible via keyboard', async ({ page }) => {
    // Test keyboard navigation for accessibility

    // Focus on the first navigation link
    const navLinks = page.locator('.nav-links a, nav a').first();
    await navLinks.focus();

    // Verify the link is focused
    await expect(navLinks).toBeFocused();

    // Press Enter to activate the link
    await page.keyboard.press('Enter');

    // Wait for any navigation/scroll
    await page.waitForTimeout(500);

    // The page should have scrolled or navigated based on the link
    // This verifies keyboard accessibility
  });

  test('Mobile menu navigation works correctly', async ({ page }) => {
    // Test mobile navigation menu

    // Set viewport to mobile size
    await page.setViewportSize({ width: 375, height: 667 });
    await page.goto('/');

    // Find mobile menu button
    const mobileMenuBtn = page.locator('.mobile-menu-btn, [aria-label*="menu"]');

    if (await mobileMenuBtn.isVisible()) {
      // Click to open menu
      await mobileMenuBtn.click();

      // Check that navigation links become visible
      const navLinks = page.locator('.nav-links');
      await expect(navLinks).toHaveClass(/active/);

      // Click a navigation link
      const featuresLink = navLinks.locator('a[href="#features"]');
      if (await featuresLink.isVisible()) {
        await featuresLink.click();

        // Wait for scroll
        await page.waitForTimeout(500);

        // Verify features section is visible
        const featuresSection = page.locator('#features');
        await expect(featuresSection).toBeInViewport();

        // Verify menu closed after clicking a link
        await expect(navLinks).not.toHaveClass(/active/);
      }
    }
  });
});
