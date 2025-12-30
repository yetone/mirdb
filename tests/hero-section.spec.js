// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Test Suite: Hero Section Value Proposition
 * Scenario: Verify the hero section presents a clear value proposition explaining
 * Memcached-compatible persistent storage with appropriate CTAs
 */

test.describe('Hero Section Value Proposition', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 1: Headline mentions persistent storage and/or Memcached compatibility
   * Input: Inspect hero section for headline text
   * Expected: Headline mentions persistent storage and/or Memcached compatibility
   */
  test('TC1: Hero headline mentions persistent storage and/or Memcached compatibility', async ({ page }) => {
    // Verify hero section exists and is visible
    const heroSection = page.locator('#hero.hero-section');
    await expect(heroSection).toBeVisible();

    // Verify headline exists within hero section
    const headline = page.locator('#hero h1');
    await expect(headline).toBeVisible();

    // Get the headline text
    const headlineText = await headline.textContent();
    expect(headlineText).toBeTruthy();

    // Verify headline mentions persistent storage and/or Memcached
    const lowerText = headlineText.toLowerCase();
    const mentionsPersistent = lowerText.includes('persistent');
    const mentionsMemcached = lowerText.includes('memcached');

    expect(mentionsPersistent || mentionsMemcached).toBeTruthy();

    // Verify headline is prominently displayed (has appropriate font size)
    const fontSize = await headline.evaluate(el => window.getComputedStyle(el).fontSize);
    const fontSizeValue = parseFloat(fontSize);
    expect(fontSizeValue).toBeGreaterThanOrEqual(28); // At least 28px for h1
  });

  /**
   * Test Case 2: Value proposition explains Memcached protocol with persistent storage
   * Input: Check value proposition content
   * Expected: Value proposition explains the combination of Memcached protocol with persistent storage in 2-3 sentences
   */
  test('TC2: Value proposition explains Memcached protocol with persistent storage', async ({ page }) => {
    // Verify hero description/value proposition exists
    const heroDescription = page.locator('#hero .hero-description');
    await expect(heroDescription).toBeVisible();

    // Get the value proposition text
    const descriptionText = await heroDescription.textContent();
    expect(descriptionText).toBeTruthy();

    // Verify it mentions both Memcached and persistence
    const lowerText = descriptionText.toLowerCase();
    expect(lowerText).toContain('memcached');
    expect(lowerText).toContain('persist');

    // Verify the description is a reasonable length (2-3 sentences, roughly 50-300 characters)
    const textLength = descriptionText.trim().length;
    expect(textLength).toBeGreaterThanOrEqual(50);
    expect(textLength).toBeLessThanOrEqual(500);

    // Verify it mentions key value proposition elements
    const mentionsDurability = lowerText.includes('durability') || lowerText.includes('durable') || lowerText.includes('disk');
    const mentionsSimplicity = lowerText.includes('simplicity') || lowerText.includes('simple') || lowerText.includes('compatibility');
    expect(mentionsDurability || mentionsSimplicity).toBeTruthy();
  });

  /**
   * Test Case 3: Get Started button is visible and clickable
   * Input: Locate primary CTA button
   * Expected: 'Get Started' button is visible and clickable
   */
  test('TC3: Get Started button is visible and clickable', async ({ page }) => {
    // Verify hero CTA container exists
    const heroCta = page.locator('#hero .hero-cta');
    await expect(heroCta).toBeVisible();

    // Find the Get Started button (primary CTA)
    const getStartedButton = page.locator('#hero .btn-primary');
    await expect(getStartedButton).toBeVisible();

    // Verify button text
    await expect(getStartedButton).toContainText('Get Started');

    // Verify button is clickable (not disabled)
    await expect(getStartedButton).toBeEnabled();

    // Verify button has proper styling (visually prominent)
    const backgroundColor = await getStartedButton.evaluate(el =>
      window.getComputedStyle(el).backgroundColor
    );
    // Should have a background color (not transparent)
    expect(backgroundColor).not.toBe('transparent');
    expect(backgroundColor).not.toBe('rgba(0, 0, 0, 0)');

    // Verify button has href attribute pointing to getting-started section
    await expect(getStartedButton).toHaveAttribute('href', '#getting-started');
  });

  /**
   * Test Case 4: View on GitHub link is visible and links to GitHub repository
   * Input: Locate secondary CTA link
   * Expected: 'View on GitHub' link is visible and links to GitHub repository
   */
  test('TC4: View on GitHub link is visible and links to GitHub repository', async ({ page }) => {
    // Find the View on GitHub link (secondary CTA)
    const githubLink = page.locator('#hero .btn-secondary');
    await expect(githubLink).toBeVisible();

    // Verify link text
    await expect(githubLink).toContainText('View on GitHub');

    // Verify link points to GitHub repository
    const href = await githubLink.getAttribute('href');
    expect(href).toBeTruthy();
    expect(href).toContain('github.com');
    expect(href).toContain('mirdb');

    // Verify link opens in new tab
    await expect(githubLink).toHaveAttribute('target', '_blank');

    // Verify security attributes
    await expect(githubLink).toHaveAttribute('rel', 'noopener noreferrer');

    // Verify link is clickable
    await expect(githubLink).toBeEnabled();
  });

  /**
   * Test Case 5: Get Started button navigates to Getting Started section
   * Input: Click 'Get Started' button
   * Expected: User is navigated to Getting Started section or documentation
   */
  test('TC5: Get Started button navigates to Getting Started section', async ({ page }) => {
    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY);

    // Find and click the Get Started button
    const getStartedButton = page.locator('#hero .btn-primary');
    await expect(getStartedButton).toBeVisible();
    await getStartedButton.click();

    // Wait for scroll animation to complete
    await page.waitForTimeout(500);

    // Verify Getting Started section is now visible
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeVisible();

    // Verify the section is near the top of the viewport
    const sectionBox = await gettingStartedSection.boundingBox();
    expect(sectionBox).not.toBeNull();
    expect(sectionBox.y).toBeLessThan(200);

    // Verify the URL hash has changed
    const currentUrl = page.url();
    expect(currentUrl).toContain('#getting-started');

    // Verify the Getting Started heading is visible
    const gettingStartedHeading = page.locator('#getting-started h2');
    await expect(gettingStartedHeading).toBeVisible();
    await expect(gettingStartedHeading).toHaveText('Getting Started');
  });

  /**
   * Additional test: Hero section is above the fold
   */
  test('Hero section is visible above the fold', async ({ page }) => {
    // Verify hero section is at the top of the page
    const heroSection = page.locator('#hero.hero-section');
    await expect(heroSection).toBeVisible();

    const heroBox = await heroSection.boundingBox();
    expect(heroBox).not.toBeNull();

    // Hero section should start near the top (after header)
    expect(heroBox.y).toBeLessThan(200);

    // Hero section should have substantial height
    expect(heroBox.height).toBeGreaterThan(200);
  });

  /**
   * Additional test: Hero section has proper semantic structure
   */
  test('Hero section has proper semantic structure', async ({ page }) => {
    // Verify hero section exists within main
    const heroInMain = page.locator('main #hero');
    await expect(heroInMain).toBeVisible();

    // Verify h1 exists (only one per page ideally)
    const h1Elements = page.locator('h1');
    const h1Count = await h1Elements.count();
    expect(h1Count).toBe(1);

    // Verify hero has container structure
    const heroContainer = page.locator('#hero .hero-container');
    await expect(heroContainer).toBeVisible();
  });
});
