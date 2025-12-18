// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * User Journey - Developer Discovery E2E Tests
 * Scenario ID: 15 (ac37d307-449f-434d-8a55-dde6aad23b44)
 *
 * This test suite verifies the complete user journey for Developer Dan persona
 * discovering and evaluating MirDB through the landing page.
 *
 * Steps:
 * 1. Land on homepage - First impression within 10 seconds
 * 2. Read hero section - Understand value proposition quickly
 * 3. Scroll through features - Evaluate capabilities
 * 4. View code example - Assess ease of adoption
 * 5. Take action - Click CTA to proceed to deeper evaluation
 */

test.describe('User Journey - Developer Discovery', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 1: Complete user journey from landing to CTA click
   * Input: Complete user journey from landing to CTA click
   * Expected: User can complete full journey without confusion or blockers
   */
  test('TC-1: Complete user journey from landing to CTA click', async ({ page }) => {
    // Step 1: Land on homepage - verify page loads successfully
    await expect(page).toHaveTitle(/MirDB/);
    const body = page.locator('body');
    await expect(body).toBeVisible();

    // Step 2: Read hero section - verify value proposition is visible
    const heroSection = page.locator('.hero-section');
    await expect(heroSection).toBeVisible();

    const productName = page.locator('.product-name');
    await expect(productName).toBeVisible();
    await expect(productName).toContainText('MirDB');

    const tagline = page.locator('.tagline, .hero-tagline');
    await expect(tagline).toBeVisible();

    const valueProposition = page.locator('.value-proposition, .hero-description');
    await expect(valueProposition).toBeVisible();

    // Step 3: Scroll through features section
    const featuresSection = page.locator('.features-section');
    await featuresSection.scrollIntoViewIfNeeded();
    await expect(featuresSection).toBeVisible();

    const featureCards = page.locator('.feature-card');
    await expect(featureCards).toHaveCount(3);

    // Step 4: View code example in quick start section
    const quickStartSection = page.locator('.quickstart-section');
    await quickStartSection.scrollIntoViewIfNeeded();
    await expect(quickStartSection).toBeVisible();

    const codeBlock = page.locator('.code-block');
    await expect(codeBlock).toBeVisible();

    const codeContent = await codeBlock.locator('code').textContent();
    expect(codeContent).toContain('mirdb');
    expect(codeContent).toContain('set');
    expect(codeContent).toContain('get');

    // Step 5: Take action - verify CTA is clickable
    // Scroll back to hero to click CTA
    await heroSection.scrollIntoViewIfNeeded();
    const primaryCTA = page.locator('.cta-primary, .btn-primary');
    await expect(primaryCTA).toBeVisible();
    await expect(primaryCTA).toBeEnabled();

    // Verify CTA has valid href
    const ctaHref = await primaryCTA.getAttribute('href');
    expect(ctaHref).toBeTruthy();

    // Click the CTA and verify navigation works
    await primaryCTA.click();
    // The CTA links to #documentation which scrolls to quick start section
    await expect(quickStartSection).toBeInViewport();
  });

  /**
   * Test Case 2: Time from page load to understanding value proposition
   * Input: Time from page load to understanding value proposition
   * Expected: Value proposition is understandable within 10 seconds of page load
   * Type: Manual - This test validates that content is immediately visible
   */
  test('TC-2: Value proposition is understandable within 10 seconds', async ({ page }) => {
    // Start timing from page load
    const startTime = Date.now();

    // Wait for hero section to be visible (should be immediate)
    const heroSection = page.locator('.hero-section');
    await expect(heroSection).toBeVisible();

    // Verify product name is visible
    const productName = page.locator('.product-name');
    await expect(productName).toBeVisible();

    // Verify tagline is visible - contains key value proposition
    const tagline = page.locator('.tagline, .hero-tagline');
    await expect(tagline).toBeVisible();
    const taglineText = await tagline.textContent();
    expect(taglineText?.toLowerCase()).toContain('persistent');
    expect(taglineText?.toLowerCase()).toContain('memcached');

    // Verify value proposition paragraph is visible
    const valueProposition = page.locator('.value-proposition, .hero-description');
    await expect(valueProposition).toBeVisible();

    // Verify CTA buttons are visible above the fold
    const ctaButtons = page.locator('.cta-buttons');
    await expect(ctaButtons).toBeVisible();

    // Calculate elapsed time
    const elapsedTime = Date.now() - startTime;

    // All critical content should be visible within 10 seconds
    // In practice, this should happen within 2-3 seconds for a static page
    expect(elapsedTime).toBeLessThan(10000);

    // Verify the content explains what MirDB does:
    // 1. It's a key-value store
    // 2. It's persistent (data survives restarts)
    // 3. It's compatible with memcached
    const vpText = await valueProposition.textContent();
    expect(vpText?.toLowerCase()).toMatch(/persist|durable|storage/);
    expect(vpText?.toLowerCase()).toMatch(/rust|performance|fast|high/);
  });

  /**
   * Test Case 3: Navigate from hero to features to quick-start
   * Input: Navigate from hero to features to quick-start
   * Expected: Natural scroll flow guides user through content sections
   */
  test('TC-3: Natural scroll flow from hero to features to quick-start', async ({ page }) => {
    // Verify sections are in correct order for natural scroll flow
    const heroSection = page.locator('.hero-section');
    const featuresSection = page.locator('.features-section');
    const quickStartSection = page.locator('.quickstart-section');

    // Get bounding boxes to verify vertical order
    const heroBB = await heroSection.boundingBox();
    const featuresBB = await featuresSection.boundingBox();
    const quickStartBB = await quickStartSection.boundingBox();

    expect(heroBB).not.toBeNull();
    expect(featuresBB).not.toBeNull();
    expect(quickStartBB).not.toBeNull();

    // Verify sections are in correct order (top to bottom)
    expect(heroBB.y).toBeLessThan(featuresBB.y);
    expect(featuresBB.y).toBeLessThan(quickStartBB.y);

    // Simulate user scroll journey
    // Step 1: Start at hero (already there)
    await expect(heroSection).toBeInViewport();

    // Step 2: Scroll to features
    await featuresSection.scrollIntoViewIfNeeded();
    await expect(featuresSection).toBeInViewport();

    // Verify features content is visible
    const featureCards = page.locator('.feature-card');
    await expect(featureCards.first()).toBeVisible();

    // Step 3: Scroll to quick start
    await quickStartSection.scrollIntoViewIfNeeded();
    await expect(quickStartSection).toBeInViewport();

    // Verify code example is visible
    const codeBlock = page.locator('.code-block');
    await expect(codeBlock).toBeVisible();

    // Verify there are no large gaps between hero and features sections
    const heroBottom = heroBB.y + heroBB.height;
    const featuresTop = featuresBB.y;
    // Allow for small padding/margins but no large gaps
    expect(featuresTop - heroBottom).toBeLessThan(100);

    // Note: There is a commands section between features and quickstart
    // which is expected as part of the user journey
    // Verify all sections are present and accessible via scroll
    const commandsSection = page.locator('.commands-section');
    await commandsSection.scrollIntoViewIfNeeded();
    await expect(commandsSection).toBeInViewport();
  });

  /**
   * Test Case 4: Find and click primary CTA from any section
   * Input: Find and click primary CTA from any section
   * Expected: CTA buttons are accessible from multiple page locations
   */
  test('TC-4: CTA buttons accessible from multiple locations', async ({ page }) => {
    // Check hero section CTAs
    const heroSection = page.locator('.hero-section');
    const heroPrimaryCTA = heroSection.locator('.cta-primary, .btn-primary');
    const heroSecondaryCTA = heroSection.locator('.cta-secondary');

    await expect(heroPrimaryCTA).toBeVisible();
    await expect(heroSecondaryCTA).toBeVisible();

    // Verify primary CTA has meaningful text
    const primaryText = await heroPrimaryCTA.textContent();
    expect(primaryText?.toLowerCase()).toMatch(/get started|documentation|docs/);

    // Verify secondary CTA links to GitHub
    const secondaryHref = await heroSecondaryCTA.getAttribute('href');
    expect(secondaryHref).toContain('github');

    // Check footer for additional CTAs/links
    const footer = page.locator('.footer');
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();

    const footerLinks = page.locator('.footer-links a');
    const footerLinkCount = await footerLinks.count();
    expect(footerLinkCount).toBeGreaterThanOrEqual(2);

    // Verify footer has Documentation link
    const docLink = page.locator('.footer-links a[href*="doc"], .footer-links a:has-text("Documentation")');
    await expect(docLink).toBeVisible();

    // Verify footer has GitHub link (use first() to handle multiple github links)
    const githubLink = page.locator('.footer-links a[href*="github"]').first();
    await expect(githubLink).toBeVisible();

    // Test that primary CTA works from hero section
    await heroSection.scrollIntoViewIfNeeded();
    await heroPrimaryCTA.click();
    // Should navigate to documentation section
    const quickStartSection = page.locator('.quickstart-section');
    await expect(quickStartSection).toBeInViewport();
  });

  /**
   * Test Case 5: User journey on mobile device
   * Input: User journey on mobile device
   * Expected: Complete journey is achievable on mobile without issues
   */
  test('TC-5: Complete journey achievable on mobile', async ({ page }) => {
    // Set mobile viewport (iPhone SE size)
    await page.setViewportSize({ width: 375, height: 667 });
    await page.goto('/');

    // Step 1: Verify hero section is visible and readable on mobile
    const heroSection = page.locator('.hero-section');
    await expect(heroSection).toBeVisible();

    const productName = page.locator('.product-name');
    await expect(productName).toBeVisible();

    const tagline = page.locator('.tagline, .hero-tagline');
    await expect(tagline).toBeVisible();

    // Verify CTA buttons are visible and appropriately sized for mobile
    const ctaButtons = page.locator('.cta-buttons');
    await expect(ctaButtons).toBeVisible();

    const primaryCTA = page.locator('.cta-primary, .btn-primary');
    await expect(primaryCTA).toBeVisible();

    // Verify CTA is large enough for touch (minimum 44x44 px)
    const ctaBB = await primaryCTA.boundingBox();
    expect(ctaBB).not.toBeNull();
    expect(ctaBB.height).toBeGreaterThanOrEqual(44);

    // Step 2: Scroll to features section
    const featuresSection = page.locator('.features-section');
    await featuresSection.scrollIntoViewIfNeeded();
    await expect(featuresSection).toBeVisible();

    // Verify feature cards are visible (should stack on mobile)
    const featureCards = page.locator('.feature-card');
    await expect(featureCards.first()).toBeVisible();

    // Step 3: Scroll to quick start section
    const quickStartSection = page.locator('.quickstart-section');
    await quickStartSection.scrollIntoViewIfNeeded();
    await expect(quickStartSection).toBeVisible();

    // Verify code block is visible and scrollable if needed
    const codeBlock = page.locator('.code-block');
    await expect(codeBlock).toBeVisible();

    // Step 4: Verify footer is accessible
    const footer = page.locator('.footer');
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();

    // Step 5: Verify no horizontal scroll (content doesn't overflow)
    const scrollWidth = await page.evaluate(() => document.body.scrollWidth);
    const clientWidth = await page.evaluate(() => document.body.clientWidth);
    expect(scrollWidth).toBeLessThanOrEqual(clientWidth + 1);

    // Step 6: Test CTA navigation works on mobile
    await heroSection.scrollIntoViewIfNeeded();
    await primaryCTA.click();
    await expect(quickStartSection).toBeInViewport();
  });
});

test.describe('User Journey - Additional Validations', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Verify all sections are present for complete journey
   */
  test('All required sections present for complete journey', async ({ page }) => {
    // Hero section with value proposition
    const heroSection = page.locator('.hero-section');
    await expect(heroSection).toBeVisible();

    // Features section with capability cards
    const featuresSection = page.locator('.features-section');
    await expect(featuresSection).toBeVisible();
    const featureCards = page.locator('.feature-card');
    await expect(featureCards).toHaveCount(3);

    // Commands section showing supported operations
    const commandsSection = page.locator('.commands-section');
    await expect(commandsSection).toBeVisible();

    // Quick start section with code example
    const quickStartSection = page.locator('.quickstart-section');
    await expect(quickStartSection).toBeVisible();
    const codeBlock = page.locator('.code-block');
    await expect(codeBlock).toBeVisible();

    // Footer with navigation links
    const footer = page.locator('.footer');
    await expect(footer).toBeVisible();
  });

  /**
   * Verify page accessibility for keyboard navigation
   */
  test('Page supports keyboard navigation for complete journey', async ({ page }) => {
    // Verify skip link is available
    const skipLink = page.locator('.skip-link');
    await expect(skipLink).toBeAttached();

    // Tab through page elements
    await page.keyboard.press('Tab');

    // Primary CTA should be focusable
    const primaryCTA = page.locator('.cta-primary, .btn-primary');
    await primaryCTA.focus();
    await expect(primaryCTA).toBeFocused();

    // Verify focus is visible
    const outline = await primaryCTA.evaluate((el) =>
      window.getComputedStyle(el).outlineWidth
    );
    expect(parseInt(outline)).toBeGreaterThan(0);
  });

  /**
   * Verify tablet viewport journey
   */
  test('Complete journey on tablet viewport (768px)', async ({ page }) => {
    await page.setViewportSize({ width: 768, height: 1024 });
    await page.goto('/');

    // Verify all sections are visible and properly laid out
    const heroSection = page.locator('.hero-section');
    await expect(heroSection).toBeVisible();

    const featuresSection = page.locator('.features-section');
    await featuresSection.scrollIntoViewIfNeeded();
    await expect(featuresSection).toBeVisible();

    const quickStartSection = page.locator('.quickstart-section');
    await quickStartSection.scrollIntoViewIfNeeded();
    await expect(quickStartSection).toBeVisible();

    // Verify no horizontal scroll
    const scrollWidth = await page.evaluate(() => document.body.scrollWidth);
    const clientWidth = await page.evaluate(() => document.body.clientWidth);
    expect(scrollWidth).toBeLessThanOrEqual(clientWidth + 1);
  });
});
