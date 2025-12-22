// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * User Journey - Developer Evaluation
 *
 * Scenario: Verify complete user journey for a developer evaluating MirDB
 *
 * This test suite validates the end-to-end experience of a developer
 * landing on the MirDB homepage to evaluate whether it fits their needs.
 *
 * User Journey Steps:
 * 1. Land on homepage (fresh session)
 * 2. Understand value proposition (hero section)
 * 3. Explore features (features section)
 * 4. Review quick start (code examples and config)
 * 5. Navigate to documentation (decision to learn more)
 */

test.describe('User Journey - Developer Evaluation', () => {
  /**
   * Test Case 1: Complete evaluation journey in under 5 minutes
   *
   * This test simulates a developer completing the full evaluation journey:
   * - Landing on homepage
   * - Understanding value proposition
   * - Exploring features
   * - Reviewing quick start
   * - Finding documentation navigation
   *
   * The test measures performance and ensures all steps can be completed quickly.
   */
  test('should allow complete evaluation journey within reasonable time', async ({ page }) => {
    const startTime = Date.now();

    // Step 1: Land on homepage
    await page.goto('/');
    await expect(page).toHaveTitle(/MirDB/i);

    // Step 2: Understand value proposition (hero section immediately visible)
    const heroSection = page.locator('.hero');
    await expect(heroSection).toBeVisible();

    // Verify key value proposition elements are present
    const heroHeading = heroSection.locator('h1');
    await expect(heroHeading).toContainText('MirDB');

    const tagline = heroSection.locator('.hero-tagline');
    await expect(tagline).toBeVisible();
    const taglineText = await tagline.textContent();
    expect(taglineText.toLowerCase()).toContain('persistent');
    expect(taglineText.toLowerCase()).toContain('memcached');

    // Step 3: Explore features (scroll to features section)
    const featuresSection = page.locator('#features, [data-testid="features-section"]');
    await featuresSection.scrollIntoViewIfNeeded();
    await expect(featuresSection).toBeVisible();

    // Verify at least 3 key features are visible
    const featureCards = page.locator('.feature-card, [data-testid="feature-card"]');
    const featureCount = await featureCards.count();
    expect(featureCount).toBeGreaterThanOrEqual(3);

    // Step 4: Review quick start (scroll to quickstart section)
    const quickstartSection = page.locator('#quickstart, [data-testid="quickstart-section"]');
    await quickstartSection.scrollIntoViewIfNeeded();
    await expect(quickstartSection).toBeVisible();

    // Verify code example is present
    const codeBlock = quickstartSection.locator('.code-block, [data-testid="code-block"]').first();
    await expect(codeBlock).toBeVisible();

    // Verify configuration info is present
    const configSection = quickstartSection.locator('[data-testid="config-section"], .config-section');
    await expect(configSection).toBeVisible();

    // Step 5: Navigate to documentation (verify link exists and is accessible)
    const docLink = page.locator('a[href*="github.com"][href*="readme"], a:has-text("Documentation")').first();
    await expect(docLink).toBeVisible();

    // Also verify GitHub link exists
    const githubLink = page.locator('a[href*="github.com/yetone/mirdb"]:not([href*="readme"])').first();
    await expect(githubLink).toBeVisible();

    // Calculate journey time
    const endTime = Date.now();
    const journeyTimeMs = endTime - startTime;
    const journeyTimeSeconds = journeyTimeMs / 1000;

    // Log journey time for reference
    console.log(`Complete evaluation journey time: ${journeyTimeSeconds.toFixed(2)} seconds`);

    // Verify journey can be completed in under 5 minutes (300 seconds)
    // In practice, this automated test should complete in seconds
    expect(journeyTimeSeconds).toBeLessThan(300);
  });

  /**
   * Test Case 2: Identify use case fit within 30 seconds
   *
   * Developer should be able to determine if MirDB fits their needs
   * from the hero section alone (above the fold).
   */
  test('should enable quick use case identification from hero section', async ({ page }) => {
    await page.goto('/');

    const startTime = Date.now();

    // Hero section should be immediately visible
    const heroSection = page.locator('.hero');
    await expect(heroSection).toBeVisible();

    // All key information should be visible without scrolling
    // Check product name
    const productName = heroSection.locator('h1');
    await expect(productName).toBeVisible();
    await expect(productName).toContainText('MirDB');

    // Check tagline conveys core value proposition
    const tagline = heroSection.locator('.hero-tagline');
    await expect(tagline).toBeVisible();
    const taglineText = await tagline.textContent();

    // Developer should understand these key points:
    // 1. It's a persistent key-value store
    expect(taglineText.toLowerCase()).toContain('persistent');

    // 2. It's Memcached compatible
    expect(taglineText.toLowerCase()).toContain('memcached');

    // Check description provides additional context
    const description = heroSection.locator('.hero-description');
    await expect(description).toBeVisible();

    // Check CTAs are visible for next steps
    const getStartedBtn = heroSection.locator('a:has-text("Get Started")');
    const githubBtn = heroSection.locator('a:has-text("GitHub")');
    await expect(getStartedBtn).toBeVisible();
    await expect(githubBtn).toBeVisible();

    const endTime = Date.now();
    const identificationTimeMs = endTime - startTime;

    // All critical information should be accessible within 30 seconds
    // (automated test should complete much faster)
    expect(identificationTimeMs).toBeLessThan(30000);
  });

  /**
   * Test Case 3: Average time on page (engagement metric proxy)
   *
   * This test validates that the page has enough content to engage
   * users for more than 2 minutes by verifying substantial content
   * exists across all sections.
   */
  test('should have sufficient content for extended engagement', async ({ page }) => {
    await page.goto('/');

    // Count total sections for engagement
    const sections = [
      '.hero',
      '#features, [data-testid="features-section"]',
      '#architecture, [data-testid="architecture-section"]',
      '#status, [data-testid="status-section"]',
      '#quickstart, [data-testid="quickstart-section"]'
    ];

    let visibleSections = 0;
    for (const selector of sections) {
      const section = page.locator(selector).first();
      if (await section.count() > 0) {
        await expect(section).toBeVisible();
        visibleSections++;
      }
    }

    // Should have at least 4 major content sections
    expect(visibleSections).toBeGreaterThanOrEqual(4);

    // Hero section has substantial content
    const heroContent = await page.locator('.hero').textContent();
    expect(heroContent.length).toBeGreaterThan(100);

    // Features section has detailed cards
    const featureCards = page.locator('.feature-card');
    const featureCount = await featureCards.count();
    expect(featureCount).toBeGreaterThanOrEqual(3);

    // Architecture section has diagram and explanation
    const architectureSection = page.locator('#architecture, [data-testid="architecture-section"]');
    if (await architectureSection.count() > 0) {
      const archContent = await architectureSection.textContent();
      expect(archContent.length).toBeGreaterThan(200);
    }

    // Quick start has code examples and config
    const quickstartSection = page.locator('#quickstart');
    const quickstartContent = await quickstartSection.textContent();
    expect(quickstartContent.length).toBeGreaterThan(200);

    // Calculate total page content length as engagement proxy
    const pageContent = await page.locator('main').textContent();
    expect(pageContent.length).toBeGreaterThan(1000);

    // Count interactive elements
    const copyButtons = await page.locator('.copy-button').count();
    const links = await page.locator('a').count();

    console.log(`Page engagement metrics:`);
    console.log(`- ${visibleSections} major sections`);
    console.log(`- ${featureCount} feature cards`);
    console.log(`- ${copyButtons} copy buttons`);
    console.log(`- ${links} links`);
    console.log(`- ${pageContent.length} characters of content`);
  });

  /**
   * Test Case 4: Conversion to documentation/GitHub
   *
   * Verify that clear pathways exist for users to navigate to
   * documentation or GitHub (15%+ conversion target).
   */
  test('should provide clear pathways to documentation and GitHub', async ({ page }) => {
    await page.goto('/');

    // Count all documentation/GitHub links
    const docLinks = page.locator('a[href*="readme"], a[href*="docs"], a:has-text("Documentation")');
    const githubLinks = page.locator('a[href*="github.com"]');

    const docLinkCount = await docLinks.count();
    const githubLinkCount = await githubLinks.count();

    // Should have multiple conversion opportunities
    expect(docLinkCount).toBeGreaterThanOrEqual(1);
    expect(githubLinkCount).toBeGreaterThanOrEqual(2);

    // Verify links are distributed across the page
    // Navigation should have links
    const navGithubLink = page.locator('nav a[href*="github.com"]');
    await expect(navGithubLink.first()).toBeVisible();

    // Hero section should have links
    const heroGithubLink = page.locator('.hero a[href*="github.com"]');
    await expect(heroGithubLink.first()).toBeVisible();

    // Footer should have links
    const footerGithubLink = page.locator('footer a[href*="github.com"]');
    await expect(footerGithubLink.first()).toBeVisible();

    // All external links should have proper security attributes
    const externalLinks = await page.locator('a[target="_blank"]').all();
    for (const link of externalLinks) {
      const rel = await link.getAttribute('rel');
      expect(rel).toContain('noopener');
    }

    console.log(`Conversion pathways:`);
    console.log(`- ${docLinkCount} documentation links`);
    console.log(`- ${githubLinkCount} GitHub links`);
    console.log(`- Links in: navigation, hero, footer`);
  });

  /**
   * Integration test: Complete user flow simulation
   *
   * Simulates a realistic user flow through the page
   */
  test('should support realistic user evaluation flow', async ({ page }) => {
    // 1. Developer lands on page from search/referral
    await page.goto('/');

    // 2. Immediately sees product name and value prop
    await expect(page.locator('h1:has-text("MirDB")')).toBeVisible();
    await expect(page.locator('.hero-tagline')).toContainText(/persistent.*memcached/i);

    // 3. Scrolls to explore features
    await page.locator('#features').scrollIntoViewIfNeeded();

    // 4. Reads about Memcached compatibility
    const memcachedFeature = page.locator('.feature-card').filter({ hasText: /memcached/i });
    await expect(memcachedFeature).toBeVisible();

    // 5. Checks persistence feature
    const persistenceFeature = page.locator('.feature-card').filter({ hasText: /persistent/i });
    await expect(persistenceFeature).toBeVisible();

    // 6. Reviews architecture (if interested in technical details)
    const archSection = page.locator('#architecture');
    if (await archSection.count() > 0) {
      await archSection.scrollIntoViewIfNeeded();
      await expect(page.locator('[data-testid="architecture-diagram"]')).toBeVisible();
    }

    // 7. Checks project status
    const statusSection = page.locator('#status');
    if (await statusSection.count() > 0) {
      await statusSection.scrollIntoViewIfNeeded();
      await expect(page.locator('[data-testid="implemented-features"]')).toBeVisible();
    }

    // 8. Goes to quick start to see how to get started
    await page.locator('#quickstart').scrollIntoViewIfNeeded();

    // 9. Views code example
    const codeBlock = page.locator('.code-block').first();
    await expect(codeBlock).toBeVisible();

    // 10. Copies code (tests copy functionality)
    const copyButton = page.locator('[data-testid="copy-button"]').first();
    if (await copyButton.count() > 0) {
      await copyButton.click();
      // Wait for copy feedback
      await page.waitForTimeout(500);
    }

    // 11. Checks configuration defaults
    const configSection = page.locator('[data-testid="config-section"]');
    if (await configSection.count() > 0) {
      await expect(configSection).toBeVisible();
      await expect(configSection).toContainText('12333');
    }

    // 12. Decision point: clicks through to GitHub for more info
    const githubLink = page.locator('a[href*="github.com/yetone/mirdb"]:not([href*="issues"])').first();
    const href = await githubLink.getAttribute('href');
    expect(href).toContain('github.com');

    // Verify link opens in new tab (for good UX)
    const target = await githubLink.getAttribute('target');
    expect(target).toBe('_blank');
  });
});

test.describe('User Journey - Performance Metrics', () => {
  /**
   * Verify page loads quickly enough for good user experience
   */
  test('should load quickly for immediate engagement', async ({ page }) => {
    const startTime = Date.now();
    await page.goto('/');

    // Wait for main content to be visible
    await expect(page.locator('.hero')).toBeVisible();

    const loadTime = Date.now() - startTime;

    // Page should load in under 3 seconds (PRD requirement)
    console.log(`Page load time: ${loadTime}ms`);
    expect(loadTime).toBeLessThan(3000);
  });

  /**
   * Verify smooth scrolling experience
   */
  test('should provide smooth scrolling between sections', async ({ page }) => {
    await page.goto('/');

    // Click Get Started to scroll to quickstart
    const getStartedBtn = page.locator('a[href="#quickstart"]').first();
    await getStartedBtn.click();

    // Wait for scroll
    await page.waitForTimeout(600);

    // Verify quickstart section is now in viewport
    const quickstart = page.locator('#quickstart');
    await expect(quickstart).toBeInViewport();
  });
});
