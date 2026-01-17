import { test, expect } from '@playwright/test';

/**
 * E2E Tests for User Journey - Discovery to Action
 *
 * This test suite verifies the complete user journey from discovery through to getting started action.
 * Covers the 5 steps:
 * 1. Land on homepage (Discovery)
 * 2. Read hero section (Understanding)
 * 3. Review features (Evaluation)
 * 4. Make decision (Decision)
 * 5. Click Get Started (Action)
 */

test.describe('User Journey - Discovery to Action', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Test Case 1: User can understand MirDB value proposition from hero section', async ({ page }) => {
    // Step 1: Land on homepage - User arrives at homepage (simulating search/referral)
    // Verify the page loads successfully
    await expect(page).toHaveTitle(/MirDB/);

    // Step 2: Read hero section - User understands what MirDB is from hero
    // Verify hero section is visible and contains value proposition elements
    const heroSection = page.locator('.hero');
    await expect(heroSection).toBeVisible();

    // Verify the main heading is visible (MirDB name)
    const heading = heroSection.locator('h1');
    await expect(heading).toBeVisible();
    await expect(heading).toContainText('MirDB');

    // Verify the tagline communicates core value proposition
    const tagline = heroSection.locator('.tagline');
    await expect(tagline).toBeVisible();
    await expect(tagline).toContainText('Persistent Key-Value Store');
    await expect(tagline).toContainText('Memcached Protocol');

    // Verify description provides additional context
    const description = heroSection.locator('.description');
    await expect(description).toBeVisible();
    await expect(description).toContainText('Rust');

    // Verify CTA buttons are visible and actionable
    const getStartedBtn = page.locator('[data-link="get-started"]');
    await expect(getStartedBtn).toBeVisible();
    await expect(getStartedBtn).toHaveText('Get Started');

    const githubBtn = page.locator('[data-link="github-hero"]');
    await expect(githubBtn).toBeVisible();
    await expect(githubBtn).toHaveText('View on GitHub');
  });

  test('Test Case 2: Click Get Started button from hero navigates to Getting Started section', async ({ page }) => {
    // Find the Get Started button in the hero section
    const getStartedBtn = page.locator('[data-link="get-started"]');
    await expect(getStartedBtn).toBeVisible();
    await expect(getStartedBtn).toHaveAttribute('href', '#getting-started');

    // Click the Get Started button
    await getStartedBtn.click();

    // Verify navigation to Getting Started section
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeInViewport();

    // Verify the section has proper content
    const sectionHeading = gettingStartedSection.locator('h2');
    await expect(sectionHeading).toContainText('Getting Started');

    // Verify installation instructions are present
    const codeBlocks = gettingStartedSection.locator('pre code');
    const codeCount = await codeBlocks.count();
    expect(codeCount).toBeGreaterThan(0);

    // Verify URL contains the anchor
    await expect(page).toHaveURL(/#getting-started/);
  });

  test('Test Case 3: Click View on GitHub button opens GitHub repository in new tab', async ({ page, context }) => {
    // Find the GitHub button in the hero section
    const githubBtn = page.locator('[data-link="github-hero"]');
    await expect(githubBtn).toBeVisible();

    // Verify it has target="_blank" for new tab
    await expect(githubBtn).toHaveAttribute('target', '_blank');

    // Verify it has rel="noopener" for security
    await expect(githubBtn).toHaveAttribute('rel', /noopener/);

    // Verify the href points to GitHub
    const href = await githubBtn.getAttribute('href');
    expect(href).toContain('github.com');
    expect(href).toContain('mirdb');

    // Test that clicking opens a new page
    const [newPage] = await Promise.all([
      context.waitForEvent('page'),
      githubBtn.click()
    ]);

    // Verify the new page opened with the GitHub URL
    const newPageUrl = newPage.url();
    expect(newPageUrl).toContain('github.com');
  });

  test('Test Case 4: Scroll through entire page - smooth scrolling and all sections load properly', async ({ page }) => {
    // Test smooth scrolling through all main sections
    const sections = [
      { selector: '.hero', name: 'Hero' },
      { selector: '#features', name: 'Features' },
      { selector: '#commands', name: 'Commands' },
      { selector: '#code-example', name: 'Code Example' },
      { selector: '#architecture', name: 'Architecture' },
      { selector: '#getting-started', name: 'Getting Started' },
      { selector: '#configuration', name: 'Configuration' },
      { selector: '.footer', name: 'Footer' }
    ];

    for (const section of sections) {
      const element = page.locator(section.selector);

      // Check if section exists (some may be optional)
      const exists = await element.count() > 0;
      if (exists) {
        // Scroll to the section
        await element.scrollIntoViewIfNeeded();

        // Verify section is visible after scrolling
        await expect(element).toBeVisible();
      }
    }

    // Scroll back to top to verify smooth navigation
    await page.evaluate(() => window.scrollTo({ top: 0, behavior: 'smooth' }));

    // Wait for scroll to complete
    await page.waitForTimeout(500);

    // Verify hero section is back in view
    const heroSection = page.locator('.hero');
    await expect(heroSection).toBeInViewport();
  });

  test('Complete user journey flow - Discovery to Action', async ({ page }) => {
    // Step 1: Discovery - Land on homepage
    await expect(page).toHaveTitle(/MirDB/);
    const heroSection = page.locator('.hero');
    await expect(heroSection).toBeVisible();

    // Step 2: Understanding - Read hero section
    const tagline = heroSection.locator('.tagline');
    await expect(tagline).toContainText('Persistent Key-Value Store');

    // Step 3: Evaluation - Review features
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();
    await expect(featuresSection).toBeVisible();

    // Verify key features are displayed
    const featureCards = featuresSection.locator('.feature-card');
    const featureCount = await featureCards.count();
    expect(featureCount).toBeGreaterThanOrEqual(4);

    // Verify key differentiators are mentioned
    const featuresContent = await featuresSection.textContent();
    expect(featuresContent).toContain('Memcached');
    expect(featuresContent).toContain('Persistent');
    expect(featuresContent).toContain('Rust');

    // Step 4: Decision - User decides based on benefits
    // Verify architecture section provides technical depth
    const architectureSection = page.locator('#architecture');
    const hasArchitecture = await architectureSection.count() > 0;
    if (hasArchitecture) {
      await architectureSection.scrollIntoViewIfNeeded();
      await expect(architectureSection).toBeVisible();
    }

    // Step 5: Action - Click Get Started
    const getStartedBtn = page.locator('[data-link="get-started"]');
    await getStartedBtn.click();

    // Verify navigation to Getting Started section
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeInViewport();

    // Verify actionable content is present (installation steps)
    const steps = gettingStartedSection.locator('.step');
    const stepCount = await steps.count();
    expect(stepCount).toBeGreaterThanOrEqual(2);
  });

  test('Hero section provides clear value proposition for understanding', async ({ page }) => {
    const heroSection = page.locator('.hero');
    await expect(heroSection).toBeVisible();

    // Verify all key value proposition elements are present
    // 1. Logo/branding
    const logo = heroSection.locator('.logo');
    if (await logo.count() > 0) {
      await expect(logo).toBeVisible();
    }

    // 2. Product name
    const productName = heroSection.locator('h1');
    await expect(productName).toContainText('MirDB');

    // 3. Tagline explaining what it is
    const tagline = heroSection.locator('.tagline');
    await expect(tagline).toBeVisible();

    // 4. Description with key benefits
    const description = heroSection.locator('.description');
    await expect(description).toBeVisible();
    const descText = await description.textContent();
    expect(descText).toContain('high-performance');

    // 5. Clear CTAs
    const ctaButtons = heroSection.locator('.cta-buttons');
    await expect(ctaButtons).toBeVisible();

    // Verify both primary and secondary CTAs exist
    const primaryBtn = ctaButtons.locator('.btn-primary');
    const secondaryBtn = ctaButtons.locator('.btn-secondary');
    await expect(primaryBtn).toBeVisible();
    await expect(secondaryBtn).toBeVisible();
  });

  test('Features section supports evaluation phase of user journey', async ({ page }) => {
    // Scroll to features section
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();
    await expect(featuresSection).toBeVisible();

    // Verify section has clear heading
    const heading = featuresSection.locator('h2');
    await expect(heading).toContainText('Features');

    // Verify features are organized in cards
    const featureCards = featuresSection.locator('.feature-card');
    const cardCount = await featureCards.count();
    expect(cardCount).toBeGreaterThanOrEqual(4);

    // Verify each feature card has a heading and description
    for (let i = 0; i < Math.min(cardCount, 4); i++) {
      const card = featureCards.nth(i);
      const cardHeading = card.locator('h3');
      const cardDescription = card.locator('p');

      await expect(cardHeading).toBeVisible();
      await expect(cardDescription).toBeVisible();
    }

    // Verify key differentiators from PRD are present
    const sectionContent = await featuresSection.textContent();
    expect(sectionContent).toContain('Memcached Protocol');
    expect(sectionContent).toContain('SSTable');
    expect(sectionContent).toContain('LSM');
    expect(sectionContent).toContain('Rust');
    expect(sectionContent).toContain('WAL');
    expect(sectionContent).toContain('Compaction');
  });
});
