/**
 * User Journey E2E Tests
 * Owner: Scenario 13 - User Journey - Developer Evaluation Flow
 *
 * Tests for:
 * - Developer evaluation flow from arrival to action
 * - Value proposition visibility within first viewport
 * - Navigation to key sections
 * - GitHub link accessibility
 * - Complete evaluation journey
 */

import { test, expect } from '@playwright/test';

test.describe('User Journey - Developer Evaluation Flow', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');
  });

  test('TC1: Key value proposition visible within first viewport (above fold)', async ({ page }) => {
    // Get viewport dimensions
    const viewportSize = page.viewportSize();

    // Check hero section is visible immediately
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // Check value proposition (tagline) is visible
    const tagline = page.locator('.hero__tagline');
    await expect(tagline).toBeVisible();

    // Verify tagline is within viewport (above fold)
    const taglineBox = await tagline.boundingBox();
    expect(taglineBox).not.toBeNull();
    expect(taglineBox.y).toBeLessThan(viewportSize.height);
    expect(taglineBox.y + taglineBox.height).toBeLessThan(viewportSize.height);

    // Verify key product information is visible without scrolling
    const taglineText = await tagline.textContent();
    expect(taglineText.toLowerCase()).toContain('persistent');
    expect(taglineText.toLowerCase()).toContain('key-value');
    expect(taglineText.toLowerCase()).toContain('memcached');

    // Verify title is also above fold
    const title = page.locator('.hero__title');
    await expect(title).toBeVisible();
    await expect(title).toHaveText('MiRDB');
    const titleBox = await title.boundingBox();
    expect(titleBox.y + titleBox.height).toBeLessThan(viewportSize.height);

    // Verify CTA buttons are visible above fold
    const ctaSection = page.locator('.hero__cta');
    await expect(ctaSection).toBeVisible();
    const ctaBox = await ctaSection.boundingBox();
    expect(ctaBox.y + ctaBox.height).toBeLessThan(viewportSize.height);
  });

  test('TC2: Features section reachable within 1-2 scroll actions', async ({ page }) => {
    // Features section should be reachable within 1-2 viewport scrolls
    const viewportHeight = page.viewportSize().height;
    const featuresSection = page.locator('#features');

    // Check features section exists
    await expect(featuresSection).toBeAttached();

    // Get features section position
    const featuresBox = await featuresSection.boundingBox();
    expect(featuresBox).not.toBeNull();

    // Features should be within 2 viewport heights from top
    const maxDistance = viewportHeight * 2;
    expect(featuresBox.y).toBeLessThan(maxDistance);

    // Navigate to features via scroll
    await page.evaluate(() => {
      document.getElementById('features').scrollIntoView({ behavior: 'instant' });
    });

    // Features section should now be visible
    await expect(featuresSection).toBeInViewport();

    // Verify features content is accessible
    const featuresTitle = featuresSection.locator('.features__title, h2');
    await expect(featuresTitle).toBeVisible();

    // Verify at least one feature card is visible
    const featureCards = featuresSection.locator('.feature-card');
    await expect(featureCards.first()).toBeVisible();
  });

  test('TC3: Code example reachable within 2 clicks or scroll', async ({ page }) => {
    // Method 1: Check if code example is reachable by scrolling
    const codeExampleSection = page.locator('#code-example');
    await expect(codeExampleSection).toBeAttached();

    // Method 2: Check if there's a direct link to code examples (via navigation)
    // First check for any navigation link that might lead to code
    const headerNavLinks = page.locator('.header__nav a, nav a');

    // Scroll to code example section
    await page.evaluate(() => {
      const codeSection = document.getElementById('code-example');
      if (codeSection) {
        codeSection.scrollIntoView({ behavior: 'instant' });
      }
    });

    // Verify code section is now visible
    await expect(codeExampleSection).toBeInViewport();

    // Verify code blocks are present and visible
    const codeBlocks = codeExampleSection.locator('.code-block');
    const codeBlockCount = await codeBlocks.count();
    expect(codeBlockCount).toBeGreaterThan(0);

    // Verify at least one code block has actual code content
    const firstCodeBlock = codeBlocks.first();
    await expect(firstCodeBlock).toBeVisible();

    const codeContent = firstCodeBlock.locator('code, .code-block__code');
    await expect(codeContent).toBeVisible();
    const codeText = await codeContent.textContent();
    expect(codeText.length).toBeGreaterThan(5);
  });

  test('TC4: GitHub link always visible in header or within current section', async ({ page }) => {
    // Check GitHub link is visible in header
    const headerGithubLink = page.locator('.header__github, header a[href*="github"]');
    await expect(headerGithubLink.first()).toBeVisible();

    // Verify link points to correct repository
    const href = await headerGithubLink.first().getAttribute('href');
    expect(href).toContain('github.com/yetone/mirdb');

    // Verify opens in new tab for external link
    const target = await headerGithubLink.first().getAttribute('target');
    expect(target).toBe('_blank');

    // Scroll down to different sections and verify GitHub is still accessible
    const sections = ['#features', '#quickstart', '#code-example', '#configuration'];

    for (const sectionId of sections) {
      const section = page.locator(sectionId);
      if ((await section.count()) > 0) {
        await page.evaluate((id) => {
          document.querySelector(id)?.scrollIntoView({ behavior: 'instant' });
        }, sectionId);

        // Header should still be accessible (either visible or via scroll-to-top)
        // Check that at least one GitHub link exists in visible area or header
        const anyGithubLink = page.locator('a[href*="github.com/yetone/mirdb"]');
        const count = await anyGithubLink.count();
        expect(count).toBeGreaterThan(0);
      }
    }

    // Check footer also has GitHub link
    const footerGithubLink = page.locator('.footer a[href*="github"]');
    if (await footerGithubLink.count() > 0) {
      await page.evaluate(() => {
        document.querySelector('footer')?.scrollIntoView({ behavior: 'instant' });
      });
      await expect(footerGithubLink.first()).toBeVisible();
    }
  });

  test('TC5: Get Started button leads to installation within 1 click', async ({ page }) => {
    // Find the Get Started button in hero section
    const getStartedBtn = page.locator('.hero__cta a:has-text("Get Started"), #hero a.btn-secondary');
    await expect(getStartedBtn.first()).toBeVisible();

    // Click the Get Started button
    await getStartedBtn.first().click();

    // Should navigate to quickstart section
    await expect(page).toHaveURL(/#quickstart/);

    // Quickstart section should be visible
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeInViewport();

    // Verify installation instructions are visible
    const installationSteps = quickstartSection.locator('.quickstart__steps, ol, .quickstart__step');
    const stepCount = await installationSteps.count();
    expect(stepCount).toBeGreaterThan(0);

    // Check for actual installation-related content
    const quickstartText = await quickstartSection.textContent();
    const hasInstallContent =
      quickstartText.toLowerCase().includes('install') ||
      quickstartText.toLowerCase().includes('download') ||
      quickstartText.toLowerCase().includes('cargo') ||
      quickstartText.toLowerCase().includes('binary');
    expect(hasInstallContent).toBeTruthy();
  });

  test('TC6: Full evaluation journey completable under simulated time', async ({ page }) => {
    // This test validates the complete evaluation flow a developer would take
    // Note: Actual 2-minute timing is a manual test; this validates the flow exists

    const journeyStartTime = Date.now();

    // Step 1: Arrival and Understanding (verify key info visible immediately)
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    const tagline = page.locator('.hero__tagline');
    await expect(tagline).toBeVisible();
    const taglineText = await tagline.textContent();
    expect(taglineText.toLowerCase()).toContain('persistent');
    expect(taglineText.toLowerCase()).toContain('key-value');

    // Step 2: Feature Exploration
    await page.evaluate(() => {
      document.getElementById('features')?.scrollIntoView({ behavior: 'instant' });
    });

    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport();

    // Check for feature status indicators (completed vs planned)
    const completedFeatures = page.locator('[data-status="completed"], .status-badge--completed');
    const completedCount = await completedFeatures.count();
    expect(completedCount).toBeGreaterThan(0);

    const plannedFeatures = page.locator('[data-status="planned"], .status-badge--planned');
    const plannedCount = await plannedFeatures.count();
    // Planned features may or may not exist, but should be able to check

    // Step 3: Code Evaluation
    await page.evaluate(() => {
      document.getElementById('code-example')?.scrollIntoView({ behavior: 'instant' });
    });

    const codeSection = page.locator('#code-example');
    await expect(codeSection).toBeInViewport();

    // Verify code is readable
    const codeBlocks = codeSection.locator('.code-block code, pre code');
    await expect(codeBlocks.first()).toBeVisible();
    const codeText = await codeBlocks.first().textContent();
    expect(codeText.length).toBeGreaterThan(10);

    // Step 4: Take Action - verify paths to action exist
    // Option A: GitHub navigation
    const githubLinks = page.locator('a[href*="github.com/yetone/mirdb"]');
    const githubCount = await githubLinks.count();
    expect(githubCount).toBeGreaterThan(0);

    // Option B: Installation path exists
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeAttached();

    // Record journey completion time for logging purposes
    const journeyEndTime = Date.now();
    const journeyDuration = journeyEndTime - journeyStartTime;

    // Journey should complete programmatically in under a few seconds
    // The 2-minute requirement is for human manual testing
    expect(journeyDuration).toBeLessThan(30000); // Under 30 seconds for automated test
  });

  test('Navigation elements are accessible throughout the page', async ({ page }) => {
    // Verify header navigation is always present
    const headerNav = page.locator('.header__nav, header nav');
    await expect(headerNav.first()).toBeAttached();

    // Verify skip link exists for accessibility
    const skipLink = page.locator('.skip-link, a[href="#main"]');
    await expect(skipLink.first()).toBeAttached();

    // Test navigation to each major section
    const sectionLinks = [
      { selector: 'a[href="#features"]', target: '#features' },
      { selector: 'a[href="#quickstart"]', target: '#quickstart' },
    ];

    for (const { selector, target } of sectionLinks) {
      const link = page.locator(selector);
      if (await link.count() > 0) {
        await link.first().click();
        const targetSection = page.locator(target);
        await expect(targetSection).toBeInViewport({ timeout: 2000 });

        // Reset to top for next test
        await page.evaluate(() => window.scrollTo(0, 0));
      }
    }
  });

  test('Developer can assess production readiness via feature status', async ({ page }) => {
    // Navigate to features section
    await page.evaluate(() => {
      document.getElementById('features')?.scrollIntoView({ behavior: 'instant' });
    });

    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport();

    // Verify feature cards exist
    const featureCards = featuresSection.locator('.feature-card');
    const cardCount = await featureCards.count();
    expect(cardCount).toBeGreaterThan(0);

    // Verify each feature card has a status indicator
    for (let i = 0; i < cardCount; i++) {
      const card = featureCards.nth(i);
      const statusBadge = card.locator('.status-badge');

      // Each card should have a status badge
      await expect(statusBadge).toBeVisible();

      // Status should be either completed or planned
      const statusClasses = await statusBadge.getAttribute('class');
      const isCompleted = statusClasses?.includes('completed');
      const isPlanned = statusClasses?.includes('planned');
      expect(isCompleted || isPlanned).toBeTruthy();
    }

    // Specifically check that Raft is marked as planned (per PRD)
    const raftFeature = featuresSection.locator('[data-feature="raft"], .feature-card:has-text("Raft")');
    if (await raftFeature.count() > 0) {
      const raftStatus = raftFeature.locator('.status-badge');
      const raftStatusText = await raftStatus.textContent();
      expect(raftStatusText.toLowerCase()).toContain('planned');
    }
  });
});
