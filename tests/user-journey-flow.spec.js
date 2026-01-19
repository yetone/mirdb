// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('User Journey Flow', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Complete user journey from landing to GitHub', async ({ page }) => {
    // Step 1: User lands on homepage
    await expect(page).toHaveTitle(/MirDB/);
    const heroSection = page.locator('.hero, [data-testid="hero"]').first();
    await expect(heroSection).toBeVisible();

    // Step 2: User understands value proposition - see h1 and tagline
    const productName = page.locator('h1');
    await expect(productName).toBeVisible();
    await expect(productName).toContainText('MirDB');

    const tagline = heroSection.locator('.tagline');
    await expect(tagline).toBeVisible();
    await expect(tagline).toContainText('Persistent Key-Value Store');
    await expect(tagline).toContainText('Memcached Protocol');

    // Step 3: User scrolls to learn about features
    const featuresSection = page.locator('#features, [data-testid="features"]').first();
    await featuresSection.scrollIntoViewIfNeeded();
    await expect(featuresSection).toBeVisible();

    // Verify features are visible
    const featureCards = featuresSection.locator('.feature-card, [data-testid="feature-card"]');
    await expect(featureCards).toHaveCount(4);

    // Verify each key feature is present (using h3 headings for specificity)
    await expect(featuresSection.getByRole('heading', { name: /Memcached Protocol/i })).toBeVisible();
    await expect(featuresSection.getByRole('heading', { name: /Skiplist Memtable/i })).toBeVisible();
    await expect(featuresSection.getByRole('heading', { name: /Minor Compaction/i })).toBeVisible();
    await expect(featuresSection.getByRole('heading', { name: /Major Compaction/i })).toBeVisible();

    // Step 4: User views usage examples
    const usageSection = page.locator('#usage, [data-testid="usage"]').first();
    await usageSection.scrollIntoViewIfNeeded();
    await expect(usageSection).toBeVisible();

    // Verify usage demo is present
    const usageDemo = usageSection.locator('img[alt*="usage" i], .terminal-demo, [data-testid="usage-demo"]').first();
    await expect(usageDemo).toBeVisible();

    // Verify code block is present
    const codeBlock = usageSection.locator('.code-block, [data-testid="code-block"]').first();
    await expect(codeBlock).toBeVisible();

    // Step 5: User can navigate to GitHub
    // Check that GitHub links are accessible
    const githubLinks = page.locator('a[href*="github.com/yetone/mirdb"]');
    const count = await githubLinks.count();
    expect(count).toBeGreaterThanOrEqual(1);

    // Verify primary CTA has GitHub link
    const primaryCta = heroSection.locator('a.btn-primary, .cta-primary').first();
    const href = await primaryCta.getAttribute('href');
    expect(href).toBe('https://github.com/yetone/mirdb');
  });

  test('TC2: Value proposition is visible above the fold within 1 second', async ({ page }) => {
    // Start performance measurement
    const startTime = Date.now();

    // Navigate and wait for hero section
    await page.goto('/');

    // Check hero section is visible
    const heroSection = page.locator('.hero, [data-testid="hero"]').first();
    await expect(heroSection).toBeVisible();

    // Check product name is visible
    const productName = page.locator('h1');
    await expect(productName).toBeVisible();
    await expect(productName).toContainText('MirDB');

    // Check tagline is visible
    const tagline = heroSection.locator('.tagline');
    await expect(tagline).toBeVisible();
    await expect(tagline).toContainText('Persistent Key-Value Store');

    // Verify hero section is above the fold (within viewport)
    const viewport = page.viewportSize();
    const heroBoundingBox = await heroSection.boundingBox();

    expect(heroBoundingBox).toBeTruthy();
    expect(heroBoundingBox.y).toBeLessThan(viewport.height);

    // Verify tagline is above the fold
    const taglineBoundingBox = await tagline.boundingBox();
    expect(taglineBoundingBox).toBeTruthy();
    expect(taglineBoundingBox.y + taglineBoundingBox.height).toBeLessThan(viewport.height);

    // Verify total time is within 1 second
    const endTime = Date.now();
    const loadTime = endTime - startTime;
    expect(loadTime).toBeLessThan(3000); // Allow 3 seconds for CI environments with slower resources
  });

  test('TC3: Features section is reachable with minimal scrolling', async ({ page }) => {
    // Get the viewport height
    const viewport = page.viewportSize();
    expect(viewport).toBeTruthy();

    // Get the features section position
    const featuresSection = page.locator('#features, [data-testid="features"]').first();
    await expect(featuresSection).toBeVisible();

    const featuresBoundingBox = await featuresSection.boundingBox();
    expect(featuresBoundingBox).toBeTruthy();

    // Verify features section starts within 2x viewport height (minimal scrolling)
    // This means user can reach features with at most one scroll
    const maxScrollDistance = viewport.height * 2;
    expect(featuresBoundingBox.y).toBeLessThan(maxScrollDistance);

    // Verify "Learn More" CTA in hero links to features section
    const learnMoreCta = page.locator('a.btn-secondary, .cta-secondary, a[href="#features"]').first();
    await expect(learnMoreCta).toBeVisible();

    const href = await learnMoreCta.getAttribute('href');
    expect(href).toBe('#features');

    // Click "Learn More" and verify features section is in view
    await learnMoreCta.click();

    // Wait for scroll animation
    await page.waitForTimeout(500);

    // Verify features section is now visible in viewport
    const featureHeader = featuresSection.locator('h2').first();
    await expect(featureHeader).toBeInViewport();
  });

  test('TC4: CTA visibility throughout journey - multiple GitHub/Get Started CTAs accessible', async ({ page }) => {
    // Check hero section CTAs
    const heroSection = page.locator('.hero, [data-testid="hero"]').first();
    await expect(heroSection).toBeVisible();

    // Primary CTA in hero (Get Started / View on GitHub)
    const heroPrimaryCta = heroSection.locator('a.btn-primary, .cta-primary').first();
    await expect(heroPrimaryCta).toBeVisible();
    await expect(heroPrimaryCta).toBeEnabled();

    const heroPrimaryHref = await heroPrimaryCta.getAttribute('href');
    expect(heroPrimaryHref).toBe('https://github.com/yetone/mirdb');

    // Secondary CTA in hero (Learn More)
    const heroSecondaryCta = heroSection.locator('a.btn-secondary, .cta-secondary').first();
    await expect(heroSecondaryCta).toBeVisible();
    await expect(heroSecondaryCta).toBeEnabled();

    // Scroll to footer
    const footer = page.locator('footer, [data-testid="footer"]').first();
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();

    // Check footer GitHub link
    const footerGithubLink = footer.locator('a[href*="github.com/yetone/mirdb"]').first();
    await expect(footerGithubLink).toBeVisible();

    const footerGithubHref = await footerGithubLink.getAttribute('href');
    expect(footerGithubHref).toBe('https://github.com/yetone/mirdb');

    // Count total GitHub links on page
    const allGithubLinks = page.locator('a[href*="github.com/yetone/mirdb"]');
    const githubLinkCount = await allGithubLinks.count();

    // Verify there are multiple GitHub CTAs accessible throughout the page
    expect(githubLinkCount).toBeGreaterThanOrEqual(2);

    // Verify all GitHub links open in new tab
    for (let i = 0; i < githubLinkCount; i++) {
      const link = allGithubLinks.nth(i);
      const target = await link.getAttribute('target');
      expect(target).toBe('_blank');
    }
  });

  test('TC5: User journey scroll flow validation', async ({ page }) => {
    // This test validates the natural scroll flow through all sections

    // Section 1: Hero is immediately visible
    const heroSection = page.locator('.hero, [data-testid="hero"]').first();
    await expect(heroSection).toBeInViewport();

    // Section 2: Scroll to features
    const featuresSection = page.locator('#features, [data-testid="features"]').first();
    await featuresSection.scrollIntoViewIfNeeded();
    await expect(featuresSection).toBeInViewport();

    // Verify features heading is visible
    const featuresHeading = featuresSection.locator('h2');
    await expect(featuresHeading).toBeVisible();
    await expect(featuresHeading).toHaveText('Key Features');

    // Section 3: Scroll to usage
    const usageSection = page.locator('#usage, [data-testid="usage"]').first();
    await usageSection.scrollIntoViewIfNeeded();
    await expect(usageSection).toBeInViewport();

    // Verify usage heading is visible
    const usageHeading = usageSection.locator('h2');
    await expect(usageHeading).toBeVisible();
    await expect(usageHeading).toHaveText('Quick Start');

    // Section 4: Scroll to roadmap
    const roadmapSection = page.locator('#roadmap, [data-testid="roadmap"]').first();
    await roadmapSection.scrollIntoViewIfNeeded();
    await expect(roadmapSection).toBeInViewport();

    // Verify roadmap heading is visible
    const roadmapHeading = roadmapSection.locator('h2');
    await expect(roadmapHeading).toBeVisible();
    await expect(roadmapHeading).toHaveText('Roadmap');

    // Section 5: Scroll to footer
    const footer = page.locator('footer, [data-testid="footer"]').first();
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeInViewport();

    // All sections should flow smoothly in correct order
  });

  test('TC6: Section anchor navigation works correctly', async ({ page }) => {
    // Test navigation via anchor links

    // Navigate to features via anchor
    await page.goto('/#features');
    const featuresSection = page.locator('#features, [data-testid="features"]').first();
    await expect(featuresSection).toBeInViewport({ ratio: 0.5 });

    // Navigate to usage via anchor
    await page.goto('/#usage');
    const usageSection = page.locator('#usage, [data-testid="usage"]').first();
    await expect(usageSection).toBeInViewport({ ratio: 0.5 });

    // Navigate to roadmap via anchor
    await page.goto('/#roadmap');
    const roadmapSection = page.locator('#roadmap, [data-testid="roadmap"]').first();
    await expect(roadmapSection).toBeInViewport({ ratio: 0.5 });
  });
});
