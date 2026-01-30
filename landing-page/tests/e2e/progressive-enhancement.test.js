/**
 * MirDB Landing Page - Progressive Enhancement Tests
 * Owner: Scenario 17 - Progressive Enhancement
 *
 * Tests verify that core content is accessible without JavaScript.
 * Only interactive features (copy buttons, animations) may degrade gracefully.
 */

const { test, expect } = require('@playwright/test');

test.describe('Progressive Enhancement - No JavaScript', () => {
  // Use a context with JavaScript disabled for all tests
  test.use({ javaScriptEnabled: false });

  test('should load page and display all content with JavaScript disabled', async ({ page }) => {
    // Navigate to the landing page with JS disabled
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    // Verify the page has loaded by checking for basic structure
    const html = page.locator('html');
    await expect(html).toHaveAttribute('lang', 'en');

    // Verify main content area exists
    const mainContent = page.locator('#main-content');
    await expect(mainContent).toBeVisible();

    // Verify all major sections exist and are visible
    const sections = ['#hero', '#features', '#usage', '#architecture', '#configuration', '#getting-started'];
    for (const section of sections) {
      await expect(page.locator(section)).toBeVisible();
    }

    // Verify footer exists
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();
  });

  test('should display hero section content including logo and text without JS', async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    // Verify hero section is visible
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // Verify hero logo is visible and has proper alt text
    const heroLogo = page.locator('.hero-logo');
    await expect(heroLogo).toBeVisible();
    await expect(heroLogo).toHaveAttribute('alt', 'MirDB Logo');

    // Verify hero title
    const heroTitle = page.locator('#hero-title');
    await expect(heroTitle).toBeVisible();
    await expect(heroTitle).toHaveText('MirDB');

    // Verify tagline is visible
    const tagline = page.locator('.hero-tagline');
    await expect(tagline).toBeVisible();
    await expect(tagline).toContainText('Persistent Key-Value Store');

    // Verify value proposition text
    const valueProp = page.locator('.hero-value-proposition');
    await expect(valueProp).toBeVisible();
    await expect(valueProp).toContainText('memcached replacement');

    // Verify CTA buttons are visible and have correct hrefs
    const getStartedBtn = page.locator('.hero-cta a[href="#getting-started"]');
    await expect(getStartedBtn).toBeVisible();
    await expect(getStartedBtn).toHaveText('Get Started');

    const githubBtn = page.locator('.hero-cta a[href="https://github.com/yetone/mirdb"]');
    await expect(githubBtn).toBeVisible();
    await expect(githubBtn).toContainText('GitHub');
  });

  test('should display all feature cards and descriptions without JS', async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    // Verify features section is visible
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Verify features title
    const featuresTitle = page.locator('#features-title');
    await expect(featuresTitle).toBeVisible();
    await expect(featuresTitle).toHaveText('Key Features');

    // Verify all 4 feature cards exist and are visible
    const featureCards = page.locator('.feature-card');
    await expect(featureCards).toHaveCount(4);

    // Verify each feature card has a title and description
    const featureTitles = [
      'Memcached Compatible',
      'Persistent Storage',
      'High Performance',
      'Configurable'
    ];

    for (const title of featureTitles) {
      const card = page.locator('.feature-card', { has: page.locator(`h3:has-text("${title}")`) });
      await expect(card).toBeVisible();

      // Verify each card has a description
      const description = card.locator('.feature-description');
      await expect(description).toBeVisible();
      const descText = await description.textContent();
      expect(descText.length).toBeGreaterThan(20); // Ensure meaningful content
    }

    // Verify feature icons are visible (even if decorative)
    const featureIcons = page.locator('.feature-icon');
    await expect(featureIcons).toHaveCount(4);
  });

  test('should display code examples without JS (copy functionality may be disabled)', async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    // Verify usage section exists
    const usageSection = page.locator('#usage');
    await expect(usageSection).toBeVisible();

    // Verify usage title
    const usageTitle = page.locator('#usage-title');
    await expect(usageTitle).toBeVisible();
    await expect(usageTitle).toHaveText('Usage');

    // Verify code blocks exist throughout the page (in configuration, getting-started, etc.)
    // These are the primary code examples users need to see
    const codeBlocks = page.locator('.code-block, pre code');
    const codeBlockCount = await codeBlocks.count();
    expect(codeBlockCount).toBeGreaterThan(0);

    // Verify configuration code block is visible in the configuration section
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();
    const configCode = configSection.locator('.code-block');
    await expect(configCode).toBeVisible();

    // Verify getting started section has code examples
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeVisible();
    const gettingStartedCode = gettingStartedSection.locator('.code-block');
    const gsCodeCount = await gettingStartedCode.count();
    expect(gsCodeCount).toBeGreaterThan(0);

    // Verify the code content is readable (not empty)
    const firstCodeBlock = codeBlocks.first();
    const codeContent = await firstCodeBlock.textContent();
    expect(codeContent.trim().length).toBeGreaterThan(0);
  });

  test('should have navigation links that work using anchor jumps without JS', async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    // Verify navigation is visible
    const nav = page.locator('nav');
    await expect(nav).toBeVisible();

    // Verify navigation links exist and have correct href attributes
    const navLinks = page.locator('.nav-links a');
    await expect(navLinks).toHaveCount(4);

    // Verify each anchor link has proper href attribute
    // Check Features link
    const featuresLink = page.locator('.nav-links a[href="#features"]');
    await expect(featuresLink).toBeVisible();
    await expect(featuresLink).toHaveText('Features');

    // Check Usage link
    const usageLink = page.locator('.nav-links a[href="#usage"]');
    await expect(usageLink).toBeVisible();
    await expect(usageLink).toHaveText('Usage');

    // Check Architecture link
    const architectureLink = page.locator('.nav-links a[href="#architecture"]');
    await expect(architectureLink).toBeVisible();
    await expect(architectureLink).toHaveText('Architecture');

    // Check GitHub external link (should still work without JS)
    const githubLink = page.locator('.nav-links a[href="https://github.com/yetone/mirdb"]');
    await expect(githubLink).toBeVisible();
    await expect(githubLink).toHaveText('GitHub');
    await expect(githubLink).toHaveAttribute('target', '_blank');
    await expect(githubLink).toHaveAttribute('rel', /noopener/);

    // Test that clicking anchor links navigates to the correct section (without JS, uses native browser behavior)
    // Click on Features link and verify we can reach that section
    await featuresLink.click();

    // After clicking, verify the URL hash changed
    await expect(page).toHaveURL(/#features$/);

    // Verify the features section target exists
    const featuresTarget = page.locator('#features');
    await expect(featuresTarget).toBeVisible();
  });

  test('should have skip link for accessibility that works without JS', async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    // Verify skip link exists
    const skipLink = page.locator('.skip-link');
    await expect(skipLink).toHaveAttribute('href', '#main-content');

    // Verify the target for skip link exists
    const mainContent = page.locator('#main-content');
    await expect(mainContent).toBeAttached();
  });

  test('should display architecture section content without JS', async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    // Verify architecture section is visible
    const archSection = page.locator('#architecture');
    await expect(archSection).toBeVisible();

    // Verify architecture title
    const archTitle = page.locator('#architecture-title');
    await expect(archTitle).toBeVisible();
    await expect(archTitle).toHaveText('Architecture');

    // Verify architecture intro text is visible
    const archIntro = page.locator('.architecture-intro');
    await expect(archIntro).toBeVisible();
    await expect(archIntro).toContainText('LSM');

    // Verify architecture diagram is visible
    const archDiagram = page.locator('.architecture-diagram');
    await expect(archDiagram).toBeVisible();

    // Verify explanation cards are visible
    const explanationCards = page.locator('.explanation-card');
    const cardCount = await explanationCards.count();
    expect(cardCount).toBeGreaterThan(0);
  });

  test('should display footer content without JS', async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    // Verify footer is visible
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    // Verify GitHub link in footer
    const footerGithub = footer.locator('a[href="https://github.com/yetone/mirdb"]');
    await expect(footerGithub).toBeVisible();

    // Verify license information is visible
    const licenseInfo = footer.locator('.footer-license');
    await expect(licenseInfo).toBeVisible();
    await expect(licenseInfo).toContainText('MIT');

    // Verify attribution is visible
    const attribution = footer.locator('.footer-attribution');
    await expect(attribution).toBeVisible();
  });

  test('should render semantic HTML structure without JS', async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    // Verify semantic HTML elements are used
    await expect(page.locator('nav')).toBeVisible();
    await expect(page.locator('main')).toBeVisible();
    await expect(page.locator('footer')).toBeVisible();

    // Verify heading hierarchy
    const h1 = page.locator('h1');
    await expect(h1).toHaveCount(1);
    await expect(h1).toBeVisible();

    // Verify sections have proper headings
    const h2Elements = page.locator('h2');
    const h2Count = await h2Elements.count();
    expect(h2Count).toBeGreaterThanOrEqual(5); // At least: Features, Usage, Architecture, Configuration, Getting Started

    // Verify ARIA landmarks are present
    await expect(page.locator('[aria-label="Main navigation"]')).toBeVisible();
    await expect(page.locator('[role="contentinfo"]')).toBeVisible(); // Footer
  });
});
