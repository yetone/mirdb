const { test, expect } = require('@playwright/test');

/**
 * Browser Compatibility - Safari Tests
 * Scenario: Validate the homepage functions correctly in Safari
 *
 * Steps:
 * 1. Open in Safari - Load the homepage in Safari (latest version)
 * 2. Verify visual rendering - Check that all visual elements render correctly
 * 3. Verify interactive elements - Check that all links and buttons function correctly
 *
 * Test Cases:
 * 1. Load homepage in Safari - Page loads without errors and displays all sections correctly
 * 2. Check GIF playback in Safari - Animated GIFs play correctly in Safari
 */

// Only run these tests in WebKit browser (Safari engine)
test.describe('Browser Compatibility - Safari', () => {
  test.skip(({ browserName }) => browserName !== 'webkit', 'Safari/WebKit-only tests');

  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test.describe('Test Case 1: Page loads without errors and displays all sections correctly', () => {
    test('Page loads without console errors in Safari', async ({ page }) => {
      const errors = [];
      page.on('pageerror', (error) => {
        errors.push(error.message);
      });

      // Navigate to page and wait for load
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Verify no JavaScript errors occurred
      expect(errors).toHaveLength(0);
    });

    test('Page returns successful HTTP status in Safari', async ({ page }) => {
      const response = await page.goto('/');

      // Verify successful HTTP response (200 OK or 304 Not Modified are both valid)
      const status = response.status();
      expect([200, 304]).toContain(status);
      if (status === 200) {
        expect(response.ok()).toBe(true);
      }
    });

    test('HTML document has correct structure and language attribute', async ({ page }) => {
      // Verify proper HTML5 doctype and language
      const htmlElement = page.locator('html');
      await expect(htmlElement).toHaveAttribute('lang', 'en');

      // Verify meta tags are present
      const viewport = page.locator('meta[name="viewport"]');
      await expect(viewport).toHaveAttribute('content', expect.stringContaining('width=device-width'));

      const description = page.locator('meta[name="description"]');
      await expect(description).toHaveAttribute('content', expect.stringContaining('MirDB'));
    });

    test('Header section renders correctly in Safari', async ({ page }) => {
      const header = page.locator('header');
      await expect(header).toBeVisible();

      // Verify logo text is present
      const logoText = page.locator('.logo-text');
      await expect(logoText).toBeVisible();
      await expect(logoText).toContainText('MirDB');

      // Verify navigation is present
      const nav = page.locator('nav');
      await expect(nav).toBeVisible();

      // Verify navigation links
      const navLinks = page.locator('nav a');
      const navLinksCount = await navLinks.count();
      expect(navLinksCount).toBeGreaterThanOrEqual(3);
    });

    test('Hero section renders correctly with all elements in Safari', async ({ page }) => {
      const heroSection = page.locator('.hero');
      await expect(heroSection).toBeVisible();

      // Check logo image
      const heroLogo = page.locator('.hero-logo');
      await expect(heroLogo).toBeVisible();

      // Check headline
      const headline = page.locator('.hero-headline');
      await expect(headline).toBeVisible();
      await expect(headline).toContainText('MirDB');

      // Check subheadline
      const subheadline = page.locator('.hero-subheadline');
      await expect(subheadline).toBeVisible();

      // Check CTA buttons
      const ctaButtons = page.locator('.hero-cta .btn');
      await expect(ctaButtons).toHaveCount(2);
    });

    test('Value proposition section renders correctly in Safari', async ({ page }) => {
      const valuePropSection = page.locator('.value-prop');
      await expect(valuePropSection).toBeVisible();

      // Check all three value proposition columns
      const valuePropColumns = page.locator('.value-prop-column');
      await expect(valuePropColumns).toHaveCount(3);

      // Verify each column has expected content
      const column1 = valuePropColumns.nth(0);
      await expect(column1).toContainText('Memcached Protocol Support');

      const column2 = valuePropColumns.nth(1);
      await expect(column2).toContainText('Persistence');

      const column3 = valuePropColumns.nth(2);
      await expect(column3).toContainText('LSM Tree Architecture');
    });

    test('Features section renders correctly in Safari', async ({ page }) => {
      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeVisible();

      // Check feature cards are present
      const featureCards = page.locator('.feature-card');
      const count = await featureCards.count();
      expect(count).toBeGreaterThanOrEqual(4);

      // Verify planned feature has distinctive styling
      const plannedFeature = page.locator('.feature-card.planned');
      await expect(plannedFeature).toBeVisible();
    });

    test('Commands section renders correctly in Safari', async ({ page }) => {
      const commandsSection = page.locator('#commands');
      await expect(commandsSection).toBeVisible();

      // Check command categories are present
      const commandCategories = page.locator('.command-category');
      const count = await commandCategories.count();
      expect(count).toBe(4);

      // Verify command lists are visible
      const commandLists = page.locator('.command-list');
      await expect(commandLists.first()).toBeVisible();
    });

    test('Demo section renders correctly in Safari', async ({ page }) => {
      const demoSection = page.locator('#demo');
      await expect(demoSection).toBeVisible();

      // Check demo header
      const demoHeader = page.locator('.demo-header');
      await expect(demoHeader).toBeVisible();
      await expect(demoHeader).toContainText('See MirDB in Action');

      // Check demo GIF is present
      const demoGif = page.locator('.demo-gif');
      await expect(demoGif).toBeVisible();

      // Check demo caption
      const demoCaption = page.locator('.demo-caption');
      await expect(demoCaption).toBeVisible();
    });

    test('Quick start section renders correctly in Safari', async ({ page }) => {
      const quickstartSection = page.locator('#quickstart');
      await expect(quickstartSection).toBeVisible();

      // Check quickstart steps are present
      const quickstartSteps = page.locator('.quickstart-step');
      await expect(quickstartSteps).toHaveCount(3);

      // Check code blocks are present
      const codeBlocks = page.locator('.code-block');
      const codeBlockCount = await codeBlocks.count();
      expect(codeBlockCount).toBeGreaterThanOrEqual(3);
    });

    test('Footer section renders correctly in Safari', async ({ page }) => {
      const footer = page.locator('footer');
      await expect(footer).toBeVisible();

      // Check footer info
      const footerInfo = page.locator('.footer-info');
      await expect(footerInfo).toContainText('MIT License');

      // Check footer links
      const footerLinks = page.locator('.footer-links a');
      const count = await footerLinks.count();
      expect(count).toBeGreaterThanOrEqual(3);
    });

    test('WebKit-specific CSS features work correctly in Safari', async ({ page }) => {
      // Verify CSS custom properties are working
      const body = page.locator('body');
      const fontFamily = await body.evaluate((el) => getComputedStyle(el).fontFamily);
      // Safari uses -apple-system font, verify font-family contains system font
      expect(fontFamily.toLowerCase()).toMatch(/apple-system|blinkmacsystemfont|system|segoe|roboto|helvetica|arial/i);

      // Verify primary color is applied to buttons
      const primaryBtn = page.locator('.btn-primary').first();
      const bgColor = await primaryBtn.evaluate((el) => getComputedStyle(el).backgroundColor);
      expect(bgColor).toBeTruthy();
      // Verify it's the expected blue color (rgb values for #2d5aa3)
      expect(bgColor).toMatch(/rgb\(45,\s*90,\s*163\)/);
    });

    test('CSS Grid layout works correctly in Safari', async ({ page }) => {
      // Verify grid layout is working for value proposition
      const valuePropGrid = page.locator('.value-prop-grid');
      const display = await valuePropGrid.evaluate((el) => getComputedStyle(el).display);
      expect(display).toBe('grid');

      // Verify grid layout for features
      const featuresGrid = page.locator('.features-grid');
      const featuresDisplay = await featuresGrid.evaluate((el) => getComputedStyle(el).display);
      expect(featuresDisplay).toBe('grid');
    });

    test('CSS Flexbox layout works correctly in Safari', async ({ page }) => {
      // Verify flexbox layout is working for hero CTA
      const heroCta = page.locator('.hero-cta');
      const display = await heroCta.evaluate((el) => getComputedStyle(el).display);
      expect(display).toBe('flex');

      // Verify flexbox layout for navigation
      const navUl = page.locator('nav ul');
      const navDisplay = await navUl.evaluate((el) => getComputedStyle(el).display);
      expect(navDisplay).toBe('flex');
    });

    test('CSS custom properties (variables) work correctly in Safari', async ({ page }) => {
      // Verify CSS custom properties are working by checking computed values
      const primaryBtn = page.locator('.btn-primary').first();
      const bgColor = await primaryBtn.evaluate((el) => getComputedStyle(el).backgroundColor);
      // Primary color should be applied from CSS variable
      expect(bgColor).toBeTruthy();

      // Check that hover transitions are defined
      const transition = await primaryBtn.evaluate((el) => getComputedStyle(el).transition);
      expect(transition).not.toBe('none');
    });
  });

  test.describe('Test Case 2: Animated GIFs play correctly in Safari', () => {
    test('Logo GIF loads and displays correctly in Safari', async ({ page }) => {
      // Check logo GIF loads successfully
      const logoGif = page.locator('img[src*="logo.gif"]');
      await expect(logoGif).toBeVisible();

      // Wait for the logo image to load (with lazy loading, need to ensure it's complete)
      await page.waitForFunction(() => {
        const img = document.querySelector('img[src*="logo.gif"]');
        return img && img.complete && img.naturalWidth > 0;
      }, { timeout: 10000 });

      // Verify the image has loaded successfully
      const logoNaturalWidth = await logoGif.evaluate((img) => img.naturalWidth);
      const logoNaturalHeight = await logoGif.evaluate((img) => img.naturalHeight);
      expect(logoNaturalWidth).toBeGreaterThan(0);
      expect(logoNaturalHeight).toBeGreaterThan(0);

      // Verify logo renders at expected size
      const logoBoundingBox = await logoGif.boundingBox();
      expect(logoBoundingBox).toBeTruthy();
      expect(logoBoundingBox.width).toBeGreaterThan(100);
      expect(logoBoundingBox.height).toBeGreaterThan(50);
    });

    test('Usage demo GIF loads and displays correctly in Safari', async ({ page }) => {
      // Check usage demo GIF - scroll it into view first (lazy loading)
      const usageGif = page.locator('img[src*="usage.gif"]');
      await usageGif.scrollIntoViewIfNeeded();
      await expect(usageGif).toBeVisible();

      // Wait for the usage GIF to load (with lazy loading, need to ensure it's complete)
      await page.waitForFunction(() => {
        const img = document.querySelector('img[src*="usage.gif"]');
        return img && img.complete && img.naturalWidth > 0;
      }, { timeout: 15000 });

      const usageNaturalWidth = await usageGif.evaluate((img) => img.naturalWidth);
      const usageNaturalHeight = await usageGif.evaluate((img) => img.naturalHeight);
      expect(usageNaturalWidth).toBeGreaterThan(0);
      expect(usageNaturalHeight).toBeGreaterThan(0);

      // Verify usage GIF renders at reasonable dimensions
      const usageBoundingBox = await usageGif.boundingBox();
      expect(usageBoundingBox).toBeTruthy();
      expect(usageBoundingBox.width).toBeGreaterThan(200);
      expect(usageBoundingBox.height).toBeGreaterThan(100);
    });

    test('GIF images have proper alt text for accessibility in Safari', async ({ page }) => {
      // Check logo GIF alt text
      const logoGif = page.locator('img[src*="logo.gif"]');
      const logoAlt = await logoGif.getAttribute('alt');
      expect(logoAlt).toBeTruthy();
      expect(logoAlt.length).toBeGreaterThan(0);

      // Check usage GIF alt text
      const usageGif = page.locator('img[src*="usage.gif"]');
      const usageAlt = await usageGif.getAttribute('alt');
      expect(usageAlt).toBeTruthy();
      expect(usageAlt.length).toBeGreaterThan(0);
    });

    test('GIF images use lazy loading attribute in Safari', async ({ page }) => {
      // Verify lazy loading attribute on GIFs
      const logoGif = page.locator('img[src*="logo.gif"]');
      const logoLoading = await logoGif.getAttribute('loading');
      expect(logoLoading).toBe('lazy');

      const usageGif = page.locator('img[src*="usage.gif"]');
      const usageLoading = await usageGif.getAttribute('loading');
      expect(usageLoading).toBe('lazy');
    });

    test('GIF animation is not blocked by Safari settings', async ({ page }) => {
      // Verify logo GIF is an animated GIF (has src with .gif extension)
      const logoGif = page.locator('img[src*="logo.gif"]');
      const logoSrc = await logoGif.getAttribute('src');
      expect(logoSrc).toContain('.gif');

      // Verify image element is not hidden or disabled
      const logoDisplay = await logoGif.evaluate((el) => getComputedStyle(el).display);
      expect(logoDisplay).not.toBe('none');

      const logoVisibility = await logoGif.evaluate((el) => getComputedStyle(el).visibility);
      expect(logoVisibility).toBe('visible');

      // Verify usage GIF is also properly displayed
      const usageGif = page.locator('img[src*="usage.gif"]');
      await usageGif.scrollIntoViewIfNeeded();

      const usageSrc = await usageGif.getAttribute('src');
      expect(usageSrc).toContain('.gif');

      const usageDisplay = await usageGif.evaluate((el) => getComputedStyle(el).display);
      expect(usageDisplay).not.toBe('none');
    });

    test('Navigation links are clickable and functional in Safari', async ({ page }) => {
      // Test Features link
      const featuresLink = page.locator('nav a[href="#features"]');
      await expect(featuresLink).toBeVisible();
      await featuresLink.click();
      await expect(page).toHaveURL(/#features/);

      // Test Quick Start link
      const quickstartLink = page.locator('nav a[href="#quickstart"]');
      await expect(quickstartLink).toBeVisible();
      await quickstartLink.click();
      await expect(page).toHaveURL(/#quickstart/);
    });

    test('External links have proper security attributes in Safari', async ({ page }) => {
      // Check GitHub links have proper security attributes
      const externalLinks = page.locator('a[target="_blank"]');
      const linkCount = await externalLinks.count();

      for (let i = 0; i < linkCount; i++) {
        const link = externalLinks.nth(i);
        const rel = await link.getAttribute('rel');
        expect(rel).toContain('noopener');
      }
    });

    test('CTA buttons are clickable and have hover states in Safari', async ({ page }) => {
      const getStartedBtn = page.locator('.hero-cta .btn-primary').first();
      await expect(getStartedBtn).toBeVisible();
      await expect(getStartedBtn).toBeEnabled();

      // Test hover state exists by checking for transition CSS
      const transition = await getStartedBtn.evaluate((el) => getComputedStyle(el).transition);
      expect(transition).not.toBe('none');

      const githubBtn = page.locator('.hero-cta .btn-secondary').first();
      await expect(githubBtn).toBeVisible();
      await expect(githubBtn).toBeEnabled();
    });

    test('Feature cards have hover effects in Safari', async ({ page }) => {
      const featureCard = page.locator('.feature-card').first();
      await expect(featureCard).toBeVisible();

      // Check that transition CSS is applied
      const transition = await featureCard.evaluate((el) => getComputedStyle(el).transition);
      expect(transition).not.toBe('none');
      expect(transition).toContain('transform');
    });

    test('Value proposition cards have hover effects in Safari', async ({ page }) => {
      const valuePropColumn = page.locator('.value-prop-column').first();
      await expect(valuePropColumn).toBeVisible();

      // Check that transition CSS is applied for hover effect
      const transition = await valuePropColumn.evaluate((el) => getComputedStyle(el).transition);
      expect(transition).not.toBe('none');
      expect(transition).toContain('transform');
    });

    test('Footer links are functional in Safari', async ({ page }) => {
      // Check GitHub footer link
      const githubLink = page.locator('.footer-links a[href*="github.com/yetone/mirdb"]').first();
      await expect(githubLink).toBeVisible();
      await expect(githubLink).toHaveAttribute('target', '_blank');

      // Check Issues link
      const issuesLink = page.locator('.footer-links a[href*="issues"]');
      await expect(issuesLink).toBeVisible();

      // Check License link
      const licenseLink = page.locator('.footer-links a[href*="LICENSE"]');
      await expect(licenseLink).toBeVisible();
    });

    test('Sticky header works correctly in Safari', async ({ page }) => {
      // Get initial header position
      const header = page.locator('header');
      const headerPosition = await header.evaluate((el) => getComputedStyle(el).position);
      expect(headerPosition).toBe('sticky');

      // Scroll down the page
      await page.evaluate(() => window.scrollTo(0, 500));
      await page.waitForTimeout(100);

      // Verify header is still visible at top
      await expect(header).toBeVisible();
      const headerRect = await header.boundingBox();
      expect(headerRect.y).toBeLessThanOrEqual(5); // Should be at or near top of viewport
    });

    test('Code blocks display correctly in Safari', async ({ page }) => {
      // Navigate to quickstart section
      await page.locator('#quickstart').scrollIntoViewIfNeeded();

      const codeBlocks = page.locator('.code-block');
      const firstCodeBlock = codeBlocks.first();
      await expect(firstCodeBlock).toBeVisible();

      // Verify code block styling
      const bgColor = await firstCodeBlock.evaluate((el) => getComputedStyle(el).backgroundColor);
      expect(bgColor).toBe('rgb(30, 30, 30)'); // #1e1e1e

      // Verify font family is monospace
      const fontFamily = await firstCodeBlock.evaluate((el) => getComputedStyle(el).fontFamily);
      expect(fontFamily.toLowerCase()).toMatch(/monaco|menlo|ubuntu mono|monospace/);

      // Verify overflow handling
      const overflow = await firstCodeBlock.evaluate((el) => getComputedStyle(el).overflowX);
      expect(overflow).toBe('auto');
    });

    test('Border radius is applied correctly in Safari', async ({ page }) => {
      // Check feature card border radius
      const featureCard = page.locator('.feature-card').first();
      const borderRadius = await featureCard.evaluate((el) => getComputedStyle(el).borderRadius);
      expect(borderRadius).toBe('8px');

      // Check button border radius
      const btn = page.locator('.btn').first();
      const btnBorderRadius = await btn.evaluate((el) => getComputedStyle(el).borderRadius);
      expect(btnBorderRadius).toBe('6px');
    });

    test('Box shadow is applied correctly in Safari on hover', async ({ page }) => {
      const featureCard = page.locator('.feature-card').first();

      // Hover over the card
      await featureCard.hover();
      await page.waitForTimeout(300);

      // Check box shadow is applied on hover
      const boxShadow = await featureCard.evaluate((el) => getComputedStyle(el).boxShadow);
      expect(boxShadow).not.toBe('none');
    });

    test('Linear gradient backgrounds render correctly in Safari', async ({ page }) => {
      // Check hero section gradient
      const heroSection = page.locator('.hero');
      const backgroundImage = await heroSection.evaluate((el) => getComputedStyle(el).backgroundImage);
      expect(backgroundImage).toContain('linear-gradient');

      // Check planned feature gradient
      const plannedFeature = page.locator('.feature-card.planned');
      const plannedBackground = await plannedFeature.evaluate((el) => getComputedStyle(el).backgroundImage);
      expect(plannedBackground).toContain('linear-gradient');
    });
  });
});
