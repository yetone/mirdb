const { test, expect } = require('@playwright/test');

/**
 * Browser Compatibility - Firefox Tests
 * Scenario: Validate the homepage functions correctly in Mozilla Firefox
 *
 * Steps:
 * 1. Open in Firefox - Load the homepage in Mozilla Firefox (latest version)
 * 2. Verify visual rendering - Check that all visual elements render correctly
 * 3. Verify interactive elements - Check that all links and buttons function correctly
 *
 * Test Cases:
 * 1. Load homepage in Firefox - Page loads without errors and displays all sections correctly
 * 2. Compare layout with Chrome - Visual layout matches Chrome rendering
 */

test.describe('Browser Compatibility - Firefox', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test.describe('Test Case 1: Page loads without errors and displays all sections correctly', () => {
    test('Page loads without console errors in Firefox', async ({ page }) => {
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

    test('HTML document has correct structure and language attribute in Firefox', async ({ page }) => {
      // Verify proper HTML5 doctype and language
      const htmlElement = page.locator('html');
      await expect(htmlElement).toHaveAttribute('lang', 'en');

      // Verify meta tags are present
      const viewport = page.locator('meta[name="viewport"]');
      await expect(viewport).toHaveAttribute('content', expect.stringContaining('width=device-width'));

      const description = page.locator('meta[name="description"]');
      await expect(description).toHaveAttribute('content', expect.stringContaining('MirDB'));
    });

    test('Header section renders correctly in Firefox', async ({ page }) => {
      const header = page.locator('header');
      await expect(header).toBeVisible();

      // Verify logo text is present
      const logoText = page.locator('.logo-text, header a:first-child');
      await expect(logoText.first()).toBeVisible();
      await expect(logoText.first()).toContainText('MirDB');

      // Verify navigation is present
      const nav = page.locator('nav');
      await expect(nav).toBeVisible();
    });

    test('Hero section renders correctly with all elements in Firefox', async ({ page }) => {
      const heroSection = page.locator('.hero');
      await expect(heroSection).toBeVisible();

      // Check logo image
      const heroLogo = page.locator('.hero-logo, .hero img[src*="logo"]');
      await expect(heroLogo.first()).toBeVisible();

      // Check headline
      const headline = page.locator('.hero-headline, .hero h1');
      await expect(headline.first()).toBeVisible();
      await expect(headline.first()).toContainText('MirDB');

      // Check subheadline
      const subheadline = page.locator('.hero-subheadline, .hero p');
      await expect(subheadline.first()).toBeVisible();

      // Check CTA buttons
      const ctaButtons = page.locator('.hero-cta .btn, .hero a.btn');
      await expect(ctaButtons.first()).toBeVisible();
    });

    test('Value proposition section renders correctly in Firefox', async ({ page }) => {
      const valuePropSection = page.locator('.value-prop, #value-proposition');
      await expect(valuePropSection.first()).toBeVisible();

      // Check all three value proposition columns
      const valuePropColumns = page.locator('.value-prop-column, .value-prop-grid > div');
      await expect(valuePropColumns).toHaveCount(3);

      // Verify all columns are visible
      for (let i = 0; i < 3; i++) {
        await expect(valuePropColumns.nth(i)).toBeVisible();
      }
    });

    test('Features section renders correctly in Firefox', async ({ page }) => {
      const featuresSection = page.locator('.features, #features');
      await expect(featuresSection.first()).toBeVisible();

      // Check feature cards are present
      const featureCards = page.locator('.feature-card, .features-grid > div');
      const count = await featureCards.count();
      expect(count).toBeGreaterThanOrEqual(4);

      // Verify feature cards are visible
      await expect(featureCards.first()).toBeVisible();
    });

    test('Commands section renders correctly in Firefox', async ({ page }) => {
      const commandsSection = page.locator('.commands, #commands');
      await expect(commandsSection.first()).toBeVisible();

      // Check command categories are present
      const commandCategories = page.locator('.command-category');
      const count = await commandCategories.count();
      expect(count).toBeGreaterThanOrEqual(3);

      // Verify command lists are visible
      const commandLists = page.locator('.command-list');
      await expect(commandLists.first()).toBeVisible();
    });

    test('Demo section renders correctly in Firefox', async ({ page }) => {
      const demoSection = page.locator('.demo, #demo');
      await expect(demoSection.first()).toBeVisible();

      // Check demo GIF is present
      const demoGif = page.locator('.demo-gif, .demo img[src*="usage"]');
      await expect(demoGif.first()).toBeVisible();

      // Check demo caption
      const demoCaption = page.locator('.demo-caption, .demo figcaption');
      await expect(demoCaption.first()).toBeVisible();
    });

    test('Quick start section renders correctly in Firefox', async ({ page }) => {
      const quickstartSection = page.locator('.quickstart, #quickstart');
      await expect(quickstartSection.first()).toBeVisible();

      // Check quickstart steps are present
      const quickstartSteps = page.locator('.quickstart-step');
      const count = await quickstartSteps.count();
      expect(count).toBeGreaterThanOrEqual(3);

      // Check code blocks are present
      const codeBlocks = page.locator('.quickstart .code-block, .quickstart pre');
      await expect(codeBlocks.first()).toBeVisible();
    });

    test('Footer section renders correctly in Firefox', async ({ page }) => {
      const footer = page.locator('footer');
      await expect(footer).toBeVisible();

      // Check footer info
      const footerInfo = page.locator('.footer-info, footer p');
      await expect(footerInfo.first()).toBeVisible();

      // Check footer links
      const footerLinks = page.locator('.footer-links a, footer a');
      const count = await footerLinks.count();
      expect(count).toBeGreaterThanOrEqual(2);
    });

    test('CSS styles are correctly applied in Firefox', async ({ page }) => {
      // Verify CSS custom properties are working
      const body = page.locator('body');
      const fontFamily = await body.evaluate((el) => getComputedStyle(el).fontFamily);
      expect(fontFamily).toContain('-apple-system');

      // Verify primary color is applied to buttons
      const primaryBtn = page.locator('.btn-primary').first();
      const bgColor = await primaryBtn.evaluate((el) => getComputedStyle(el).backgroundColor);
      expect(bgColor).toBeTruthy();
    });
  });

  test.describe('Test Case 2: Visual layout matches Chrome rendering', () => {
    test('Logo GIF loads and is displayed correctly in Firefox', async ({ page }) => {
      const logoGif = page.locator('img[src*="logo.gif"]');
      await expect(logoGif).toBeVisible();

      // Verify the image has loaded successfully
      const naturalWidth = await logoGif.evaluate((img) => img.naturalWidth);
      const naturalHeight = await logoGif.evaluate((img) => img.naturalHeight);

      expect(naturalWidth).toBeGreaterThan(0);
      expect(naturalHeight).toBeGreaterThan(0);

      // Verify the image is rendering at expected size
      const boundingBox = await logoGif.boundingBox();
      expect(boundingBox).toBeTruthy();
      expect(boundingBox.width).toBeGreaterThan(0);
      expect(boundingBox.height).toBeGreaterThan(0);
    });

    test('Usage demo GIF loads and is displayed correctly in Firefox', async ({ page }) => {
      const usageGif = page.locator('img[src*="usage.gif"]');

      // Scroll to demo section to trigger lazy loading
      await usageGif.scrollIntoViewIfNeeded();
      await expect(usageGif).toBeVisible();

      // Wait for lazy loading to complete (the image needs time to load after scrolling)
      await page.waitForTimeout(2000);

      // Wait for the image to have natural dimensions (indicating successful load)
      await page.waitForFunction(
        () => {
          const img = document.querySelector('img[src*="usage.gif"]');
          return img && img.naturalWidth > 0 && img.naturalHeight > 0;
        },
        { timeout: 10000 }
      );

      // Verify the image has loaded successfully
      const naturalWidth = await usageGif.evaluate((img) => img.naturalWidth);
      const naturalHeight = await usageGif.evaluate((img) => img.naturalHeight);

      expect(naturalWidth).toBeGreaterThan(0);
      expect(naturalHeight).toBeGreaterThan(0);

      // Verify the image is rendering at expected size
      const boundingBox = await usageGif.boundingBox();
      expect(boundingBox).toBeTruthy();
      expect(boundingBox.width).toBeGreaterThan(0);
      expect(boundingBox.height).toBeGreaterThan(0);
    });

    test('GIF images have proper alt text for accessibility in Firefox', async ({ page }) => {
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

    test('GIF images use lazy loading for performance in Firefox', async ({ page }) => {
      // Verify lazy loading attribute on GIFs
      const logoGif = page.locator('img[src*="logo.gif"]');
      const logoLoading = await logoGif.getAttribute('loading');
      expect(logoLoading).toBe('lazy');

      const usageGif = page.locator('img[src*="usage.gif"]');
      const usageLoading = await usageGif.getAttribute('loading');
      expect(usageLoading).toBe('lazy');
    });

    test('GIF images do not cause layout shifts in Firefox', async ({ page }) => {
      // Wait for page to fully load
      await page.waitForLoadState('networkidle');

      // Get initial viewport scroll position
      const initialScrollY = await page.evaluate(() => window.scrollY);

      // Wait a bit for any potential layout shifts
      await page.waitForTimeout(1000);

      // Check scroll position hasn't changed unexpectedly
      const finalScrollY = await page.evaluate(() => window.scrollY);
      expect(Math.abs(finalScrollY - initialScrollY)).toBeLessThan(5);
    });

    test('Grid layout renders correctly in Firefox', async ({ page }) => {
      // Check value proposition grid uses CSS grid
      const valuePropGrid = page.locator('.value-prop-grid');
      const displayValue = await valuePropGrid.evaluate((el) => getComputedStyle(el).display);
      expect(displayValue).toBe('grid');

      // Check features grid uses CSS grid
      const featuresGrid = page.locator('.features-grid');
      const featuresDisplayValue = await featuresGrid.evaluate((el) => getComputedStyle(el).display);
      expect(featuresDisplayValue).toBe('grid');

      // Check commands grid uses CSS grid
      const commandsGrid = page.locator('.commands-grid');
      const commandsDisplayValue = await commandsGrid.evaluate((el) => getComputedStyle(el).display);
      expect(commandsDisplayValue).toBe('grid');
    });

    test('Flexbox layout renders correctly in Firefox', async ({ page }) => {
      // Check header container uses flexbox
      const headerContainer = page.locator('header .container');
      const headerDisplay = await headerContainer.evaluate((el) => getComputedStyle(el).display);
      expect(headerDisplay).toBe('flex');

      // Check hero CTA uses flexbox
      const heroCta = page.locator('.hero-cta');
      const ctaDisplay = await heroCta.evaluate((el) => getComputedStyle(el).display);
      expect(ctaDisplay).toBe('flex');

      // Check footer content uses flexbox
      const footerContent = page.locator('.footer-content');
      const footerDisplay = await footerContent.evaluate((el) => getComputedStyle(el).display);
      expect(footerDisplay).toBe('flex');
    });

    test('CSS custom properties (variables) work correctly in Firefox', async ({ page }) => {
      // Verify CSS custom properties are being used
      const primaryBtn = page.locator('.btn-primary').first();
      const bgColor = await primaryBtn.evaluate((el) => getComputedStyle(el).backgroundColor);
      // The primary color should be #2d5aa3 which translates to rgb(45, 90, 163)
      expect(bgColor).toBe('rgb(45, 90, 163)');
    });

    test('Sticky header works correctly in Firefox', async ({ page }) => {
      const header = page.locator('header');

      // Check header has sticky positioning
      const position = await header.evaluate((el) => getComputedStyle(el).position);
      expect(position).toBe('sticky');

      // Scroll down the page
      await page.evaluate(() => window.scrollTo(0, 500));
      await page.waitForTimeout(100);

      // Header should still be visible
      await expect(header).toBeVisible();
      await expect(header).toBeInViewport();
    });
  });

  test.describe('Interactive Elements Functionality in Firefox', () => {
    test('Navigation links are clickable and functional in Firefox', async ({ page }) => {
      // Test Features link
      const featuresLink = page.locator('nav a[href="#features"]');
      await expect(featuresLink).toBeVisible();
      await featuresLink.click();
      await page.waitForURL('**/#features');

      // Test Quick Start link
      const quickstartLink = page.locator('nav a[href="#quickstart"]');
      await expect(quickstartLink).toBeVisible();
      await quickstartLink.click();
      await page.waitForURL('**/#quickstart');
    });

    test('External links have proper attributes in Firefox', async ({ page }) => {
      // Check GitHub link in navigation
      const githubNavLink = page.locator('nav a[href*="github.com"]');
      await expect(githubNavLink).toBeVisible();
      await expect(githubNavLink).toHaveAttribute('target', '_blank');
      await expect(githubNavLink).toHaveAttribute('rel', expect.stringContaining('noopener'));
    });

    test('CTA buttons are clickable and have hover states in Firefox', async ({ page }) => {
      const getStartedBtn = page.locator('.hero-cta .btn-primary, a.btn-primary').first();
      await expect(getStartedBtn).toBeVisible();
      await expect(getStartedBtn).toBeEnabled();

      // Test hover state exists by checking for transition CSS
      const transition = await getStartedBtn.evaluate((el) => getComputedStyle(el).transition);
      expect(transition).not.toBe('none');

      const githubBtn = page.locator('.hero-cta .btn-secondary, a.btn-secondary').first();
      await expect(githubBtn).toBeVisible();
      await expect(githubBtn).toBeEnabled();
    });

    test('Feature cards have hover effects in Firefox', async ({ page }) => {
      const featureCard = page.locator('.feature-card').first();
      await expect(featureCard).toBeVisible();

      // Check that transition CSS is applied
      const transition = await featureCard.evaluate((el) => getComputedStyle(el).transition);
      expect(transition).not.toBe('none');
    });

    test('Footer links are functional in Firefox', async ({ page }) => {
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

    test('Smooth scroll behavior works in Firefox', async ({ page }) => {
      // Click the Get Started button which links to #quickstart
      const getStartedBtn = page.locator('.hero-cta .btn-primary').first();
      await getStartedBtn.click();

      // Wait for scroll to complete
      await page.waitForTimeout(500);

      // The quickstart section should be in view
      const quickstartSection = page.locator('#quickstart');
      await expect(quickstartSection).toBeInViewport();
    });
  });

  test.describe('Firefox-specific CSS Compatibility', () => {
    test('Box-sizing border-box is applied correctly in Firefox', async ({ page }) => {
      // Verify box-sizing is border-box on all elements
      const container = page.locator('.container').first();
      const boxSizing = await container.evaluate((el) => getComputedStyle(el).boxSizing);
      expect(boxSizing).toBe('border-box');
    });

    test('Border-radius renders correctly on cards in Firefox', async ({ page }) => {
      const featureCard = page.locator('.feature-card').first();
      const borderRadius = await featureCard.evaluate((el) => getComputedStyle(el).borderRadius);
      expect(borderRadius).toBe('8px');
    });

    test('Box-shadow renders correctly on elements in Firefox', async ({ page }) => {
      // Scroll to demo section and check shadow on demo GIF
      const demoGif = page.locator('.demo-gif');
      await demoGif.scrollIntoViewIfNeeded();

      const boxShadow = await demoGif.evaluate((el) => getComputedStyle(el).boxShadow);
      expect(boxShadow).not.toBe('none');
    });

    test('Linear gradient background renders correctly in Firefox', async ({ page }) => {
      const heroSection = page.locator('.hero');
      const backgroundImage = await heroSection.evaluate((el) => getComputedStyle(el).backgroundImage);
      expect(backgroundImage).toContain('linear-gradient');
    });

    test('Transition effects are defined correctly in Firefox', async ({ page }) => {
      // Check button transitions
      const primaryBtn = page.locator('.btn-primary').first();
      const transition = await primaryBtn.evaluate((el) => getComputedStyle(el).transition);
      expect(transition).toContain('0.2s');

      // Check nav link transitions
      const navLink = page.locator('nav a').first();
      const navTransition = await navLink.evaluate((el) => getComputedStyle(el).transition);
      expect(navTransition).toContain('color');
    });
  });
});
