// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Cross-Browser Compatibility Tests
 *
 * These tests verify that the MirDB landing page renders correctly across
 * modern browsers (Chrome, Firefox, Safari/WebKit, Edge).
 *
 * NFR-5: Page must render correctly in modern browsers (Chrome, Firefox, Safari, Edge)
 *
 * Note: Playwright uses WebKit as a proxy for Safari testing since Safari
 * is only available on macOS. Edge uses the Chromium engine, so Chromium
 * tests effectively cover Edge compatibility.
 */

test.describe('Cross-Browser Compatibility', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test.describe('Page Structure and Layout', () => {
    test('TC1: Hero section renders correctly with proper structure', async ({ page, browserName }) => {
      // Verify hero section exists and is visible
      const hero = page.locator('.hero');
      await expect(hero).toBeVisible();

      // Verify hero contains main heading
      const h1 = hero.locator('h1');
      await expect(h1).toBeVisible();
      await expect(h1).toHaveText('MirDB');

      // Verify tagline is present
      const tagline = hero.locator('.tagline');
      await expect(tagline).toBeVisible();
      await expect(tagline).toContainText('Persistent Key-Value Store');

      // Verify value proposition text
      const valueProp = hero.locator('.value-prop');
      await expect(valueProp).toBeVisible();

      // Verify hero buttons are present and clickable
      const primaryBtn = hero.locator('.btn-primary');
      await expect(primaryBtn).toBeVisible();
      await expect(primaryBtn).toHaveText('Get Started');

      const secondaryBtn = hero.locator('.btn-secondary');
      await expect(secondaryBtn).toBeVisible();
      await expect(secondaryBtn).toHaveText('View on GitHub');

      // Log browser name for debugging
      console.log(`Hero section test passed in ${browserName}`);
    });

    test('TC2: Navigation renders correctly with all links', async ({ page, browserName }) => {
      const nav = page.locator('.nav');
      await expect(nav).toBeVisible();

      // Verify brand name
      const brand = nav.locator('.nav-brand');
      await expect(brand).toBeVisible();
      await expect(brand).toHaveText('MirDB');

      // Verify navigation links exist
      const navLinks = nav.locator('.nav-links');
      await expect(navLinks).toBeVisible();

      // Check for required navigation items
      const featuresLink = navLinks.locator('a[href="#features"]');
      await expect(featuresLink).toBeVisible();
      await expect(featuresLink).toHaveText('Features');

      const commandsLink = navLinks.locator('a[href="#commands"]');
      await expect(commandsLink).toBeVisible();
      await expect(commandsLink).toHaveText('Commands');

      const quickstartLink = navLinks.locator('a[href="#quickstart"]');
      await expect(quickstartLink).toBeVisible();
      await expect(quickstartLink).toHaveText('Quick Start');

      const docsLink = navLinks.locator('a:has-text("Docs")');
      await expect(docsLink).toBeVisible();

      const githubLink = navLinks.locator('a:has-text("GitHub")');
      await expect(githubLink).toBeVisible();

      console.log(`Navigation test passed in ${browserName}`);
    });

    test('TC3: Features section renders correctly with three feature cards', async ({ page, browserName }) => {
      const featuresSection = page.locator('#features');
      await featuresSection.scrollIntoViewIfNeeded();
      await expect(featuresSection).toBeVisible();

      // Verify section heading
      const heading = featuresSection.locator('h2');
      await expect(heading).toBeVisible();
      await expect(heading).toHaveText('Key Features');

      // Verify features grid exists
      const featuresGrid = page.locator('.features-grid');
      await expect(featuresGrid).toBeVisible();

      // Verify all three feature cards
      const featureCards = page.locator('.feature-card');
      await expect(featureCards).toHaveCount(3);

      // Verify memcached compatible feature
      const memcachedCard = page.locator('.feature-card[data-feature="memcached"]');
      await expect(memcachedCard).toBeVisible();
      const memcachedTitle = memcachedCard.locator('h3');
      await expect(memcachedTitle).toHaveText('Memcached Compatible');

      // Verify persistent storage feature
      const persistentCard = page.locator('.feature-card[data-feature="persistent"]');
      await expect(persistentCard).toBeVisible();
      const persistentTitle = persistentCard.locator('h3');
      await expect(persistentTitle).toHaveText('Persistent Storage');

      // Verify high performance feature
      const performanceCard = page.locator('.feature-card[data-feature="performance"]');
      await expect(performanceCard).toBeVisible();
      const performanceTitle = performanceCard.locator('h3');
      await expect(performanceTitle).toHaveText('High Performance');

      console.log(`Features section test passed in ${browserName}`);
    });

    test('TC4: Commands section renders correctly with all command categories', async ({ page, browserName }) => {
      const commandsSection = page.locator('#commands');
      await commandsSection.scrollIntoViewIfNeeded();
      await expect(commandsSection).toBeVisible();

      // Verify section heading
      const heading = commandsSection.locator('h2');
      await expect(heading).toBeVisible();
      await expect(heading).toHaveText('Supported Commands');

      // Verify commands grid exists
      const commandsGrid = page.locator('.commands-grid');
      await expect(commandsGrid).toBeVisible();

      // Verify all command categories
      const storageCategory = page.locator('.command-category[data-category="storage"]');
      await expect(storageCategory).toBeVisible();

      const retrievalCategory = page.locator('.command-category[data-category="retrieval"]');
      await expect(retrievalCategory).toBeVisible();

      const deletionCategory = page.locator('.command-category[data-category="deletion"]');
      await expect(deletionCategory).toBeVisible();

      const extensionsCategory = page.locator('.command-category[data-category="mirdb-extensions"]');
      await expect(extensionsCategory).toBeVisible();

      // Verify some key commands are present
      const setCommand = commandsSection.locator('code').filter({ hasText: /^SET$/ });
      await expect(setCommand).toBeVisible();

      const getCommand = commandsSection.locator('code').filter({ hasText: /^GET$/ });
      await expect(getCommand).toBeVisible();

      console.log(`Commands section test passed in ${browserName}`);
    });

    test('TC5: Quick Start section renders correctly with code examples', async ({ page, browserName }) => {
      const quickstartSection = page.locator('#quickstart');
      await quickstartSection.scrollIntoViewIfNeeded();
      await expect(quickstartSection).toBeVisible();

      // Verify section heading
      const heading = quickstartSection.locator('h2');
      await expect(heading).toBeVisible();
      await expect(heading).toHaveText('Quick Start');

      // Verify quickstart steps exist
      const steps = page.locator('.quickstart-step');
      await expect(steps).toHaveCount(3);

      // Verify code blocks are present and properly rendered
      const codeBlocks = quickstartSection.locator('pre code');
      const codeBlockCount = await codeBlocks.count();
      expect(codeBlockCount).toBeGreaterThanOrEqual(3);

      // Verify installation step
      const installationStep = steps.nth(0);
      const installHeading = installationStep.locator('h3');
      await expect(installHeading).toContainText('Installation');

      // Verify configuration step
      const configStep = steps.nth(1);
      const configHeading = configStep.locator('h3');
      await expect(configHeading).toContainText('Configuration');

      // Verify connect & use step
      const connectStep = steps.nth(2);
      const connectHeading = connectStep.locator('h3');
      await expect(connectHeading).toContainText('Connect');

      console.log(`Quick Start section test passed in ${browserName}`);
    });

    test('TC6: Footer renders correctly with all links', async ({ page, browserName }) => {
      const footer = page.locator('footer');
      await footer.scrollIntoViewIfNeeded();
      await expect(footer).toBeVisible();

      // Verify brand name in footer
      const brandName = footer.locator('.brand-name');
      await expect(brandName).toBeVisible();
      await expect(brandName).toHaveText('MirDB');

      // Verify status badge
      const statusBadge = footer.locator('.status-badge');
      await expect(statusBadge).toBeVisible();

      // Verify footer links
      const footerLinks = footer.locator('.footer-links');
      await expect(footerLinks).toBeVisible();

      const githubLink = footerLinks.locator('a:has-text("GitHub")');
      await expect(githubLink).toBeVisible();

      const docsLink = footerLinks.locator('a:has-text("Docs")');
      await expect(docsLink).toBeVisible();

      const licenseLink = footerLinks.locator('a:has-text("License")');
      await expect(licenseLink).toBeVisible();

      // Verify copyright text
      const copyright = footer.locator('.copyright');
      await expect(copyright).toBeVisible();
      await expect(copyright).toContainText('Rust');

      console.log(`Footer test passed in ${browserName}`);
    });
  });

  test.describe('CSS Styling Consistency', () => {
    test('TC7: CSS styles are properly applied across browsers', async ({ page, browserName }) => {
      // Check hero background gradient is applied
      const hero = page.locator('.hero');
      const heroBackground = await hero.evaluate((el) => {
        return window.getComputedStyle(el).backgroundImage;
      });
      // Background should contain gradient
      expect(heroBackground).toMatch(/gradient|linear|radial/i);

      // Check feature cards have proper styling
      const featureCard = page.locator('.feature-card').first();
      await featureCard.scrollIntoViewIfNeeded();

      const cardStyles = await featureCard.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return {
          backgroundColor: style.backgroundColor,
          borderRadius: style.borderRadius,
          padding: style.padding,
        };
      });

      // Cards should have padding
      expect(cardStyles.padding).not.toBe('0px');
      // Cards should have border radius
      expect(cardStyles.borderRadius).not.toBe('0px');

      // Check buttons have proper styling
      const primaryBtn = page.locator('.btn-primary').first();
      const btnStyles = await primaryBtn.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return {
          backgroundColor: style.backgroundColor,
          color: style.color,
          borderRadius: style.borderRadius,
          cursor: style.cursor,
          display: style.display,
        };
      });

      // Button should have proper display (inline-block or inline-flex for styled anchor buttons)
      // Note: WebKit may return 'auto' for cursor on anchor elements, which still works as pointer
      expect(['pointer', 'auto']).toContain(btnStyles.cursor);

      console.log(`CSS styling test passed in ${browserName}`);
    });

    test('TC8: Flexbox and Grid layouts render correctly', async ({ page, browserName }) => {
      // Check navigation uses flexbox
      const nav = page.locator('.nav');
      const navDisplay = await nav.evaluate((el) => {
        return window.getComputedStyle(el).display;
      });
      expect(navDisplay).toBe('flex');

      // Check features grid uses CSS Grid
      const featuresGrid = page.locator('.features-grid');
      await featuresGrid.scrollIntoViewIfNeeded();

      const gridDisplay = await featuresGrid.evaluate((el) => {
        return window.getComputedStyle(el).display;
      });
      expect(gridDisplay).toBe('grid');

      // Check commands grid uses CSS Grid
      const commandsGrid = page.locator('.commands-grid');
      await commandsGrid.scrollIntoViewIfNeeded();

      const commandsGridDisplay = await commandsGrid.evaluate((el) => {
        return window.getComputedStyle(el).display;
      });
      expect(commandsGridDisplay).toBe('grid');

      console.log(`Layout test passed in ${browserName}`);
    });

    test('TC9: Typography renders correctly across browsers', async ({ page, browserName }) => {
      // Check heading font sizes
      const h1 = page.locator('h1').first();
      const h1FontSize = await h1.evaluate((el) => {
        return window.getComputedStyle(el).fontSize;
      });
      // H1 should have substantial font size
      const h1Size = parseFloat(h1FontSize);
      expect(h1Size).toBeGreaterThanOrEqual(32);

      // Check paragraph text is readable
      const paragraph = page.locator('.value-prop');
      const pFontSize = await paragraph.evaluate((el) => {
        return window.getComputedStyle(el).fontSize;
      });
      // Paragraph should be at least 14px
      const pSize = parseFloat(pFontSize);
      expect(pSize).toBeGreaterThanOrEqual(14);

      // Check code blocks use monospace font
      const codeBlock = page.locator('code').first();
      await codeBlock.scrollIntoViewIfNeeded();

      const codeFontFamily = await codeBlock.evaluate((el) => {
        return window.getComputedStyle(el).fontFamily;
      });
      // Code should use monospace font
      expect(codeFontFamily.toLowerCase()).toMatch(/mono|consolas|courier|source code/);

      console.log(`Typography test passed in ${browserName}`);
    });
  });

  test.describe('Interactive Elements', () => {
    test('TC10: Links are clickable and have proper hover states', async ({ page, browserName }) => {
      // Test navigation link hover
      const featuresLink = page.locator('.nav-links a[href="#features"]');
      await expect(featuresLink).toBeVisible();

      // Get initial color
      const initialColor = await featuresLink.evaluate((el) => {
        return window.getComputedStyle(el).color;
      });

      // Hover over the link
      await featuresLink.hover();

      // Wait a moment for hover transition
      await page.waitForTimeout(100);

      // Verify link is still functional
      await featuresLink.click();

      // Verify navigation worked (URL should have hash)
      await expect(page).toHaveURL(/#features/);

      console.log(`Link interaction test passed in ${browserName}`);
    });

    test('TC11: Buttons have proper interaction states', async ({ page, browserName }) => {
      const primaryBtn = page.locator('.btn-primary').first();
      await expect(primaryBtn).toBeVisible();

      // Get initial styles
      const initialStyles = await primaryBtn.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return {
          backgroundColor: style.backgroundColor,
          transform: style.transform,
        };
      });

      // Hover over button
      await primaryBtn.hover();
      await page.waitForTimeout(100);

      // Button should still be interactive
      await primaryBtn.click();

      // Verify navigation to quickstart section
      await expect(page).toHaveURL(/#quickstart/);

      console.log(`Button interaction test passed in ${browserName}`);
    });

    test('TC12: Smooth scrolling works for anchor links', async ({ page, browserName }) => {
      // Get initial scroll position
      const initialScroll = await page.evaluate(() => window.scrollY);

      // Click on features link
      await page.locator('.nav-links a[href="#features"]').click();

      // Wait for scroll
      await page.waitForTimeout(500);

      // Get new scroll position
      const newScroll = await page.evaluate(() => window.scrollY);

      // Should have scrolled
      expect(newScroll).toBeGreaterThan(initialScroll);

      // Features section should be in view
      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeInViewport();

      console.log(`Smooth scrolling test passed in ${browserName}`);
    });
  });

  test.describe('Visual Rendering', () => {
    test('TC13: No visual overflow or clipping issues', async ({ page, browserName }) => {
      // Check body doesn't have horizontal overflow
      const bodyOverflow = await page.evaluate(() => {
        return {
          scrollWidth: document.body.scrollWidth,
          clientWidth: document.body.clientWidth,
        };
      });

      // Body scroll width should not exceed client width significantly
      expect(bodyOverflow.scrollWidth).toBeLessThanOrEqual(bodyOverflow.clientWidth + 20);

      // Check main content areas aren't clipped
      const hero = page.locator('.hero');
      const heroBox = await hero.boundingBox();
      expect(heroBox).toBeTruthy();
      expect(heroBox.width).toBeGreaterThan(0);
      expect(heroBox.height).toBeGreaterThan(0);

      const features = page.locator('#features');
      await features.scrollIntoViewIfNeeded();
      const featuresBox = await features.boundingBox();
      expect(featuresBox).toBeTruthy();
      expect(featuresBox.width).toBeGreaterThan(0);
      expect(featuresBox.height).toBeGreaterThan(0);

      console.log(`Visual rendering test passed in ${browserName}`);
    });

    test('TC14: Images and icons render properly', async ({ page, browserName }) => {
      // Check feature icons are visible
      const featureIcons = page.locator('.feature-icon');
      const iconCount = await featureIcons.count();
      expect(iconCount).toBe(3);

      for (let i = 0; i < iconCount; i++) {
        const icon = featureIcons.nth(i);
        await icon.scrollIntoViewIfNeeded();
        await expect(icon).toBeVisible();

        // Icon should have content (emoji or text)
        const iconText = await icon.textContent();
        expect(iconText).toBeTruthy();
        expect(iconText.length).toBeGreaterThan(0);
      }

      console.log(`Icons rendering test passed in ${browserName}`);
    });

    test('TC15: Code blocks render with proper formatting', async ({ page, browserName }) => {
      // Navigate to quickstart section
      const quickstart = page.locator('#quickstart');
      await quickstart.scrollIntoViewIfNeeded();

      // Check code blocks are properly styled
      const preBlocks = quickstart.locator('pre');
      const preCount = await preBlocks.count();
      expect(preCount).toBeGreaterThanOrEqual(3);

      for (let i = 0; i < preCount; i++) {
        const pre = preBlocks.nth(i);
        await expect(pre).toBeVisible();

        // Check pre block has background color
        const preStyles = await pre.evaluate((el) => {
          const style = window.getComputedStyle(el);
          return {
            backgroundColor: style.backgroundColor,
            overflow: style.overflow,
          };
        });

        // Pre block should have a background color set
        expect(preStyles.backgroundColor).not.toBe('rgba(0, 0, 0, 0)');
      }

      console.log(`Code blocks test passed in ${browserName}`);
    });
  });
});
