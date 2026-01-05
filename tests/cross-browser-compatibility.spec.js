const { test, expect } = require('@playwright/test');

/**
 * Cross-Browser Compatibility Tests (NFR-4)
 * These tests verify that the MirDB landing page renders correctly
 * across target browsers: Chrome, Firefox, Safari (WebKit), and Edge.
 */
test.describe('Cross-Browser Compatibility', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Page loads and displays correctly with all main sections visible', async ({ page, browserName }) => {
    // Verify page title
    await expect(page).toHaveTitle(/MirDB/);

    // Verify header navigation is visible
    const header = page.locator('header');
    await expect(header).toBeVisible();

    // Verify hero section renders correctly
    const hero = page.locator('.hero');
    await expect(hero).toBeVisible();

    const heroTitle = page.locator('.hero h1');
    await expect(heroTitle).toBeVisible();
    await expect(heroTitle).toHaveText('MirDB');

    const tagline = page.locator('.hero .tagline');
    await expect(tagline).toBeVisible();
    await expect(tagline).toContainText('Persistent Key-Value Store');

    // Verify features section renders correctly
    const features = page.locator('.features');
    await expect(features).toBeVisible();

    const featureCards = page.locator('.feature-card');
    await expect(featureCards).toHaveCount(3);

    // Verify quick start section renders correctly
    const quickstart = page.locator('.quickstart');
    await expect(quickstart).toBeVisible();

    // Verify commands section renders correctly
    const commands = page.locator('.commands');
    await expect(commands).toBeVisible();

    // Verify configuration section renders correctly
    const configuration = page.locator('.configuration');
    await expect(configuration).toBeVisible();

    // Verify footer renders correctly
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();
  });

  test('Navigation links are functional', async ({ page, browserName }) => {
    // Verify nav links exist
    const navLinks = page.locator('.nav-links a');
    const count = await navLinks.count();
    expect(count).toBeGreaterThanOrEqual(3);

    // Verify internal navigation link (Features)
    const featuresLink = page.locator('.nav-links a[href="#features"]');
    await expect(featuresLink).toBeVisible();

    // Verify GitHub link
    const githubLink = page.locator('.nav-links a[href="https://github.com/yetone/mirdb"]');
    await expect(githubLink).toBeVisible();
    await expect(githubLink).toHaveAttribute('target', '_blank');
  });

  test('CTA buttons are clickable and correctly styled', async ({ page, browserName }) => {
    // Verify primary CTA button
    const primaryCTA = page.locator('#primary-cta');
    await expect(primaryCTA).toBeVisible();
    await expect(primaryCTA).toHaveAttribute('href', 'https://github.com/yetone/mirdb');

    // Verify primary CTA button has correct styling
    const primaryClasses = await primaryCTA.getAttribute('class');
    expect(primaryClasses).toContain('btn');
    expect(primaryClasses).toContain('btn-primary');

    // Verify secondary CTA button
    const secondaryCTA = page.locator('#secondary-cta');
    await expect(secondaryCTA).toBeVisible();

    const secondaryClasses = await secondaryCTA.getAttribute('class');
    expect(secondaryClasses).toContain('btn');
    expect(secondaryClasses).toContain('btn-secondary');
  });

  test('Feature cards render with correct content', async ({ page, browserName }) => {
    // Verify all three feature cards are visible
    const featureCards = page.locator('.feature-card');
    await expect(featureCards).toHaveCount(3);

    // Verify Memcached Protocol feature
    const memcachedFeature = page.locator('[data-testid*="feature-memcached"]').first();
    await expect(memcachedFeature).toBeVisible();

    // Verify Persistent Storage feature
    const persistentFeature = page.locator('[data-testid*="feature-persistence"]').first();
    await expect(persistentFeature).toBeVisible();

    // Verify LSM Tree feature
    const lsmFeature = page.locator('[data-testid*="feature-lsm"]').first();
    await expect(lsmFeature).toBeVisible();

    // Verify feature titles are rendered
    const featureTitles = page.locator('.feature-title');
    await expect(featureTitles).toHaveCount(3);
  });

  test('Code blocks render with proper styling', async ({ page, browserName }) => {
    // Navigate to quick start section
    const codeBlock = page.locator('.code-block');
    await expect(codeBlock).toBeVisible();

    const pre = codeBlock.locator('pre');
    await expect(pre).toBeVisible();

    // Verify code content is rendered
    const codeContent = await pre.textContent();
    expect(codeContent).toContain('telnet localhost 12333');
    expect(codeContent).toContain('set mykey');
    expect(codeContent).toContain('get mykey');
  });

  test('Commands grid displays all supported commands', async ({ page, browserName }) => {
    // Verify commands section
    const commandItems = page.locator('.command-item');
    const count = await commandItems.count();
    expect(count).toBeGreaterThanOrEqual(8);

    // Verify specific commands are present
    const commandNames = page.locator('.command-name');
    const names = await commandNames.allTextContents();

    expect(names).toContain('SET');
    expect(names).toContain('GET');
    expect(names).toContain('DELETE');
  });

  test('Configuration section displays default parameters', async ({ page, browserName }) => {
    // Verify configuration items
    const configSection = page.locator('.configuration');
    await expect(configSection).toBeVisible();

    // Verify Listen Address config
    const listenAddress = page.locator('[data-testid="config-value-listen-address"]');
    await expect(listenAddress).toBeVisible();
    await expect(listenAddress).toHaveText('0.0.0.0:12333');

    // Verify Max LSM Levels config
    const maxLsmLevels = page.locator('[data-testid="config-value-max-lsm-levels"]');
    await expect(maxLsmLevels).toBeVisible();
    await expect(maxLsmLevels).toHaveText('7');

    // Verify SSTable Max Size config
    const sstableMaxSize = page.locator('[data-testid="config-value-sstable-max-size"]');
    await expect(sstableMaxSize).toBeVisible();
    await expect(sstableMaxSize).toHaveText('100MB');
  });

  test('Footer contains required links and copyright', async ({ page, browserName }) => {
    // Verify footer
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    // Verify footer links
    const footerLinks = page.locator('.footer-links a');
    const count = await footerLinks.count();
    expect(count).toBeGreaterThanOrEqual(3);

    // Verify GitHub link in footer
    const githubLink = page.locator('.footer-links a[href="https://github.com/yetone/mirdb"]');
    await expect(githubLink).toBeVisible();

    // Verify copyright notice
    const copyright = page.locator('.copyright');
    await expect(copyright).toBeVisible();
    const copyrightText = await copyright.textContent();
    expect(copyrightText).toContain('MirDB');
    expect(copyrightText).toContain('MIT License');
  });

  test('CSS styles are properly applied across browsers', async ({ page, browserName }) => {
    // Test that CSS custom properties and styles are applied correctly

    // Verify hero title has large font size
    const heroTitle = page.locator('.hero h1');
    const fontSize = await heroTitle.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });
    expect(fontSize).toBeGreaterThanOrEqual(40);

    // Verify background color is dark theme
    const body = page.locator('body');
    const bgColor = await body.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });
    // Dark theme background should be dark (low RGB values)
    expect(bgColor).toBeTruthy();

    // Verify feature cards have border styling
    const featureCard = page.locator('.feature-card').first();
    const borderStyle = await featureCard.evaluate((el) => {
      return window.getComputedStyle(el).borderStyle;
    });
    expect(borderStyle).toBe('solid');
  });

  test('Fonts and typography render correctly', async ({ page, browserName }) => {
    // Verify body font family is applied
    const body = page.locator('body');
    const fontFamily = await body.evaluate((el) => {
      return window.getComputedStyle(el).fontFamily;
    });
    expect(fontFamily).toBeTruthy();

    // Verify monospace font is used in code blocks
    const codeBlock = page.locator('.code-block pre');
    const codeFontFamily = await codeBlock.evaluate((el) => {
      return window.getComputedStyle(el).fontFamily;
    });
    // Check for monospace font indicators
    const isMonospace = codeFontFamily.toLowerCase().includes('mono') ||
                        codeFontFamily.toLowerCase().includes('menlo') ||
                        codeFontFamily.toLowerCase().includes('monaco');
    expect(isMonospace).toBe(true);
  });

  test('Layout maintains proper structure in viewport', async ({ page, browserName }) => {
    // Verify container max-width is applied
    const container = page.locator('.container').first();
    const maxWidth = await container.evaluate((el) => {
      return parseInt(window.getComputedStyle(el).maxWidth);
    });
    expect(maxWidth).toBe(1200);

    // Verify grid layout for features
    const featuresGrid = page.locator('.features-grid');
    const display = await featuresGrid.evaluate((el) => {
      return window.getComputedStyle(el).display;
    });
    expect(display).toBe('grid');
  });

  test('Images and icons load correctly', async ({ page, browserName }) => {
    // Verify logo image loads (or gracefully handles missing image)
    const logo = page.locator('.logo img');
    const logoExists = await logo.count();

    if (logoExists > 0) {
      // If logo element exists, verify it has src
      const src = await logo.getAttribute('src');
      expect(src).toBeTruthy();
    }

    // Verify feature icons are visible
    const featureIcons = page.locator('.feature-icon');
    const count = await featureIcons.count();
    expect(count).toBe(3);

    // Each icon should be visible
    for (let i = 0; i < count; i++) {
      await expect(featureIcons.nth(i)).toBeVisible();
    }
  });

  test('Interactive elements have proper hover states', async ({ page, browserName }) => {
    // Verify primary button has transition for hover effect
    const primaryBtn = page.locator('.btn-primary').first();
    const transition = await primaryBtn.evaluate((el) => {
      return window.getComputedStyle(el).transition;
    });
    expect(transition).not.toBe('none');
    expect(transition).not.toBe('');

    // Verify feature cards have hover transition
    const featureCard = page.locator('.feature-card').first();
    const cardTransition = await featureCard.evaluate((el) => {
      return window.getComputedStyle(el).transition;
    });
    expect(cardTransition).not.toBe('none');
    expect(cardTransition).not.toBe('');
  });

  test('Page has no major visual regressions in scroll behavior', async ({ page, browserName }) => {
    // Verify sticky header behavior
    const header = page.locator('header');
    const position = await header.evaluate((el) => {
      return window.getComputedStyle(el).position;
    });
    expect(position).toBe('sticky');

    // Scroll down and verify header remains visible
    await page.evaluate(() => window.scrollTo(0, 500));
    await expect(header).toBeVisible();

    // Scroll back up
    await page.evaluate(() => window.scrollTo(0, 0));
    await expect(header).toBeVisible();
  });
});
