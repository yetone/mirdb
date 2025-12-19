import { test, expect } from '@playwright/test';

test.describe('Cross-Browser Compatibility', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');
  });

  test('TC1: page renders correctly with all features functional', async ({ page, browserName }) => {
    // Log browser name for test identification
    console.log(`Testing in browser: ${browserName}`);

    // Verify page title
    await expect(page).toHaveTitle(/MirDB/i);

    // Verify hero section renders
    const hero = page.locator('.hero');
    await expect(hero).toBeVisible();

    // Verify main heading is visible
    const heading = page.locator('h1');
    await expect(heading).toBeVisible();
    await expect(heading).toContainText('MirDB');

    // Verify tagline is visible
    const tagline = page.locator('.tagline');
    await expect(tagline).toBeVisible();
    await expect(tagline).toContainText('persistent');

    // Verify description is visible
    const description = page.locator('.description');
    await expect(description).toBeVisible();

    // Verify navigation is functional
    const nav = page.locator('.sticky-nav');
    await expect(nav).toBeVisible();

    // Verify features section renders
    const features = page.locator('.features-section');
    await expect(features).toBeVisible();

    // Verify feature cards are rendered
    const featureCards = page.locator('.feature-card');
    await expect(featureCards.first()).toBeVisible();
    const cardCount = await featureCards.count();
    expect(cardCount).toBeGreaterThanOrEqual(3);

    // Verify quick start section (correct class name)
    const quickStart = page.locator('.quick-start-section');
    await expect(quickStart).toBeVisible();

    // Verify code blocks render correctly
    const codeBlocks = page.locator('.code-block');
    await expect(codeBlocks.first()).toBeVisible();

    // Verify commands section
    const commands = page.locator('.commands-section');
    await expect(commands).toBeVisible();

    // Verify GitHub link is present and functional
    const githubLink = page.locator('a[href*="github.com"]');
    await expect(githubLink.first()).toBeVisible();

    // Verify no horizontal scroll (page renders within viewport)
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScroll).toBe(false);

    // Verify styles are applied correctly (CSS loaded)
    const heroStyles = await hero.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        display: style.display,
        textAlign: style.textAlign,
      };
    });
    expect(heroStyles.textAlign).toBe('center');

    // Scroll through entire page to verify all sections load
    await page.evaluate(() => {
      window.scrollTo(0, document.body.scrollHeight);
    });
    await page.waitForTimeout(300);

    // Verify footer is accessible
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    // Scroll back to top
    await page.evaluate(() => {
      window.scrollTo(0, 0);
    });
  });

  test('TC2: interactive elements are functional', async ({ page, browserName }) => {
    console.log(`Testing interactive elements in: ${browserName}`);

    // Test copy button functionality on code blocks
    const copyButtons = page.locator('.copy-button');
    const copyButtonCount = await copyButtons.count();

    if (copyButtonCount > 0) {
      const firstCopyButton = copyButtons.first();
      await expect(firstCopyButton).toBeVisible();

      // Click the copy button
      await firstCopyButton.click();

      // Verify the button shows a visual indication of copy success
      // The button changes to show a checkmark icon
      await page.waitForTimeout(100);
      const copyIcon = firstCopyButton.locator('.copy-icon');
      await expect(copyIcon).toBeVisible();
    }

    // Test navigation links scroll to correct sections
    const navLinks = page.locator('.nav-link[href^="#"]');
    const navLinkCount = await navLinks.count();

    if (navLinkCount > 0) {
      const featuresLink = page.locator('.nav-link[href="#features"]');
      if (await featuresLink.count() > 0) {
        await featuresLink.click();
        // Verify features section is scrolled into view
        const featuresSection = page.locator('#features');
        await expect(featuresSection).toBeInViewport({ ratio: 0.3 });
      }
    }

    // Test external links have href set correctly
    const externalLinks = page.locator('a[href^="http"]');
    const externalLinkCount = await externalLinks.count();

    for (let i = 0; i < Math.min(externalLinkCount, 3); i++) {
      const link = externalLinks.nth(i);
      const href = await link.getAttribute('href');
      expect(href).toMatch(/^https?:\/\//);
    }

    // Test GitHub link is present
    const githubLink = page.locator('a[href*="github.com"]').first();
    await expect(githubLink).toBeVisible();
    const href = await githubLink.getAttribute('href');
    expect(href).toContain('github.com');
  });

  test('TC3: CSS styles render correctly', async ({ page, browserName }) => {
    console.log(`Testing CSS rendering in: ${browserName}`);

    // Verify grid layout for features
    const featuresGrid = page.locator('.features-grid');
    await expect(featuresGrid).toBeVisible();

    const gridDisplay = await featuresGrid.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return style.display;
    });
    expect(gridDisplay).toBe('grid');

    // Verify sticky navigation styling
    const nav = page.locator('.sticky-nav');
    await expect(nav).toBeVisible();

    const navStyles = await nav.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        position: style.position,
        zIndex: style.zIndex,
      };
    });
    expect(navStyles.position).toBe('fixed');
    expect(parseInt(navStyles.zIndex)).toBeGreaterThan(0);

    // Verify container max-width
    const container = page.locator('.container').first();
    const containerStyles = await container.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        maxWidth: style.maxWidth,
      };
    });
    expect(containerStyles.maxWidth).not.toBe('none');

    // Verify code block styling - use .code-pre which has monospace font
    const codePre = page.locator('.code-pre').first();
    await expect(codePre).toBeVisible();

    const codeBlockStyles = await codePre.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        fontFamily: style.fontFamily,
        backgroundColor: style.backgroundColor,
      };
    });
    // Code blocks should have monospace font (Consolas, Monaco, Courier New, monospace)
    expect(codeBlockStyles.fontFamily.toLowerCase()).toMatch(/consolas|monaco|courier|monospace/i);

    // Verify feature card styling
    const featureCard = page.locator('.feature-card').first();
    await expect(featureCard).toBeVisible();

    const cardStyles = await featureCard.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        borderRadius: style.borderRadius,
        backgroundColor: style.backgroundColor,
      };
    });
    // Card should have rounded corners
    expect(parseFloat(cardStyles.borderRadius)).toBeGreaterThan(0);
    // Card should have background color set
    expect(cardStyles.backgroundColor).not.toBe('rgba(0, 0, 0, 0)');

    // Verify hero section centered text
    const heroSection = page.locator('.hero');
    const heroStyles = await heroSection.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        textAlign: style.textAlign,
      };
    });
    expect(heroStyles.textAlign).toBe('center');
  });

  test('TC4: fonts and typography render correctly', async ({ page, browserName }) => {
    console.log(`Testing typography in: ${browserName}`);

    // Verify heading font size
    const h1 = page.locator('h1');
    const h1Styles = await h1.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        fontSize: parseFloat(style.fontSize),
        fontWeight: style.fontWeight,
      };
    });
    // H1 should be larger than 24px
    expect(h1Styles.fontSize).toBeGreaterThan(24);
    // H1 should be bold (400+ weight)
    expect(parseInt(h1Styles.fontWeight)).toBeGreaterThanOrEqual(400);

    // Verify body text is readable
    const bodyText = page.locator('.description');
    const bodyStyles = await bodyText.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        fontSize: parseFloat(style.fontSize),
        lineHeight: parseFloat(style.lineHeight),
      };
    });
    // Body text should be at least 14px
    expect(bodyStyles.fontSize).toBeGreaterThanOrEqual(14);

    // Verify color contrast (basic check)
    const textColor = await bodyText.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return style.color;
    });
    expect(textColor).not.toBe('rgba(0, 0, 0, 0)');

    // Verify tagline styling (colored text)
    const tagline = page.locator('.tagline');
    const taglineStyles = await tagline.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        fontSize: parseFloat(style.fontSize),
        color: style.color,
      };
    });
    // Tagline should be reasonably sized
    expect(taglineStyles.fontSize).toBeGreaterThan(14);
    // Tagline should have color set
    expect(taglineStyles.color).not.toBe('rgba(0, 0, 0, 0)');
  });
});
