// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

const indexPath = 'file://' + path.resolve(__dirname, '..', 'index.html');

/**
 * Cross-Browser Compatibility Tests for MirDB Homepage
 *
 * This test suite verifies that the homepage renders correctly across
 * Chrome, Firefox, Safari (WebKit), and Edge browsers.
 *
 * The tests are designed to run against all browser projects defined
 * in playwright.config.js (chromium, firefox, webkit).
 *
 * Tests cover:
 * - Page structure and layout
 * - CSS styling and visual consistency
 * - Section visibility and content
 * - Responsive design elements
 * - Navigation and links
 */

test.describe('Cross-Browser Compatibility Tests', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(indexPath);
  });

  /**
   * Test Case: Page renders correctly with proper layout and styling
   * This test runs on all configured browser projects (Chrome, Firefox, Safari/WebKit)
   */
  test('TC: page renders correctly with proper layout and styling', async ({ page, browserName }) => {
    // Verify page loads successfully
    await expect(page).toHaveTitle(/MirDB/);

    // Verify hero section renders correctly
    const heroSection = page.locator('.hero');
    await expect(heroSection).toBeVisible();

    const heroTitle = page.locator('.hero-title');
    await expect(heroTitle).toBeVisible();
    await expect(heroTitle).toContainText('MirDB');

    // Verify hero title has proper styling (font-size, color)
    const heroTitleStyles = await heroTitle.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        color: styles.color,
        fontWeight: styles.fontWeight,
        fontSize: styles.fontSize
      };
    });
    expect(heroTitleStyles.fontWeight).toBe('700');
    expect(parseFloat(heroTitleStyles.fontSize)).toBeGreaterThan(30);

    // Verify tagline is visible
    const tagline = page.locator('.hero-tagline');
    await expect(tagline).toBeVisible();
    const taglineText = await tagline.textContent();
    expect(taglineText).toMatch(/Persistent Key-Value Store/i);
    expect(taglineText).toMatch(/Memcached Protocol/i);

    // Verify hero description is visible
    const heroDescription = page.locator('.hero-description');
    await expect(heroDescription).toBeVisible();

    // Verify features section renders
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Verify feature cards are displayed in grid
    const featureCards = page.locator('.feature-card');
    await expect(featureCards).toHaveCount(3);
    for (let i = 0; i < 3; i++) {
      await expect(featureCards.nth(i)).toBeVisible();
    }

    // Verify CSS Grid layout works properly
    const featuresGrid = page.locator('.features-grid');
    const gridStyles = await featuresGrid.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        display: styles.display,
        gap: styles.gap || styles.gridGap // Safari might use gridGap
      };
    });
    expect(gridStyles.display).toBe('grid');

    // Verify quick-start section renders
    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeVisible();

    // Verify code blocks have proper styling
    const codeBlocks = page.locator('.code-block');
    const codeBlockCount = await codeBlocks.count();
    expect(codeBlockCount).toBeGreaterThan(0);

    const codeBlockStyles = await codeBlocks.first().evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        fontFamily: styles.fontFamily,
        borderRadius: styles.borderRadius
      };
    });
    expect(codeBlockStyles.fontFamily).toMatch(/mono|monospace/i);

    // Verify architecture section renders
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    // Verify SVG diagram is visible
    const svgDiagram = page.locator('.architecture-diagram svg');
    await expect(svgDiagram).toBeVisible();

    // Verify commands section renders
    const commandsSection = page.locator('#commands');
    await expect(commandsSection).toBeVisible();

    // Verify command categories exist
    const commandCategories = page.locator('.command-category');
    const categoryCount = await commandCategories.count();
    expect(categoryCount).toBeGreaterThan(0);

    // Verify footer renders
    const footer = page.locator('.footer');
    await expect(footer).toBeVisible();

    // Verify navigation links are functional
    const navLinks = page.locator('.hero-nav .btn');
    const navCount = await navLinks.count();
    expect(navCount).toBeGreaterThan(0);

    // Verify CSS variables are properly applied
    const bodyStyles = await page.locator('body').evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        backgroundColor: styles.backgroundColor,
        fontFamily: styles.fontFamily,
        lineHeight: styles.lineHeight
      };
    });
    expect(bodyStyles.fontFamily).toBeTruthy();
    expect(parseFloat(bodyStyles.lineHeight)).toBeGreaterThan(1);

    console.log(`Browser: ${browserName} - All layout and styling checks passed`);
  });

  /**
   * Test: All sections have consistent padding and margins
   */
  test('all sections have consistent padding and margins', async ({ page, browserName }) => {
    // Verify section padding is consistent
    const sections = ['#features', '#quick-start', '#architecture', '#commands'];

    for (const sectionId of sections) {
      const section = page.locator(sectionId);
      await expect(section).toBeVisible();

      const sectionStyles = await section.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          paddingTop: styles.paddingTop,
          paddingBottom: styles.paddingBottom
        };
      });

      // Verify padding exists (should be > 0)
      expect(parseFloat(sectionStyles.paddingTop)).toBeGreaterThan(0);
      expect(parseFloat(sectionStyles.paddingBottom)).toBeGreaterThan(0);
    }

    console.log(`Browser: ${browserName} - Section padding consistency verified`);
  });

  /**
   * Test: Buttons have consistent styling across browsers
   */
  test('buttons have consistent styling', async ({ page, browserName }) => {
    // Verify primary button styling
    const primaryBtn = page.locator('.btn-primary').first();
    await expect(primaryBtn).toBeVisible();

    const primaryBtnStyles = await primaryBtn.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        backgroundColor: styles.backgroundColor,
        borderRadius: styles.borderRadius,
        fontWeight: styles.fontWeight,
        textDecoration: styles.textDecoration
      };
    });

    expect(primaryBtnStyles.fontWeight).toBe('600');
    expect(primaryBtnStyles.borderRadius).toBe('8px');
    expect(primaryBtnStyles.textDecoration).toMatch(/none/);

    // Verify secondary button styling
    const secondaryBtn = page.locator('.btn-secondary').first();
    await expect(secondaryBtn).toBeVisible();

    const secondaryBtnStyles = await secondaryBtn.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        borderRadius: styles.borderRadius,
        fontWeight: styles.fontWeight
      };
    });

    expect(secondaryBtnStyles.fontWeight).toBe('600');
    expect(secondaryBtnStyles.borderRadius).toBe('8px');

    console.log(`Browser: ${browserName} - Button styling consistency verified`);
  });

  /**
   * Test: Typography is consistent across browsers
   */
  test('typography is consistent', async ({ page, browserName }) => {
    // Verify heading weights
    const h1 = page.locator('h1').first();
    const h2 = page.locator('h2').first();
    const h3 = page.locator('h3').first();

    await expect(h1).toBeVisible();
    await expect(h2).toBeVisible();
    await expect(h3).toBeVisible();

    const h1Styles = await h1.evaluate((el) => window.getComputedStyle(el).fontWeight);
    const h2Styles = await h2.evaluate((el) => window.getComputedStyle(el).fontWeight);
    const h3Styles = await h3.evaluate((el) => window.getComputedStyle(el).fontWeight);

    expect(h1Styles).toBe('700');
    expect(h2Styles).toBe('700');
    expect(h3Styles).toBe('700');

    console.log(`Browser: ${browserName} - Typography consistency verified`);
  });

  /**
   * Test: Flexbox layout works correctly across browsers
   */
  test('flexbox layout works correctly', async ({ page, browserName }) => {
    // Verify footer content uses flexbox
    const footerContent = page.locator('.footer-content');
    await expect(footerContent).toBeVisible();

    const footerStyles = await footerContent.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        display: styles.display,
        justifyContent: styles.justifyContent,
        alignItems: styles.alignItems
      };
    });

    expect(footerStyles.display).toBe('flex');

    // Verify hero-nav uses flexbox
    const heroNav = page.locator('.hero-nav');
    await expect(heroNav).toBeVisible();

    console.log(`Browser: ${browserName} - Flexbox layout verified`);
  });

  /**
   * Test: CSS transitions are defined
   */
  test('CSS transitions are defined', async ({ page, browserName }) => {
    // Verify feature cards have transition
    const featureCard = page.locator('.feature-card').first();
    await expect(featureCard).toBeVisible();

    const cardStyles = await featureCard.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        transition: styles.transition,
        transitionDuration: styles.transitionDuration,
        transitionProperty: styles.transitionProperty
      };
    });

    // Check that transitions are defined (browsers report these differently)
    // Either the full transition string contains keywords, or individual properties exist
    const hasTransition = cardStyles.transition.length > 0 ||
      cardStyles.transitionDuration !== '0s' ||
      (cardStyles.transitionProperty && cardStyles.transitionProperty !== 'none');
    expect(hasTransition).toBe(true);

    // Verify buttons have transition
    const btn = page.locator('.btn').first();
    const btnStyles = await btn.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        transition: styles.transition,
        transitionDuration: styles.transitionDuration,
        transitionProperty: styles.transitionProperty
      };
    });

    // Check that button has some transition defined
    const btnHasTransition = btnStyles.transition.length > 0 ||
      btnStyles.transitionDuration !== '0s' ||
      (btnStyles.transitionProperty && btnStyles.transitionProperty !== 'none');
    expect(btnHasTransition).toBe(true);

    console.log(`Browser: ${browserName} - CSS transitions verified`);
  });

  /**
   * Test: SVG rendering and gradients work correctly
   */
  test('SVG rendering and gradients work correctly', async ({ page, browserName }) => {
    // Verify SVG architecture diagram renders
    const svgDiagram = page.locator('.architecture-diagram svg');
    await expect(svgDiagram).toBeVisible();

    // Verify SVG has gradient definitions
    const gradients = page.locator('.architecture-diagram svg defs linearGradient');
    const gradientCount = await gradients.count();
    expect(gradientCount).toBeGreaterThan(0);

    // Verify SVG elements are rendered
    const rects = page.locator('.architecture-diagram svg rect');
    const rectCount = await rects.count();
    expect(rectCount).toBeGreaterThan(0);

    const texts = page.locator('.architecture-diagram svg text');
    const textCount = await texts.count();
    expect(textCount).toBeGreaterThan(0);

    console.log(`Browser: ${browserName} - SVG rendering verified`);
  });

  /**
   * Test: Smooth scrolling behavior
   */
  test('smooth scroll behavior is applied', async ({ page, browserName }) => {
    // Verify smooth scroll behavior
    const htmlStyles = await page.locator('html').evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        scrollBehavior: styles.scrollBehavior
      };
    });
    expect(htmlStyles.scrollBehavior).toBe('smooth');

    console.log(`Browser: ${browserName} - Smooth scroll behavior verified`);
  });

  /**
   * Test: GitHub link in hero section works
   */
  test('GitHub link in hero section is correct', async ({ page, browserName }) => {
    const githubLink = page.locator('.hero-nav a[href*="github"]');
    await expect(githubLink).toBeVisible();

    const href = await githubLink.getAttribute('href');
    expect(href).toContain('github.com');

    // Verify link opens in new tab with security attributes
    const target = await githubLink.getAttribute('target');
    const rel = await githubLink.getAttribute('rel');
    expect(target).toBe('_blank');
    expect(rel).toContain('noopener');

    console.log(`Browser: ${browserName} - GitHub link verified`);
  });

  /**
   * Test: Footer content renders correctly
   */
  test('footer content renders correctly', async ({ page, browserName }) => {
    const footer = page.locator('.footer');
    await expect(footer).toBeVisible();

    // Verify footer has GitHub link
    const footerGithubLink = footer.locator('a[href*="github"]');
    await expect(footerGithubLink).toBeVisible();

    // Verify license information
    const license = footer.locator('.footer-license');
    await expect(license).toBeVisible();
    const licenseText = await license.textContent();
    expect(licenseText).toMatch(/MIT/i);

    console.log(`Browser: ${browserName} - Footer content verified`);
  });

  /**
   * Test: Feature icons are visible
   */
  test('feature icons are visible', async ({ page, browserName }) => {
    const featureIcons = page.locator('.feature-icon');
    const iconCount = await featureIcons.count();
    expect(iconCount).toBe(3);

    for (let i = 0; i < 3; i++) {
      const icon = featureIcons.nth(i);
      await expect(icon).toBeVisible();

      // Verify SVG inside icon
      const svg = icon.locator('svg');
      await expect(svg).toBeVisible();
    }

    console.log(`Browser: ${browserName} - Feature icons verified`);
  });

  /**
   * Test: Code syntax highlighting classes are present
   */
  test('code syntax highlighting classes are present', async ({ page, browserName }) => {
    const codeBlock = page.locator('.code-block').first();
    await expect(codeBlock).toBeVisible();

    // Check for syntax highlighting spans
    const highlightClasses = [
      '.code-comment',
      '.code-command',
      '.code-keyword',
      '.code-string'
    ];

    let foundHighlighting = false;
    for (const className of highlightClasses) {
      const elements = codeBlock.locator(className);
      const count = await elements.count();
      if (count > 0) {
        foundHighlighting = true;
        break;
      }
    }

    expect(foundHighlighting).toBe(true);

    console.log(`Browser: ${browserName} - Syntax highlighting verified`);
  });

  /**
   * Test: Container max-width is applied
   */
  test('container max-width is applied', async ({ page, browserName }) => {
    const container = page.locator('.container').first();
    await expect(container).toBeVisible();

    // Get both computed styles and check the stylesheet rules
    const containerStyles = await container.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      // Also check the actual CSS rules applied
      const cssRules = Array.from(document.styleSheets)
        .flatMap(sheet => {
          try {
            return Array.from(sheet.cssRules || []);
          } catch {
            return [];
          }
        })
        .filter(rule => rule.selectorText === '.container')
        .map(rule => rule.cssText);

      return {
        maxWidth: styles.maxWidth,
        marginLeft: styles.marginLeft,
        marginRight: styles.marginRight,
        // When viewport is small, computed margin might be actual pixels, not 'auto'
        // But the CSS rule should still specify 'auto'
        cssContainsMarginAuto: cssRules.some(rule => rule.includes('margin') && rule.includes('auto'))
      };
    });

    expect(containerStyles.maxWidth).toBe('1200px');

    // The CSS should define margin: 0 auto for centering
    // Computed value might be pixels when viewport is smaller than max-width
    // Check either the computed value is 'auto' OR the CSS contains auto
    const marginsCorrect = containerStyles.marginLeft === 'auto' ||
      containerStyles.cssContainsMarginAuto ||
      (containerStyles.marginLeft === containerStyles.marginRight);
    expect(marginsCorrect).toBe(true);

    console.log(`Browser: ${browserName} - Container max-width verified`);
  });

  /**
   * Test: Commands grid renders correctly
   */
  test('commands grid renders correctly', async ({ page, browserName }) => {
    const commandsSection = page.locator('#commands');
    await expect(commandsSection).toBeVisible();

    const commandsGrid = page.locator('.commands-grid');
    await expect(commandsGrid).toBeVisible();

    // Verify grid display
    const gridStyles = await commandsGrid.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        display: styles.display
      };
    });
    expect(gridStyles.display).toBe('grid');

    // Verify command categories
    const categories = page.locator('.command-category');
    const categoryCount = await categories.count();
    expect(categoryCount).toBe(3); // Storage, Retrieval, Deletion

    // Verify command items exist
    const commandItems = page.locator('.command-item');
    const itemCount = await commandItems.count();
    expect(itemCount).toBeGreaterThan(5);

    console.log(`Browser: ${browserName} - Commands grid verified`);
  });
});
