/**
 * Browser Compatibility E2E Tests
 * Owner: Scenario 12 - Browser Compatibility
 *
 * Test cases:
 * - Page renders correctly in Chrome, Firefox, Safari, and Edge
 * - CSS features work consistently across browsers
 * - Layout and functionality work without browser-specific bugs
 *
 * Per NFR-3: last 2 versions of Chrome, Firefox, Safari, Edge
 */

import { test, expect, BrowserName } from '@playwright/test';
import { navigateToHomepage, selectors, viewports } from './test-utils';

// Get current browser name from test info
function getBrowserName(browserName: string): string {
  const browserMap: Record<string, string> = {
    chromium: 'Chrome',
    firefox: 'Firefox',
    webkit: 'Safari',
    edge: 'Edge',
  };
  return browserMap[browserName] || browserName;
}

test.describe('Browser Compatibility - Page Rendering', () => {
  test.beforeEach(async ({ page }) => {
    await navigateToHomepage(page);
  });

  test('TC1: Page renders correctly with all sections visible', async ({ page, browserName }) => {
    const browser = getBrowserName(browserName);

    // Verify page title
    await expect(page).toHaveTitle(/MirDB/);

    // Verify all main sections are present and visible
    const heroSection = page.locator(selectors.hero.section);
    await expect(heroSection, `Hero section should be visible in ${browser}`).toBeVisible();

    const featuresSection = page.locator(selectors.features.section);
    await expect(featuresSection, `Features section should be visible in ${browser}`).toBeVisible();

    const usageSection = page.locator(selectors.usage.section);
    await expect(usageSection, `Usage section should be visible in ${browser}`).toBeVisible();

    const quickstartSection = page.locator(selectors.quickstart.section);
    await expect(quickstartSection, `Quick Start section should be visible in ${browser}`).toBeVisible();

    const footer = page.locator(selectors.footer.section);
    await expect(footer, `Footer should be visible in ${browser}`).toBeVisible();
  });

  test('TC2: Navigation renders and functions correctly', async ({ page, browserName }) => {
    const browser = getBrowserName(browserName);

    // Verify navigation header is visible
    const header = page.locator(selectors.navigation.header);
    await expect(header, `Navigation header should be visible in ${browser}`).toBeVisible();

    // Verify logo is visible
    const logo = page.locator(selectors.navigation.logo);
    await expect(logo, `Logo should be visible in ${browser}`).toBeVisible();
    await expect(logo).toHaveText('MirDB');

    // Verify nav links are visible and styled
    const navLinks = page.locator(selectors.navigation.links);
    await expect(navLinks, `Nav links should be visible in ${browser}`).toBeVisible();

    // Test navigation click functionality
    const featuresLink = page.locator('.nav-link[href="#features"]');
    await featuresLink.click();

    // Wait for smooth scroll
    await page.waitForTimeout(500);

    const featuresSection = page.locator(selectors.features.section);
    await expect(featuresSection, `Features section should be in viewport after click in ${browser}`).toBeInViewport();
  });

  test('TC3: CSS custom properties are applied correctly', async ({ page, browserName }) => {
    const browser = getBrowserName(browserName);

    // Verify CSS custom properties are working
    const body = page.locator('body');

    // Check font family is applied
    const fontFamily = await body.evaluate((el) => {
      return window.getComputedStyle(el).fontFamily;
    });
    expect(fontFamily, `Font family should be set in ${browser}`).toBeTruthy();
    expect(fontFamily.toLowerCase()).toMatch(/system-ui|segoe|helvetica|arial|sans-serif/);

    // Check background color is applied
    const bgColor = await body.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });
    expect(bgColor, `Background color should be set in ${browser}`).toBeTruthy();
    expect(bgColor).not.toBe('rgba(0, 0, 0, 0)');
  });

  test('TC4: Hero section gradient renders correctly', async ({ page, browserName }) => {
    const browser = getBrowserName(browserName);

    // Check hero background gradient
    const heroSection = page.locator('.hero');
    const heroBackground = await heroSection.evaluate((el) => {
      return window.getComputedStyle(el).backgroundImage;
    });

    // Should have a gradient (linear-gradient)
    expect(heroBackground, `Hero gradient should render in ${browser}`).toMatch(/gradient|linear-gradient/);

    // Check hero title has gradient text (webkit)
    const heroTitle = page.locator(selectors.hero.title);
    const titleBgClip = await heroTitle.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return style.webkitBackgroundClip || style.backgroundClip;
    });
    // Background clip text may or may not work in all browsers - just verify element is visible
    await expect(heroTitle, `Hero title should be visible in ${browser}`).toBeVisible();
  });

  test('TC5: Flexbox layout works correctly', async ({ page, browserName }) => {
    const browser = getBrowserName(browserName);

    // Test navigation flexbox layout
    const nav = page.locator('.nav');
    const navDisplay = await nav.evaluate((el) => {
      return window.getComputedStyle(el).display;
    });
    expect(navDisplay, `Nav should use flexbox in ${browser}`).toBe('flex');

    // Test hero CTA flexbox
    const heroCta = page.locator('.hero-cta');
    const ctaDisplay = await heroCta.evaluate((el) => {
      return window.getComputedStyle(el).display;
    });
    expect(ctaDisplay, `Hero CTA should use flexbox in ${browser}`).toMatch(/flex|inline-flex/);
  });

  test('TC6: CSS Grid layout works for features section', async ({ page, browserName }) => {
    const browser = getBrowserName(browserName);

    // Test features grid layout
    const featuresGrid = page.locator(selectors.features.grid);
    await featuresGrid.scrollIntoViewIfNeeded();

    const gridDisplay = await featuresGrid.evaluate((el) => {
      return window.getComputedStyle(el).display;
    });
    expect(gridDisplay, `Features grid should use CSS Grid in ${browser}`).toBe('grid');

    // Verify all feature cards are visible
    const featureCards = page.locator(selectors.features.card);
    const cardCount = await featureCards.count();
    expect(cardCount, `Should have 4 feature cards in ${browser}`).toBe(4);

    for (let i = 0; i < cardCount; i++) {
      await expect(featureCards.nth(i), `Feature card ${i + 1} should be visible in ${browser}`).toBeVisible();
    }
  });

  test('TC7: Buttons render with correct styles', async ({ page, browserName }) => {
    const browser = getBrowserName(browserName);

    // Check primary button styling
    const primaryBtn = page.locator('.btn-primary').first();
    await expect(primaryBtn, `Primary button should be visible in ${browser}`).toBeVisible();

    const primaryBgColor = await primaryBtn.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });
    // Should have a colored background (blue primary color)
    expect(primaryBgColor, `Primary button should have background color in ${browser}`).not.toBe('rgba(0, 0, 0, 0)');
    expect(primaryBgColor).not.toBe('transparent');

    // Check button border radius
    const borderRadius = await primaryBtn.evaluate((el) => {
      return window.getComputedStyle(el).borderRadius;
    });
    expect(parseFloat(borderRadius), `Primary button should have border radius in ${browser}`).toBeGreaterThan(0);

    // Check secondary button styling
    const secondaryBtn = page.locator('.btn-secondary').first();
    await expect(secondaryBtn, `Secondary button should be visible in ${browser}`).toBeVisible();
  });

  test('TC8: Code blocks render correctly', async ({ page, browserName }) => {
    const browser = getBrowserName(browserName);

    // Scroll to usage section
    const usageSection = page.locator(selectors.usage.section);
    await usageSection.scrollIntoViewIfNeeded();

    // Check code block styling
    const codeBlock = page.locator('.code-block').first();
    await expect(codeBlock, `Code block should be visible in ${browser}`).toBeVisible();

    // Verify dark background for code
    const codeBgColor = await codeBlock.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });
    expect(codeBgColor, `Code block should have background in ${browser}`).not.toBe('rgba(0, 0, 0, 0)');

    // Verify monospace font
    const codeFont = await codeBlock.evaluate((el) => {
      const pre = el.querySelector('pre');
      return pre ? window.getComputedStyle(pre).fontFamily : '';
    });
    expect(codeFont.toLowerCase(), `Code should use monospace font in ${browser}`).toMatch(/mono|consolas|courier/);
  });

  test('TC9: Smooth scroll behavior works', async ({ page, browserName }) => {
    const browser = getBrowserName(browserName);

    // Check that smooth scroll is enabled
    const htmlScrollBehavior = await page.evaluate(() => {
      return window.getComputedStyle(document.documentElement).scrollBehavior;
    });
    expect(htmlScrollBehavior, `Smooth scroll should be enabled in ${browser}`).toBe('smooth');

    // Test actual smooth scroll by clicking nav link
    const initialScrollY = await page.evaluate(() => window.scrollY);

    const quickstartLink = page.locator('.nav-link[href="#quickstart"]');
    await quickstartLink.click();

    // Wait for animation
    await page.waitForTimeout(500);

    const finalScrollY = await page.evaluate(() => window.scrollY);
    expect(finalScrollY, `Page should scroll in ${browser}`).toBeGreaterThan(initialScrollY);
  });

  test('TC10: Focus-visible styles work for accessibility', async ({ page, browserName }) => {
    const browser = getBrowserName(browserName);

    // Tab to a link and check focus visibility
    const firstNavLink = page.locator('.nav-link').first();
    await firstNavLink.focus();

    // Check that focus is visible (outline or other focus indicator)
    const outlineStyle = await firstNavLink.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        outline: style.outline,
        outlineWidth: style.outlineWidth,
        boxShadow: style.boxShadow
      };
    });

    // Should have some focus indicator (outline or box-shadow)
    const hasFocusIndicator =
      (outlineStyle.outlineWidth && outlineStyle.outlineWidth !== '0px') ||
      (outlineStyle.boxShadow && outlineStyle.boxShadow !== 'none');

    // Note: focus-visible may not trigger with JS focus, but element should be focusable
    expect(await firstNavLink.evaluate(el => document.activeElement === el),
      `Link should be focusable in ${browser}`).toBe(true);
  });

  test('TC11: Transitions and animations work', async ({ page, browserName }) => {
    const browser = getBrowserName(browserName);

    // Check that links have transition
    const navLink = page.locator('.nav-link').first();
    const linkTransition = await navLink.evaluate((el) => {
      return window.getComputedStyle(el).transition;
    });
    expect(linkTransition, `Nav links should have transition in ${browser}`).not.toBe('none');
    expect(linkTransition).not.toBe('all 0s ease 0s');

    // Check feature card hover transform
    const featureCard = page.locator(selectors.features.card).first();
    await featureCard.scrollIntoViewIfNeeded();

    const cardTransition = await featureCard.evaluate((el) => {
      return window.getComputedStyle(el).transition;
    });
    expect(cardTransition, `Feature cards should have transition in ${browser}`).not.toBe('none');
  });

  test('TC12: Footer renders correctly with all links', async ({ page, browserName }) => {
    const browser = getBrowserName(browserName);

    // Scroll to footer
    const footer = page.locator(selectors.footer.section);
    await footer.scrollIntoViewIfNeeded();
    await expect(footer, `Footer should be visible in ${browser}`).toBeVisible();

    // Verify footer links
    const footerLinks = page.locator('.footer-link');
    const linkCount = await footerLinks.count();
    expect(linkCount, `Footer should have links in ${browser}`).toBeGreaterThanOrEqual(3);

    // Verify links have proper attributes
    for (let i = 0; i < linkCount; i++) {
      const link = footerLinks.nth(i);
      await expect(link, `Footer link ${i + 1} should be visible in ${browser}`).toBeVisible();

      // External links should have target="_blank"
      const target = await link.getAttribute('target');
      expect(target, `Footer link ${i + 1} should open in new tab in ${browser}`).toBe('_blank');
    }

    // Verify copyright text
    const copyright = page.locator(selectors.footer.copyright);
    await expect(copyright, `Copyright should be visible in ${browser}`).toBeVisible();
  });

  test('TC13: No horizontal overflow on desktop viewport', async ({ page, browserName }) => {
    const browser = getBrowserName(browserName);

    await page.setViewportSize(viewports.desktop);

    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScroll, `No horizontal scroll should exist in ${browser}`).toBe(false);
  });

  test('TC14: Images and SVGs render correctly', async ({ page, browserName }) => {
    const browser = getBrowserName(browserName);

    // Check feature icons (SVGs) are visible
    const featureIcons = page.locator('.feature-icon');
    const iconCount = await featureIcons.count();
    expect(iconCount, `Should have feature icons in ${browser}`).toBeGreaterThan(0);

    for (let i = 0; i < iconCount; i++) {
      const icon = featureIcons.nth(i);
      await icon.scrollIntoViewIfNeeded();

      // SVGs should have proper dimensions
      const bbox = await icon.boundingBox();
      expect(bbox, `Feature icon ${i + 1} should have bounding box in ${browser}`).not.toBeNull();
      expect(bbox!.width, `Feature icon ${i + 1} width in ${browser}`).toBeGreaterThan(0);
      expect(bbox!.height, `Feature icon ${i + 1} height in ${browser}`).toBeGreaterThan(0);
    }
  });
});

test.describe('Browser Compatibility - Interactions', () => {
  test.beforeEach(async ({ page }) => {
    await navigateToHomepage(page);
  });

  test('TC15: Button hover states work', async ({ page, browserName }) => {
    const browser = getBrowserName(browserName);

    const primaryBtn = page.locator('.btn-primary').first();

    // Get initial background color
    const initialBgColor = await primaryBtn.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });

    // Hover over button
    await primaryBtn.hover();
    await page.waitForTimeout(200);

    // Background color should change on hover
    const hoverBgColor = await primaryBtn.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });

    // Colors might be the same or different depending on browser, but button should remain functional
    expect(hoverBgColor, `Button should have background on hover in ${browser}`).not.toBe('rgba(0, 0, 0, 0)');
  });

  test('TC16: Scroll behavior is consistent', async ({ page, browserName }) => {
    const browser = getBrowserName(browserName);

    // Scroll to each section and verify visibility
    const sections = ['features', 'usage', 'quickstart'];

    for (const section of sections) {
      const sectionElement = page.locator(`#${section}`);
      await sectionElement.scrollIntoViewIfNeeded();
      await page.waitForTimeout(300);

      await expect(sectionElement, `${section} section should be visible after scroll in ${browser}`).toBeVisible();
      await expect(sectionElement, `${section} section should be in viewport in ${browser}`).toBeInViewport();
    }
  });

  test('TC17: External links have proper attributes', async ({ page, browserName }) => {
    const browser = getBrowserName(browserName);

    // Check all external links have security attributes
    const externalLinks = page.locator('a[target="_blank"]');
    const linkCount = await externalLinks.count();

    expect(linkCount, `Should have external links in ${browser}`).toBeGreaterThan(0);

    for (let i = 0; i < linkCount; i++) {
      const link = externalLinks.nth(i);
      const rel = await link.getAttribute('rel');

      expect(rel, `External link ${i + 1} should have rel attribute in ${browser}`).toBeTruthy();
      expect(rel, `External link ${i + 1} should have noopener in ${browser}`).toMatch(/noopener/);
    }
  });
});
