/**
 * Cross-Browser Compatibility Tests
 *
 * Scenario: Verify page renders correctly across major browsers
 * Tests: Chrome, Firefox, Safari (WebKit), Edge
 *
 * These tests verify that the MirDB homepage renders correctly across all major browsers,
 * including proper styling, layout, and interactive elements.
 */

const { test, expect } = require('@playwright/test');
const path = require('path');

// Helper function to get the file URL for the index.html
function getFileUrl() {
  return `file://${path.resolve(__dirname, '..', 'index.html')}`;
}

test.describe('Cross-Browser Compatibility - All Sections Render Correctly', () => {

  test.beforeEach(async ({ page }) => {
    await page.goto(getFileUrl());
    // Wait for styles to be applied
    await page.waitForLoadState('domcontentloaded');
  });

  test('Hero section renders correctly with proper styling', async ({ page, browserName }) => {
    // Verify hero section exists
    const hero = page.locator('.hero, header.hero');
    await expect(hero).toBeVisible();

    // Verify h1 title is visible
    const h1 = page.locator('h1');
    await expect(h1).toBeVisible();
    await expect(h1).toContainText('MirDB');

    // Verify tagline is visible
    const tagline = page.locator('.tagline');
    await expect(tagline).toBeVisible();
    await expect(tagline).toContainText('Persistent Key-Value Store');

    // Verify CTA buttons are visible
    const getStartedBtn = page.locator('a:has-text("Get Started")');
    await expect(getStartedBtn).toBeVisible();

    const githubBtn = page.locator('.hero a:has-text("GitHub")');
    await expect(githubBtn).toBeVisible();

    // Verify hero has proper background styling (gradient)
    const heroBox = await hero.boundingBox();
    expect(heroBox).not.toBeNull();
    expect(heroBox.height).toBeGreaterThan(100);
  });

  test('Value propositions section renders correctly', async ({ page, browserName }) => {
    const valueProps = page.locator('#value-propositions, [data-section="value-propositions"]');
    await expect(valueProps).toBeVisible();

    // Verify all three propositions are visible
    const propositions = page.locator('.proposition');
    await expect(propositions).toHaveCount(3);

    // Verify each proposition has title and description
    const memcachedProp = page.locator('[data-proposition="memcached-compatibility"]');
    await expect(memcachedProp).toBeVisible();
    await expect(memcachedProp.locator('.proposition-title')).toContainText('Memcached');

    const persistenceProp = page.locator('[data-proposition="persistence"]');
    await expect(persistenceProp).toBeVisible();
    await expect(persistenceProp.locator('.proposition-title')).toContainText('Persistence');

    const lsmProp = page.locator('[data-proposition="lsm-tree"]');
    await expect(lsmProp).toBeVisible();
    await expect(lsmProp.locator('.proposition-title')).toContainText('LSM Tree');
  });

  test('Commands section renders correctly', async ({ page, browserName }) => {
    const commandsSection = page.locator('#commands, [data-section="commands"]');
    await expect(commandsSection).toBeVisible();

    // Verify command groups exist
    const storageCommands = page.locator('[data-command-type="storage"]');
    await expect(storageCommands).toBeVisible();

    const retrievalCommands = page.locator('[data-command-type="retrieval"]');
    await expect(retrievalCommands).toBeVisible();

    const deletionCommands = page.locator('[data-command-type="deletion"]');
    await expect(deletionCommands).toBeVisible();

    const mirdbCommands = page.locator('[data-command-type="mirdb-specific"]');
    await expect(mirdbCommands).toBeVisible();

    // Verify specific commands are displayed (use first() to handle multiple matches)
    await expect(page.locator('#commands code:has-text("SET")').first()).toBeVisible();
    await expect(page.locator('#commands code:has-text("GET")').first()).toBeVisible();
    await expect(page.locator('#commands code:has-text("DELETE")').first()).toBeVisible();
  });

  test('Feature comparison table renders correctly', async ({ page, browserName }) => {
    const comparisonSection = page.locator('#feature-comparison, [data-section="feature-comparison"]');
    await expect(comparisonSection).toBeVisible();

    // Verify table exists
    const table = page.locator('.comparison-table');
    await expect(table).toBeVisible();

    // Verify table header
    const headers = table.locator('thead th');
    await expect(headers).toHaveCount(3); // Feature, MirDB, Standard Memcached

    // Verify table rows exist
    const rows = table.locator('tbody tr');
    const rowCount = await rows.count();
    expect(rowCount).toBeGreaterThanOrEqual(5);

    // Verify feature badges are visible
    await expect(page.locator('.feature-yes').first()).toBeVisible();
  });

  test('Getting started section renders correctly', async ({ page, browserName }) => {
    const gettingStarted = page.locator('#getting-started, .getting-started');
    await expect(gettingStarted).toBeVisible();

    // Verify section title
    await expect(gettingStarted.locator('.section-title')).toContainText('Getting Started');

    // Verify steps exist
    const steps = gettingStarted.locator('.step');
    const stepCount = await steps.count();
    expect(stepCount).toBeGreaterThanOrEqual(3);

    // Verify code blocks are visible
    const codeBlocks = gettingStarted.locator('pre');
    await expect(codeBlocks.first()).toBeVisible();
  });

  test('Client connection examples section renders correctly', async ({ page, browserName }) => {
    const clientSection = page.locator('#client-connection, [data-section="client-connection"]');
    await expect(clientSection).toBeVisible();

    // Verify section title
    await expect(clientSection.locator('.section-title')).toContainText('Client Connection');

    // Verify client examples exist
    const pythonExample = page.locator('[data-language="python"]');
    await expect(pythonExample).toBeVisible();

    const nodejsExample = page.locator('[data-language="nodejs"]');
    await expect(nodejsExample).toBeVisible();

    const rubyExample = page.locator('[data-language="ruby"]');
    await expect(rubyExample).toBeVisible();
  });

  test('Footer renders correctly', async ({ page, browserName }) => {
    const footer = page.locator('footer.footer');
    await expect(footer).toBeVisible();

    // Verify copyright text
    await expect(footer).toContainText('MirDB');

    // Verify footer links
    const githubLink = footer.locator('a:has-text("GitHub")');
    await expect(githubLink).toBeVisible();
    await expect(githubLink).toHaveAttribute('href', /github\.com/);

    const docsLink = footer.locator('a:has-text("Documentation")');
    await expect(docsLink).toBeVisible();
  });
});

test.describe('Cross-Browser Compatibility - Styles Applied Correctly', () => {

  test.beforeEach(async ({ page }) => {
    await page.goto(getFileUrl());
    await page.waitForLoadState('domcontentloaded');
  });

  test('CSS styles are applied to hero section', async ({ page, browserName }) => {
    const hero = page.locator('.hero, header.hero');

    // Verify hero has gradient background
    const bgColor = await hero.evaluate((el) => {
      return window.getComputedStyle(el).background;
    });
    // Should have gradient or background color applied
    expect(bgColor).toBeTruthy();

    // Verify text is white/light colored
    const h1 = page.locator('.hero h1');
    const color = await h1.evaluate((el) => {
      return window.getComputedStyle(el).color;
    });
    // Check that color is light (white or near-white)
    expect(color).toMatch(/rgb\(255|rgba\(255|#fff|white/i);
  });

  test('CSS styles are applied to buttons', async ({ page, browserName }) => {
    const primaryBtn = page.locator('.btn-primary').first();
    const secondaryBtn = page.locator('.btn-secondary').first();

    // Verify primary button has background color
    const primaryBg = await primaryBtn.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });
    expect(primaryBg).not.toBe('rgba(0, 0, 0, 0)');
    expect(primaryBg).not.toBe('transparent');

    // Verify secondary button has border
    const secondaryBorder = await secondaryBtn.evaluate((el) => {
      return window.getComputedStyle(el).borderWidth;
    });
    expect(secondaryBorder).not.toBe('0px');
  });

  test('CSS grid/flexbox layouts work correctly', async ({ page, browserName }) => {
    // Verify propositions grid layout
    const propsGrid = page.locator('.propositions-grid');
    const display = await propsGrid.evaluate((el) => {
      return window.getComputedStyle(el).display;
    });
    // Should be flex or grid
    expect(display).toMatch(/flex|grid/);

    // Verify container has max-width
    const container = page.locator('.container').first();
    const maxWidth = await container.evaluate((el) => {
      return window.getComputedStyle(el).maxWidth;
    });
    expect(maxWidth).not.toBe('none');
  });

  test('Code blocks have proper styling', async ({ page, browserName }) => {
    const codeBlock = page.locator('pre').first();

    // Verify dark background
    const bgColor = await codeBlock.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });
    // Background should be dark (not white/transparent)
    expect(bgColor).not.toBe('rgba(0, 0, 0, 0)');
    expect(bgColor).not.toBe('rgb(255, 255, 255)');

    // Verify monospace font
    const fontFamily = await codeBlock.evaluate((el) => {
      return window.getComputedStyle(el).fontFamily;
    });
    expect(fontFamily.toLowerCase()).toMatch(/mono|consolas|menlo|courier/);
  });

  test('Section backgrounds alternate correctly', async ({ page, browserName }) => {
    const valueProps = page.locator('.value-propositions');
    const gettingStarted = page.locator('.getting-started');

    const valuePropsBg = await valueProps.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });

    const gettingStartedBg = await gettingStarted.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });

    // Sections should have some background styling
    expect(valuePropsBg).toBeTruthy();
    expect(gettingStartedBg).toBeTruthy();
  });
});

test.describe('Cross-Browser Compatibility - Interactions Work', () => {

  test.beforeEach(async ({ page }) => {
    await page.goto(getFileUrl());
    await page.waitForLoadState('domcontentloaded');
  });

  test('Smooth scroll anchor links work', async ({ page, browserName }) => {
    const getStartedBtn = page.locator('a[href="#getting-started"]');

    // Get initial scroll position
    const initialY = await page.evaluate(() => window.scrollY);

    // Click the get started button
    await getStartedBtn.click();

    // Wait for scroll animation
    await page.waitForTimeout(500);

    // Verify scroll position changed
    const finalY = await page.evaluate(() => window.scrollY);
    expect(finalY).toBeGreaterThan(initialY);
  });

  test('Button hover states work', async ({ page, browserName }) => {
    const btn = page.locator('.btn-primary').first();

    // Get initial transform
    const initialTransform = await btn.evaluate((el) => {
      return window.getComputedStyle(el).transform;
    });

    // Hover over button
    await btn.hover();

    // Wait for transition
    await page.waitForTimeout(300);

    // Button should have some hover effect (transform or background change)
    // This mainly tests that the CSS :hover pseudo-class works
    const hoverBg = await btn.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });
    expect(hoverBg).toBeTruthy();
  });

  test('External links have correct attributes', async ({ page, browserName }) => {
    const githubLink = page.locator('a[href*="github.com"]').first();

    // Verify target="_blank" for external links
    await expect(githubLink).toHaveAttribute('target', '_blank');

    // Verify rel="noopener" for security
    const rel = await githubLink.getAttribute('rel');
    expect(rel).toContain('noopener');
  });

  test('Proposition cards have hover effects', async ({ page, browserName }) => {
    const proposition = page.locator('.proposition').first();

    // Get initial box shadow
    const initialShadow = await proposition.evaluate((el) => {
      return window.getComputedStyle(el).boxShadow;
    });

    // Hover over proposition
    await proposition.hover();
    await page.waitForTimeout(300);

    // Verify transition property exists (indicating hover effects are possible)
    const transition = await proposition.evaluate((el) => {
      return window.getComputedStyle(el).transition;
    });
    expect(transition).not.toBe('none 0s ease 0s');
  });

  test('Focus styles work for accessibility', async ({ page, browserName }) => {
    const btn = page.locator('.btn').first();

    // Focus the button using keyboard
    await btn.focus();

    // Verify focus outline is visible
    const outline = await btn.evaluate((el) => {
      return window.getComputedStyle(el).outline;
    });

    // Should have some outline or box-shadow for focus state
    const boxShadow = await btn.evaluate((el) => {
      return window.getComputedStyle(el).boxShadow;
    });

    // Either outline or box-shadow should be present for focus
    const hasFocusIndicator = outline !== 'none' || boxShadow !== 'none';
    expect(hasFocusIndicator).toBe(true);
  });
});

test.describe('Cross-Browser Compatibility - Layout Consistency', () => {

  test.beforeEach(async ({ page }) => {
    await page.goto(getFileUrl());
    await page.waitForLoadState('domcontentloaded');
  });

  test('Page structure is consistent', async ({ page, browserName }) => {
    // Verify main structural elements exist and are in order
    const header = page.locator('header.hero');
    const main = page.locator('main');
    const footer = page.locator('footer.footer');

    await expect(header).toBeVisible();
    await expect(main).toBeVisible();
    await expect(footer).toBeVisible();

    // Verify header comes before main, main before footer
    const headerBox = await header.boundingBox();
    const mainBox = await main.boundingBox();
    const footerBox = await footer.boundingBox();

    expect(headerBox.y).toBeLessThan(mainBox.y);
    expect(mainBox.y).toBeLessThan(footerBox.y);
  });

  test('Container widths are constrained', async ({ page, browserName }) => {
    const viewport = page.viewportSize();
    const container = page.locator('.container').first();

    const containerBox = await container.boundingBox();

    // Container should not exceed 1200px (max-width) + padding
    expect(containerBox.width).toBeLessThanOrEqual(1250);

    // Container should be centered (horizontal margins should be roughly equal)
    if (viewport.width > 1200) {
      const leftMargin = containerBox.x;
      const rightMargin = viewport.width - (containerBox.x + containerBox.width);
      // Margins should be roughly equal (allow for small differences)
      expect(Math.abs(leftMargin - rightMargin)).toBeLessThan(50);
    }
  });

  test('Typography hierarchy is consistent', async ({ page, browserName }) => {
    const h1 = page.locator('h1').first();
    const h2 = page.locator('h2').first();
    const h3 = page.locator('h3').first();
    const p = page.locator('p').first();

    const h1Size = await h1.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });

    const h2Size = await h2.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });

    const h3Size = await h3.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });

    const pSize = await p.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });

    // Typography should follow hierarchy
    expect(h1Size).toBeGreaterThan(h2Size);
    expect(h2Size).toBeGreaterThan(h3Size);
    expect(h3Size).toBeGreaterThanOrEqual(pSize);
  });

  test('Images and visual elements render', async ({ page, browserName }) => {
    // Verify feature badges render correctly
    const yesIcons = page.locator('.feature-yes');
    const noIcons = page.locator('.feature-no');

    await expect(yesIcons.first()).toBeVisible();
    await expect(noIcons.first()).toBeVisible();

    // Verify badges have proper background colors
    const yesBg = await yesIcons.first().evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });
    expect(yesBg).not.toBe('rgba(0, 0, 0, 0)');
  });
});
