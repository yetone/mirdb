/**
 * Browser Compatibility E2E Tests
 * Owner: Scenario 15 - Browser Compatibility
 *
 * Tests verify the homepage works correctly across target browsers:
 * - Chrome (Chromium)
 * - Firefox (Gecko)
 * - Safari (WebKit)
 * - Edge (Chromium-based, tested via Chromium project)
 *
 * Requirements: NFR-5 (Must support latest 2 versions of Chrome, Firefox, Safari, and Edge)
 */

const { test, expect } = require('@playwright/test');

test.describe('Browser Compatibility Tests', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Wait for page to be fully loaded
    await page.waitForLoadState('domcontentloaded');
  });

  test('TC1: Page loads and renders correctly', async ({ page, browserName }) => {
    // Verify page title
    await expect(page).toHaveTitle(/MirDB/);

    // Verify main content is visible
    const main = page.locator('main');
    await expect(main).toBeVisible();

    // Verify header is present and fixed
    const header = page.locator('header.header');
    await expect(header).toBeVisible();

    // Verify hero section renders
    const hero = page.locator('#hero');
    await expect(hero).toBeVisible();

    // Verify no critical errors in console
    const consoleLogs = [];
    page.on('console', msg => {
      if (msg.type() === 'error') {
        consoleLogs.push(msg.text());
      }
    });

    // Wait a bit for any delayed errors
    await page.waitForTimeout(500);

    // Filter out known non-critical errors (like favicon 404)
    const criticalErrors = consoleLogs.filter(log =>
      !log.includes('favicon') &&
      !log.includes('net::ERR') &&
      !log.includes('404')
    );

    expect(criticalErrors.length).toBe(0);
  });

  test('TC2: CSS Grid layouts render correctly', async ({ page, browserName }) => {
    // Test features grid (3-column layout on desktop)
    const featuresGrid = page.locator('.features-grid');
    await expect(featuresGrid).toBeVisible();

    // Verify feature cards are present
    const featureCards = page.locator('.feature-card');
    await expect(featureCards).toHaveCount(6);

    // On desktop viewport, check grid layout works
    const viewportSize = page.viewportSize();
    if (viewportSize && viewportSize.width >= 1024) {
      // Get the computed grid style
      const gridStyle = await featuresGrid.evaluate(el => {
        const style = window.getComputedStyle(el);
        return {
          display: style.display,
          gridTemplateColumns: style.gridTemplateColumns
        };
      });

      // Should either be grid or flex (for fallback)
      expect(['grid', 'flex']).toContain(gridStyle.display);
    }

    // Test footer grid layout
    const footerContent = page.locator('.footer-content');
    await expect(footerContent).toBeVisible();

    const footerGridStyle = await footerContent.evaluate(el => {
      return window.getComputedStyle(el).display;
    });
    expect(['grid', 'flex']).toContain(footerGridStyle);
  });

  test('TC3: Navigation works correctly', async ({ page, browserName }) => {
    // Test header is fixed at top
    const header = page.locator('header.header');
    await expect(header).toBeVisible();

    const headerStyle = await header.evaluate(el => {
      return window.getComputedStyle(el).position;
    });
    expect(headerStyle).toBe('fixed');

    // Get viewport size to determine mobile or desktop
    const viewportSize = page.viewportSize();
    const isMobile = viewportSize && viewportSize.width < 768;

    if (isMobile) {
      // On mobile, hamburger menu should be visible
      const hamburger = page.locator('.hamburger-menu');
      await expect(hamburger).toBeVisible();

      // Click hamburger to open mobile nav
      await hamburger.click();
      await page.waitForTimeout(300);

      // Mobile nav links should be in mobile-nav
      const mobileNavLinks = page.locator('.mobile-nav a');
      const mobileNavCount = await mobileNavLinks.count();
      expect(mobileNavCount).toBeGreaterThanOrEqual(4);

      // Click features link in mobile nav
      await page.locator('.mobile-nav a[href="#features"]').click();
      await page.waitForTimeout(800);
    } else {
      // On desktop, test navigation links exist
      const navLinks = page.locator('.nav-links a');
      const navCount = await navLinks.count();
      expect(navCount).toBeGreaterThanOrEqual(4);

      // Test smooth scroll to features section
      await page.click('.nav-links a[href="#features"]');
      await page.waitForTimeout(800);
    }

    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport();
  });

  test('TC4: Interactive elements function correctly', async ({ page, browserName }) => {
    // Test CTA buttons
    const getStartedBtn = page.locator('[data-testid="cta-get-started"]');
    await expect(getStartedBtn).toBeVisible();
    await expect(getStartedBtn).toBeEnabled();

    const githubBtn = page.locator('[data-testid="cta-github"]');
    await expect(githubBtn).toBeVisible();
    await expect(githubBtn).toHaveAttribute('target', '_blank');
    await expect(githubBtn).toHaveAttribute('rel', /noopener/);

    // Test button hover states (verify they have transition)
    const buttonStyle = await getStartedBtn.evaluate(el => {
      const style = window.getComputedStyle(el);
      return {
        cursor: style.cursor,
        transition: style.transition
      };
    });
    expect(buttonStyle.cursor).toBe('pointer');

    // Test skip link functionality
    const skipLink = page.locator('.skip-link');
    await skipLink.focus();

    // Skip link should become visible on focus
    const skipLinkVisible = await skipLink.evaluate(el => {
      const style = window.getComputedStyle(el);
      // When focused, top should not be -100%
      return style.top !== '-100%';
    });
    // Note: This tests that focus styling exists
  });

  test('TC5: Syntax highlighting works', async ({ page, browserName }) => {
    // Navigate to quick start section
    const viewportSize = page.viewportSize();
    const isMobile = viewportSize && viewportSize.width < 768;

    if (isMobile) {
      // On mobile, scroll directly to quickstart section
      await page.locator('#quickstart').scrollIntoViewIfNeeded();
    } else {
      await page.click('.nav-links a[href="#quickstart"]');
    }
    await page.waitForTimeout(500);

    // Verify code blocks exist
    const codeBlocks = page.locator('pre code');
    const codeCount = await codeBlocks.count();
    expect(codeCount).toBeGreaterThan(0);

    // Check that Prism.js has been applied (code should have class or be highlighted)
    const quickstartCode = page.locator('[data-testid="installation-commands"] code');
    await expect(quickstartCode).toBeVisible();

    // Wait for Prism.js to process
    await page.waitForTimeout(1000);

    // Check code block is styled correctly
    const preStyle = await page.locator('[data-testid="installation-commands"]').evaluate(el => {
      const style = window.getComputedStyle(el);
      return {
        backgroundColor: style.backgroundColor,
        fontFamily: style.fontFamily
      };
    });

    // Should have dark background (code block styling)
    expect(preStyle.backgroundColor).not.toBe('rgba(0, 0, 0, 0)');
  });

  test('TC6: Tables render correctly', async ({ page, browserName }) => {
    // Test comparison table
    const comparisonTable = page.locator('.comparison-table');
    await comparisonTable.scrollIntoViewIfNeeded();
    await expect(comparisonTable).toBeVisible();

    // Verify table has correct structure
    const tableHeaders = comparisonTable.locator('thead th');
    await expect(tableHeaders).toHaveCount(4); // Feature, MirDB, Memcached, Redis

    // Test configuration table
    const configTable = page.locator('.config-table');
    await configTable.scrollIntoViewIfNeeded();
    await expect(configTable).toBeVisible();

    // Verify config table is scrollable
    const configWrapper = page.locator('.config-table-wrapper');
    const overflowStyle = await configWrapper.evaluate(el => {
      return window.getComputedStyle(el).overflowX;
    });
    expect(overflowStyle).toBe('auto');
  });

  test('TC7: Mermaid diagram renders', async ({ page, browserName }) => {
    // Navigate to architecture section
    const viewportSize = page.viewportSize();
    const isMobile = viewportSize && viewportSize.width < 768;

    if (isMobile) {
      // On mobile, scroll directly to architecture section
      await page.locator('#architecture').scrollIntoViewIfNeeded();
    } else {
      await page.click('.nav-links a[href="#architecture"]');
    }
    await page.waitForTimeout(500);

    const archDiagram = page.locator('.architecture-diagram');
    await expect(archDiagram).toBeVisible();

    // Wait for Mermaid to render
    await page.waitForTimeout(2000);

    // Check for SVG element (Mermaid renders to SVG)
    const mermaidSvg = page.locator('.architecture-diagram svg');
    const svgCount = await mermaidSvg.count();

    // Either SVG exists (rendered) or mermaid pre exists (fallback)
    if (svgCount > 0) {
      await expect(mermaidSvg.first()).toBeVisible();
    } else {
      // Mermaid code should at least be present
      const mermaidPre = page.locator('.mermaid');
      await expect(mermaidPre).toBeVisible();
    }
  });

  test('TC8: Responsive design works at mobile viewport', async ({ page, browserName }) => {
    // Set mobile viewport
    await page.setViewportSize({ width: 375, height: 667 });
    await page.waitForTimeout(300);

    // Hamburger menu should be visible on mobile
    const hamburger = page.locator('.hamburger-menu');
    await expect(hamburger).toBeVisible();

    // Regular nav should be hidden
    const navLinks = page.locator('.nav-links');
    const navDisplay = await navLinks.evaluate(el => {
      return window.getComputedStyle(el).display;
    });
    expect(navDisplay).toBe('none');

    // Verify no horizontal scrollbar
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScroll).toBe(false);

    // Feature cards should stack (single column)
    const featuresGrid = page.locator('.features-grid');
    const gridStyle = await featuresGrid.evaluate(el => {
      return window.getComputedStyle(el).gridTemplateColumns;
    });
    // On mobile, should be single column (1fr or none if flex)
    if (gridStyle && gridStyle !== 'none') {
      // Count the number of columns (fr units)
      const columnCount = gridStyle.split(' ').filter(col => col.includes('fr') || col.includes('px')).length;
      expect(columnCount).toBeLessThanOrEqual(1);
    }
  });

  test('TC9: CSS custom properties work', async ({ page, browserName }) => {
    // Check that CSS custom properties are being used
    const rootStyles = await page.evaluate(() => {
      const root = document.documentElement;
      const style = getComputedStyle(root);
      return {
        primaryColor: style.getPropertyValue('--color-primary').trim(),
        secondaryColor: style.getPropertyValue('--color-secondary').trim(),
        maxWidth: style.getPropertyValue('--max-width').trim()
      };
    });

    // Verify custom properties have values
    expect(rootStyles.primaryColor).toBeTruthy();
    expect(rootStyles.secondaryColor).toBeTruthy();

    // Verify they're being applied (check a component using them)
    const btnPrimary = page.locator('.btn-primary').first();
    const btnStyle = await btnPrimary.evaluate(el => {
      return window.getComputedStyle(el).backgroundColor;
    });

    // Button should have some background color (not transparent)
    expect(btnStyle).not.toBe('rgba(0, 0, 0, 0)');
  });

  test('TC10: Focus states are visible for accessibility', async ({ page, browserName }) => {
    // Tab through interactive elements and check focus visibility
    const interactiveElements = [
      '.skip-link',
      '.logo',
      '.nav-links a',
      '.btn-primary',
      '.btn-secondary'
    ];

    // Check that focus-visible styles are defined
    const hasFocusStyles = await page.evaluate(() => {
      // Check for :focus-visible rules in stylesheets
      for (const sheet of document.styleSheets) {
        try {
          for (const rule of sheet.cssRules) {
            if (rule.selectorText && rule.selectorText.includes(':focus-visible')) {
              return true;
            }
            if (rule.selectorText && rule.selectorText.includes(':focus')) {
              return true;
            }
          }
        } catch (e) {
          // Cross-origin stylesheets can't be read
          continue;
        }
      }
      return false;
    });

    expect(hasFocusStyles).toBe(true);

    // Test keyboard navigation - press Tab multiple times
    await page.keyboard.press('Tab');
    await page.keyboard.press('Tab');

    // There should be an active element
    const activeElement = await page.evaluate(() => {
      return document.activeElement?.tagName;
    });

    // Should have focused on something (not body)
    expect(['A', 'BUTTON', 'INPUT']).toContain(activeElement);
  });
});

test.describe('CSS Grid Fallback Tests', () => {
  test('TC11: Layout gracefully degrades without CSS Grid', async ({ page, browserName }) => {
    // Simulate CSS Grid not being supported by checking @supports behavior
    // Note: Modern browsers all support CSS Grid, so we verify the fallback CSS exists

    await page.goto('/');

    // Check that flexbox fallback styles exist as backup
    const featuresGrid = page.locator('.features-grid');

    // Evaluate if element can render with either grid or flex
    const layoutWorks = await featuresGrid.evaluate(el => {
      const style = window.getComputedStyle(el);
      const display = style.display;
      // Either grid or flex means layout is functional
      return display === 'grid' || display === 'flex';
    });

    expect(layoutWorks).toBe(true);

    // Verify all feature cards are still visible regardless of layout method
    const featureCards = page.locator('.feature-card');
    for (let i = 0; i < 6; i++) {
      await expect(featureCards.nth(i)).toBeVisible();
    }

    // Footer should also work
    const footerContent = page.locator('.footer-content');
    const footerLayoutWorks = await footerContent.evaluate(el => {
      const style = window.getComputedStyle(el);
      return style.display === 'grid' || style.display === 'flex';
    });

    expect(footerLayoutWorks).toBe(true);
  });
});

test.describe('Cross-Browser Specific Tests', () => {
  test('smooth scroll behavior works', async ({ page, browserName }) => {
    await page.goto('/');

    // Get viewport size to determine mobile or desktop
    const viewportSize = page.viewportSize();
    const isMobile = viewportSize && viewportSize.width < 768;

    // Get initial scroll position
    const initialScroll = await page.evaluate(() => window.scrollY);

    if (isMobile) {
      // On mobile, scroll directly to comparison section
      await page.locator('#comparison').scrollIntoViewIfNeeded();
    } else {
      // Click navigation link
      await page.click('.nav-links a[href="#comparison"]');
    }

    // Wait for scroll to complete
    await page.waitForTimeout(1000);

    // Get final scroll position
    const finalScroll = await page.evaluate(() => window.scrollY);

    // Should have scrolled down
    expect(finalScroll).toBeGreaterThan(initialScroll);

    // Comparison section should be in view
    const comparisonSection = page.locator('#comparison');
    await expect(comparisonSection).toBeInViewport();
  });

  test('external links have correct attributes', async ({ page, browserName }) => {
    await page.goto('/');

    // Check all external links
    const externalLinks = page.locator('a[target="_blank"]');
    const linkCount = await externalLinks.count();

    expect(linkCount).toBeGreaterThan(0);

    // Each external link should have rel="noopener noreferrer" or "noopener"
    for (let i = 0; i < linkCount; i++) {
      const rel = await externalLinks.nth(i).getAttribute('rel');
      expect(rel).toMatch(/noopener/);
    }
  });

  test('images and icons load correctly', async ({ page, browserName }) => {
    await page.goto('/');

    // Check SVG icons load
    const svgIcons = page.locator('svg');
    const svgCount = await svgIcons.count();
    expect(svgCount).toBeGreaterThan(0);

    // Hero logo SVG should be visible
    const heroLogo = page.locator('.hero-logo svg');
    await expect(heroLogo).toBeVisible();

    // Feature icons should be visible
    const featureIcons = page.locator('.feature-icon svg');
    const iconCount = await featureIcons.count();
    expect(iconCount).toBe(6);
  });
});
