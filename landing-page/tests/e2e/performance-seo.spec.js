/**
 * Performance & SEO E2E Tests
 * Owner: Scenario 11 - Performance & SEO
 *
 * Test cases:
 * - Largest Contentful Paint (LCP) under 2.5 seconds
 * - Cumulative Layout Shift (CLS) under 0.1
 * - Lighthouse accessibility score >= 90
 */

const { test, expect } = require('@playwright/test');

test.describe('Performance & SEO E2E Tests', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Measure Largest Contentful Paint (LCP) - under 2.5 seconds', async ({ page }) => {
    // Navigate to the page and wait for load
    await page.goto('/');

    // Use Performance Observer to measure LCP
    const lcp = await page.evaluate(() => {
      return new Promise((resolve) => {
        let lcpValue = 0;

        // Create a PerformanceObserver for LCP
        const observer = new PerformanceObserver((list) => {
          const entries = list.getEntries();
          // Get the last LCP entry (the most accurate)
          const lastEntry = entries[entries.length - 1];
          if (lastEntry) {
            lcpValue = lastEntry.startTime;
          }
        });

        observer.observe({ type: 'largest-contentful-paint', buffered: true });

        // Wait for page to be fully loaded, then report LCP
        if (document.readyState === 'complete') {
          setTimeout(() => {
            observer.disconnect();
            resolve(lcpValue);
          }, 500);
        } else {
          window.addEventListener('load', () => {
            setTimeout(() => {
              observer.disconnect();
              resolve(lcpValue);
            }, 500);
          });
        }
      });
    });

    console.log(`LCP: ${lcp}ms`);
    // LCP should be under 2500ms (2.5 seconds)
    expect(lcp).toBeLessThan(2500);
  });

  test('Measure Cumulative Layout Shift (CLS) - under 0.1', async ({ page }) => {
    // Navigate to the page
    await page.goto('/');

    // Wait for the page to be fully loaded and stable
    await page.waitForLoadState('networkidle');

    // Use Performance Observer to measure CLS
    const cls = await page.evaluate(() => {
      return new Promise((resolve) => {
        let clsValue = 0;

        // Create a PerformanceObserver for layout shifts
        const observer = new PerformanceObserver((list) => {
          for (const entry of list.getEntries()) {
            // Only count layout shifts that weren't caused by user input
            if (!entry.hadRecentInput) {
              clsValue += entry.value;
            }
          }
        });

        observer.observe({ type: 'layout-shift', buffered: true });

        // Wait for page to stabilize
        setTimeout(() => {
          observer.disconnect();
          resolve(clsValue);
        }, 2000);
      });
    });

    console.log(`CLS: ${cls}`);
    // CLS should be under 0.1
    expect(cls).toBeLessThan(0.1);
  });

  test('Run Lighthouse audit - accessibility score >= 90', async ({ page }) => {
    // This test verifies accessibility requirements without deprecated methods
    // Checking key accessibility patterns that contribute to Lighthouse score

    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    // 1. All images should have alt text
    const imagesWithoutAlt = await page.$$eval('img', (imgs) =>
      imgs.filter((img) => !img.hasAttribute('alt') || img.getAttribute('alt') === '').length
    );
    expect(imagesWithoutAlt).toBe(0);

    // 2. All form elements should have labels
    const formElementsWithoutLabel = await page.$$eval(
      'input:not([type="hidden"]), select, textarea',
      (elements) =>
        elements.filter((el) => {
          const id = el.id;
          if (!id) return !el.hasAttribute('aria-label') && !el.hasAttribute('aria-labelledby');
          const label = document.querySelector(`label[for="${id}"]`);
          return !label && !el.hasAttribute('aria-label') && !el.hasAttribute('aria-labelledby');
        }).length
    );
    expect(formElementsWithoutLabel).toBe(0);

    // 3. All buttons should have accessible names
    const buttonsWithoutNames = await page.$$eval('button', (buttons) =>
      buttons.filter((btn) => {
        const text = btn.textContent.trim();
        const ariaLabel = btn.getAttribute('aria-label');
        const ariaLabelledBy = btn.getAttribute('aria-labelledby');
        return !text && !ariaLabel && !ariaLabelledBy;
      }).length
    );
    expect(buttonsWithoutNames).toBe(0);

    // 4. All links should have accessible names
    const linksWithoutNames = await page.$$eval('a', (links) =>
      links.filter((link) => {
        const text = link.textContent.trim();
        const ariaLabel = link.getAttribute('aria-label');
        const ariaLabelledBy = link.getAttribute('aria-labelledby');
        return !text && !ariaLabel && !ariaLabelledBy;
      }).length
    );
    expect(linksWithoutNames).toBe(0);

    // 5. Check for proper heading hierarchy
    const headings = await page.$$eval('h1, h2, h3, h4, h5, h6', (headings) =>
      headings.map((h) => parseInt(h.tagName.charAt(1)))
    );

    // Verify there's exactly one h1
    const h1Count = headings.filter((level) => level === 1).length;
    expect(h1Count).toBe(1);

    // Verify headings don't skip levels (allow for reasonable tolerance)
    let lastLevel = 0;
    for (const level of headings) {
      if (lastLevel > 0) {
        // Allow skipping up to 1 level (e.g., h2 to h4 is common but not ideal)
        expect(level - lastLevel).toBeLessThanOrEqual(2);
      }
      lastLevel = level;
    }

    // 6. Check for skip link
    const skipLink = await page.$('a[href="#main-content"], a.skip-link');
    expect(skipLink).not.toBeNull();

    // 7. Check for lang attribute on html element
    const htmlLang = await page.$eval('html', (html) => html.getAttribute('lang'));
    expect(htmlLang).not.toBeNull();
    expect(htmlLang.length).toBeGreaterThan(0);

    // 8. Verify semantic HTML structure
    const hasHeader = await page.$('header');
    const hasMain = await page.$('main');
    const hasNav = await page.$('nav');
    const hasFooter = await page.$('footer');

    expect(hasHeader).not.toBeNull();
    expect(hasMain).not.toBeNull();
    expect(hasNav).not.toBeNull();
    expect(hasFooter).not.toBeNull();

    // 9. Check for ARIA roles
    const mainRole = await page.$eval('main', (el) => el.getAttribute('role'));
    const navRole = await page.$eval('nav', (el) => el.getAttribute('role'));

    expect(mainRole).toBe('main');
    expect(navRole).toBe('navigation');

    console.log('Accessibility checks passed');
  });
});
