// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

test.describe('Error Handling - Invalid Navigation', () => {
  test.beforeEach(async ({ page }) => {
    const filePath = path.resolve(__dirname, '../index.html');
    await page.goto(`file://${filePath}`);
  });

  // Test Case 1: Click all internal anchor links
  // Expected: Each link navigates to its target section without errors
  test('TC1: All internal anchor links navigate to their target sections', async ({ page }) => {
    // Get all internal anchor links (links that start with #)
    const anchorLinks = page.locator('a[href^="#"]');
    const count = await anchorLinks.count();

    expect(count).toBeGreaterThan(0);

    // Collect all unique anchor link hrefs
    const hrefSet = new Set();
    for (let i = 0; i < count; i++) {
      const link = anchorLinks.nth(i);
      const href = await link.getAttribute('href');
      if (href && href.startsWith('#') && href.length > 1) {
        hrefSet.add(href);
      }
    }
    const hrefs = Array.from(hrefSet);

    const failedNavigations = [];

    // Test each anchor link
    for (const href of hrefs) {
      // Reload page to reset scroll position
      const filePath = path.resolve(__dirname, '../index.html');
      await page.goto(`file://${filePath}`);

      const targetId = href.substring(1); // Remove the # prefix
      const targetElement = page.locator(`#${targetId}`);

      // Check that target element exists
      const targetExists = await targetElement.count();
      if (targetExists === 0) {
        failedNavigations.push({
          href,
          reason: `Target element with id="${targetId}" does not exist`
        });
        continue;
      }

      // Get initial scroll position
      const initialScrollY = await page.evaluate(() => window.scrollY);

      // Click the anchor link
      const linkToClick = page.locator(`a[href="${href}"]`).first();

      // Scroll link into view if it's not visible (e.g., skip link)
      await linkToClick.scrollIntoViewIfNeeded();
      await linkToClick.click();

      // Wait for smooth scroll animation to complete
      // Use a longer wait and poll for scroll stability
      await page.waitForTimeout(300);
      let lastScrollY = await page.evaluate(() => window.scrollY);
      for (let attempt = 0; attempt < 10; attempt++) {
        await page.waitForTimeout(150);
        const currentScrollY = await page.evaluate(() => window.scrollY);
        if (Math.abs(currentScrollY - lastScrollY) < 5) {
          break; // Scroll has stabilized
        }
        lastScrollY = currentScrollY;
      }

      // Get the target element's bounding box
      const targetBoundingBox = await targetElement.boundingBox();
      if (!targetBoundingBox) {
        failedNavigations.push({
          href,
          reason: `Target element with id="${targetId}" has no bounding box`
        });
        continue;
      }

      // Verify the target element is now visible
      const isVisible = await targetElement.isVisible();
      if (!isVisible) {
        failedNavigations.push({
          href,
          reason: `Target element with id="${targetId}" is not visible after click`
        });
        continue;
      }

      // Verify the page actually scrolled to the target
      // The scroll should have moved OR target was already in view
      const finalScrollY = await page.evaluate(() => window.scrollY);
      const viewportHeight = await page.evaluate(() => window.innerHeight);
      const documentHeight = await page.evaluate(() => document.documentElement.scrollHeight);

      // Get target's position relative to viewport
      const targetTop = await targetElement.evaluate((el) => {
        const rect = el.getBoundingClientRect();
        return rect.top;
      });

      // The target should be visible in viewport after navigation
      // Either at the top, or visible if the page can't scroll further
      // (e.g., target is near bottom of page)
      const maxScrollY = documentHeight - viewportHeight;
      const isAtMaxScroll = Math.abs(finalScrollY - maxScrollY) < 5;

      // Target is properly navigated to if:
      // 1. Target is at the top of viewport (within reasonable margin)
      // 2. OR target is visible and page is at maximum scroll (can't scroll further)
      // 3. OR page scrolled from initial position toward the target
      // 4. OR target is within 100px of being visible (bottom of page scenarios)
      const targetIsAtTop = targetTop >= -10 && targetTop < viewportHeight * 0.5;
      const targetIsVisible = targetTop >= 0 && targetTop < viewportHeight;
      const targetIsNearlyVisible = targetTop >= -50 && targetTop < viewportHeight + 100;
      const pageScrolled = Math.abs(finalScrollY - initialScrollY) > 10;
      const pageIsNearMaxScroll = finalScrollY > maxScrollY - 200;

      // For elements near the bottom of the page, the browser can't scroll past the document end
      // So we consider navigation successful if the page scrolled and target is nearly visible
      const navigationSuccessful =
        targetIsAtTop ||
        (targetIsVisible && isAtMaxScroll) ||
        (targetIsVisible && pageScrolled) ||
        (pageScrolled && targetIsNearlyVisible && pageIsNearMaxScroll);

      if (!navigationSuccessful) {
        failedNavigations.push({
          href,
          reason: `Target element with id="${targetId}" navigation failed. targetTop: ${targetTop}, viewportHeight: ${viewportHeight}, scrolled: ${pageScrolled}`
        });
      }
    }

    // Report any failed navigations
    if (failedNavigations.length > 0) {
      console.log('Failed navigations:', JSON.stringify(failedNavigations, null, 2));
    }

    expect(failedNavigations).toHaveLength(0);
  });

  // Test Case 2: Scan page for broken links
  // Expected: No internal links return 404 or point to non-existent elements
  test('TC2: No internal links point to non-existent elements', async ({ page }) => {
    // Get all anchor links on the page
    const allLinks = page.locator('a[href]');
    const count = await allLinks.count();

    expect(count).toBeGreaterThan(0);

    const brokenLinks = [];

    for (let i = 0; i < count; i++) {
      const link = allLinks.nth(i);
      const href = await link.getAttribute('href');

      if (!href) continue;

      // Check internal anchor links (starting with #)
      if (href.startsWith('#')) {
        if (href === '#') {
          // Empty anchor is valid (scrolls to top)
          continue;
        }

        const targetId = href.substring(1);

        // Use CSS.escape for IDs that might contain special characters
        const escapedId = targetId.replace(/([^\w-])/g, '\\$1');
        const targetElement = await page.$(`#${escapedId}`);

        if (!targetElement) {
          const linkText = await link.textContent();
          brokenLinks.push({
            href,
            linkText: linkText?.trim(),
            reason: `Target element with id="${targetId}" not found`
          });
        }
      }
    }

    // Report any broken links found
    if (brokenLinks.length > 0) {
      console.log('Broken links found:', JSON.stringify(brokenLinks, null, 2));
    }

    expect(brokenLinks).toHaveLength(0);
  });

  // Additional test: Verify skip link navigation
  test('TC3: Skip link navigates to main content', async ({ page }) => {
    // Look for skip link
    const skipLink = page.locator('a.skip-link, a[href="#main-content"]').first();
    const skipLinkExists = await skipLink.count();

    if (skipLinkExists > 0) {
      // Make skip link visible (usually hidden until focused)
      await skipLink.focus();
      await expect(skipLink).toBeFocused();

      // Click the skip link
      await skipLink.click();

      // Verify main content is accessible
      const mainContent = page.locator('#main-content');
      await expect(mainContent).toBeVisible();
    }
  });

  // Additional test: Verify CTA button anchor links work
  test('TC4: CTA buttons with anchor links navigate correctly', async ({ page }) => {
    // Check primary CTA (Get Started)
    const primaryCta = page.locator('[data-testid="cta-primary"]');
    const primaryHref = await primaryCta.getAttribute('href');

    if (primaryHref && primaryHref.startsWith('#')) {
      const targetId = primaryHref.substring(1);
      const targetElement = page.locator(`#${targetId}`);

      // Verify target exists
      await expect(targetElement).toBeVisible();

      // Click and verify navigation
      await primaryCta.click();
      await page.waitForTimeout(500);

      // Target should be near top of viewport after click
      const targetTop = await targetElement.evaluate((el) => {
        const rect = el.getBoundingClientRect();
        return rect.top;
      });

      const viewportHeight = await page.evaluate(() => window.innerHeight);
      expect(targetTop).toBeLessThan(viewportHeight);
    }

    // Check secondary CTA (View Documentation)
    const secondaryCta = page.locator('[data-testid="cta-secondary"]');
    const secondaryHref = await secondaryCta.getAttribute('href');

    if (secondaryHref && secondaryHref.startsWith('#')) {
      // Reload to reset scroll
      const filePath = path.resolve(__dirname, '../index.html');
      await page.goto(`file://${filePath}`);

      const targetId = secondaryHref.substring(1);
      const targetElement = page.locator(`#${targetId}`);

      // Verify target exists
      const targetExists = await targetElement.count();
      expect(targetExists).toBeGreaterThan(0);

      // Click and verify navigation
      await secondaryCta.click();
      await page.waitForTimeout(500);

      // Target should be visible
      await expect(targetElement).toBeVisible();
    }
  });

  // Additional test: Verify no duplicate IDs
  test('TC5: No duplicate IDs on the page', async ({ page }) => {
    const duplicateIds = await page.evaluate(() => {
      const allElements = document.querySelectorAll('[id]');
      const idCounts = {};
      const duplicates = [];

      allElements.forEach((el) => {
        const id = el.id;
        if (id) {
          idCounts[id] = (idCounts[id] || 0) + 1;
        }
      });

      Object.entries(idCounts).forEach(([id, count]) => {
        if (count > 1) {
          duplicates.push({ id, count });
        }
      });

      return duplicates;
    });

    if (duplicateIds.length > 0) {
      console.log('Duplicate IDs found:', duplicateIds);
    }

    expect(duplicateIds).toHaveLength(0);
  });

  // Additional test: All href values are well-formed
  test('TC6: All href values are well-formed', async ({ page }) => {
    const malformedLinks = await page.evaluate(() => {
      const links = document.querySelectorAll('a[href]');
      const issues = [];

      links.forEach((link) => {
        const href = link.getAttribute('href');

        if (!href) return;

        // Check for common issues
        if (href.includes('undefined') || href.includes('null')) {
          issues.push({
            href,
            text: link.textContent?.trim().substring(0, 50),
            issue: 'Contains undefined or null'
          });
        }

        if (href.startsWith('#') && href.length > 1) {
          // Anchor link - check for valid ID characters
          const targetId = href.substring(1);
          if (!/^[a-zA-Z][\w\-:.]*$/.test(targetId)) {
            // Relaxed check - just warn about unusual IDs
            if (targetId.includes(' ')) {
              issues.push({
                href,
                text: link.textContent?.trim().substring(0, 50),
                issue: 'Anchor ID contains spaces'
              });
            }
          }
        }

        // Check for empty fragments
        if (href === '#') {
          // Empty fragment is technically valid but could be intentional
          // Don't report as an issue
        }
      });

      return issues;
    });

    if (malformedLinks.length > 0) {
      console.log('Malformed links found:', malformedLinks);
    }

    expect(malformedLinks).toHaveLength(0);
  });
});
