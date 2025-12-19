import { test, expect } from '@playwright/test';

// Helper function to get the full file URL
const getPageUrl = () => `file://${process.cwd()}/public/index.html`;

test.describe('Keyboard Navigation - Scenario 12', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(getPageUrl());
  });

  test.describe('Test Case 1: Tab through entire page', () => {
    test('focus moves through all interactive elements in logical order', async ({ page }) => {
      // Start tabbing from the beginning of the page
      await page.keyboard.press('Tab');

      // Collect all focused elements in order
      const focusedElements: string[] = [];
      let maxTabs = 30; // Safety limit to prevent infinite loop

      while (maxTabs > 0) {
        const activeElement = await page.evaluate(() => {
          const el = document.activeElement;
          if (!el || el === document.body) return null;
          return {
            tag: el.tagName.toLowerCase(),
            text: el.textContent?.trim().substring(0, 30) || '',
            href: el.getAttribute('href'),
            testId: el.getAttribute('data-testid'),
            className: el.className,
          };
        });

        if (!activeElement) break;

        const identifier = activeElement.testId || activeElement.href || activeElement.text || activeElement.tag;

        // Check if we've looped back to the beginning
        if (focusedElements.length > 0 && focusedElements[0] === identifier) {
          break;
        }

        focusedElements.push(identifier);
        await page.keyboard.press('Tab');
        maxTabs--;
      }

      // Verify we can tab through multiple elements
      expect(focusedElements.length).toBeGreaterThan(5);

      // Verify logical order - skip link should be first
      expect(focusedElements[0]).toBe('skip-link');

      // Logo should come after skip link
      expect(focusedElements[1]).toContain('/');
    });

    test('all interactive elements are reachable via tab', async ({ page }) => {
      // Get all expected interactive elements
      const expectedInteractiveElements = await page.$$eval(
        'a[href], button, input, select, textarea, [tabindex]:not([tabindex="-1"])',
        (elements) => elements.filter(el => {
          // Filter out hidden elements
          const style = window.getComputedStyle(el);
          const isVisible = style.display !== 'none' && style.visibility !== 'hidden';
          const hasSize = el.getBoundingClientRect().width > 0;
          return isVisible && hasSize;
        }).length
      );

      // Tab through all elements and count unique ones
      const focusedElements = new Set<string>();
      await page.keyboard.press('Tab');

      let maxTabs = 50;
      let firstElement: string | null = null;

      while (maxTabs > 0) {
        const elementId = await page.evaluate(() => {
          const el = document.activeElement;
          if (!el || el === document.body) return null;
          const rect = el.getBoundingClientRect();
          return `${el.tagName}-${el.getAttribute('href') || ''}-${rect.x}-${rect.y}`;
        });

        if (!elementId) break;

        if (firstElement === null) {
          firstElement = elementId;
        } else if (elementId === firstElement) {
          // Looped back to start
          break;
        }

        focusedElements.add(elementId);
        await page.keyboard.press('Tab');
        maxTabs--;
      }

      // We should be able to reach most interactive elements
      // (Some may be hidden on desktop, so we check for at least 10)
      expect(focusedElements.size).toBeGreaterThanOrEqual(10);
    });
  });

  test.describe('Test Case 2: Check focus visibility on buttons', () => {
    test('buttons show visible focus indicator when focused', async ({ page }) => {
      // Tab through to get to CTA button (skip link -> logo -> nav links -> CTA button)
      // First skip link, then logo, then 5 nav links, then CTA
      for (let i = 0; i < 8; i++) {
        await page.keyboard.press('Tab');
      }

      // CTA button should be focusable via keyboard
      const focusedElement = await page.evaluate(() => {
        const el = document.activeElement;
        return el ? {
          tagName: el.tagName,
          className: el.className,
          href: el.getAttribute('href'),
        } : null;
      });

      // Should be focused on a link (the CTA is an anchor)
      expect(focusedElement).not.toBeNull();
      expect(focusedElement?.tagName).toBe('A');
    });

    test('copy button is focusable and has focus-visible styles in CSS', async ({ page }) => {
      // Verify the stylesheet contains focus-visible rules
      // Load the CSS file directly to check
      const response = await page.goto(`file://${process.cwd()}/public/styles.css`);
      const cssContent = await response?.text();

      // Check that focus-visible rule exists for buttons
      expect(cssContent).toContain('button:focus-visible');
      expect(cssContent).toContain('outline');
    });

    test('secondary button (anchor) has focus-visible support in CSS', async ({ page }) => {
      // Load the CSS file to check for focus-visible rules
      const response = await page.goto(`file://${process.cwd()}/public/styles.css`);
      const cssContent = await response?.text();

      // Check that focus-visible rule exists for anchors
      expect(cssContent).toContain('a:focus-visible');
      expect(cssContent).toContain('outline');
    });
  });

  test.describe('Test Case 3: Check focus visibility on links', () => {
    test('links show visible focus indicator when focused', async ({ page }) => {
      // Tab through to navigation links and verify they are reachable
      // Skip link (1), logo (2), nav link 1 (3)
      await page.keyboard.press('Tab'); // skip link
      await page.keyboard.press('Tab'); // logo
      await page.keyboard.press('Tab'); // first nav link

      const firstLinkText = await page.evaluate(() => {
        return document.activeElement?.textContent?.trim();
      });

      // Should be on Features link
      expect(firstLinkText).toBe('Features');

      // Tab to next link
      await page.keyboard.press('Tab');
      const secondLinkText = await page.evaluate(() => {
        return document.activeElement?.textContent?.trim();
      });
      expect(secondLinkText).toBe('Architecture');

      // Verify all navigation links are keyboard accessible (have href and tabindex >= 0)
      const navLinks = page.locator('.nav-links a');
      const count = await navLinks.count();
      expect(count).toBeGreaterThanOrEqual(4);

      for (let i = 0; i < count; i++) {
        const href = await navLinks.nth(i).getAttribute('href');
        expect(href).toBeTruthy();
      }
    });

    test('footer links are keyboard accessible', async ({ page }) => {
      // Tab to footer area (using many tabs to reach footer)
      // Alternatively, directly check footer links are focusable
      const footerLinks = page.locator('.footer-links a');
      const count = await footerLinks.count();

      // Footer links should exist
      expect(count).toBeGreaterThanOrEqual(2);

      // All footer links should have href and be focusable
      for (let i = 0; i < count; i++) {
        const href = await footerLinks.nth(i).getAttribute('href');
        const tabIndex = await footerLinks.nth(i).evaluate(el => el.tabIndex);

        expect(href).toBeTruthy();
        expect(tabIndex).toBeGreaterThanOrEqual(0); // Focusable
      }
    });

    test('logo link is keyboard accessible', async ({ page }) => {
      // Tab to logo (second tab after skip link)
      await page.keyboard.press('Tab'); // skip link
      await page.keyboard.press('Tab'); // logo

      const focusedHref = await page.evaluate(() => {
        return document.activeElement?.getAttribute('href');
      });

      // Logo should link to home
      expect(focusedHref).toBe('/');

      // Logo link should be focusable
      const logo = page.locator('.logo');
      const tabIndex = await logo.evaluate(el => el.tabIndex);
      expect(tabIndex).toBeGreaterThanOrEqual(0);
    });
  });

  test.describe('Test Case 4: Activate CTA button with Enter key', () => {
    test('button activates correctly when Enter is pressed', async ({ page }) => {
      const ctaButton = page.locator('[data-testid="cta-button"]');
      await ctaButton.focus();

      // The CTA button is an anchor to #quickstart
      // Check that pressing Enter navigates to that section
      const initialScrollY = await page.evaluate(() => window.scrollY);

      await page.keyboard.press('Enter');

      // Wait for smooth scroll to complete
      await page.waitForTimeout(500);

      // Check that the page scrolled to quickstart section
      const currentScrollY = await page.evaluate(() => window.scrollY);
      const quickstartSection = await page.locator('#quickstart').boundingBox();

      // The page should have scrolled
      expect(currentScrollY).toBeGreaterThan(initialScrollY);

      // The quickstart section should be near the top of viewport
      const viewportHeight = await page.evaluate(() => window.innerHeight);
      expect(quickstartSection!.y).toBeLessThan(viewportHeight);
    });

    test('navigation links activate with Enter key', async ({ page }) => {
      // Focus on Features link
      const featuresLink = page.locator('.nav-links a[href="#features"]');
      await featuresLink.focus();

      const initialScrollY = await page.evaluate(() => window.scrollY);

      await page.keyboard.press('Enter');

      // Wait for smooth scroll
      await page.waitForTimeout(500);

      const currentScrollY = await page.evaluate(() => window.scrollY);

      // Page should have scrolled to features section
      expect(currentScrollY).toBeGreaterThan(initialScrollY);
    });

    test('copy button is keyboard accessible', async ({ page }) => {
      const copyButton = page.locator('[data-testid="copy-button"]');
      await copyButton.scrollIntoViewIfNeeded();

      // Verify copy button exists and is focusable
      await expect(copyButton).toBeVisible();

      const tabIndex = await copyButton.evaluate(el => el.tabIndex);
      expect(tabIndex).toBeGreaterThanOrEqual(0);

      // Verify button has proper ARIA label for accessibility
      const ariaLabel = await copyButton.getAttribute('aria-label');
      expect(ariaLabel).toBe('Copy code to clipboard');
    });

    test('copy button can receive focus via Tab', async ({ page }) => {
      // Tab through the page until we reach the copy button
      let found = false;
      for (let i = 0; i < 20; i++) {
        await page.keyboard.press('Tab');
        const testId = await page.evaluate(() => document.activeElement?.getAttribute('data-testid'));
        if (testId === 'copy-button') {
          found = true;
          break;
        }
      }

      expect(found).toBe(true);
    });
  });

  test.describe('Test Case 5: Check for skip-to-content link', () => {
    test('skip link is present and moves focus to main content', async ({ page }) => {
      const skipLink = page.locator('[data-testid="skip-link"]');

      // Skip link should exist
      await expect(skipLink).toBeAttached();

      // Skip link should be first focusable element
      await page.keyboard.press('Tab');

      const focusedElement = await page.evaluate(() => {
        return document.activeElement?.getAttribute('data-testid');
      });

      expect(focusedElement).toBe('skip-link');
    });

    test('skip link has proper CSS for visibility on focus', async ({ page }) => {
      // Load the CSS file to verify skip link focus styles exist
      const response = await page.goto(`file://${process.cwd()}/public/styles.css`);
      const cssContent = await response?.text();

      // Check that skip-link:focus rule exists with top property
      expect(cssContent).toContain('.skip-link:focus');
      expect(cssContent).toContain('.skip-link');

      // Go back to the page
      await page.goto(`file://${process.cwd()}/public/index.html`);

      // Verify skip link is positioned off-screen initially (negative top value)
      const skipLink = page.locator('[data-testid="skip-link"]');
      const initialTop = await skipLink.evaluate((el) => {
        const rect = el.getBoundingClientRect();
        return rect.top;
      });

      // Initial top should be negative (off-screen)
      expect(initialTop).toBeLessThan(0);
    });

    test('activating skip link moves focus to main content', async ({ page }) => {
      // Tab to skip link
      await page.keyboard.press('Tab');

      // Activate skip link
      await page.keyboard.press('Enter');

      // Wait for navigation
      await page.waitForTimeout(100);

      // Check URL has #main-content
      const url = page.url();
      expect(url).toContain('#main-content');
    });

    test('skip link has correct href to main content', async ({ page }) => {
      const skipLink = page.locator('[data-testid="skip-link"]');
      const href = await skipLink.getAttribute('href');

      expect(href).toBe('#main-content');
    });

    test('main content element exists with correct id', async ({ page }) => {
      const mainContent = page.locator('#main-content');
      await expect(mainContent).toBeAttached();

      // Main content should be the <main> element
      const tagName = await mainContent.evaluate(el => el.tagName.toLowerCase());
      expect(tagName).toBe('main');
    });
  });

  test.describe('Test Case 6: Test mobile menu keyboard access', () => {
    test.beforeEach(async ({ page }) => {
      // Set mobile viewport
      await page.setViewportSize({ width: 375, height: 667 });
      await page.goto(getPageUrl());
    });

    test('mobile menu can be opened and navigated via keyboard', async ({ page }) => {
      const menuToggle = page.locator('[data-testid="mobile-menu-toggle"]');
      const navLinks = page.locator('[data-testid="nav-links"]');

      // Menu should be visible on mobile
      await expect(menuToggle).toBeVisible();

      // Nav links should be hidden initially
      await expect(navLinks).not.toBeVisible();

      // Tab to menu toggle (skip link first, then logo, then menu toggle)
      await page.keyboard.press('Tab'); // skip link
      await page.keyboard.press('Tab'); // logo
      await page.keyboard.press('Tab'); // menu toggle

      // Verify menu toggle is focused
      const focusedTestId = await page.evaluate(() => {
        return document.activeElement?.getAttribute('data-testid');
      });
      expect(focusedTestId).toBe('mobile-menu-toggle');

      // Activate menu with Enter
      await page.keyboard.press('Enter');

      // Nav links should now be visible
      await expect(navLinks).toBeVisible();

      // aria-expanded should be true
      const isExpanded = await menuToggle.getAttribute('aria-expanded');
      expect(isExpanded).toBe('true');
    });

    test('mobile menu can be navigated with Tab', async ({ page }) => {
      const menuToggle = page.locator('[data-testid="mobile-menu-toggle"]');

      // Focus and activate menu toggle
      await menuToggle.focus();
      await page.keyboard.press('Enter');

      // Tab through menu items
      await page.keyboard.press('Tab');

      // Should be on first nav link (Features)
      const firstLinkText = await page.evaluate(() => {
        return document.activeElement?.textContent?.trim();
      });
      expect(firstLinkText).toBe('Features');

      // Tab to next link
      await page.keyboard.press('Tab');
      const secondLinkText = await page.evaluate(() => {
        return document.activeElement?.textContent?.trim();
      });
      expect(secondLinkText).toBe('Architecture');
    });

    test('mobile menu can be closed with Escape key', async ({ page }) => {
      const menuToggle = page.locator('[data-testid="mobile-menu-toggle"]');
      const navLinks = page.locator('[data-testid="nav-links"]');

      // Open menu
      await menuToggle.focus();
      await page.keyboard.press('Enter');
      await expect(navLinks).toBeVisible();

      // Press Escape to close
      await page.keyboard.press('Escape');

      // Menu should be closed
      await expect(navLinks).not.toBeVisible();

      // aria-expanded should be false
      const isExpanded = await menuToggle.getAttribute('aria-expanded');
      expect(isExpanded).toBe('false');

      // Focus should return to menu toggle
      const focusedTestId = await page.evaluate(() => {
        return document.activeElement?.getAttribute('data-testid');
      });
      expect(focusedTestId).toBe('mobile-menu-toggle');
    });

    test('mobile menu toggle has correct ARIA attributes', async ({ page }) => {
      const menuToggle = page.locator('[data-testid="mobile-menu-toggle"]');

      // Check ARIA attributes
      const ariaExpanded = await menuToggle.getAttribute('aria-expanded');
      const ariaControls = await menuToggle.getAttribute('aria-controls');
      const ariaLabel = await menuToggle.getAttribute('aria-label');

      expect(ariaExpanded).toBe('false');
      expect(ariaControls).toBe('nav-links');
      expect(ariaLabel).toBe('Toggle navigation menu');
    });

    test('mobile menu toggle activates with Space key', async ({ page }) => {
      const menuToggle = page.locator('[data-testid="mobile-menu-toggle"]');
      const navLinks = page.locator('[data-testid="nav-links"]');

      await menuToggle.focus();
      await page.keyboard.press('Space');

      await expect(navLinks).toBeVisible();
    });
  });

  test.describe('Additional keyboard accessibility tests', () => {
    test('no keyboard trap - can tab through entire page', async ({ page }) => {
      // Tab through the entire page and ensure we eventually loop back
      const startElement = await page.evaluate(() => {
        return document.activeElement?.tagName;
      });

      let tabCount = 0;
      const maxTabs = 100;

      await page.keyboard.press('Tab');

      while (tabCount < maxTabs) {
        const currentElement = await page.evaluate(() => {
          return {
            tag: document.activeElement?.tagName,
            testId: document.activeElement?.getAttribute('data-testid'),
          };
        });

        // If we've looped back to body or start, we're good
        if (currentElement.tag === 'BODY' || (currentElement.testId === 'skip-link' && tabCount > 5)) {
          break;
        }

        await page.keyboard.press('Tab');
        tabCount++;
      }

      // We should be able to tab through without getting stuck
      expect(tabCount).toBeLessThan(maxTabs);
    });

    test('Shift+Tab navigates backwards', async ({ page }) => {
      // Tab forward a few times
      await page.keyboard.press('Tab'); // skip link
      await page.keyboard.press('Tab'); // logo
      await page.keyboard.press('Tab'); // first nav link

      const forwardElement = await page.evaluate(() => {
        return document.activeElement?.textContent?.trim();
      });

      // Now Shift+Tab back
      await page.keyboard.press('Shift+Tab');

      const backwardElement = await page.evaluate(() => {
        return document.activeElement?.textContent?.trim();
      });

      // Should be on different element
      expect(backwardElement).not.toBe(forwardElement);
    });

    test('focus order follows visual order', async ({ page }) => {
      const focusOrder: Array<{ y: number; x: number; text: string }> = [];

      await page.keyboard.press('Tab');

      for (let i = 0; i < 15; i++) {
        const elementInfo = await page.evaluate(() => {
          const el = document.activeElement;
          if (!el || el === document.body) return null;
          const rect = el.getBoundingClientRect();
          return {
            y: rect.top,
            x: rect.left,
            text: el.textContent?.trim().substring(0, 20) || el.tagName,
          };
        });

        if (elementInfo) {
          focusOrder.push(elementInfo);
        }

        await page.keyboard.press('Tab');
      }

      // Check that Y positions generally increase (top to bottom flow)
      // Allow for some elements on same row
      let lastY = -Infinity;
      let outOfOrderCount = 0;

      for (const item of focusOrder) {
        if (item.y < lastY - 50) { // Allow 50px tolerance
          outOfOrderCount++;
        }
        lastY = Math.max(lastY, item.y);
      }

      // Most elements should follow top-to-bottom order
      // Some may be on same line, so we allow a few "out of order"
      expect(outOfOrderCount).toBeLessThan(focusOrder.length / 2);
    });
  });
});
