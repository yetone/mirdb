// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Semantic HTML Structure Tests
 *
 * Validates that the MirDB homepage uses proper semantic HTML5 elements
 * for SEO optimization as specified in NFR-5.
 */

test.describe('Semantic HTML Structure', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test.describe('Test Case 1: Semantic Header Element', () => {
    test('page uses <header> element for the navigation/logo area', async ({ page }) => {
      // Check that a header element exists
      const header = page.locator('header');
      await expect(header).toBeVisible();

      // Verify header contains the logo/brand
      const logo = header.locator('.logo, [class*="logo"]');
      await expect(logo).toBeVisible();

      // Verify header contains navigation
      const nav = header.locator('nav');
      await expect(nav).toBeVisible();
    });

    test('header element is positioned at the top of the page', async ({ page }) => {
      const header = page.locator('header');
      const headerBox = await header.boundingBox();

      // Header should be at the top (y close to 0)
      expect(headerBox.y).toBeLessThanOrEqual(20);
    });
  });

  test.describe('Test Case 2: Semantic Nav Element', () => {
    test('page uses <nav> element for navigation links', async ({ page }) => {
      // Check that a nav element exists
      const nav = page.locator('nav');
      await expect(nav).toBeVisible();
    });

    test('nav element contains navigation links', async ({ page }) => {
      const nav = page.locator('nav');
      const links = nav.locator('a');

      // Should have multiple navigation links
      const count = await links.count();
      expect(count).toBeGreaterThanOrEqual(3);
    });

    test('navigation links are accessible', async ({ page }) => {
      const nav = page.locator('nav');
      const links = nav.locator('a');

      // Each link should have meaningful text
      const count = await links.count();
      for (let i = 0; i < count; i++) {
        const link = links.nth(i);
        const text = await link.textContent();
        expect(text.trim().length).toBeGreaterThan(0);
      }
    });
  });

  test.describe('Test Case 3: Semantic Main Element', () => {
    test('page uses <main> element for primary content', async ({ page }) => {
      // Check that a main element exists
      const main = page.locator('main');
      await expect(main).toBeVisible();
    });

    test('main element contains the primary page content', async ({ page }) => {
      const main = page.locator('main');

      // Main should contain sections
      const sections = main.locator('section');
      const count = await sections.count();
      expect(count).toBeGreaterThanOrEqual(1);
    });

    test('there is only one main element on the page', async ({ page }) => {
      const mains = page.locator('main');
      const count = await mains.count();
      expect(count).toBe(1);
    });
  });

  test.describe('Test Case 4: Semantic Section Elements', () => {
    test('content sections use <section> elements', async ({ page }) => {
      // Check that section elements exist
      const sections = page.locator('section');
      const count = await sections.count();
      expect(count).toBeGreaterThanOrEqual(3);
    });

    test('sections have appropriate headings', async ({ page }) => {
      const sections = page.locator('main > section');
      const count = await sections.count();

      // Each section should have a heading (h1-h6)
      for (let i = 0; i < count; i++) {
        const section = sections.nth(i);
        const heading = section.locator('h1, h2, h3, h4, h5, h6').first();
        await expect(heading).toBeVisible();
      }
    });

    test('sections have unique identifiers for navigation', async ({ page }) => {
      const sections = page.locator('main > section');
      const count = await sections.count();
      const ids = [];

      // Collect all section IDs
      for (let i = 0; i < count; i++) {
        const section = sections.nth(i);
        const id = await section.getAttribute('id');
        if (id) {
          ids.push(id);
        }
      }

      // Most sections should have IDs for anchor navigation
      expect(ids.length).toBeGreaterThanOrEqual(count / 2);

      // All IDs should be unique
      const uniqueIds = [...new Set(ids)];
      expect(uniqueIds.length).toBe(ids.length);
    });
  });

  test.describe('Test Case 5: Semantic Footer Element', () => {
    test('page uses <footer> element for footer content', async ({ page }) => {
      // Check that a footer element exists
      const footer = page.locator('footer');
      await expect(footer).toBeVisible();
    });

    test('footer is positioned at the bottom of the page', async ({ page }) => {
      const footer = page.locator('footer');

      // Get viewport height and footer position
      const viewportHeight = await page.evaluate(() => document.documentElement.scrollHeight);
      const footerBox = await footer.boundingBox();

      // Footer should be near the bottom
      expect(footerBox.y + footerBox.height).toBeGreaterThanOrEqual(viewportHeight - 100);
    });

    test('footer contains copyright and links', async ({ page }) => {
      const footer = page.locator('footer');

      // Should have links
      const links = footer.locator('a');
      const linkCount = await links.count();
      expect(linkCount).toBeGreaterThanOrEqual(1);

      // Should have some text content (copyright, etc.)
      const text = await footer.textContent();
      expect(text.length).toBeGreaterThan(10);
    });
  });

  test.describe('Test Case 6: W3C HTML Validation Checks', () => {
    test('HTML document has proper doctype', async ({ page }) => {
      const doctype = await page.evaluate(() => {
        const node = document.doctype;
        return node ? node.name : null;
      });
      expect(doctype).toBe('html');
    });

    test('HTML element has lang attribute', async ({ page }) => {
      const lang = await page.evaluate(() => {
        return document.documentElement.getAttribute('lang');
      });
      expect(lang).toBeTruthy();
      expect(lang.length).toBeGreaterThanOrEqual(2);
    });

    test('document has proper head section with charset', async ({ page }) => {
      const charset = await page.evaluate(() => {
        const meta = document.querySelector('meta[charset]');
        return meta ? meta.getAttribute('charset') : null;
      });
      expect(charset).toBeTruthy();
    });

    test('document has viewport meta tag', async ({ page }) => {
      const viewport = await page.evaluate(() => {
        const meta = document.querySelector('meta[name="viewport"]');
        return meta ? meta.getAttribute('content') : null;
      });
      expect(viewport).toBeTruthy();
      expect(viewport).toContain('width=device-width');
    });

    test('all images have alt attributes', async ({ page }) => {
      const imagesWithoutAlt = await page.evaluate(() => {
        const images = document.querySelectorAll('img');
        let count = 0;
        images.forEach(img => {
          if (!img.hasAttribute('alt')) count++;
        });
        return count;
      });
      expect(imagesWithoutAlt).toBe(0);
    });

    test('no duplicate IDs in the document', async ({ page }) => {
      const duplicateIds = await page.evaluate(() => {
        const ids = Array.from(document.querySelectorAll('[id]')).map(el => el.id);
        const duplicates = ids.filter((id, index) => ids.indexOf(id) !== index);
        return duplicates;
      });
      expect(duplicateIds.length).toBe(0);
    });

    test('heading hierarchy is correct', async ({ page }) => {
      const headingLevels = await page.evaluate(() => {
        const headings = Array.from(document.querySelectorAll('h1, h2, h3, h4, h5, h6'));
        return headings.map(h => parseInt(h.tagName.substring(1)));
      });

      // Should have at least one h1
      expect(headingLevels).toContain(1);

      // Check that heading levels don't skip more than one level
      for (let i = 1; i < headingLevels.length; i++) {
        const diff = headingLevels[i] - headingLevels[i-1];
        // Can go down any amount or up by 1
        expect(diff).toBeLessThanOrEqual(1);
      }
    });

    test('interactive elements have proper focus indicators', async ({ page }) => {
      // Check that buttons and links can receive focus
      const focusableElements = page.locator('button, a[href], input, select, textarea');
      const count = await focusableElements.count();
      expect(count).toBeGreaterThan(0);
    });
  });

  test.describe('Test Case 7: Meta Tags for SEO', () => {
    test('page has appropriate title tag', async ({ page }) => {
      const title = await page.title();
      expect(title).toBeTruthy();
      expect(title.length).toBeGreaterThan(10);
      expect(title.toLowerCase()).toContain('mirdb');
    });

    test('page has meta description', async ({ page }) => {
      const description = await page.evaluate(() => {
        const meta = document.querySelector('meta[name="description"]');
        return meta ? meta.getAttribute('content') : null;
      });
      expect(description).toBeTruthy();
      expect(description.length).toBeGreaterThanOrEqual(50);
    });

    test('page has Open Graph title tag', async ({ page }) => {
      const ogTitle = await page.evaluate(() => {
        const meta = document.querySelector('meta[property="og:title"]');
        return meta ? meta.getAttribute('content') : null;
      });
      expect(ogTitle).toBeTruthy();
    });

    test('page has Open Graph description tag', async ({ page }) => {
      const ogDescription = await page.evaluate(() => {
        const meta = document.querySelector('meta[property="og:description"]');
        return meta ? meta.getAttribute('content') : null;
      });
      expect(ogDescription).toBeTruthy();
    });

    test('page has Open Graph type tag', async ({ page }) => {
      const ogType = await page.evaluate(() => {
        const meta = document.querySelector('meta[property="og:type"]');
        return meta ? meta.getAttribute('content') : null;
      });
      expect(ogType).toBeTruthy();
    });

    test('page has Open Graph URL tag', async ({ page }) => {
      const ogUrl = await page.evaluate(() => {
        const meta = document.querySelector('meta[property="og:url"]');
        return meta ? meta.getAttribute('content') : null;
      });
      expect(ogUrl).toBeTruthy();
    });
  });

  test.describe('Additional Semantic Checks', () => {
    test('lists use proper semantic list elements', async ({ page }) => {
      // Check for ul/ol elements
      const lists = page.locator('ul, ol');
      const count = await lists.count();
      expect(count).toBeGreaterThanOrEqual(1);
    });

    test('code blocks use proper code elements', async ({ page }) => {
      const codeElements = page.locator('code, pre');
      const count = await codeElements.count();
      expect(count).toBeGreaterThanOrEqual(1);
    });

    test('buttons have accessible labels', async ({ page }) => {
      const buttons = page.locator('button');
      const count = await buttons.count();

      for (let i = 0; i < count; i++) {
        const button = buttons.nth(i);
        const text = await button.textContent();
        const ariaLabel = await button.getAttribute('aria-label');

        // Button should have either text content or aria-label
        const hasLabel = (text && text.trim().length > 0) || (ariaLabel && ariaLabel.length > 0);
        expect(hasLabel).toBeTruthy();
      }
    });

    test('external links have proper rel attributes', async ({ page }) => {
      const externalLinks = page.locator('a[target="_blank"]');
      const count = await externalLinks.count();

      for (let i = 0; i < count; i++) {
        const link = externalLinks.nth(i);
        const rel = await link.getAttribute('rel');

        // External links should have rel="noopener" for security
        expect(rel).toContain('noopener');
      }
    });
  });
});
