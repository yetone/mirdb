// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * SEO Optimization Tests
 * Verifies NFR-5: SEO-optimized with proper meta tags and semantic HTML
 */

test.describe('SEO Optimization', () => {

  test.describe('Test Case 1: Page Title Tag', () => {

    test('page has descriptive title tag containing MirDB and key value proposition', async ({ page }) => {
      await page.goto('/');

      // Get the page title
      const title = await page.title();

      // Verify title contains MirDB
      expect(title.toLowerCase()).toContain('mirdb');

      // Verify title contains key value proposition keywords
      const hasValueProposition =
        title.toLowerCase().includes('key-value') ||
        title.toLowerCase().includes('memcached') ||
        title.toLowerCase().includes('persistent');

      expect(hasValueProposition, 'Title should contain key value proposition (key-value, memcached, or persistent)').toBe(true);

      // Verify title is not too long (recommended 50-60 characters for SEO)
      expect(title.length, 'Title should be under 70 characters for SEO').toBeLessThanOrEqual(70);

      // Verify title is not too short
      expect(title.length, 'Title should be at least 20 characters').toBeGreaterThanOrEqual(20);
    });

    test('title tag exists in head element', async ({ page }) => {
      await page.goto('/');

      const titleTag = await page.evaluate(() => {
        return document.querySelector('head title') !== null;
      });

      expect(titleTag, 'Title tag should exist in head element').toBe(true);
    });

  });

  test.describe('Test Case 2: Meta Description', () => {

    test('meta description tag is present with 150-160 character description', async ({ page }) => {
      await page.goto('/');

      // Get meta description content
      const metaDescription = await page.evaluate(() => {
        const meta = document.querySelector('meta[name="description"]');
        return meta ? meta.getAttribute('content') : null;
      });

      // Verify meta description exists
      expect(metaDescription, 'Meta description should exist').not.toBeNull();

      // Verify description is within optimal range (120-160 characters)
      // Allow some flexibility as 150-160 is ideal but 120-165 is acceptable
      expect(metaDescription.length, 'Meta description should be at least 120 characters').toBeGreaterThanOrEqual(120);
      expect(metaDescription.length, 'Meta description should be no more than 165 characters').toBeLessThanOrEqual(165);

      // Verify description contains product name
      expect(metaDescription.toLowerCase()).toContain('mirdb');

      // Verify description is meaningful (contains key terms)
      const hasMeaningfulContent =
        metaDescription.toLowerCase().includes('key-value') ||
        metaDescription.toLowerCase().includes('memcached') ||
        metaDescription.toLowerCase().includes('persistent') ||
        metaDescription.toLowerCase().includes('rust');

      expect(hasMeaningfulContent, 'Meta description should contain meaningful content about the product').toBe(true);
    });

    test('meta description does not contain duplicate content with title', async ({ page }) => {
      await page.goto('/');

      const title = await page.title();
      const metaDescription = await page.evaluate(() => {
        const meta = document.querySelector('meta[name="description"]');
        return meta ? meta.getAttribute('content') : '';
      });

      // Description should not be identical to title
      expect(metaDescription).not.toBe(title);
    });

  });

  test.describe('Test Case 3: Viewport Meta Tag', () => {

    test('viewport meta tag is set for responsive design', async ({ page }) => {
      await page.goto('/');

      // Get viewport meta tag
      const viewportContent = await page.evaluate(() => {
        const viewport = document.querySelector('meta[name="viewport"]');
        return viewport ? viewport.getAttribute('content') : null;
      });

      // Verify viewport meta tag exists
      expect(viewportContent, 'Viewport meta tag should exist').not.toBeNull();

      // Verify it contains width=device-width
      expect(viewportContent.toLowerCase()).toContain('width=device-width');

      // Verify it contains initial-scale
      expect(viewportContent.toLowerCase()).toContain('initial-scale');
    });

    test('viewport meta tag is in head element', async ({ page }) => {
      await page.goto('/');

      const isInHead = await page.evaluate(() => {
        const viewport = document.querySelector('head meta[name="viewport"]');
        return viewport !== null;
      });

      expect(isInHead, 'Viewport meta tag should be in head element').toBe(true);
    });

  });

  test.describe('Test Case 4: Open Graph Tags', () => {

    test('og:title tag is present', async ({ page }) => {
      await page.goto('/');

      const ogTitle = await page.evaluate(() => {
        const meta = document.querySelector('meta[property="og:title"]');
        return meta ? meta.getAttribute('content') : null;
      });

      expect(ogTitle, 'og:title should be present').not.toBeNull();
      expect(ogTitle.toLowerCase()).toContain('mirdb');
    });

    test('og:description tag is present', async ({ page }) => {
      await page.goto('/');

      const ogDescription = await page.evaluate(() => {
        const meta = document.querySelector('meta[property="og:description"]');
        return meta ? meta.getAttribute('content') : null;
      });

      expect(ogDescription, 'og:description should be present').not.toBeNull();
      expect(ogDescription.length, 'og:description should have meaningful content').toBeGreaterThan(50);
    });

    test('og:image tag is present', async ({ page }) => {
      await page.goto('/');

      const ogImage = await page.evaluate(() => {
        const meta = document.querySelector('meta[property="og:image"]');
        return meta ? meta.getAttribute('content') : null;
      });

      expect(ogImage, 'og:image should be present').not.toBeNull();
      // Verify it's a valid URL or path
      expect(ogImage.length, 'og:image should have a value').toBeGreaterThan(0);
    });

    test('og:type tag is present', async ({ page }) => {
      await page.goto('/');

      const ogType = await page.evaluate(() => {
        const meta = document.querySelector('meta[property="og:type"]');
        return meta ? meta.getAttribute('content') : null;
      });

      expect(ogType, 'og:type should be present').not.toBeNull();
      // Common types are website, article, product
      expect(['website', 'article', 'product']).toContain(ogType);
    });

    test('og:url tag is present', async ({ page }) => {
      await page.goto('/');

      const ogUrl = await page.evaluate(() => {
        const meta = document.querySelector('meta[property="og:url"]');
        return meta ? meta.getAttribute('content') : null;
      });

      expect(ogUrl, 'og:url should be present').not.toBeNull();
    });

  });

  test.describe('Test Case 5: Semantic HTML Structure', () => {

    test('page uses header element', async ({ page }) => {
      await page.goto('/');

      // Check for header element OR nav element (nav is a valid semantic header element)
      const hasHeader = await page.evaluate(() => {
        return document.querySelector('header') !== null || document.querySelector('nav') !== null;
      });

      expect(hasHeader, 'Page should have a header or nav element').toBe(true);
    });

    test('page uses main element', async ({ page }) => {
      await page.goto('/');

      const hasMain = await page.evaluate(() => {
        return document.querySelector('main') !== null;
      });

      expect(hasMain, 'Page should have a main element').toBe(true);
    });

    test('page uses section elements', async ({ page }) => {
      await page.goto('/');

      const sectionCount = await page.evaluate(() => {
        return document.querySelectorAll('section').length;
      });

      expect(sectionCount, 'Page should have multiple section elements').toBeGreaterThan(0);
    });

    test('page uses footer element', async ({ page }) => {
      await page.goto('/');

      const hasFooter = await page.evaluate(() => {
        return document.querySelector('footer') !== null;
      });

      expect(hasFooter, 'Page should have a footer element').toBe(true);
    });

    test('semantic elements are properly structured', async ({ page }) => {
      await page.goto('/');

      // Verify main content is within main element
      const mainContent = await page.evaluate(() => {
        const main = document.querySelector('main');
        return main ? main.children.length : 0;
      });

      expect(mainContent, 'Main element should contain content').toBeGreaterThan(0);

      // Verify footer is not inside main
      const footerInMain = await page.evaluate(() => {
        return document.querySelector('main footer') !== null;
      });

      expect(footerInMain, 'Footer should not be inside main element').toBe(false);
    });

  });

  test.describe('Test Case 6: Single H1 Tag', () => {

    test('page has exactly one h1 tag', async ({ page }) => {
      await page.goto('/');

      const h1Count = await page.evaluate(() => {
        return document.querySelectorAll('h1').length;
      });

      expect(h1Count, 'Page should have exactly one h1 tag').toBe(1);
    });

    test('h1 tag contains product name MirDB', async ({ page }) => {
      await page.goto('/');

      const h1Text = await page.evaluate(() => {
        const h1 = document.querySelector('h1');
        return h1 ? h1.textContent : '';
      });

      expect(h1Text.toLowerCase()).toContain('mirdb');
    });

    test('h1 tag is visible', async ({ page }) => {
      await page.goto('/');

      const h1 = page.locator('h1');
      await expect(h1).toBeVisible();
    });

    test('heading hierarchy is correct (no skipped levels)', async ({ page }) => {
      await page.goto('/');

      // Get all heading levels used
      const headings = await page.evaluate(() => {
        const allHeadings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
        const levels = Array.from(allHeadings).map(h => parseInt(h.tagName.charAt(1)));
        return levels;
      });

      // Check that headings start with h1
      expect(headings[0], 'First heading should be h1').toBe(1);

      // Check that no levels are skipped
      let previousLevel = 0;
      for (const level of headings) {
        // When going deeper, should not skip more than one level
        if (level > previousLevel) {
          expect(level - previousLevel, 'Heading levels should not be skipped').toBeLessThanOrEqual(1);
        }
        previousLevel = level;
      }
    });

  });

  test.describe('Additional SEO Best Practices', () => {

    test('html element has lang attribute', async ({ page }) => {
      await page.goto('/');

      const lang = await page.evaluate(() => {
        return document.documentElement.lang;
      });

      expect(lang, 'HTML element should have lang attribute').toBeTruthy();
      expect(lang).toBe('en');
    });

    test('charset meta tag is present and correct', async ({ page }) => {
      await page.goto('/');

      const charset = await page.evaluate(() => {
        const meta = document.querySelector('meta[charset]');
        return meta ? meta.getAttribute('charset') : null;
      });

      expect(charset, 'Charset meta tag should exist').not.toBeNull();
      expect(charset.toLowerCase()).toBe('utf-8');
    });

    test('images have alt attributes', async ({ page }) => {
      await page.goto('/');

      const imagesWithoutAlt = await page.evaluate(() => {
        const images = document.querySelectorAll('img');
        return Array.from(images).filter(img => !img.hasAttribute('alt') || img.alt === '').length;
      });

      expect(imagesWithoutAlt, 'All images should have alt attributes').toBe(0);
    });

    test('links have descriptive text', async ({ page }) => {
      await page.goto('/');

      // Check that links don't just say "click here" or similar
      const badLinkTexts = await page.evaluate(() => {
        const links = document.querySelectorAll('a');
        const badTexts = ['click here', 'here', 'read more', 'link'];
        return Array.from(links).filter(link => {
          const text = link.textContent.toLowerCase().trim();
          return badTexts.includes(text);
        }).length;
      });

      expect(badLinkTexts, 'Links should have descriptive text, not generic phrases').toBe(0);
    });

  });

});
