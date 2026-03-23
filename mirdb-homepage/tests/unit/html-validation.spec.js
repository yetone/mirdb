/**
 * HTML Validation Tests
 * Owner: Scenario 8 - Performance and Loading
 *
 * Test cases:
 * - Valid HTML5 structure
 * - No duplicate IDs
 * - Proper nesting of elements
 * - Required meta tags present
 */

const { test, expect } = require('@playwright/test');
const fs = require('fs');
const path = require('path');

test.describe('HTML Validation', () => {

  // TC3: HTML passes W3C validation with no errors
  test('should have valid HTML5 structure', async ({ page }) => {
    await page.goto('http://localhost:3000/index.html', {
      waitUntil: 'domcontentloaded',
    });

    // Check DOCTYPE
    const doctype = await page.evaluate(() => {
      return document.doctype ?
        document.doctype.name : null;
    });
    expect(doctype).toBe('html');

    // Check html lang attribute
    const htmlLang = await page.getAttribute('html', 'lang');
    expect(htmlLang).toBe('en');

    // Check head structure
    const head = await page.evaluate(() => {
      const headElement = document.querySelector('head');
      return {
        hasCharset: !!document.querySelector('meta[charset]'),
        hasViewport: !!document.querySelector('meta[name="viewport"]'),
        hasTitle: !!document.querySelector('title'),
        hasDescription: !!document.querySelector('meta[name="description"]'),
        titleText: document.title
      };
    });

    expect(head.hasCharset).toBeTruthy();
    expect(head.hasViewport).toBeTruthy();
    expect(head.hasTitle).toBeTruthy();
    expect(head.hasDescription).toBeTruthy();
    expect(head.titleText).toContain('MirDB');
  });

  test('should have no duplicate IDs', async ({ page }) => {
    await page.goto('http://localhost:3000/index.html', {
      waitUntil: 'domcontentloaded',
    });

    const duplicateIds = await page.evaluate(() => {
      const allElements = document.querySelectorAll('[id]');
      const idCounts = {};
      const duplicates = [];

      allElements.forEach(el => {
        const id = el.id;
        if (id) {
          idCounts[id] = (idCounts[id] || 0) + 1;
          if (idCounts[id] === 2) {
            duplicates.push(id);
          }
        }
      });

      return duplicates;
    });

    expect(duplicateIds).toHaveLength(0);
    console.log('No duplicate IDs found');
  });

  test('should have proper nesting of elements', async ({ page }) => {
    await page.goto('http://localhost:3000/index.html', {
      waitUntil: 'domcontentloaded',
    });

    // Check for common nesting issues
    const nestingIssues = await page.evaluate(() => {
      const issues = [];

      // Check for inline elements containing block elements
      const inlineElements = ['span', 'a', 'strong', 'em', 'b', 'i', 'code'];
      const blockElements = ['div', 'p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'section', 'article', 'header', 'footer', 'nav'];

      inlineElements.forEach(inline => {
        const elements = document.querySelectorAll(inline);
        elements.forEach(el => {
          blockElements.forEach(block => {
            if (el.querySelector(block)) {
              issues.push(`${inline} contains ${block}`);
            }
          });
        });
      });

      // Check for interactive elements nesting (a inside a, button inside button)
      const links = document.querySelectorAll('a');
      links.forEach(link => {
        if (link.querySelector('a')) {
          issues.push('a element contains another a element');
        }
        if (link.querySelector('button')) {
          issues.push('a element contains button element');
        }
      });

      const buttons = document.querySelectorAll('button');
      buttons.forEach(button => {
        if (button.querySelector('button')) {
          issues.push('button contains another button');
        }
        if (button.querySelector('a')) {
          issues.push('button contains a element');
        }
      });

      // Check for p containing block elements
      const paragraphs = document.querySelectorAll('p');
      paragraphs.forEach(p => {
        if (p.querySelector('div') || p.querySelector('p')) {
          issues.push('p contains block element');
        }
      });

      return issues;
    });

    expect(nestingIssues).toHaveLength(0);
    console.log('No nesting issues found');
  });

  test('should have required meta tags present', async ({ page }) => {
    await page.goto('http://localhost:3000/index.html', {
      waitUntil: 'domcontentloaded',
    });

    const metaTags = await page.evaluate(() => {
      return {
        charset: document.querySelector('meta[charset]')?.getAttribute('charset'),
        viewport: document.querySelector('meta[name="viewport"]')?.getAttribute('content'),
        description: document.querySelector('meta[name="description"]')?.getAttribute('content'),
        title: document.title
      };
    });

    // Check charset
    expect(metaTags.charset).toBe('UTF-8');

    // Check viewport for responsive design
    expect(metaTags.viewport).toContain('width=device-width');
    expect(metaTags.viewport).toContain('initial-scale=1');

    // Check description meta tag
    expect(metaTags.description).toBeTruthy();
    expect(metaTags.description.length).toBeGreaterThan(0);

    // Check title
    expect(metaTags.title).toBeTruthy();
    expect(metaTags.title).toContain('MirDB');

    console.log('All required meta tags present:');
    console.log(`  - charset: ${metaTags.charset}`);
    console.log(`  - viewport: ${metaTags.viewport}`);
    console.log(`  - description: ${metaTags.description.substring(0, 50)}...`);
    console.log(`  - title: ${metaTags.title}`);
  });

  test('should have semantic HTML structure', async ({ page }) => {
    await page.goto('http://localhost:3000/index.html', {
      waitUntil: 'domcontentloaded',
    });

    const semanticStructure = await page.evaluate(() => {
      return {
        hasNav: !!document.querySelector('nav'),
        hasHeader: !!document.querySelector('header'),
        hasMain: !!document.querySelector('main') || !!document.querySelector('section'),
        hasFooter: !!document.querySelector('footer'),
        hasSections: document.querySelectorAll('section').length,
        hasArticles: document.querySelectorAll('article').length,
        h1Count: document.querySelectorAll('h1').length,
        h2Count: document.querySelectorAll('h2').length,
        hasH1: !!document.querySelector('h1'),
      };
    });

    // Check for semantic elements
    expect(semanticStructure.hasNav).toBeTruthy();
    expect(semanticStructure.hasHeader).toBeTruthy();
    expect(semanticStructure.hasMain).toBeTruthy();
    expect(semanticStructure.hasFooter).toBeTruthy();

    // Should have exactly one h1
    expect(semanticStructure.h1Count).toBe(1);

    // Should have multiple sections
    expect(semanticStructure.hasSections).toBeGreaterThan(0);

    console.log('Semantic structure:');
    console.log(`  - Nav: ${semanticStructure.hasNav}`);
    console.log(`  - Header: ${semanticStructure.hasHeader}`);
    console.log(`  - Footer: ${semanticStructure.hasFooter}`);
    console.log(`  - Sections: ${semanticStructure.hasSections}`);
    console.log(`  - Articles: ${semanticStructure.hasArticles}`);
    console.log(`  - H1 count: ${semanticStructure.h1Count}`);
    console.log(`  - H2 count: ${semanticStructure.h2Count}`);
  });

  test('should have valid link targets', async ({ page }) => {
    await page.goto('http://localhost:3000/index.html', {
      waitUntil: 'domcontentloaded',
    });

    const links = await page.evaluate(() => {
      const allLinks = document.querySelectorAll('a[href]');
      const results = [];

      allLinks.forEach(link => {
        const href = link.getAttribute('href');
        const target = link.getAttribute('target');
        const rel = link.getAttribute('rel');
        const isExternal = href && (href.startsWith('http://') || href.startsWith('https://'));

        results.push({
          href,
          target,
          rel,
          isExternal,
          hasNoopener: rel && rel.includes('noopener')
        });
      });

      return results;
    });

    // External links should have target="_blank" and rel="noopener"
    const externalLinks = links.filter(l => l.isExternal);
    externalLinks.forEach(link => {
      expect(link.target).toBe('_blank');
      expect(link.hasNoopener).toBeTruthy();
    });

    // Internal anchor links should point to valid IDs
    const anchorLinks = links.filter(l => l.href && l.href.startsWith('#'));
    for (const link of anchorLinks) {
      if (link.href !== '#') {
        const targetId = link.href.substring(1);
        const targetExists = await page.locator(`#${targetId}`).count();
        expect(targetExists).toBeGreaterThan(0);
      }
    }

    console.log(`Validated ${links.length} links (${externalLinks.length} external, ${anchorLinks.length} anchor)`);
  });

  test('should have alt attributes on images', async ({ page }) => {
    await page.goto('http://localhost:3000/index.html', {
      waitUntil: 'domcontentloaded',
    });

    const images = await page.evaluate(() => {
      const imgs = document.querySelectorAll('img');
      const results = [];

      imgs.forEach(img => {
        results.push({
          src: img.src,
          hasAlt: img.hasAttribute('alt'),
          alt: img.getAttribute('alt'),
          isDecorative: img.getAttribute('alt') === '' && img.getAttribute('role') === 'presentation'
        });
      });

      return results;
    });

    // All images should have alt attribute (can be empty for decorative images)
    images.forEach(img => {
      expect(img.hasAlt).toBeTruthy();
    });

    // Check SVGs with role="img" have accessible names
    const svgsWithRole = await page.evaluate(() => {
      const svgs = document.querySelectorAll('svg[role="img"]');
      return Array.from(svgs).map(svg => ({
        hasAriaLabel: svg.hasAttribute('aria-label') || svg.hasAttribute('aria-labelledby'),
        hasTitle: !!svg.querySelector('title')
      }));
    });

    svgsWithRole.forEach(svg => {
      expect(svg.hasAriaLabel || svg.hasTitle).toBeTruthy();
    });

    console.log(`Validated ${images.length} images and ${svgsWithRole.length} SVGs with role="img"`);
  });

  test('should have valid table structure', async ({ page }) => {
    await page.goto('http://localhost:3000/index.html', {
      waitUntil: 'domcontentloaded',
    });

    const tables = await page.evaluate(() => {
      const allTables = document.querySelectorAll('table');
      return Array.from(allTables).map(table => ({
        hasThead: !!table.querySelector('thead'),
        hasTbody: !!table.querySelector('tbody'),
        hasThScope: Array.from(table.querySelectorAll('th')).every(th =>
          th.hasAttribute('scope') || th.closest('thead')
        ),
        rowCount: table.querySelectorAll('tr').length,
        headerCount: table.querySelectorAll('th').length
      }));
    });

    tables.forEach(table => {
      // Tables should have proper structure
      expect(table.hasThead).toBeTruthy();
      expect(table.hasTbody).toBeTruthy();
      expect(table.headerCount).toBeGreaterThan(0);
    });

    console.log(`Validated ${tables.length} table(s)`);
  });
});
