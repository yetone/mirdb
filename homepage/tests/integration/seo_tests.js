/**
 * SEO & Meta Tags Integration Tests
 * Owner: Scenario 12 - SEO & Meta Tags
 *
 * Tests verify:
 * - Title tag contains 'MirDB'
 * - Meta description exists and is under 160 characters
 * - Charset is UTF-8 and viewport meta is present
 * - Open Graph tags (og:title, og:description, og:type, og:url, og:image)
 * - Twitter Card tags (twitter:card, twitter:title, twitter:description, twitter:image)
 * - Canonical link tag
 * - Robots meta tag
 * - JSON-LD structured data with schema.org SoftwareApplication
 * - Semantic HTML (main, article, section, header, nav, footer)
 */

const { test, describe } = require('node:test');
const assert = require('node:assert');
const fs = require('node:fs');
const path = require('node:path');
const { JSDOM } = require('jsdom');

const HOMEPAGE_DIR = path.join(__dirname, '../..');
const PARTIALS_DIR = path.join(HOMEPAGE_DIR, 'templates/partials');

/**
 * Preprocess Zola template syntax into valid HTML for parsing.
 */
function preprocessTemplate(html) {
  return html
    .replace(/\{\{\s*get_url\(path=['"]([^'"]+)['"]\)\s*\}\}/g, (match, p1) => `/${p1}`)
    .replace(/\{%\s*include\s+["']([^"']+)["']\s*%\}/g, '')
    .replace(/\{%\s*block\s+\w+\s*%\}/g, '')
    .replace(/\{%\s*endblock\s*%\}/g, '');
}

/**
 * Load and parse an HTML template file.
 */
function loadTemplate(filename) {
  const filePath = path.join(PARTIALS_DIR, filename);
  const raw = fs.readFileSync(filePath, 'utf-8');
  const processed = preprocessTemplate(raw);
  return new JSDOM(processed);
}

/**
 * Build a complete HTML document from templates for end-to-end testing.
 */
function buildCompleteHtml() {
  const basePath = path.join(HOMEPAGE_DIR, 'templates/base.html');
  const indexPath = path.join(HOMEPAGE_DIR, 'templates/index.html');
  let html = fs.readFileSync(basePath, 'utf-8');
  const indexHtml = fs.readFileSync(indexPath, 'utf-8');

  // Extract content blocks from index.html and inject them into base.html
  const blockRegex = /\{%\s*block\s+(\w+)\s*%\}([\s\S]*?)\{%\s*endblock(?:\s+\w+)?\s*%\}/g;
  let blockMatch;
  while ((blockMatch = blockRegex.exec(indexHtml)) !== null) {
    const blockName = blockMatch[1];
    const blockContent = blockMatch[2];
    const baseBlockPattern = new RegExp(`\\{%\\s*block\\s+${blockName}\\s*%\\}([\\s\\S]*?)\\{%\\s*endblock(?:\\s+\\w+)?\\s*%\\}`);
    html = html.replace(baseBlockPattern, blockContent);
  }

  // Replace remaining empty blocks
  html = html.replace(/\{%\s*block\s+\w+\s*%\}\{%\s*endblock(?:\s+\w+)?\s*%\}/g, '');

  // Replace includes (iteratively until no more)
  const includeRegex = /\{%\s*include\s+["']([^"']+)["']\s*%\}/g;
  let safety = 0;
  while (safety++ < 20) {
    const matches = [...html.matchAll(includeRegex)];
    if (matches.length === 0) break;
    for (const m of matches) {
      const includePath = m[1];
      const partialFile = path.basename(includePath).replace('.html', '') + '.html';
      const partialPath = path.join(PARTIALS_DIR, partialFile);
      let partialContent = '';
      if (fs.existsSync(partialPath)) {
        partialContent = fs.readFileSync(partialPath, 'utf-8');
      }
      html = html.replace(m[0], partialContent);
    }
  }

  // Replace get_url
  html = html.replace(/\{\{\s*get_url\(path=['"]([^'"]+)['"]\)\s*\}\}/g, (m, p1) => `/${p1}`);

  return new JSDOM(preprocessTemplate(html));
}

describe('SEO & Meta Tags - Test Case 1: HTML Head Element', () => {
  const dom = loadTemplate('head.html');
  const document = dom.window.document;

  test('title tag contains MirDB', () => {
    const title = document.querySelector('title');
    assert.ok(title, 'Title tag should exist');
    assert.ok(title.textContent.includes('MirDB'), 'Title should contain "MirDB"');
  });

  test('meta charset is UTF-8', () => {
    const charsetMeta = document.querySelector('meta[charset="UTF-8"], meta[charset="utf-8"]');
    assert.ok(charsetMeta, 'Charset meta tag should be UTF-8');
  });

  test('meta viewport is present', () => {
    const viewportMeta = document.querySelector('meta[name="viewport"]');
    assert.ok(viewportMeta, 'Viewport meta tag should exist');
    const content = viewportMeta.getAttribute('content');
    assert.ok(content, 'Viewport meta should have content');
    assert.ok(content.includes('width=device-width'), 'Viewport should include width=device-width');
  });

  test('meta description exists and is under 160 characters', () => {
    const descMeta = document.querySelector('meta[name="description"]');
    assert.ok(descMeta, 'Meta description should exist');
    const content = descMeta.getAttribute('content');
    assert.ok(content, 'Meta description should have content');
    assert.ok(content.length > 0, 'Meta description should not be empty');
    assert.ok(content.length <= 160, `Meta description should be under 160 chars, got ${content.length}`);
  });
});

describe('SEO & Meta Tags - Test Case 2: Open Graph Tags', () => {
  const dom = loadTemplate('head.html');
  const document = dom.window.document;

  test('og:title is present', () => {
    const tag = document.querySelector('meta[property="og:title"]');
    assert.ok(tag, 'og:title should exist');
    assert.ok(tag.getAttribute('content').includes('MirDB'), 'og:title should contain MirDB');
  });

  test('og:description is present', () => {
    const tag = document.querySelector('meta[property="og:description"]');
    assert.ok(tag, 'og:description should exist');
    assert.ok(tag.getAttribute('content').length > 0, 'og:description should have content');
  });

  test('og:type is website', () => {
    const tag = document.querySelector('meta[property="og:type"]');
    assert.ok(tag, 'og:type should exist');
    assert.strictEqual(tag.getAttribute('content'), 'website', 'og:type should be "website"');
  });

  test('og:url is present', () => {
    const tag = document.querySelector('meta[property="og:url"]');
    assert.ok(tag, 'og:url should exist');
    const content = tag.getAttribute('content');
    assert.ok(content, 'og:url should have content');
    assert.ok(content.startsWith('http'), 'og:url should be a valid URL');
  });

  test('og:image is present', () => {
    const tag = document.querySelector('meta[property="og:image"]');
    assert.ok(tag, 'og:image should exist');
    const content = tag.getAttribute('content');
    assert.ok(content, 'og:image should have content');
    assert.ok(content.startsWith('http'), 'og:image should be a valid URL');
  });

  test('og:site_name is present', () => {
    const tag = document.querySelector('meta[property="og:site_name"]');
    assert.ok(tag, 'og:site_name should exist');
  });

  test('og:locale is present', () => {
    const tag = document.querySelector('meta[property="og:locale"]');
    assert.ok(tag, 'og:locale should exist');
  });
});

describe('SEO & Meta Tags - Test Case 3: Twitter Card Tags', () => {
  const dom = loadTemplate('head.html');
  const document = dom.window.document;

  test('twitter:card is summary_large_image', () => {
    const tag = document.querySelector('meta[name="twitter:card"]');
    assert.ok(tag, 'twitter:card should exist');
    assert.strictEqual(tag.getAttribute('content'), 'summary_large_image', 'twitter:card should be "summary_large_image"');
  });

  test('twitter:title is present', () => {
    const tag = document.querySelector('meta[name="twitter:title"]');
    assert.ok(tag, 'twitter:title should exist');
    assert.ok(tag.getAttribute('content').includes('MirDB'), 'twitter:title should contain MirDB');
  });

  test('twitter:description is present', () => {
    const tag = document.querySelector('meta[name="twitter:description"]');
    assert.ok(tag, 'twitter:description should exist');
    assert.ok(tag.getAttribute('content').length > 0, 'twitter:description should have content');
  });

  test('twitter:image is present', () => {
    const tag = document.querySelector('meta[name="twitter:image"]');
    assert.ok(tag, 'twitter:image should exist');
    const content = tag.getAttribute('content');
    assert.ok(content, 'twitter:image should have content');
    assert.ok(content.startsWith('http'), 'twitter:image should be a valid URL');
  });
});

describe('SEO & Meta Tags - Test Case 4: Structured Data (JSON-LD)', () => {
  const dom = loadTemplate('head.html');
  const document = dom.window.document;

  test('JSON-LD script tag exists', () => {
    const script = document.querySelector('script[type="application/ld+json"]');
    assert.ok(script, 'JSON-LD script tag should exist');
  });

  test('JSON-LD contains schema.org SoftwareApplication type', () => {
    const script = document.querySelector('script[type="application/ld+json"]');
    const data = JSON.parse(script.textContent);
    assert.ok(data['@type'] === 'SoftwareApplication' || data['@type'] === 'WebSite',
      'JSON-LD should contain SoftwareApplication or WebSite type');
  });

  test('JSON-LD contains name "MirDB"', () => {
    const script = document.querySelector('script[type="application/ld+json"]');
    const data = JSON.parse(script.textContent);
    assert.strictEqual(data.name, 'MirDB', 'JSON-LD should have name "MirDB"');
  });

  test('JSON-LD contains description', () => {
    const script = document.querySelector('script[type="application/ld+json"]');
    const data = JSON.parse(script.textContent);
    assert.ok(data.description, 'JSON-LD should have description');
    assert.ok(data.description.length > 0, 'JSON-LD description should not be empty');
  });

  test('JSON-LD contains url', () => {
    const script = document.querySelector('script[type="application/ld+json"]');
    const data = JSON.parse(script.textContent);
    assert.ok(data.url, 'JSON-LD should have url');
    assert.ok(data.url.startsWith('http'), 'JSON-LD url should be a valid URL');
  });
});

describe('SEO & Meta Tags - Test Case 5: Canonical and Hreflang Tags', () => {
  const dom = loadTemplate('head.html');
  const document = dom.window.document;

  test('canonical link tag points to root URL', () => {
    const canonical = document.querySelector('link[rel="canonical"]');
    assert.ok(canonical, 'Canonical link tag should exist');
    const href = canonical.getAttribute('href');
    assert.ok(href, 'Canonical link should have href');
    assert.ok(href.endsWith('/'), 'Canonical should point to root URL');
  });

  test('no conflicting canonicals exist', () => {
    const canonicals = document.querySelectorAll('link[rel="canonical"]');
    assert.strictEqual(canonicals.length, 1, 'Should have exactly one canonical link');
  });
});

describe('SEO & Meta Tags - Test Case 6: Robots Meta Tag', () => {
  const dom = loadTemplate('head.html');
  const document = dom.window.document;

  test('no robots=noindex directive', () => {
    const robotsMeta = document.querySelector('meta[name="robots"]');
    if (robotsMeta) {
      const content = robotsMeta.getAttribute('content') || '';
      assert.ok(!content.includes('noindex'), 'Robots meta should not contain noindex');
    }
  });

  test('robots meta includes index, follow', () => {
    const robotsMeta = document.querySelector('meta[name="robots"]');
    assert.ok(robotsMeta, 'Robots meta tag should exist');
    const content = robotsMeta.getAttribute('content');
    assert.ok(content.includes('index'), 'Robots meta should include "index"');
    assert.ok(content.includes('follow'), 'Robots meta should include "follow"');
  });
});

describe('SEO & Meta Tags - Test Case 7: Structured Data Validation', () => {
  const dom = loadTemplate('head.html');
  const document = dom.window.document;

  test('JSON-LD parses as valid JSON', () => {
    const script = document.querySelector('script[type="application/ld+json"]');
    assert.ok(script, 'JSON-LD script should exist');
    let data;
    assert.doesNotThrow(() => {
      data = JSON.parse(script.textContent);
    }, 'JSON-LD should parse as valid JSON');
    assert.ok(data, 'Parsed JSON-LD should not be null');
  });

  test('JSON-LD contains valid schema.org @context', () => {
    const script = document.querySelector('script[type="application/ld+json"]');
    const data = JSON.parse(script.textContent);
    assert.ok(data['@context'], 'JSON-LD should have @context');
    assert.ok(data['@context'].includes('schema.org'), '@context should reference schema.org');
  });

  test('JSON-LD has required properties for SoftwareApplication', () => {
    const script = document.querySelector('script[type="application/ld+json"]');
    const data = JSON.parse(script.textContent);
    assert.ok(data['@type'], 'Should have @type');
    assert.ok(data.name, 'Should have name');
    assert.ok(data.description, 'Should have description');
    assert.ok(data.url, 'Should have url');
    assert.ok(data.applicationCategory, 'Should have applicationCategory');
    assert.ok(data.author, 'Should have author');
    assert.ok(data.offers, 'Should have offers');
  });

  test('author contains Organization type with name', () => {
    const script = document.querySelector('script[type="application/ld+json"]');
    const data = JSON.parse(script.textContent);
    assert.ok(data.author, 'Should have author');
    assert.strictEqual(data.author['@type'], 'Organization', 'Author should be Organization');
    assert.ok(data.author.name, 'Author should have name');
  });

  test('offers contains free price offer', () => {
    const script = document.querySelector('script[type="application/ld+json"]');
    const data = JSON.parse(script.textContent);
    assert.ok(data.offers, 'Should have offers');
    assert.strictEqual(data.offers['@type'], 'Offer', 'Offers should be Offer type');
    assert.strictEqual(data.offers.price, '0', 'Price should be 0 (free)');
    assert.strictEqual(data.offers.priceCurrency, 'USD', 'Currency should be USD');
  });
});

describe('SEO & Meta Tags - Semantic HTML Verification', () => {
  const dom = buildCompleteHtml();
  const document = dom.window.document;

  test('document has main element', () => {
    const main = document.querySelector('main');
    assert.ok(main, 'Document should have a main element');
  });

  test('document has article or div.feature-card elements', () => {
    const articles = document.querySelectorAll('article');
    const featureCards = document.querySelectorAll('.feature-card');
    assert.ok(articles.length >= 1 || featureCards.length >= 1,
      'Document should have article elements or semantic alternatives (feature cards)');
  });

  test('document has section elements', () => {
    const sections = document.querySelectorAll('section');
    assert.ok(sections.length >= 1, 'Document should have at least one section element');
  });

  test('document has header element', () => {
    const header = document.querySelector('header');
    assert.ok(header, 'Document should have a header element');
  });

  test('document has nav element', () => {
    const nav = document.querySelector('nav');
    assert.ok(nav, 'Document should have a nav element');
  });

  test('document has footer element', () => {
    const footer = document.querySelector('footer');
    assert.ok(footer, 'Document should have a footer element');
  });

  test('html element has lang attribute', () => {
    const html = document.querySelector('html');
    assert.ok(html, 'Document should have html element');
    assert.ok(html.getAttribute('lang'), 'Html element should have lang attribute');
  });

  test('skip navigation link exists', () => {
    const skipLink = document.querySelector('a[href="#main-content"]');
    assert.ok(skipLink, 'Document should have a skip navigation link');
  });

  test('main content has id for skip link target', () => {
    const main = document.querySelector('main#main-content');
    assert.ok(main, 'Main element should have id="main-content"');
  });
});

describe('SEO & Meta Tags - Complete Head Tag Inventory', () => {
  const dom = loadTemplate('head.html');
  const document = dom.window.document;

  test('favicon links are present', () => {
    const icon = document.querySelector('link[rel="icon"]');
    assert.ok(icon, 'Favicon link should exist');
    const appleIcon = document.querySelector('link[rel="apple-touch-icon"]');
    assert.ok(appleIcon, 'Apple touch icon should exist');
  });

  test('preconnect hints are present', () => {
    const preconnects = document.querySelectorAll('link[rel="preconnect"]');
    assert.ok(preconnects.length >= 1, 'Should have at least one preconnect hint');
  });

  test('keywords meta tag is present', () => {
    const keywords = document.querySelector('meta[name="keywords"]');
    assert.ok(keywords, 'Keywords meta tag should exist');
    const content = keywords.getAttribute('content');
    assert.ok(content.includes('MirDB'), 'Keywords should include MirDB');
  });

  test('author meta tag is present', () => {
    const author = document.querySelector('meta[name="author"]');
    assert.ok(author, 'Author meta tag should exist');
  });
});
