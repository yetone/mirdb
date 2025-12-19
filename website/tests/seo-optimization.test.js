/**
 * SEO Optimization Tests - NFR-4 Compliance
 * Using JSDOM for testing HTML SEO meta tags and structured data
 */

const fs = require('fs');
const path = require('path');
const { JSDOM } = require('jsdom');

// Load the HTML file
const htmlPath = path.resolve(__dirname, '../dist/index.html');
const html = fs.readFileSync(htmlPath, 'utf8');
const dom = new JSDOM(html);
const document = dom.window.document;

// Test results tracking
let passCount = 0;
let failCount = 0;
const results = [];

function test(name, fn) {
  try {
    fn();
    passCount++;
    results.push({ name, status: 'pass' });
    console.log(`✓ ${name}`);
  } catch (error) {
    failCount++;
    results.push({ name, status: 'fail', error: error.message });
    console.log(`✗ ${name}`);
    console.log(`  Error: ${error.message}`);
  }
}

function expect(actual) {
  return {
    toBe(expected) {
      if (actual !== expected) {
        throw new Error(`Expected ${expected} but got ${actual}`);
      }
    },
    toBeTruthy() {
      if (!actual) {
        throw new Error(`Expected truthy value but got ${actual}`);
      }
    },
    toBeFalsy() {
      if (actual) {
        throw new Error(`Expected falsy value but got ${actual}`);
      }
    },
    toContain(substring) {
      if (!actual || !actual.includes(substring)) {
        throw new Error(`Expected "${actual}" to contain "${substring}"`);
      }
    },
    toMatch(regex) {
      if (!actual || !regex.test(actual)) {
        throw new Error(`Expected "${actual}" to match ${regex}`);
      }
    },
    toBeLessThanOrEqual(expected) {
      if (actual > expected) {
        throw new Error(`Expected ${actual} to be less than or equal to ${expected}`);
      }
    },
    toBeGreaterThan(expected) {
      if (actual <= expected) {
        throw new Error(`Expected ${actual} to be greater than ${expected}`);
      }
    },
    not: {
      toContain(substring) {
        if (actual && actual.includes(substring)) {
          throw new Error(`Expected "${actual}" not to contain "${substring}"`);
        }
      }
    }
  };
}

console.log('\n=== SEO Optimization Tests - NFR-4 Compliance ===\n');

// Test Case 1: Page Title Tag
console.log('Test Case 1: Page Title Tag');
console.log('----------------------------');

test('Title tag should contain "MirDB"', () => {
  const title = document.querySelector('title');
  expect(title).toBeTruthy();
  expect(title.textContent).toContain('MirDB');
});

test('Title tag should be under 60 characters', () => {
  const title = document.querySelector('title');
  expect(title).toBeTruthy();
  expect(title.textContent.length).toBeLessThanOrEqual(60);
});

test('Title should be descriptive and relevant', () => {
  const title = document.querySelector('title');
  expect(title).toBeTruthy();
  expect(title.textContent.length).toBeGreaterThan(10);
  // Title should contain key product terms
  const titleLower = title.textContent.toLowerCase();
  expect(titleLower).toMatch(/mirdb|key-value|memcached/);
});

// Test Case 2: Meta Description
console.log('\nTest Case 2: Meta Description');
console.log('-----------------------------');

test('Meta description should be present', () => {
  const description = document.querySelector('meta[name="description"]');
  expect(description).toBeTruthy();
  expect(description.getAttribute('content')).toBeTruthy();
});

test('Meta description should be under 160 characters', () => {
  const description = document.querySelector('meta[name="description"]');
  expect(description).toBeTruthy();
  const content = description.getAttribute('content');
  expect(content.length).toBeLessThanOrEqual(160);
});

test('Meta description should contain key terms', () => {
  const description = document.querySelector('meta[name="description"]');
  expect(description).toBeTruthy();
  const content = description.getAttribute('content').toLowerCase();
  expect(content).toMatch(/mirdb|key-value|memcached|persistent/i);
});

// Test Case 3: Open Graph Tags
console.log('\nTest Case 3: Open Graph Tags');
console.log('----------------------------');

test('og:title tag should be present', () => {
  const ogTitle = document.querySelector('meta[property="og:title"]');
  expect(ogTitle).toBeTruthy();
  expect(ogTitle.getAttribute('content')).toBeTruthy();
  expect(ogTitle.getAttribute('content').length).toBeGreaterThan(0);
});

test('og:description tag should be present', () => {
  const ogDescription = document.querySelector('meta[property="og:description"]');
  expect(ogDescription).toBeTruthy();
  expect(ogDescription.getAttribute('content')).toBeTruthy();
  expect(ogDescription.getAttribute('content').length).toBeGreaterThan(0);
});

test('og:image tag should be present', () => {
  const ogImage = document.querySelector('meta[property="og:image"]');
  expect(ogImage).toBeTruthy();
  expect(ogImage.getAttribute('content')).toBeTruthy();
  expect(ogImage.getAttribute('content').length).toBeGreaterThan(0);
});

test('og:type tag should be "website"', () => {
  const ogType = document.querySelector('meta[property="og:type"]');
  expect(ogType).toBeTruthy();
  expect(ogType.getAttribute('content')).toBe('website');
});

test('og:url tag should be present', () => {
  const ogUrl = document.querySelector('meta[property="og:url"]');
  expect(ogUrl).toBeTruthy();
  expect(ogUrl.getAttribute('content')).toBeTruthy();
});

// Test Case 4: Canonical URL
console.log('\nTest Case 4: Canonical URL');
console.log('--------------------------');

test('Canonical URL link tag should be present', () => {
  const canonical = document.querySelector('link[rel="canonical"]');
  expect(canonical).toBeTruthy();
  expect(canonical.getAttribute('href')).toBeTruthy();
});

test('Canonical URL should be valid format', () => {
  const canonical = document.querySelector('link[rel="canonical"]');
  expect(canonical).toBeTruthy();
  const href = canonical.getAttribute('href');
  expect(href).toMatch(/^https?:\/\//);
});

// Test Case 5: Robots Meta Tag
console.log('\nTest Case 5: Robots Meta Tag');
console.log('----------------------------');

test('Page should be indexable (no noindex directive)', () => {
  const robots = document.querySelector('meta[name="robots"]');
  if (robots) {
    const content = robots.getAttribute('content').toLowerCase();
    expect(content).not.toContain('noindex');
  }
  // If no robots meta, defaults to indexable - test passes
});

test('Robots meta tag should have proper indexing directives', () => {
  const robots = document.querySelector('meta[name="robots"]');
  expect(robots).toBeTruthy();
  const content = robots.getAttribute('content').toLowerCase();
  expect(content).toContain('index');
  expect(content).toContain('follow');
});

// Additional SEO Checks
console.log('\nAdditional SEO Checks');
console.log('---------------------');

test('HTML lang attribute should be "en"', () => {
  const lang = document.documentElement.lang;
  expect(lang).toBe('en');
});

test('Charset meta tag should be UTF-8', () => {
  const charset = document.querySelector('meta[charset]');
  expect(charset).toBeTruthy();
  expect(charset.getAttribute('charset').toLowerCase()).toBe('utf-8');
});

test('Viewport meta tag should be properly set', () => {
  const viewport = document.querySelector('meta[name="viewport"]');
  expect(viewport).toBeTruthy();
  const content = viewport.getAttribute('content');
  expect(content).toContain('width=device-width');
  expect(content).toContain('initial-scale=1');
});

test('Should have exactly one H1 tag', () => {
  const h1Tags = document.querySelectorAll('h1');
  expect(h1Tags.length).toBe(1);
});

test('H1 should contain product name', () => {
  const h1 = document.querySelector('h1');
  expect(h1).toBeTruthy();
  expect(h1.textContent).toContain('MirDB');
});

test('Structured data (JSON-LD) should be present', () => {
  const jsonLd = document.querySelector('script[type="application/ld+json"]');
  expect(jsonLd).toBeTruthy();
  const data = JSON.parse(jsonLd.textContent);
  expect(data['@context']).toBe('https://schema.org');
});

test('Structured data should have SoftwareApplication type', () => {
  const jsonLd = document.querySelector('script[type="application/ld+json"]');
  expect(jsonLd).toBeTruthy();
  const data = JSON.parse(jsonLd.textContent);
  expect(data['@type']).toBe('SoftwareApplication');
});

test('External links should have rel="noopener"', () => {
  const externalLinks = document.querySelectorAll('a[target="_blank"]');
  externalLinks.forEach(link => {
    const rel = link.getAttribute('rel');
    if (!rel || !rel.includes('noopener')) {
      throw new Error(`Link to ${link.href} missing rel="noopener"`);
    }
  });
});

// Summary
console.log('\n=== Test Summary ===');
console.log(`Passed: ${passCount}`);
console.log(`Failed: ${failCount}`);
console.log(`Total:  ${passCount + failCount}`);

// Exit with proper code
process.exit(failCount > 0 ? 1 : 0);
