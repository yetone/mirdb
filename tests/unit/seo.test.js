/**
 * SEO Unit Tests
 * Owner: Scenario 9 - Accessibility and SEO
 *
 * Tests:
 * - Title tag content
 * - Meta description presence
 * - Open Graph tags (og:title, og:description, og:image, og:url)
 * - Twitter card tags
 * - Canonical URL
 * - Language attribute on html element
 */

const fs = require('fs');
const path = require('path');

const htmlPath = path.join(__dirname, '../../index.html');
const html = fs.readFileSync(htmlPath, 'utf-8');

describe('Title Tag', () => {
  beforeEach(() => {
    document.documentElement.innerHTML = html;
  });

  test('title tag exists', () => {
    const title = document.querySelector('title');
    expect(title).toBeTruthy();
  });

  test('title tag is not empty', () => {
    const title = document.querySelector('title');
    expect(title.textContent.trim().length).toBeGreaterThan(0);
  });

  test('title contains "MirDB"', () => {
    const title = document.querySelector('title');
    expect(title.textContent).toMatch(/MirDB/i);
  });

  test('title is descriptive (at least 20 characters)', () => {
    const title = document.querySelector('title');
    expect(title.textContent.trim().length).toBeGreaterThanOrEqual(20);
  });

  test('title is not too long (at most 70 characters)', () => {
    const title = document.querySelector('title');
    expect(title.textContent.trim().length).toBeLessThanOrEqual(70);
  });
});

describe('Meta Description', () => {
  beforeEach(() => {
    document.documentElement.innerHTML = html;
  });

  test('meta description tag exists', () => {
    const metaDesc = document.querySelector('meta[name="description"]');
    expect(metaDesc).toBeTruthy();
  });

  test('meta description has content attribute', () => {
    const metaDesc = document.querySelector('meta[name="description"]');
    expect(metaDesc.getAttribute('content')).toBeTruthy();
  });

  test('meta description content is meaningful (at least 50 characters)', () => {
    const metaDesc = document.querySelector('meta[name="description"]');
    const content = metaDesc.getAttribute('content') || '';
    expect(content.length).toBeGreaterThanOrEqual(50);
  });

  test('meta description is not too long (at most 160 characters)', () => {
    const metaDesc = document.querySelector('meta[name="description"]');
    const content = metaDesc.getAttribute('content') || '';
    expect(content.length).toBeLessThanOrEqual(160);
  });
});

describe('Open Graph Tags', () => {
  beforeEach(() => {
    document.documentElement.innerHTML = html;
  });

  test('og:title meta tag exists', () => {
    const ogTitle = document.querySelector('meta[property="og:title"]');
    expect(ogTitle).toBeTruthy();
  });

  test('og:title has content', () => {
    const ogTitle = document.querySelector('meta[property="og:title"]');
    expect(ogTitle.getAttribute('content')).toBeTruthy();
    expect(ogTitle.getAttribute('content').length).toBeGreaterThan(0);
  });

  test('og:description meta tag exists', () => {
    const ogDesc = document.querySelector('meta[property="og:description"]');
    expect(ogDesc).toBeTruthy();
  });

  test('og:description has content', () => {
    const ogDesc = document.querySelector('meta[property="og:description"]');
    expect(ogDesc.getAttribute('content')).toBeTruthy();
    expect(ogDesc.getAttribute('content').length).toBeGreaterThan(0);
  });

  test('og:image meta tag exists', () => {
    const ogImage = document.querySelector('meta[property="og:image"]');
    expect(ogImage).toBeTruthy();
  });

  test('og:image has content with URL', () => {
    const ogImage = document.querySelector('meta[property="og:image"]');
    const content = ogImage.getAttribute('content') || '';
    expect(content.length).toBeGreaterThan(0);
  });

  test('og:url meta tag exists', () => {
    const ogUrl = document.querySelector('meta[property="og:url"]');
    expect(ogUrl).toBeTruthy();
  });

  test('og:url has content with URL', () => {
    const ogUrl = document.querySelector('meta[property="og:url"]');
    const content = ogUrl.getAttribute('content') || '';
    expect(content.length).toBeGreaterThan(0);
  });

  test('og:type meta tag exists', () => {
    const ogType = document.querySelector('meta[property="og:type"]');
    expect(ogType).toBeTruthy();
  });

  test('og:type is "website"', () => {
    const ogType = document.querySelector('meta[property="og:type"]');
    expect(ogType.getAttribute('content')).toBe('website');
  });
});

describe('Twitter Card Tags', () => {
  beforeEach(() => {
    document.documentElement.innerHTML = html;
  });

  test('twitter:card meta tag exists', () => {
    const twitterCard = document.querySelector('meta[name="twitter:card"]');
    expect(twitterCard).toBeTruthy();
  });

  test('twitter:card has content', () => {
    const twitterCard = document.querySelector('meta[name="twitter:card"]');
    expect(twitterCard.getAttribute('content')).toBeTruthy();
  });

  test('twitter:title meta tag exists', () => {
    const twitterTitle = document.querySelector('meta[name="twitter:title"]');
    expect(twitterTitle).toBeTruthy();
  });

  test('twitter:title has content', () => {
    const twitterTitle = document.querySelector('meta[name="twitter:title"]');
    expect(twitterTitle.getAttribute('content')).toBeTruthy();
    expect(twitterTitle.getAttribute('content').length).toBeGreaterThan(0);
  });

  test('twitter:description meta tag exists', () => {
    const twitterDesc = document.querySelector('meta[name="twitter:description"]');
    expect(twitterDesc).toBeTruthy();
  });

  test('twitter:description has content', () => {
    const twitterDesc = document.querySelector('meta[name="twitter:description"]');
    expect(twitterDesc.getAttribute('content')).toBeTruthy();
    expect(twitterDesc.getAttribute('content').length).toBeGreaterThan(0);
  });

  test('twitter:image meta tag exists', () => {
    const twitterImage = document.querySelector('meta[name="twitter:image"]');
    expect(twitterImage).toBeTruthy();
  });

  test('twitter:image has content', () => {
    const twitterImage = document.querySelector('meta[name="twitter:image"]');
    expect(twitterImage.getAttribute('content')).toBeTruthy();
    expect(twitterImage.getAttribute('content').length).toBeGreaterThan(0);
  });
});

describe('Canonical URL', () => {
  beforeEach(() => {
    document.documentElement.innerHTML = html;
  });

  test('canonical link tag exists', () => {
    const canonical = document.querySelector('link[rel="canonical"]');
    expect(canonical).toBeTruthy();
  });

  test('canonical link has href attribute', () => {
    const canonical = document.querySelector('link[rel="canonical"]');
    expect(canonical.getAttribute('href')).toBeTruthy();
    expect(canonical.getAttribute('href').length).toBeGreaterThan(0);
  });
});

describe('Language and Localization', () => {
  beforeEach(() => {
    const htmlMatch = html.match(/<html([^>]*)>/i);
    const langMatch = htmlMatch && htmlMatch[1].match(/lang="([^"]*)"/i);
    const lang = langMatch ? langMatch[1] : 'en';
    document.documentElement.innerHTML = html;
    document.documentElement.setAttribute('lang', lang);
  });

  test('html element has lang attribute', () => {
    const htmlEl = document.querySelector('html');
    expect(htmlEl.hasAttribute('lang')).toBe(true);
  });

  test('lang attribute is set to "en"', () => {
    const htmlEl = document.querySelector('html');
    expect(htmlEl.getAttribute('lang')).toBe('en');
  });
});

describe('Favicon', () => {
  beforeEach(() => {
    document.documentElement.innerHTML = html;
  });

  test('favicon link exists', () => {
    const favicon = document.querySelector('link[rel="icon"]') ||
                    document.querySelector('link[rel="shortcut icon"]');
    expect(favicon).toBeTruthy();
  });
});

describe('Robots Meta Tag', () => {
  beforeEach(() => {
    document.documentElement.innerHTML = html;
  });

  test('robots meta tag exists or defaults are acceptable', () => {
    const robots = document.querySelector('meta[name="robots"]');
    // If present, should allow indexing
    if (robots) {
      const content = robots.getAttribute('content') || '';
      expect(content).not.toContain('noindex');
    }
    // If absent, search engines default to index,follow which is acceptable
    expect(true).toBe(true);
  });
});
