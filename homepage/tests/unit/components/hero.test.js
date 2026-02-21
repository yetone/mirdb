/**
 * Hero Section Unit Tests
 * Owner: Scenario 1 - Hero Section Display
 *
 * Tests:
 * - MirDB logo is present
 * - Tagline displays correctly
 * - Get Started button is present with correct href
 */

const fs = require('fs');
const path = require('path');

describe('Hero Section', () => {
  let htmlContent;

  beforeAll(() => {
    // Read the index.md file which contains the hero section HTML
    const indexPath = path.join(__dirname, '../../../src/index.md');
    htmlContent = fs.readFileSync(indexPath, 'utf8');
  });

  test('Hero section displays MirDB logo image', () => {
    // Test case 1: Logo image is present
    expect(htmlContent).toMatch(/<img[^>]*src="images\/logo\.gif"[^>]*>/);
    expect(htmlContent).toMatch(/alt="MirDB Logo"/);
    expect(htmlContent).toMatch(/class="[^"]*hero-logo[^"]*"/);
    expect(htmlContent).toMatch(/id="mirdb-logo"/);
  });

  test('Hero section displays tagline: A Persistent Key-Value Store with Memcached Protocol', () => {
    // Test case 2: Tagline is present with correct text
    expect(htmlContent).toMatch(/A Persistent Key-Value Store with Memcached Protocol/);
    expect(htmlContent).toMatch(/<p[^>]*class="[^"]*hero-tagline[^"]*"[^>]*>/);
  });

  test('Get Started button is present and has correct href to quick start section', () => {
    // Test case 3: CTA button exists with correct attributes
    expect(htmlContent).toMatch(/<a[^>]*href="#quickstart"[^>]*>/);
    expect(htmlContent).toMatch(/id="get-started-btn"/);
    expect(htmlContent).toMatch(/class="[^"]*cta-button[^"]*"/);
    expect(htmlContent).toMatch(/>Get Started</);
  });

  test('Hero section has proper structure', () => {
    // Additional structural tests
    expect(htmlContent).toMatch(/<section[^>]*id="hero"[^>]*class="[^"]*hero[^"]*"[^>]*>/);
    expect(htmlContent).toMatch(/<div[^>]*class="[^"]*hero-content[^"]*"[^>]*>/);
  });

  test('Hero section contains MirDB title', () => {
    // Verify main title is present
    expect(htmlContent).toMatch(/<h1[^>]*class="[^"]*hero-title[^"]*"[^>]*>MirDB<\/h1>/);
  });
});

describe('Hero Section Built HTML Tests', () => {
  let builtHtml;

  beforeAll(() => {
    const fs = require('fs');
    const path = require('path');

    // Read the built HTML file
    const htmlPath = path.join(__dirname, '../../../book/index.html');
    builtHtml = fs.readFileSync(htmlPath, 'utf8');
  });

  test('Click Get Started button - href points to quickstart section (test case 4)', () => {
    // Test case 4: Get Started button click navigates to quickstart section
    // Verify button has correct href
    expect(builtHtml).toMatch(/id="get-started-btn"[^>]*>/);
    expect(builtHtml).toMatch(/<a[^>]*href="#quickstart"[^>]*id="get-started-btn"[^>]*>Get Started<\/a>/);

    // Verify quickstart section exists as target
    expect(builtHtml).toMatch(/<section[^>]*id="quickstart"/);

    // This verifies clicking the button would scroll to quickstart section
    // The anchor href #quickstart links to id="quickstart" which exists
  });

  test('Built HTML has hero section as first content section', () => {
    // Verify hero section appears before other sections in the DOM
    const heroIndex = builtHtml.indexOf('id="hero"');
    const featuresIndex = builtHtml.indexOf('id="features"');
    const quickstartIndex = builtHtml.indexOf('id="quickstart"');

    expect(heroIndex).toBeGreaterThan(-1);
    expect(featuresIndex).toBeGreaterThan(-1);
    expect(quickstartIndex).toBeGreaterThan(-1);

    // Hero should come before other sections
    expect(heroIndex).toBeLessThan(featuresIndex);
    expect(heroIndex).toBeLessThan(quickstartIndex);
  });

  test('Built HTML has logo with correct attributes', () => {
    expect(builtHtml).toMatch(/id="mirdb-logo"/);
    expect(builtHtml).toMatch(/src="images\/logo\.gif"/);
    expect(builtHtml).toMatch(/alt="MirDB Logo"/);
  });

  test('Built HTML has tagline with correct text', () => {
    expect(builtHtml).toMatch(/<p[^>]*class="hero-tagline"[^>]*>A Persistent Key-Value Store with Memcached Protocol<\/p>/);
  });
});
