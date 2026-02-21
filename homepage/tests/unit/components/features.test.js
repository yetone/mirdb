/**
 * Features Section Unit Tests
 * Owner: Scenario 3 - Core Features Display
 *
 * Tests:
 * - Memcached Protocol Support feature is present with description
 * - Durability with SSTables feature is present with description
 * - LSM Tree Architecture feature is present with description
 * - High-Performance Rust Implementation feature is present with description
 * - Features are displayed in a grid layout
 */

const fs = require('fs');
const path = require('path');

describe('Features Section - Source Content', () => {
  let htmlContent;

  beforeAll(() => {
    const indexPath = path.join(__dirname, '../../../src/index.md');
    htmlContent = fs.readFileSync(indexPath, 'utf8');
  });

  test('Features section displays Memcached Protocol Support feature with description', () => {
    // Test case 1: Memcached Protocol Support feature
    expect(htmlContent).toMatch(/<div[^>]*class="[^"]*feature-card[^"]*"[^>]*id="feature-memcached"[^>]*>/);
    expect(htmlContent).toMatch(/<h3[^>]*class="[^"]*feature-card-title[^"]*"[^>]*>Memcached Protocol Support<\/h3>/);
    expect(htmlContent).toMatch(/Full compatibility with the Memcached protocol/);
    expect(htmlContent).toMatch(/<p[^>]*class="[^"]*feature-card-description[^"]*"[^>]*>/);
  });

  test('Features section displays Durability with SSTables feature with description', () => {
    // Test case 2: Durability with SSTables feature
    expect(htmlContent).toMatch(/<div[^>]*class="[^"]*feature-card[^"]*"[^>]*id="feature-sstables"[^>]*>/);
    expect(htmlContent).toMatch(/<h3[^>]*class="[^"]*feature-card-title[^"]*"[^>]*>Durability with SSTables<\/h3>/);
    expect(htmlContent).toMatch(/Data is persisted to disk using Sorted String Tables/);
  });

  test('Features section displays LSM Tree Architecture feature with description', () => {
    // Test case 3: LSM Tree Architecture feature
    expect(htmlContent).toMatch(/<div[^>]*class="[^"]*feature-card[^"]*"[^>]*id="feature-lsm"[^>]*>/);
    expect(htmlContent).toMatch(/<h3[^>]*class="[^"]*feature-card-title[^"]*"[^>]*>LSM Tree Architecture<\/h3>/);
    expect(htmlContent).toMatch(/Log-Structured Merge Tree architecture/);
  });

  test('Features section displays High-Performance Rust Implementation feature with description', () => {
    // Test case 4: High-Performance Rust Implementation feature
    expect(htmlContent).toMatch(/<div[^>]*class="[^"]*feature-card[^"]*"[^>]*id="feature-rust"[^>]*>/);
    expect(htmlContent).toMatch(/<h3[^>]*class="[^"]*feature-card-title[^"]*"[^>]*>High-Performance Rust Implementation<\/h3>/);
    expect(htmlContent).toMatch(/Built with Rust for memory safety and performance/);
  });

  test('Features are displayed in a grid layout', () => {
    // Test case 5: Grid layout structure
    expect(htmlContent).toMatch(/<section[^>]*id="features"[^>]*class="[^"]*features[^"]*"[^>]*>/);
    expect(htmlContent).toMatch(/<div[^>]*class="[^"]*features-grid[^"]*"[^>]*>/);

    // Verify all four feature cards are present
    const featureCardMatches = htmlContent.match(/<div[^>]*class="[^"]*feature-card[^"]*"[^>]*>/g);
    expect(featureCardMatches).not.toBeNull();
    expect(featureCardMatches.length).toBeGreaterThanOrEqual(4);
  });

  test('Features section has proper heading', () => {
    expect(htmlContent).toMatch(/<h2[^>]*class="[^"]*features-title[^"]*"[^>]*>Core Features<\/h2>/);
  });
});

describe('Features Section - Built HTML Tests', () => {
  let builtHtml;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '../../../book/index.html');
    builtHtml = fs.readFileSync(htmlPath, 'utf8');
  });

  test('Built HTML has features section with all feature cards', () => {
    expect(builtHtml).toMatch(/id="features"/);
    expect(builtHtml).toMatch(/id="feature-memcached"/);
    expect(builtHtml).toMatch(/id="feature-sstables"/);
    expect(builtHtml).toMatch(/id="feature-lsm"/);
    expect(builtHtml).toMatch(/id="feature-rust"/);
  });

  test('Built HTML features section appears after hero section', () => {
    const heroIndex = builtHtml.indexOf('id="hero"');
    const featuresIndex = builtHtml.indexOf('id="features"');

    expect(heroIndex).toBeGreaterThan(-1);
    expect(featuresIndex).toBeGreaterThan(-1);
    expect(featuresIndex).toBeGreaterThan(heroIndex);
  });

  test('Built HTML has features grid container', () => {
    expect(builtHtml).toMatch(/class="features-grid"/);
  });

  test('Built HTML has all feature descriptions', () => {
    expect(builtHtml).toMatch(/Memcached Protocol Support/);
    expect(builtHtml).toMatch(/Durability with SSTables/);
    expect(builtHtml).toMatch(/LSM Tree Architecture/);
    expect(builtHtml).toMatch(/High-Performance Rust Implementation/);
  });
});
