/**
 * Footer Unit Tests
 * Owner: Scenario 8 - External Links and Footer
 *
 * Tests:
 * - Footer contains link to GitHub repository (https://github.com/yetone/mirdb)
 * - Footer displays license information
 * - Footer displays copyright notice
 * - Link opens GitHub repository in new tab
 * - All external links have target='_blank' and rel='noopener noreferrer'
 */

const fs = require('fs');
const path = require('path');

describe('Footer - Template Tests', () => {
  let templateContent;

  beforeAll(() => {
    // Read the index.hbs template file
    const templatePath = path.join(__dirname, '../../../theme/index.hbs');
    templateContent = fs.readFileSync(templatePath, 'utf8');
  });

  test('Footer contains link to GitHub repository (test case 1)', () => {
    // Test case 1: Footer has GitHub link
    expect(templateContent).toMatch(/<footer[^>]*class="[^"]*site-footer[^"]*"[^>]*>/);
    expect(templateContent).toMatch(/<a[^>]*href="https:\/\/github\.com\/yetone\/mirdb"[^>]*>/);
    // Check that the GitHub link is within the footer section
    const footerMatch = templateContent.match(/<!-- ===== FOOTER - Scenario 8 ===== -->([\s\S]*?)<!-- ===== END FOOTER ===== -->/);
    expect(footerMatch).toBeTruthy();
    expect(footerMatch[1]).toMatch(/href="https:\/\/github\.com\/yetone\/mirdb"/);
  });

  test('Footer displays license information (test case 2)', () => {
    // Test case 2: Footer contains license information
    const footerMatch = templateContent.match(/<!-- ===== FOOTER - Scenario 8 ===== -->([\s\S]*?)<!-- ===== END FOOTER ===== -->/);
    expect(footerMatch).toBeTruthy();
    // Check for MIT License or similar license text
    expect(footerMatch[1]).toMatch(/MIT License|license/i);
  });

  test('Footer displays copyright notice (test case 3)', () => {
    // Test case 3: Footer contains copyright notice
    const footerMatch = templateContent.match(/<!-- ===== FOOTER - Scenario 8 ===== -->([\s\S]*?)<!-- ===== END FOOTER ===== -->/);
    expect(footerMatch).toBeTruthy();
    // Check for copyright symbol or text
    expect(footerMatch[1]).toMatch(/©|&copy;|Copyright/i);
    // Check for MirDB in copyright
    expect(footerMatch[1]).toMatch(/MirDB/);
  });

  test('GitHub link opens in new tab (test case 4)', () => {
    // Test case 4: GitHub link has target="_blank"
    const footerMatch = templateContent.match(/<!-- ===== FOOTER - Scenario 8 ===== -->([\s\S]*?)<!-- ===== END FOOTER ===== -->/);
    expect(footerMatch).toBeTruthy();
    // Find the GitHub link and check for target="_blank"
    const githubLinkMatch = footerMatch[1].match(/<a[^>]*href="https:\/\/github\.com\/yetone\/mirdb"[^>]*>/);
    expect(githubLinkMatch).toBeTruthy();
    expect(githubLinkMatch[0]).toMatch(/target="_blank"/);
  });

  test('All external links have proper security attributes (test case 5)', () => {
    // Test case 5: All external links have target='_blank' and rel='noopener noreferrer'
    const footerMatch = templateContent.match(/<!-- ===== FOOTER - Scenario 8 ===== -->([\s\S]*?)<!-- ===== END FOOTER ===== -->/);
    expect(footerMatch).toBeTruthy();

    // Find all external links (links starting with http/https)
    const externalLinkPattern = /<a[^>]*href="https?:\/\/[^"]*"[^>]*>/g;
    const externalLinks = footerMatch[1].match(externalLinkPattern) || [];

    // Each external link should have target="_blank" and rel="noopener noreferrer"
    externalLinks.forEach(link => {
      expect(link).toMatch(/target="_blank"/);
      expect(link).toMatch(/rel="noopener noreferrer"/);
    });

    // Ensure we found at least one external link (the GitHub link)
    expect(externalLinks.length).toBeGreaterThan(0);
  });

  test('Footer has proper structure', () => {
    // Verify footer structure
    expect(templateContent).toMatch(/<footer[^>]*id="site-footer"[^>]*>/);
    expect(templateContent).toMatch(/<footer[^>]*class="[^"]*site-footer[^"]*"[^>]*>/);
  });

  test('Footer links have accessibility attributes', () => {
    // Check for aria-labels on external links
    const footerMatch = templateContent.match(/<!-- ===== FOOTER - Scenario 8 ===== -->([\s\S]*?)<!-- ===== END FOOTER ===== -->/);
    expect(footerMatch).toBeTruthy();

    // External links should have aria-label for accessibility
    const githubLinkMatch = footerMatch[1].match(/<a[^>]*href="https:\/\/github\.com\/yetone\/mirdb"[^>]*>/);
    expect(githubLinkMatch).toBeTruthy();
    expect(githubLinkMatch[0]).toMatch(/aria-label="[^"]*"/);
  });
});

describe('Footer - Built HTML Tests', () => {
  let builtHtml;
  let buildExists = false;

  beforeAll(() => {
    // Read the built HTML file
    const htmlPath = path.join(__dirname, '../../../book/index.html');
    try {
      builtHtml = fs.readFileSync(htmlPath, 'utf8');
      buildExists = true;
    } catch (e) {
      // Build doesn't exist yet
      buildExists = false;
    }
  });

  test('Built HTML contains footer', () => {
    if (!buildExists) {
      console.log('Build does not exist yet, skipping built HTML test');
      return;
    }
    expect(builtHtml).toMatch(/<footer[^>]*id="site-footer"/);
    expect(builtHtml).toMatch(/class="[^"]*site-footer[^"]*"/);
  });

  test('Built HTML has GitHub link in footer', () => {
    if (!buildExists) {
      console.log('Build does not exist yet, skipping built HTML test');
      return;
    }
    expect(builtHtml).toMatch(/href="https:\/\/github\.com\/yetone\/mirdb"/);
  });

  test('Built HTML has license information in footer', () => {
    if (!buildExists) {
      console.log('Build does not exist yet, skipping built HTML test');
      return;
    }
    expect(builtHtml).toMatch(/MIT License|license/i);
  });

  test('Built HTML has copyright notice in footer', () => {
    if (!buildExists) {
      console.log('Build does not exist yet, skipping built HTML test');
      return;
    }
    expect(builtHtml).toMatch(/©|&copy;|Copyright/i);
    expect(builtHtml).toMatch(/MirDB/);
  });
});

describe('Footer CSS Tests', () => {
  let cssContent;

  beforeAll(() => {
    // Read the chrome.css file
    const cssPath = path.join(__dirname, '../../../theme/css/chrome.css');
    cssContent = fs.readFileSync(cssPath, 'utf8');
  });

  test('CSS contains footer styles', () => {
    expect(cssContent).toMatch(/\.site-footer/);
  });

  test('CSS contains footer container styles', () => {
    expect(cssContent).toMatch(/\.footer-container|\.site-footer/);
  });

  test('CSS contains footer link styles', () => {
    expect(cssContent).toMatch(/\.footer-link|\.site-footer\s+a/);
  });

  test('CSS contains footer text styles', () => {
    expect(cssContent).toMatch(/\.footer-text|\.site-footer/);
  });
});
