/**
 * Navigation Header Unit Tests
 * Owner: Scenario 2 - Navigation Header
 *
 * Tests:
 * - MirDB logo is present in header
 * - Navigation contains link to Features section
 * - Navigation contains link to Quick Start section
 * - Navigation contains link to Architecture section
 * - Navigation contains external link to GitHub repository
 */

const fs = require('fs');
const path = require('path');

describe('Navigation Header - Template Tests', () => {
  let templateContent;

  beforeAll(() => {
    // Read the index.hbs template file
    const templatePath = path.join(__dirname, '../../../theme/index.hbs');
    templateContent = fs.readFileSync(templatePath, 'utf8');
  });

  test('Header displays MirDB logo (test case 1)', () => {
    // Test case 1: Logo image is present in header
    expect(templateContent).toMatch(/<header[^>]*class="[^"]*site-header[^"]*"[^>]*>/);
    expect(templateContent).toMatch(/id="header-logo"/);
    expect(templateContent).toMatch(/<img[^>]*src="images\/logo\.gif"[^>]*class="[^"]*header-logo-img[^"]*"[^>]*>/);
    expect(templateContent).toMatch(/alt="MirDB Logo"/);
    // Also verify logo text
    expect(templateContent).toMatch(/<span[^>]*class="[^"]*header-logo-text[^"]*"[^>]*>MirDB<\/span>/);
  });

  test('Navigation contains link to Features section (test case 2)', () => {
    // Test case 2: Features navigation link exists
    expect(templateContent).toMatch(/<a[^>]*href="#features"[^>]*class="[^"]*nav-link[^"]*"[^>]*>/);
    expect(templateContent).toMatch(/data-section="features"/);
    expect(templateContent).toMatch(/>Features</);
  });

  test('Navigation contains link to Quick Start section (test case 3)', () => {
    // Test case 3: Quick Start navigation link exists
    expect(templateContent).toMatch(/<a[^>]*href="#quickstart"[^>]*class="[^"]*nav-link[^"]*"[^>]*>/);
    expect(templateContent).toMatch(/data-section="quickstart"/);
    expect(templateContent).toMatch(/>Quick Start</);
  });

  test('Navigation contains link to Architecture section (test case 4)', () => {
    // Test case 4: Architecture navigation link exists
    expect(templateContent).toMatch(/<a[^>]*href="architecture\.html"[^>]*class="[^"]*nav-link[^"]*"[^>]*>/);
    expect(templateContent).toMatch(/data-section="architecture"/);
    expect(templateContent).toMatch(/>Architecture</);
  });

  test('Navigation contains external link to GitHub repository (test case 5)', () => {
    // Test case 5: GitHub external link exists with proper attributes
    expect(templateContent).toMatch(/<a[^>]*href="https:\/\/github\.com\/yetone\/mirdb"[^>]*>/);
    expect(templateContent).toMatch(/class="[^"]*nav-link[^"]*nav-link-external[^"]*"/);
    expect(templateContent).toMatch(/target="_blank"/);
    expect(templateContent).toMatch(/rel="noopener noreferrer"/);
    // GitHub text may have whitespace around it due to formatting
    expect(templateContent).toMatch(/>\s*GitHub\s*</);
    // External link should have accessibility label
    expect(templateContent).toMatch(/aria-label="[^"]*GitHub[^"]*"/);
  });

  test('Navigation has proper structure', () => {
    // Verify navigation structure
    expect(templateContent).toMatch(/<nav[^>]*class="[^"]*main-nav[^"]*"[^>]*id="main-nav"[^>]*>/);
    expect(templateContent).toMatch(/aria-label="Main navigation"/);
    expect(templateContent).toMatch(/<ul[^>]*class="[^"]*nav-list[^"]*"[^>]*>/);
    expect(templateContent).toMatch(/<li[^>]*class="[^"]*nav-item[^"]*"[^>]*>/);
  });

  test('Header has skip navigation link for accessibility', () => {
    // Verify skip navigation link exists
    expect(templateContent).toMatch(/<a[^>]*href="#content"[^>]*class="[^"]*skip-nav[^"]*"[^>]*>/);
    expect(templateContent).toMatch(/>Skip to content</);
  });

  test('Header has mobile menu toggle button', () => {
    // Verify mobile menu toggle exists
    expect(templateContent).toMatch(/<button[^>]*class="[^"]*mobile-menu-toggle[^"]*"[^>]*>/);
    expect(templateContent).toMatch(/id="mobile-menu-toggle"/);
    expect(templateContent).toMatch(/aria-label="Toggle navigation menu"/);
    expect(templateContent).toMatch(/aria-expanded="false"/);
    expect(templateContent).toMatch(/aria-controls="main-nav"/);
  });
});

describe('Navigation Header - Built HTML Tests', () => {
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

  test('Built HTML contains navigation header', () => {
    if (!buildExists) {
      // Skip if build doesn't exist
      console.log('Build does not exist yet, skipping built HTML test');
      return;
    }
    expect(builtHtml).toMatch(/<header[^>]*id="site-header"/);
    expect(builtHtml).toMatch(/class="[^"]*site-header[^"]*"/);
  });

  test('Built HTML has MirDB logo in header', () => {
    if (!buildExists) {
      console.log('Build does not exist yet, skipping built HTML test');
      return;
    }
    expect(builtHtml).toMatch(/id="header-logo"/);
    expect(builtHtml).toMatch(/src="images\/logo\.gif"/);
  });

  test('Built HTML has all navigation links', () => {
    if (!buildExists) {
      console.log('Build does not exist yet, skipping built HTML test');
      return;
    }
    expect(builtHtml).toMatch(/href="#features"/);
    expect(builtHtml).toMatch(/href="#quickstart"/);
    expect(builtHtml).toMatch(/href="architecture\.html"/);
    expect(builtHtml).toMatch(/href="https:\/\/github\.com\/yetone\/mirdb"/);
  });
});

describe('Navigation CSS Tests', () => {
  let cssContent;

  beforeAll(() => {
    // Read the chrome.css file
    const cssPath = path.join(__dirname, '../../../theme/css/chrome.css');
    cssContent = fs.readFileSync(cssPath, 'utf8');
  });

  test('CSS contains header styles', () => {
    expect(cssContent).toMatch(/\.site-header/);
    expect(cssContent).toMatch(/\.header-container/);
    expect(cssContent).toMatch(/\.header-logo/);
  });

  test('CSS contains navigation styles', () => {
    expect(cssContent).toMatch(/\.main-nav/);
    expect(cssContent).toMatch(/\.nav-list/);
    expect(cssContent).toMatch(/\.nav-link/);
    expect(cssContent).toMatch(/\.nav-item/);
  });

  test('CSS contains skip navigation styles', () => {
    expect(cssContent).toMatch(/\.skip-nav/);
  });

  test('CSS contains mobile menu toggle styles', () => {
    expect(cssContent).toMatch(/\.mobile-menu-toggle/);
    expect(cssContent).toMatch(/\.hamburger-line/);
  });

  test('CSS contains external link styles', () => {
    expect(cssContent).toMatch(/\.nav-link-external/);
    expect(cssContent).toMatch(/\.external-icon/);
  });
});

describe('Navigation JavaScript Tests', () => {
  let jsContent;

  beforeAll(() => {
    // Read the navigation.js file
    const jsPath = path.join(__dirname, '../../../theme/js/navigation.js');
    jsContent = fs.readFileSync(jsPath, 'utf8');
  });

  test('JavaScript contains initSmoothScroll function', () => {
    expect(jsContent).toMatch(/function initSmoothScroll/);
  });

  test('JavaScript contains toggleMobileMenu function', () => {
    expect(jsContent).toMatch(/function toggleMobileMenu/);
  });

  test('JavaScript contains closeMobileMenuOnSelect function', () => {
    expect(jsContent).toMatch(/function closeMobileMenuOnSelect/);
  });

  test('JavaScript sets up event listeners on DOMContentLoaded', () => {
    expect(jsContent).toMatch(/document\.addEventListener\(['"]DOMContentLoaded['"]/);
    expect(jsContent).toMatch(/initSmoothScroll\(\)/);
  });

  test('JavaScript handles smooth scrolling to sections', () => {
    expect(jsContent).toMatch(/scrollTo/);
    expect(jsContent).toMatch(/behavior:\s*['"]smooth['"]/);
  });
});
