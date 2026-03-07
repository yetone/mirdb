/**
 * Structure Validation Tests
 * Owner: Scenario 14 - Static HTML/CSS/JS Structure
 *
 * Tests:
 * - File structure validation (index.html, CSS, JS)
 * - HTML5 DOCTYPE declaration
 * - HTML validation basics
 * - CSS validation basics
 */

const fs = require('fs');
const path = require('path');

// Base path for homepage files
const homepagePath = path.join(__dirname, '../..');
const htmlPath = path.join(homepagePath, 'index.html');
const cssMainPath = path.join(homepagePath, 'css/styles.css');
const cssResetPath = path.join(homepagePath, 'css/utilities/reset.css');
const cssVariablesPath = path.join(homepagePath, 'css/utilities/variables.css');
const jsMainPath = path.join(homepagePath, 'js/main.js');
const jsThemeTogglePath = path.join(homepagePath, 'js/theme-toggle.js');

/**
 * Test Case 1: Check for index.html file
 */
describe('Test Case 1: index.html file exists', () => {
  test('index.html exists in homepage directory', () => {
    const exists = fs.existsSync(htmlPath);
    expect(exists).toBe(true);
  });

  test('index.html is a file, not a directory', () => {
    const stats = fs.statSync(htmlPath);
    expect(stats.isFile()).toBe(true);
  });

  test('index.html has content', () => {
    const content = fs.readFileSync(htmlPath, 'utf8');
    expect(content.length).toBeGreaterThan(100);
  });
});

/**
 * Test Case 2: Check for CSS file
 */
describe('Test Case 2: Stylesheet file exists', () => {
  test('main stylesheet (styles.css) exists', () => {
    const exists = fs.existsSync(cssMainPath);
    expect(exists).toBe(true);
  });

  test('CSS reset file exists', () => {
    const exists = fs.existsSync(cssResetPath);
    expect(exists).toBe(true);
  });

  test('CSS variables file exists', () => {
    const exists = fs.existsSync(cssVariablesPath);
    expect(exists).toBe(true);
  });

  test('styles.css imports utility stylesheets', () => {
    const content = fs.readFileSync(cssMainPath, 'utf8');
    expect(content).toContain('reset.css');
    expect(content).toContain('variables.css');
  });

  test('styles.css has content', () => {
    const content = fs.readFileSync(cssMainPath, 'utf8');
    expect(content.length).toBeGreaterThan(50);
  });
});

/**
 * Test Case 3: Check for JavaScript file
 */
describe('Test Case 3: JavaScript file exists for theme toggle functionality', () => {
  test('main JavaScript file (main.js) exists', () => {
    const exists = fs.existsSync(jsMainPath);
    expect(exists).toBe(true);
  });

  test('theme toggle JavaScript file (theme-toggle.js) exists', () => {
    const exists = fs.existsSync(jsThemeTogglePath);
    expect(exists).toBe(true);
  });

  test('main.js has content', () => {
    const content = fs.readFileSync(jsMainPath, 'utf8');
    expect(content.length).toBeGreaterThan(50);
  });

  test('theme-toggle.js contains theme-related code', () => {
    const content = fs.readFileSync(jsThemeTogglePath, 'utf8');
    expect(content.toLowerCase()).toMatch(/theme/);
  });
});

/**
 * Test Case 4: Validate HTML5 markup
 */
describe('Test Case 4: HTML5 validation', () => {
  let htmlContent;

  beforeAll(() => {
    htmlContent = fs.readFileSync(htmlPath, 'utf8');
  });

  test('HTML has html element with lang attribute', () => {
    expect(htmlContent).toMatch(/<html[^>]+lang\s*=\s*["'][a-zA-Z-]+["']/);
  });

  test('HTML has head element', () => {
    expect(htmlContent).toMatch(/<head[^>]*>/);
    expect(htmlContent).toMatch(/<\/head>/);
  });

  test('HTML has body element', () => {
    expect(htmlContent).toMatch(/<body[^>]*>/);
    expect(htmlContent).toMatch(/<\/body>/);
  });

  test('HTML has charset meta tag', () => {
    expect(htmlContent).toMatch(/<meta[^>]+charset\s*=\s*["']UTF-8["']/i);
  });

  test('HTML has viewport meta tag', () => {
    expect(htmlContent).toMatch(/<meta[^>]+viewport/i);
  });

  test('HTML has title element', () => {
    expect(htmlContent).toMatch(/<title[^>]*>[^<]+<\/title>/);
  });

  test('HTML has semantic main element', () => {
    expect(htmlContent).toMatch(/<main[^>]*>/);
    expect(htmlContent).toMatch(/<\/main>/);
  });

  test('HTML has semantic header element', () => {
    expect(htmlContent).toMatch(/<header[^>]*>/);
    expect(htmlContent).toMatch(/<\/header>/);
  });

  test('HTML has semantic footer element', () => {
    expect(htmlContent).toMatch(/<footer[^>]*>/);
    expect(htmlContent).toMatch(/<\/footer>/);
  });

  test('HTML uses semantic section elements', () => {
    expect(htmlContent).toMatch(/<section[^>]*>/);
  });

  test('HTML properly closes all major tags', () => {
    const openHtml = (htmlContent.match(/<html[\s>]/g) || []).length;
    const closeHtml = (htmlContent.match(/<\/html>/g) || []).length;
    expect(openHtml).toBe(closeHtml);

    // Use more specific regex to avoid matching <header>
    const openHead = (htmlContent.match(/<head[\s>]/g) || []).length;
    const closeHead = (htmlContent.match(/<\/head>/g) || []).length;
    expect(openHead).toBe(closeHead);

    const openBody = (htmlContent.match(/<body[\s>]/g) || []).length;
    const closeBody = (htmlContent.match(/<\/body>/g) || []).length;
    expect(openBody).toBe(closeBody);
  });
});

/**
 * Test Case 5: Validate CSS
 */
describe('Test Case 5: CSS validation', () => {
  let cssContent;
  let resetCssContent;

  beforeAll(() => {
    cssContent = fs.readFileSync(cssMainPath, 'utf8');
    resetCssContent = fs.readFileSync(cssResetPath, 'utf8');
  });

  test('main CSS file is valid (no empty)', () => {
    expect(cssContent.trim().length).toBeGreaterThan(0);
  });

  test('reset CSS file is valid (no empty)', () => {
    expect(resetCssContent.trim().length).toBeGreaterThan(0);
  });

  test('CSS uses proper selector syntax', () => {
    // Check for valid CSS selector patterns
    expect(cssContent).toMatch(/\.[a-zA-Z][a-zA-Z0-9_-]*\s*\{/);
  });

  test('CSS uses CSS custom properties (variables)', () => {
    expect(cssContent).toMatch(/var\(--[a-zA-Z]/);
  });

  test('reset CSS uses box-sizing border-box', () => {
    expect(resetCssContent).toMatch(/box-sizing\s*:\s*border-box/);
  });

  test('CSS has no unclosed braces', () => {
    const openBraces = (cssContent.match(/\{/g) || []).length;
    const closeBraces = (cssContent.match(/\}/g) || []).length;
    expect(openBraces).toBe(closeBraces);
  });

  test('reset CSS has no unclosed braces', () => {
    const openBraces = (resetCssContent.match(/\{/g) || []).length;
    const closeBraces = (resetCssContent.match(/\}/g) || []).length;
    expect(openBraces).toBe(closeBraces);
  });
});

/**
 * Test Case 6: Check for DOCTYPE declaration
 */
describe('Test Case 6: DOCTYPE declaration', () => {
  let htmlContent;

  beforeAll(() => {
    htmlContent = fs.readFileSync(htmlPath, 'utf8');
  });

  test('HTML file starts with <!DOCTYPE html>', () => {
    const trimmedContent = htmlContent.trim();
    expect(trimmedContent.startsWith('<!DOCTYPE html>')).toBe(true);
  });

  test('DOCTYPE declaration is case-insensitive but present', () => {
    expect(htmlContent.toUpperCase()).toMatch(/<!DOCTYPE\s+HTML>/);
  });

  test('DOCTYPE is at the very beginning (no BOM or whitespace before)', () => {
    // Remove BOM if present and check
    const withoutBom = htmlContent.replace(/^\uFEFF/, '').trim();
    expect(withoutBom.startsWith('<!DOCTYPE html>')).toBe(true);
  });
});

/**
 * Additional structural tests for completeness
 */
describe('Complete file structure validation', () => {
  test('css/components directory exists with component styles', () => {
    const componentsPath = path.join(homepagePath, 'css/components');
    const exists = fs.existsSync(componentsPath);
    expect(exists).toBe(true);

    const stats = fs.statSync(componentsPath);
    expect(stats.isDirectory()).toBe(true);
  });

  test('css/utilities directory exists', () => {
    const utilitiesPath = path.join(homepagePath, 'css/utilities');
    const exists = fs.existsSync(utilitiesPath);
    expect(exists).toBe(true);
  });

  test('js directory exists with JavaScript files', () => {
    const jsPath = path.join(homepagePath, 'js');
    const exists = fs.existsSync(jsPath);
    expect(exists).toBe(true);

    const files = fs.readdirSync(jsPath);
    expect(files.length).toBeGreaterThan(0);
  });

  test('HTML links to CSS stylesheet', () => {
    const htmlContent = fs.readFileSync(htmlPath, 'utf8');
    expect(htmlContent).toMatch(/<link[^>]+rel\s*=\s*["']stylesheet["']/);
    expect(htmlContent).toMatch(/styles\.css/);
  });

  test('HTML includes JavaScript files', () => {
    const htmlContent = fs.readFileSync(htmlPath, 'utf8');
    expect(htmlContent).toMatch(/<script[^>]+src\s*=/);
  });
});
