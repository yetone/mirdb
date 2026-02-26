/**
 * Static Deployment Integration Tests
 * Owner: Scenario 7 - Accessibility and Performance
 *
 * Tests for:
 * - Static deployment capability (NFR-5)
 * - No server-side processing required
 * - All assets are self-contained
 * - No console errors expected
 */

const fs = require('fs');
const path = require('path');

describe('Static Deployment Integration Tests', () => {
  let document;
  let htmlContent;
  const homepageDir = path.join(__dirname, '../../');

  beforeAll(() => {
    // Read the HTML file
    const htmlPath = path.join(homepageDir, 'index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf-8');

    // Create a mock DOM
    document = new DOMParser().parseFromString(htmlContent, 'text/html');
  });

  describe('Test Case 9: Static Deployment Capability', () => {
    test('index.html file exists and is readable', () => {
      const htmlPath = path.join(homepageDir, 'index.html');
      expect(fs.existsSync(htmlPath)).toBe(true);

      const stats = fs.statSync(htmlPath);
      expect(stats.size).toBeGreaterThan(0);
    });

    test('All CSS files exist', () => {
      const styleLinks = document.querySelectorAll('link[rel="stylesheet"]');

      for (const link of styleLinks) {
        const href = link.getAttribute('href');
        if (href && !href.startsWith('http')) {
          const cssPath = path.join(homepageDir, href);
          expect(fs.existsSync(cssPath)).toBe(true);
        }
      }
    });

    test('All JavaScript files exist', () => {
      const scripts = document.querySelectorAll('script[src]');

      for (const script of scripts) {
        const src = script.getAttribute('src');
        if (src && !src.startsWith('http')) {
          const jsPath = path.join(homepageDir, src);
          expect(fs.existsSync(jsPath)).toBe(true);
        }
      }
    });

    test('Local images exist', () => {
      const images = document.querySelectorAll('img[src]');

      for (const img of images) {
        const src = img.getAttribute('src');
        if (src && !src.startsWith('http')) {
          const imgPath = path.join(homepageDir, src);
          expect(fs.existsSync(imgPath)).toBe(true);
        }
      }
    });

    test('No server-side template tags or PHP code', () => {
      // Check for common server-side patterns
      const phpPattern = /<\?php/i;
      const aspPattern = /<%/;
      const jspPattern = /<%/;
      const erbPattern = /<%/;
      const ejsPattern = /<%=/;
      const handlebarsPattern = /\{\{/;
      const jinja2Pattern = /\{%/;

      expect(htmlContent).not.toMatch(phpPattern);
      expect(htmlContent).not.toMatch(aspPattern);
      expect(htmlContent).not.toMatch(jspPattern);
      expect(htmlContent).not.toMatch(erbPattern);

      // Note: We allow mustache-style template literals in JavaScript
      // but HTML should not have these outside of script tags
      const htmlWithoutScripts = htmlContent.replace(/<script[\s\S]*?<\/script>/gi, '');
      expect(htmlWithoutScripts).not.toMatch(/\{\{\s*\w+\s*\}\}/);
    });

    test('No server-side includes or SSI directives', () => {
      const ssiPattern = /<!--#/;
      expect(htmlContent).not.toMatch(ssiPattern);
    });

    test('CSS files contain no server-side code', () => {
      const cssDir = path.join(homepageDir, 'css');
      const cssFiles = fs.readdirSync(cssDir).filter(f => f.endsWith('.css'));

      for (const cssFile of cssFiles) {
        const cssContent = fs.readFileSync(path.join(cssDir, cssFile), 'utf-8');
        expect(cssContent).not.toMatch(/<\?/);
        expect(cssContent).not.toMatch(/<%/);
      }
    });

    test('JavaScript files contain no server-side code', () => {
      const jsDir = path.join(homepageDir, 'js');
      const jsFiles = fs.readdirSync(jsDir).filter(f => f.endsWith('.js'));

      for (const jsFile of jsFiles) {
        const jsContent = fs.readFileSync(path.join(jsDir, jsFile), 'utf-8');
        expect(jsContent).not.toMatch(/<\?php/i);
        expect(jsContent).not.toMatch(/require\s*\(\s*['"]express['"]\)/);
        expect(jsContent).not.toMatch(/require\s*\(\s*['"]http['"]\)/);
      }
    });

    test('HTML uses relative paths for local assets', () => {
      const stylesheets = document.querySelectorAll('link[href]');
      const scripts = document.querySelectorAll('script[src]');
      const images = document.querySelectorAll('img[src]');

      // Check stylesheets
      for (const link of stylesheets) {
        const href = link.getAttribute('href');
        if (!href.startsWith('http')) {
          expect(href).not.toMatch(/^\/[^\/]/); // Not absolute root path
          expect(href).toMatch(/^(\.\/|\.\.\/|[a-zA-Z])/); // Relative path
        }
      }

      // Check scripts
      for (const script of scripts) {
        const src = script.getAttribute('src');
        if (src && !src.startsWith('http')) {
          expect(src).not.toMatch(/^\/[^\/]/);
          expect(src).toMatch(/^(\.\/|\.\.\/|[a-zA-Z])/);
        }
      }

      // Check local images (external badges are ok)
      for (const img of images) {
        const src = img.getAttribute('src');
        if (!src.startsWith('http')) {
          expect(src).not.toMatch(/^\/[^\/]/);
          expect(src).toMatch(/^(\.\/|\.\.\/|[a-zA-Z])/);
        }
      }
    });
  });

  describe('Test Case 7: Console Error Prevention', () => {
    test('All referenced CSS variables are defined', () => {
      const variablesPath = path.join(homepageDir, 'css/variables.css');
      const variablesContent = fs.readFileSync(variablesPath, 'utf-8');

      // Extract defined CSS variables
      const definedVars = new Set();
      const varDefPattern = /--([\w-]+)\s*:/g;
      let match;
      while ((match = varDefPattern.exec(variablesContent)) !== null) {
        definedVars.add('--' + match[1]);
      }

      // Check main stylesheet for var() references
      const stylesPath = path.join(homepageDir, 'css/styles.css');
      const stylesContent = fs.readFileSync(stylesPath, 'utf-8');

      const usedVarsPattern = /var\((--[\w-]+)\)/g;
      while ((match = usedVarsPattern.exec(stylesContent)) !== null) {
        const varName = match[1];
        expect(definedVars.has(varName)).toBe(true);
      }
    });

    test('JavaScript files have valid syntax structure', () => {
      const jsDir = path.join(homepageDir, 'js');
      const jsFiles = fs.readdirSync(jsDir).filter(f => f.endsWith('.js'));

      for (const jsFile of jsFiles) {
        const jsContent = fs.readFileSync(path.join(jsDir, jsFile), 'utf-8');

        // Basic syntax checks - balanced braces and parentheses
        const openBraces = (jsContent.match(/\{/g) || []).length;
        const closeBraces = (jsContent.match(/\}/g) || []).length;
        expect(openBraces).toBe(closeBraces);

        const openParens = (jsContent.match(/\(/g) || []).length;
        const closeParens = (jsContent.match(/\)/g) || []).length;
        expect(openParens).toBe(closeParens);

        const openBrackets = (jsContent.match(/\[/g) || []).length;
        const closeBrackets = (jsContent.match(/\]/g) || []).length;
        expect(openBrackets).toBe(closeBrackets);
      }
    });

    test('No broken element references in JavaScript', () => {
      const jsDir = path.join(homepageDir, 'js');
      const jsFiles = fs.readdirSync(jsDir).filter(f => f.endsWith('.js'));

      // Common ID selectors used in JS
      const idSelectors = [];
      for (const jsFile of jsFiles) {
        const jsContent = fs.readFileSync(path.join(jsDir, jsFile), 'utf-8');

        // Extract getElementById calls
        const getByIdPattern = /getElementById\s*\(\s*['"]([^'"]+)['"]\s*\)/g;
        let match;
        while ((match = getByIdPattern.exec(jsContent)) !== null) {
          idSelectors.push(match[1]);
        }

        // Extract querySelector with IDs
        const querySelectorPattern = /querySelector\s*\(\s*['"]#([^'"]+)['"]\s*\)/g;
        while ((match = querySelectorPattern.exec(jsContent)) !== null) {
          idSelectors.push(match[1]);
        }
      }

      // Verify each ID exists in HTML
      for (const id of idSelectors) {
        const element = document.getElementById(id);
        expect(element).toBeTruthy();
      }
    });

    test('HTML has no duplicate IDs', () => {
      const elementsWithId = document.querySelectorAll('[id]');
      const ids = new Set();
      const duplicates = [];

      for (const el of elementsWithId) {
        const id = el.id;
        if (ids.has(id)) {
          duplicates.push(id);
        }
        ids.add(id);
      }

      expect(duplicates).toHaveLength(0);
    });

    test('All internal href targets exist', () => {
      const internalLinks = document.querySelectorAll('a[href^="#"]');

      for (const link of internalLinks) {
        const href = link.getAttribute('href');
        if (href !== '#') {
          const targetId = href.slice(1);
          const target = document.getElementById(targetId);
          expect(target).toBeTruthy();
        }
      }
    });
  });

  describe('Deployment Readiness', () => {
    test('Package.json has serve script', () => {
      const packageJsonPath = path.join(homepageDir, 'package.json');
      const packageJson = JSON.parse(fs.readFileSync(packageJsonPath, 'utf-8'));

      expect(packageJson.scripts).toBeDefined();
      expect(packageJson.scripts.serve || packageJson.scripts.start).toBeTruthy();
    });

    test('Folder structure is deployment-ready', () => {
      // Check essential folders exist
      expect(fs.existsSync(path.join(homepageDir, 'css'))).toBe(true);
      expect(fs.existsSync(path.join(homepageDir, 'js'))).toBe(true);
      expect(fs.existsSync(path.join(homepageDir, 'assets'))).toBe(true);
    });

    test('HTML file is the main entry point', () => {
      const indexPath = path.join(homepageDir, 'index.html');
      expect(fs.existsSync(indexPath)).toBe(true);

      // Verify it's an HTML file
      const content = fs.readFileSync(indexPath, 'utf-8');
      expect(content.trim().toLowerCase()).toMatch(/^<!doctype html>/);
    });
  });
});
