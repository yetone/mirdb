/**
 * Asset Validation Unit Tests
 * Owner: Scenario 12 - Performance Requirements
 *
 * Test coverage:
 * - CSS file size and minification
 * - Image optimization
 * - Asset file sizes
 */

import { describe, it, expect } from 'vitest';
import * as fs from 'fs';
import * as path from 'path';

const HOMEPAGE_DIR = path.resolve(__dirname, '../../homepage');

describe('Asset Validation', () => {
  describe('CSS Optimization', () => {
    it('CSS file is either minified or appropriately sized for inline inclusion', () => {
      const cssPath = path.join(HOMEPAGE_DIR, 'css/styles.css');
      const cssExists = fs.existsSync(cssPath);
      expect(cssExists).toBe(true);

      const cssContent = fs.readFileSync(cssPath, 'utf-8');
      const cssSize = Buffer.byteLength(cssContent, 'utf-8');

      // CSS should be under 50KB for reasonable inline consideration
      // or should show signs of being production-ready
      const maxCssSize = 50 * 1024; // 50KB
      expect(cssSize).toBeLessThan(maxCssSize);

      // Log the actual size for debugging
      console.log(`CSS file size: ${(cssSize / 1024).toFixed(2)} KB`);
    });

    it('CSS file contains valid CSS with no obvious errors', () => {
      const cssPath = path.join(HOMEPAGE_DIR, 'css/styles.css');
      const cssContent = fs.readFileSync(cssPath, 'utf-8');

      // Check for basic CSS structure
      expect(cssContent).toContain(':root');
      expect(cssContent).toContain('{');
      expect(cssContent).toContain('}');

      // Check that CSS variables are defined
      expect(cssContent).toContain('--color-primary');
      expect(cssContent).toContain('--font-family');

      // Check for balanced braces (basic validation)
      const openBraces = (cssContent.match(/{/g) || []).length;
      const closeBraces = (cssContent.match(/}/g) || []).length;
      expect(openBraces).toBe(closeBraces);
    });

    it('CSS uses CSS variables for maintainability', () => {
      const cssPath = path.join(HOMEPAGE_DIR, 'css/styles.css');
      const cssContent = fs.readFileSync(cssPath, 'utf-8');

      // Check for CSS variable definitions
      const varDefinitions = cssContent.match(/--[\w-]+:/g) || [];
      expect(varDefinitions.length).toBeGreaterThan(5);

      // Check for CSS variable usage
      const varUsages = cssContent.match(/var\(--[\w-]+\)/g) || [];
      expect(varUsages.length).toBeGreaterThan(10);

      console.log(`CSS variables defined: ${varDefinitions.length}`);
      console.log(`CSS variable usages: ${varUsages.length}`);
    });
  });

  describe('HTML Optimization', () => {
    it('HTML file is appropriately sized', () => {
      const htmlPath = path.join(HOMEPAGE_DIR, 'index.html');
      const htmlExists = fs.existsSync(htmlPath);
      expect(htmlExists).toBe(true);

      const htmlContent = fs.readFileSync(htmlPath, 'utf-8');
      const htmlSize = Buffer.byteLength(htmlContent, 'utf-8');

      // HTML should be under 50KB
      const maxHtmlSize = 50 * 1024;
      expect(htmlSize).toBeLessThan(maxHtmlSize);

      console.log(`HTML file size: ${(htmlSize / 1024).toFixed(2)} KB`);
    });

    it('HTML uses semantic elements', () => {
      const htmlPath = path.join(HOMEPAGE_DIR, 'index.html');
      const htmlContent = fs.readFileSync(htmlPath, 'utf-8');

      // Check for semantic HTML elements
      expect(htmlContent).toContain('<header');
      expect(htmlContent).toContain('<main');
      expect(htmlContent).toContain('<footer');
      expect(htmlContent).toContain('<nav');
      expect(htmlContent).toContain('<section');
    });
  });

  describe('JavaScript Optimization', () => {
    it('JavaScript file is minimal', () => {
      const jsPath = path.join(HOMEPAGE_DIR, 'js/main.js');
      const jsExists = fs.existsSync(jsPath);
      expect(jsExists).toBe(true);

      const jsContent = fs.readFileSync(jsPath, 'utf-8');
      const jsSize = Buffer.byteLength(jsContent, 'utf-8');

      // JavaScript should be minimal (under 10KB)
      // A static site should have very little JS
      const maxJsSize = 10 * 1024;
      expect(jsSize).toBeLessThan(maxJsSize);

      console.log(`JavaScript file size: ${(jsSize / 1024).toFixed(2)} KB`);
    });
  });

  describe('Image Assets', () => {
    it('Images directory exists with required assets', () => {
      const imagesDir = path.join(HOMEPAGE_DIR, 'assets/images');
      const imagesDirExists = fs.existsSync(imagesDir);
      expect(imagesDirExists).toBe(true);

      // Check for logo
      const logoPath = path.join(imagesDir, 'logo.gif');
      expect(fs.existsSync(logoPath)).toBe(true);

      // Check for usage demo
      const usagePath = path.join(imagesDir, 'usage.gif');
      expect(fs.existsSync(usagePath)).toBe(true);
    });

    it('Total text-based assets are under 100KB', () => {
      // Text assets: HTML, CSS, JS
      const htmlSize = fs.statSync(path.join(HOMEPAGE_DIR, 'index.html')).size;
      const cssSize = fs.statSync(path.join(HOMEPAGE_DIR, 'css/styles.css')).size;
      const jsSize = fs.statSync(path.join(HOMEPAGE_DIR, 'js/main.js')).size;

      const totalTextAssets = htmlSize + cssSize + jsSize;
      const maxTextAssetsSize = 100 * 1024; // 100KB

      expect(totalTextAssets).toBeLessThan(maxTextAssetsSize);

      console.log(`Total text assets: ${(totalTextAssets / 1024).toFixed(2)} KB`);
      console.log(`  HTML: ${(htmlSize / 1024).toFixed(2)} KB`);
      console.log(`  CSS: ${(cssSize / 1024).toFixed(2)} KB`);
      console.log(`  JS: ${(jsSize / 1024).toFixed(2)} KB`);
    });
  });

  describe('Performance Best Practices', () => {
    it('HTML includes viewport meta tag', () => {
      const htmlPath = path.join(HOMEPAGE_DIR, 'index.html');
      const htmlContent = fs.readFileSync(htmlPath, 'utf-8');

      expect(htmlContent).toContain('name="viewport"');
      expect(htmlContent).toContain('width=device-width');
    });

    it('CSS link is in the head section', () => {
      const htmlPath = path.join(HOMEPAGE_DIR, 'index.html');
      const htmlContent = fs.readFileSync(htmlPath, 'utf-8');

      // CSS should be linked before </head>
      const headEnd = htmlContent.indexOf('</head>');
      const cssLink = htmlContent.indexOf('rel="stylesheet"');

      expect(cssLink).toBeLessThan(headEnd);
      expect(cssLink).toBeGreaterThan(0);
    });

    it('JavaScript is loaded at end of body', () => {
      const htmlPath = path.join(HOMEPAGE_DIR, 'index.html');
      const htmlContent = fs.readFileSync(htmlPath, 'utf-8');

      // main.js should be near the end of body
      const bodyEnd = htmlContent.lastIndexOf('</body>');
      const mainJsLink = htmlContent.lastIndexOf('src="js/main.js"');

      // JS should be loaded close to end of body
      expect(mainJsLink).toBeGreaterThan(0);
      expect(mainJsLink).toBeLessThan(bodyEnd);
      expect(bodyEnd - mainJsLink).toBeLessThan(100); // Within 100 chars of </body>
    });

    it('Images use appropriate loading attributes', () => {
      const htmlPath = path.join(HOMEPAGE_DIR, 'index.html');
      const htmlContent = fs.readFileSync(htmlPath, 'utf-8');

      // Check that the demo/usage GIF has lazy loading
      expect(htmlContent).toContain('loading="lazy"');

      // Count lazy-loaded images
      const lazyImages = (htmlContent.match(/loading="lazy"/g) || []).length;
      console.log(`Images with lazy loading: ${lazyImages}`);
      expect(lazyImages).toBeGreaterThan(0);
    });
  });
});
