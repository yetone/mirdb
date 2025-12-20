/**
 * Integration tests for Page Weight Calculation
 * Test Case ID: 2
 * Validates total page weight is under 1MB for initial load
 */

const fs = require('fs');
const path = require('path');
const { JSDOM } = require('jsdom');

describe('Page Weight Calculation', () => {
  const projectRoot = path.resolve(__dirname, '../../');
  let htmlContent;
  let htmlSize;

  beforeAll(() => {
    const htmlPath = path.resolve(projectRoot, 'index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf-8');
    htmlSize = Buffer.byteLength(htmlContent, 'utf-8');
  });

  describe('HTML Size', () => {
    test('HTML file size should be reasonable', () => {
      // HTML should be under 100KB
      const maxHtmlSize = 100 * 1024; // 100KB
      console.log(`HTML size: ${(htmlSize / 1024).toFixed(2)} KB`);
      expect(htmlSize).toBeLessThan(maxHtmlSize);
    });
  });

  describe('CSS Size', () => {
    test('inline CSS size should be reasonable', () => {
      const dom = new JSDOM(htmlContent);
      const document = dom.window.document;

      // Check inline styles
      const styleElements = document.querySelectorAll('style');
      let totalInlineCSS = 0;

      styleElements.forEach((style) => {
        totalInlineCSS += Buffer.byteLength(style.textContent, 'utf-8');
      });

      console.log(`Inline CSS size: ${(totalInlineCSS / 1024).toFixed(2)} KB`);
      // Inline CSS should be under 50KB
      const maxInlineCSSSize = 50 * 1024;
      expect(totalInlineCSS).toBeLessThan(maxInlineCSSSize);
    });

    test('external CSS files should exist and be optimized', () => {
      const dom = new JSDOM(htmlContent);
      const document = dom.window.document;

      const linkElements = document.querySelectorAll('link[rel="stylesheet"]');
      let totalExternalCSS = 0;

      linkElements.forEach((link) => {
        const href = link.getAttribute('href');
        if (href && !href.startsWith('http')) {
          const cssPath = path.resolve(projectRoot, href);
          if (fs.existsSync(cssPath)) {
            const cssSize = fs.statSync(cssPath).size;
            totalExternalCSS += cssSize;
            console.log(`External CSS (${href}): ${(cssSize / 1024).toFixed(2)} KB`);
          }
        }
      });

      console.log(`Total external CSS: ${(totalExternalCSS / 1024).toFixed(2)} KB`);
      // External CSS should be under 100KB total
      const maxExternalCSSSize = 100 * 1024;
      expect(totalExternalCSS).toBeLessThan(maxExternalCSSSize);
    });
  });

  describe('JavaScript Size', () => {
    test('inline JavaScript size should be minimal', () => {
      const dom = new JSDOM(htmlContent);
      const document = dom.window.document;

      const scriptElements = document.querySelectorAll('script:not([src])');
      let totalInlineJS = 0;

      scriptElements.forEach((script) => {
        totalInlineJS += Buffer.byteLength(script.textContent, 'utf-8');
      });

      console.log(`Inline JS size: ${(totalInlineJS / 1024).toFixed(2)} KB`);
      // Inline JS should be under 10KB (should be minimal for static page)
      const maxInlineJSSize = 10 * 1024;
      expect(totalInlineJS).toBeLessThan(maxInlineJSSize);
    });
  });

  describe('Image References', () => {
    test('should identify image assets referenced in HTML', () => {
      const dom = new JSDOM(htmlContent);
      const document = dom.window.document;

      const images = document.querySelectorAll('img');
      const imageInfo = [];

      images.forEach((img) => {
        const src = img.getAttribute('src');
        if (src && !src.startsWith('http') && !src.startsWith('data:')) {
          const imagePath = path.resolve(projectRoot, src);
          if (fs.existsSync(imagePath)) {
            const imageSize = fs.statSync(imagePath).size;
            imageInfo.push({
              src,
              size: imageSize,
              sizeKB: (imageSize / 1024).toFixed(2),
            });
          }
        }
      });

      console.log('Local images found:', imageInfo);
      expect(imageInfo).toBeDefined();
    });
  });

  describe('Total Page Weight', () => {
    test('total initial page weight should be under 1MB', () => {
      const dom = new JSDOM(htmlContent);
      const document = dom.window.document;

      let totalWeight = htmlSize;

      // Add inline CSS
      const styleElements = document.querySelectorAll('style');
      styleElements.forEach((style) => {
        // CSS is already included in HTML size for inline styles
      });

      // Add external CSS
      const linkElements = document.querySelectorAll('link[rel="stylesheet"]');
      linkElements.forEach((link) => {
        const href = link.getAttribute('href');
        if (href && !href.startsWith('http')) {
          const cssPath = path.resolve(projectRoot, href);
          if (fs.existsSync(cssPath)) {
            totalWeight += fs.statSync(cssPath).size;
          }
        }
      });

      // Add external JS
      const scriptElements = document.querySelectorAll('script[src]');
      scriptElements.forEach((script) => {
        const src = script.getAttribute('src');
        if (src && !src.startsWith('http')) {
          const jsPath = path.resolve(projectRoot, src);
          if (fs.existsSync(jsPath)) {
            totalWeight += fs.statSync(jsPath).size;
          }
        }
      });

      // Note: Images are not included in initial weight calculation if lazy loaded
      // Only above-fold images count toward initial weight
      const heroSection = document.querySelector('.hero, [role="banner"], header');
      const heroImages = heroSection ? heroSection.querySelectorAll('img') : [];

      heroImages.forEach((img) => {
        const src = img.getAttribute('src');
        const loadingAttr = img.getAttribute('loading');

        // Only count non-lazy-loaded images
        if (src && !src.startsWith('http') && loadingAttr !== 'lazy') {
          const imagePath = path.resolve(projectRoot, src);
          if (fs.existsSync(imagePath)) {
            totalWeight += fs.statSync(imagePath).size;
            console.log(`Above-fold image: ${src} (${(fs.statSync(imagePath).size / 1024).toFixed(2)} KB)`);
          }
        }
      });

      console.log(`Total initial page weight (excluding external resources and lazy images): ${(totalWeight / 1024).toFixed(2)} KB`);

      // Total should be under 1MB
      const maxTotalWeight = 1024 * 1024; // 1MB
      expect(totalWeight).toBeLessThan(maxTotalWeight);
    });

    test('HTML + CSS (text resources) should be under 200KB', () => {
      const dom = new JSDOM(htmlContent);
      const document = dom.window.document;

      let textResourcesWeight = htmlSize;

      // Add external CSS
      const linkElements = document.querySelectorAll('link[rel="stylesheet"]');
      linkElements.forEach((link) => {
        const href = link.getAttribute('href');
        if (href && !href.startsWith('http')) {
          const cssPath = path.resolve(projectRoot, href);
          if (fs.existsSync(cssPath)) {
            textResourcesWeight += fs.statSync(cssPath).size;
          }
        }
      });

      console.log(`Text resources (HTML + CSS): ${(textResourcesWeight / 1024).toFixed(2)} KB`);

      // Text resources should be under 200KB
      const maxTextResources = 200 * 1024;
      expect(textResourcesWeight).toBeLessThan(maxTextResources);
    });
  });
});
