/**
 * Asset Loading Tests
 * Owner: Scenario 15 - Asset Loading
 *
 * Tests:
 * - logo.gif loads
 * - usage.gif loads
 * - Tailwind CSS loads
 * - No broken images
 */
const { loadHTML } = require('../helpers/dom-utils');
const fs = require('fs');
const path = require('path');

describe('Asset Loading', () => {
  beforeEach(() => {
    loadHTML('index.html');
  });

  describe('Test Case 1: Logo image (logo.gif) loads without 404 error', () => {
    it('should have a logo image element on the page', () => {
      const logo = document.getElementById('logo');
      expect(logo).toBeInTheDocument();
    });

    it('should have the logo image with correct src path', () => {
      const logo = document.getElementById('logo');
      expect(logo).toHaveAttribute('src', 'assets/logo.gif');
    });

    it('should have the logo.gif file in the assets directory', () => {
      const logoPath = path.resolve(__dirname, '../../assets/logo.gif');
      expect(fs.existsSync(logoPath)).toBe(true);
    });

    it('should have logo file with non-zero size', () => {
      const logoPath = path.resolve(__dirname, '../../assets/logo.gif');
      const stats = fs.statSync(logoPath);
      expect(stats.size).toBeGreaterThan(0);
    });

    it('should have the logo image with alt text', () => {
      const logo = document.getElementById('logo');
      expect(logo).toHaveAttribute('alt');
      expect(logo.getAttribute('alt')).toBeTruthy();
    });

    it('should have the logo image in hero section', () => {
      const heroSection = document.getElementById('hero');
      const logoInHero = heroSection.querySelector('img[src*="logo"]');
      expect(logoInHero).toBeInTheDocument();
    });
  });

  describe('Test Case 2: Usage demo GIF (usage.gif) loads without 404 error', () => {
    it('should have a usage GIF element on the page', () => {
      const usageGif = document.getElementById('usage-gif');
      expect(usageGif).toBeInTheDocument();
    });

    it('should have the usage GIF with correct src path', () => {
      const usageGif = document.getElementById('usage-gif');
      expect(usageGif).toHaveAttribute('src', 'assets/usage.gif');
    });

    it('should have the usage.gif file in the assets directory', () => {
      const usagePath = path.resolve(__dirname, '../../assets/usage.gif');
      expect(fs.existsSync(usagePath)).toBe(true);
    });

    it('should have usage GIF file with non-zero size', () => {
      const usagePath = path.resolve(__dirname, '../../assets/usage.gif');
      const stats = fs.statSync(usagePath);
      expect(stats.size).toBeGreaterThan(0);
    });

    it('should have the usage GIF with alt text', () => {
      const usageGif = document.getElementById('usage-gif');
      expect(usageGif).toHaveAttribute('alt');
      expect(usageGif.getAttribute('alt')).toBeTruthy();
    });

    it('should have the usage GIF in demo section', () => {
      const demoSection = document.getElementById('demo');
      const usageInDemo = demoSection.querySelector('img[src*="usage"]');
      expect(usageInDemo).toBeInTheDocument();
    });
  });

  describe('Test Case 3: Tailwind CSS CDN loads or styles are applied', () => {
    it('should have a Tailwind CSS script tag', () => {
      const tailwindScript = document.querySelector('script[src*="tailwindcss"]');
      expect(tailwindScript).toBeInTheDocument();
    });

    it('should reference Tailwind CSS from a CDN', () => {
      const tailwindScript = document.querySelector('script[src*="tailwindcss"]');
      expect(tailwindScript.src).toMatch(/cdn\.tailwindcss\.com/);
    });

    it('should have Tailwind utility classes on elements', () => {
      // Check for common Tailwind classes in the document
      const elementsWithTailwind = document.querySelectorAll('[class*="bg-"], [class*="text-"], [class*="flex"], [class*="grid"]');
      expect(elementsWithTailwind.length).toBeGreaterThan(0);
    });

    it('should have Tailwind responsive classes', () => {
      const responsiveElements = document.querySelectorAll('[class*="md:"], [class*="lg:"], [class*="sm:"]');
      expect(responsiveElements.length).toBeGreaterThan(0);
    });

    it('should have body with Tailwind background class', () => {
      const body = document.body;
      expect(body.className).toMatch(/bg-gray-\d+/);
    });

    it('should have Tailwind spacing utilities', () => {
      const spacingElements = document.querySelectorAll('[class*="p-"], [class*="m-"], [class*="px-"], [class*="py-"]');
      expect(spacingElements.length).toBeGreaterThan(0);
    });
  });

  describe('Test Case 4: No images display broken image icons', () => {
    it('should have all images with valid src attributes', () => {
      const images = document.querySelectorAll('img');
      images.forEach(img => {
        expect(img).toHaveAttribute('src');
        expect(img.getAttribute('src')).not.toBe('');
      });
    });

    it('should have all image files exist on disk (local assets)', () => {
      const images = document.querySelectorAll('img[src^="assets/"]');
      images.forEach(img => {
        const imgSrc = img.getAttribute('src');
        const imgPath = path.resolve(__dirname, '../../', imgSrc);
        expect(fs.existsSync(imgPath)).toBe(true);
      });
    });

    it('should have all images with alt attributes for accessibility', () => {
      const images = document.querySelectorAll('img');
      images.forEach(img => {
        expect(img).toHaveAttribute('alt');
      });
    });

    it('should not have any empty image src attributes', () => {
      const images = document.querySelectorAll('img');
      images.forEach(img => {
        const src = img.getAttribute('src');
        expect(src).toBeTruthy();
        expect(src.trim()).not.toBe('');
      });
    });

    it('should have local GIF files be valid (non-zero size)', () => {
      const localImages = document.querySelectorAll('img[src^="assets/"]');
      localImages.forEach(img => {
        const imgSrc = img.getAttribute('src');
        const imgPath = path.resolve(__dirname, '../../', imgSrc);
        if (fs.existsSync(imgPath)) {
          const stats = fs.statSync(imgPath);
          expect(stats.size).toBeGreaterThan(0);
        }
      });
    });

    it('should have external images use HTTPS', () => {
      const externalImages = document.querySelectorAll('img[src^="http"]');
      externalImages.forEach(img => {
        const src = img.getAttribute('src');
        expect(src).toMatch(/^https:\/\//);
      });
    });
  });

  describe('Prism.js CDN resources', () => {
    it('should have Prism.js CSS for syntax highlighting', () => {
      const prismCSS = document.querySelector('link[href*="prism"]');
      expect(prismCSS).toBeInTheDocument();
    });

    it('should have Prism.js main script', () => {
      const prismScript = document.querySelector('script[src*="prism.min.js"]');
      expect(prismScript).toBeInTheDocument();
    });

    it('should have Prism.js bash component for code highlighting', () => {
      const prismBash = document.querySelector('script[src*="prism-bash"]');
      expect(prismBash).toBeInTheDocument();
    });

    it('should reference Prism.js from a CDN', () => {
      const prismScript = document.querySelector('script[src*="prism.min.js"]');
      expect(prismScript.src).toMatch(/cdnjs\.cloudflare\.com/);
    });
  });

  describe('Custom CSS asset', () => {
    it('should have custom CSS file linked', () => {
      const customCSS = document.querySelector('link[href*="custom.css"]');
      expect(customCSS).toBeInTheDocument();
    });

    it('should have custom CSS file exist', () => {
      const cssPath = path.resolve(__dirname, '../../assets/css/custom.css');
      expect(fs.existsSync(cssPath)).toBe(true);
    });
  });

  describe('JavaScript assets', () => {
    it('should have copy-code.js script linked', () => {
      const copyCodeScript = document.querySelector('script[src*="copy-code.js"]');
      expect(copyCodeScript).toBeInTheDocument();
    });

    it('should have main.js script linked', () => {
      const mainScript = document.querySelector('script[src*="main.js"]');
      expect(mainScript).toBeInTheDocument();
    });

    it('should have copy-code.js file exist', () => {
      const jsPath = path.resolve(__dirname, '../../assets/js/copy-code.js');
      expect(fs.existsSync(jsPath)).toBe(true);
    });

    it('should have main.js file exist', () => {
      const jsPath = path.resolve(__dirname, '../../assets/js/main.js');
      expect(fs.existsSync(jsPath)).toBe(true);
    });
  });

  describe('Asset path consistency', () => {
    it('should have consistent asset path structure (assets/ prefix)', () => {
      const localLinks = document.querySelectorAll('link[href^="assets/"], script[src^="assets/"], img[src^="assets/"]');
      localLinks.forEach(element => {
        const path = element.getAttribute('href') || element.getAttribute('src');
        expect(path).toMatch(/^assets\//);
      });
    });

    it('should not have absolute paths for local assets', () => {
      const images = document.querySelectorAll('img');
      images.forEach(img => {
        const src = img.getAttribute('src');
        // Local assets should not start with / or http for local files
        if (!src.startsWith('http')) {
          expect(src).not.toMatch(/^\//);
        }
      });
    });
  });
});
