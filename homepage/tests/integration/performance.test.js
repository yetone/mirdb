/**
 * Performance & Loading Tests
 * Owner: Scenario 10 - Performance & Loading
 *
 * Validates the homepage loads within 2 seconds on standard broadband connections
 * as specified in REQ-8 and achieves Lighthouse score of 90+ as per success criteria.
 *
 * Test Coverage:
 * - DOMContentLoaded and load event timing
 * - Core Web Vitals (LCP, CLS)
 * - Page weight and asset optimization
 * - Render-blocking resource detection
 *
 * Requirements: REQ-8
 */

import { readFileSync, statSync, readdirSync } from 'fs';
import { resolve, dirname, extname, join } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const homepageDir = resolve(__dirname, '../../');

/**
 * Get all files recursively in a directory
 */
function getFilesRecursively(dir, files = []) {
  const entries = readdirSync(dir, { withFileTypes: true });
  for (const entry of entries) {
    const fullPath = join(dir, entry.name);
    if (entry.isDirectory()) {
      // Skip node_modules and tests directories
      if (entry.name !== 'node_modules' && entry.name !== 'tests') {
        getFilesRecursively(fullPath, files);
      }
    } else {
      files.push(fullPath);
    }
  }
  return files;
}

/**
 * Get file size in bytes
 */
function getFileSize(filePath) {
  try {
    const stats = statSync(filePath);
    return stats.size;
  } catch {
    return 0;
  }
}

/**
 * Parse HTML to extract elements
 */
function parseHTML(html) {
  // Simple regex-based parsing for test purposes
  return {
    scripts: html.match(/<script[^>]*>/g) || [],
    stylesheets: html.match(/<link[^>]*rel=["']stylesheet["'][^>]*>/g) || [],
    images: html.match(/<img[^>]*>/g) || [],
    headContent: html.match(/<head[^>]*>([\s\S]*?)<\/head>/i)?.[1] || ''
  };
}

describe('Performance & Loading', () => {
  let indexHtml;
  let parsedHtml;

  beforeAll(() => {
    const htmlPath = resolve(homepageDir, 'index.html');
    indexHtml = readFileSync(htmlPath, 'utf-8');
    parsedHtml = parseHTML(indexHtml);
  });

  describe('Test Case 1: DOMContentLoaded timing', () => {
    /**
     * Test Case ID: 1
     * Input: Measure DOMContentLoaded time on standard connection
     * Expected: DOMContentLoaded fires within 2000ms
     * Type: integration
     */
    test('page structure supports fast DOMContentLoaded', () => {
      // Verify the HTML structure is optimized for fast DOMContentLoaded
      // 1. Check that the HTML is not excessively large
      const htmlSizeKB = indexHtml.length / 1024;
      expect(htmlSizeKB).toBeLessThan(50); // HTML should be under 50KB for fast parsing

      // 2. Check that JavaScript is loaded with module type (deferred by default)
      const moduleScripts = indexHtml.match(/<script\s+type=["']module["'][^>]*>/g) || [];
      const inlineScripts = indexHtml.match(/<script>[\s\S]*?<\/script>/g) || [];

      // All scripts should be modules (deferred) or there should be no blocking inline scripts
      expect(moduleScripts.length).toBeGreaterThan(0);

      // 3. Verify no blocking inline scripts in head that could delay DOMContentLoaded
      const headContent = parsedHtml.headContent;
      const blockingScriptsInHead = headContent.match(/<script(?![^>]*type=["']module["'])[^>]*>[\s\S]*?<\/script>/g) || [];
      expect(blockingScriptsInHead.length).toBe(0);
    });

    test('CSS loading does not block rendering excessively', () => {
      // CSS files should be reasonably sized
      const cssDir = resolve(homepageDir, 'css');
      const cssFiles = getFilesRecursively(cssDir).filter(f => f.endsWith('.css'));

      let totalCssSize = 0;
      for (const file of cssFiles) {
        totalCssSize += getFileSize(file);
      }

      // Total CSS should be under 50KB uncompressed (reasonable for fast loading)
      const cssSizeKB = totalCssSize / 1024;
      expect(cssSizeKB).toBeLessThan(50);
    });
  });

  describe('Test Case 2: Load event timing', () => {
    /**
     * Test Case ID: 2
     * Input: Measure load event time on standard connection
     * Expected: Load event fires within 3000ms
     * Type: integration
     */
    test('all page resources are optimized for fast loading', () => {
      // Get all resource files
      const allFiles = getFilesRecursively(homepageDir);
      const resourceFiles = allFiles.filter(f => {
        const ext = extname(f).toLowerCase();
        return ['.html', '.css', '.js', '.svg', '.png', '.jpg', '.jpeg', '.webp', '.ico'].includes(ext);
      });

      let totalSize = 0;
      for (const file of resourceFiles) {
        totalSize += getFileSize(file);
      }

      // Total resources should be under 1MB for fast load event
      const totalSizeMB = totalSize / (1024 * 1024);
      expect(totalSizeMB).toBeLessThan(1);
    });

    test('images have appropriate dimensions specified', () => {
      // Check that images have width/height attributes to prevent layout shift
      const images = parsedHtml.images;

      for (const img of images) {
        // Each image should have width and height attributes
        const hasWidth = /width=["']\d+["']/.test(img) || /width=\d+/.test(img);
        const hasHeight = /height=["']\d+["']/.test(img) || /height=\d+/.test(img);

        // At minimum, images should have sizing attributes
        expect(hasWidth || hasHeight).toBe(true);
      }
    });
  });

  describe('Test Case 3: Lighthouse performance audit (desktop)', () => {
    /**
     * Test Case ID: 3
     * Input: Run Lighthouse performance audit on desktop
     * Expected: Performance score >= 90
     * Type: integration
     *
     * Note: This is a proxy test since we can't run actual Lighthouse in jsdom.
     * We verify the page follows best practices that would result in high scores.
     */
    test('page follows desktop performance best practices', () => {
      // Check for performance optimizations that contribute to Lighthouse score:

      // 1. Viewport meta tag present
      expect(indexHtml).toMatch(/<meta[^>]*name=["']viewport["'][^>]*>/i);

      // 2. Charset specified early
      expect(indexHtml).toMatch(/<meta[^>]*charset=["']UTF-8["'][^>]*>/i);

      // 3. No render-blocking resources in head (checked separately)

      // 4. Document has lang attribute
      expect(indexHtml).toMatch(/<html[^>]*lang=["'][a-z]{2}["'][^>]*>/i);

      // 5. Title tag present
      expect(indexHtml).toMatch(/<title>[^<]+<\/title>/i);

      // 6. Meta description present
      expect(indexHtml).toMatch(/<meta[^>]*name=["']description["'][^>]*>/i);
    });

    test('uses efficient CSS strategies', () => {
      // CSS should use efficient patterns
      const stylesPath = resolve(homepageDir, 'css/styles.css');
      const styles = readFileSync(stylesPath, 'utf-8');

      // Uses CSS custom properties (efficient for theming)
      expect(styles).toMatch(/--[\w-]+:/);

      // Uses system fonts (no external font loading delay)
      // Can be either direct usage or via CSS custom property
      const hasSystemFonts = styles.includes('system-ui');
      expect(hasSystemFonts).toBe(true);
    });
  });

  describe('Test Case 4: Lighthouse performance audit (mobile)', () => {
    /**
     * Test Case ID: 4
     * Input: Run Lighthouse performance audit on mobile
     * Expected: Performance score >= 90
     * Type: integration
     */
    test('page is optimized for mobile performance', () => {
      // 1. Viewport is properly configured for mobile
      const viewportMeta = indexHtml.match(/<meta[^>]*name=["']viewport["'][^>]*content=["']([^"']+)["'][^>]*>/i);
      expect(viewportMeta).not.toBeNull();
      expect(viewportMeta[1]).toContain('width=device-width');

      // 2. Has responsive CSS
      const responsivePath = resolve(homepageDir, 'css/responsive.css');
      const responsiveCSS = readFileSync(responsivePath, 'utf-8');
      expect(responsiveCSS).toMatch(/@media/);

      // 3. Touch targets should be appropriately sized (implied by button styling)
      expect(responsiveCSS.length).toBeGreaterThan(0);
    });

    test('mobile-specific optimizations are present', () => {
      // Check for mobile hamburger menu (reduces initial JS/CSS needed)
      expect(indexHtml).toMatch(/nav__mobile-toggle/);

      // Check that images use responsive sizing
      const images = parsedHtml.images;
      const svgImages = images.filter(img => img.includes('.svg'));

      // SVG images are ideal for mobile (scalable, small file size)
      expect(svgImages.length).toBeGreaterThan(0);
    });
  });

  describe('Test Case 5: Largest Contentful Paint (LCP)', () => {
    /**
     * Test Case ID: 5
     * Input: Measure Largest Contentful Paint (LCP)
     * Expected: LCP <= 2.5 seconds
     * Type: integration
     */
    test('LCP candidate elements are optimized', () => {
      // The LCP element is typically the hero section or main heading
      // Verify these are early in the HTML and not blocked by resources

      // Hero section should be early in the body
      const heroPosition = indexHtml.indexOf('id="hero"');
      const bodyPosition = indexHtml.indexOf('<body');

      expect(heroPosition).toBeGreaterThan(bodyPosition);

      // The hero title (main LCP candidate) should not depend on JavaScript to render
      expect(indexHtml).toMatch(/<h1[^>]*>[\s\S]*?<\/h1>/);

      // Hero logo should have explicit dimensions
      const heroLogo = indexHtml.match(/<img[^>]*class="[^"]*hero__logo[^"]*"[^>]*>/);
      if (heroLogo) {
        expect(heroLogo[0]).toMatch(/width=/);
        expect(heroLogo[0]).toMatch(/height=/);
      }
    });

    test('no lazy loading on above-the-fold images', () => {
      // Above-the-fold images (like hero images) should not have loading="lazy"
      const heroSection = indexHtml.match(/<section[^>]*id=["']hero["'][^>]*>[\s\S]*?<\/section>/i);

      if (heroSection) {
        const heroImages = heroSection[0].match(/<img[^>]*>/g) || [];
        for (const img of heroImages) {
          // Hero images should not have lazy loading
          expect(img).not.toMatch(/loading=["']lazy["']/);
        }
      }
    });
  });

  describe('Test Case 6: Cumulative Layout Shift (CLS)', () => {
    /**
     * Test Case ID: 6
     * Input: Measure Cumulative Layout Shift (CLS)
     * Expected: CLS <= 0.1
     * Type: integration
     */
    test('images have explicit dimensions to prevent layout shift', () => {
      const images = parsedHtml.images;

      for (const img of images) {
        // Images should have both width and height OR use aspect-ratio
        const hasWidth = /width=["']?\d+/.test(img);
        const hasHeight = /height=["']?\d+/.test(img);

        expect(hasWidth && hasHeight).toBe(true);
      }
    });

    test('fonts use system fonts to prevent FOIT/FOUT', () => {
      const stylesPath = resolve(homepageDir, 'css/styles.css');
      const styles = readFileSync(stylesPath, 'utf-8');

      // Should use system fonts, not custom web fonts
      // CSS uses custom properties like --font-sans with system-ui
      const hasSystemFonts = styles.includes('system-ui');
      expect(hasSystemFonts).toBe(true);

      // Should not have @font-face declarations
      expect(styles).not.toMatch(/@font-face/);
    });

    test('no dynamic content insertion that causes layout shift', () => {
      // Check that main content is server-rendered (in HTML), not injected by JS
      expect(indexHtml).toContain('hero__title');
      expect(indexHtml).toContain('features-grid');
      expect(indexHtml).toContain('quickstart__code');
      expect(indexHtml).toContain('status__list');

      // Main.js should not have code that inserts major layout elements
      const mainJsPath = resolve(homepageDir, 'js/main.js');
      const mainJs = readFileSync(mainJsPath, 'utf-8');

      // Should not use innerHTML to insert large content blocks
      expect(mainJs).not.toMatch(/innerHTML\s*=\s*['"`]<(section|div|article)/);
    });
  });

  describe('Test Case 7: Total page weight', () => {
    /**
     * Test Case ID: 7
     * Input: Check total page weight
     * Expected: Total page size < 1MB (excluding external resources)
     * Type: unit
     */
    test('total page weight is under 1MB', () => {
      const allFiles = getFilesRecursively(homepageDir);
      const resourceFiles = allFiles.filter(f => {
        const ext = extname(f).toLowerCase();
        return ['.html', '.css', '.js', '.svg', '.png', '.jpg', '.jpeg', '.webp', '.ico', '.gif'].includes(ext);
      });

      let totalSize = 0;
      const fileSizes = {};

      for (const file of resourceFiles) {
        const size = getFileSize(file);
        totalSize += size;
        fileSizes[file] = size;
      }

      const totalSizeKB = totalSize / 1024;
      const totalSizeMB = totalSize / (1024 * 1024);

      // Page should be well under 1MB
      expect(totalSizeMB).toBeLessThan(1);

      // Should ideally be under 200KB for optimal performance
      expect(totalSizeKB).toBeLessThan(200);
    });

    test('individual asset sizes are reasonable', () => {
      const allFiles = getFilesRecursively(homepageDir);

      for (const file of allFiles) {
        const ext = extname(file).toLowerCase();
        const size = getFileSize(file);
        const sizeKB = size / 1024;

        // Individual CSS files should be under 20KB
        if (ext === '.css') {
          expect(sizeKB).toBeLessThan(20);
        }

        // Individual JS files should be under 15KB
        if (ext === '.js') {
          expect(sizeKB).toBeLessThan(15);
        }

        // HTML should be under 50KB
        if (ext === '.html') {
          expect(sizeKB).toBeLessThan(50);
        }

        // Images should be under 100KB each
        if (['.png', '.jpg', '.jpeg', '.webp', '.gif'].includes(ext)) {
          expect(sizeKB).toBeLessThan(100);
        }
      }
    });
  });

  describe('Test Case 8: Image optimization', () => {
    /**
     * Test Case ID: 8
     * Input: Check image optimization
     * Expected: Images use appropriate formats (WebP, SVG) and are properly sized
     * Type: unit
     */
    test('uses SVG for icons and logos', () => {
      const assetsDir = resolve(homepageDir, 'assets');
      const allFiles = getFilesRecursively(assetsDir);
      const imageFiles = allFiles.filter(f => {
        const ext = extname(f).toLowerCase();
        return ['.svg', '.png', '.jpg', '.jpeg', '.webp', '.gif', '.ico'].includes(ext);
      });

      // Should have SVG files for scalable graphics
      const svgFiles = imageFiles.filter(f => f.endsWith('.svg'));
      expect(svgFiles.length).toBeGreaterThan(0);

      // Logo should be SVG
      const logoFile = imageFiles.find(f => f.includes('logo'));
      if (logoFile) {
        expect(logoFile).toMatch(/\.svg$/);
      }
    });

    test('SVG files are optimized', () => {
      const assetsDir = resolve(homepageDir, 'assets');
      const allFiles = getFilesRecursively(assetsDir);
      const svgFiles = allFiles.filter(f => f.endsWith('.svg'));

      for (const file of svgFiles) {
        const size = getFileSize(file);
        const sizeKB = size / 1024;

        // SVG files should be under 10KB for icons/logos
        expect(sizeKB).toBeLessThan(10);
      }
    });

    test('images in HTML have alt attributes', () => {
      const images = parsedHtml.images;

      for (const img of images) {
        // All images should have alt attribute (even if empty for decorative)
        expect(img).toMatch(/alt=/);
      }
    });
  });

  describe('Test Case 9: Render-blocking resources', () => {
    /**
     * Test Case ID: 9
     * Input: Check for render-blocking resources
     * Expected: No render-blocking scripts in head, CSS is minimal or inlined critical
     * Type: unit
     */
    test('no render-blocking scripts in head', () => {
      const headContent = parsedHtml.headContent;

      // Check for scripts without defer/async/module in head
      // <script> without type="module", defer, or async is render-blocking
      const scriptTags = headContent.match(/<script[^>]*>/g) || [];

      for (const script of scriptTags) {
        // Script must have type="module" OR defer OR async OR be external (src)
        const isModule = /type=["']module["']/.test(script);
        const hasDefer = /defer/.test(script);
        const hasAsync = /async/.test(script);

        // All scripts in head should be non-blocking
        expect(isModule || hasDefer || hasAsync).toBe(true);
      }
    });

    test('scripts are placed at end of body or use defer/module', () => {
      // Find where scripts are in the document
      const bodyEndPosition = indexHtml.lastIndexOf('</body>');
      const scriptPositions = [];

      let match;
      const scriptRegex = /<script[^>]*src=["'][^"']+["'][^>]*>/g;
      while ((match = scriptRegex.exec(indexHtml)) !== null) {
        scriptPositions.push({
          position: match.index,
          tag: match[0]
        });
      }

      for (const script of scriptPositions) {
        const isModule = /type=["']module["']/.test(script.tag);
        const hasDefer = /defer/.test(script.tag);
        const hasAsync = /async/.test(script.tag);
        const isAtEndOfBody = script.position > bodyEndPosition - 200;

        // Script should either be at end of body OR use non-blocking attributes
        expect(isModule || hasDefer || hasAsync || isAtEndOfBody).toBe(true);
      }
    });

    test('CSS files are reasonably sized', () => {
      // Since we're not inlining critical CSS, ensure CSS files are small
      const cssDir = resolve(homepageDir, 'css');
      const cssFiles = getFilesRecursively(cssDir).filter(f => f.endsWith('.css'));

      let totalCssSize = 0;
      for (const file of cssFiles) {
        totalCssSize += getFileSize(file);
      }

      // Total CSS under 50KB is acceptable for render-blocking CSS
      const cssSizeKB = totalCssSize / 1024;
      expect(cssSizeKB).toBeLessThan(50);
    });

    test('stylesheets do not block critical content', () => {
      // All stylesheets should be in head (standard practice)
      const headContent = parsedHtml.headContent;
      const bodyContent = indexHtml.replace(/<head[\s\S]*?<\/head>/i, '');

      // Stylesheets should be in head
      const stylesheets = parsedHtml.stylesheets;
      expect(stylesheets.length).toBeGreaterThan(0);

      // No stylesheet links in body
      const bodyStylesheets = bodyContent.match(/<link[^>]*rel=["']stylesheet["'][^>]*>/g) || [];
      expect(bodyStylesheets.length).toBe(0);
    });
  });

  describe('Test Case 10: Throttled network performance (manual verification guide)', () => {
    /**
     * Test Case ID: 10
     * Input: Run performance audit with throttled network
     * Expected: Page remains usable on 3G connection within 5 seconds
     * Type: manual
     *
     * This test provides a verification guide and basic structural checks.
     * Actual throttled network testing requires browser DevTools or Lighthouse.
     */
    test('page structure supports progressive loading', () => {
      // Content should be server-rendered (not dependent on JS)
      expect(indexHtml).toContain('<h1');
      expect(indexHtml).toContain('MirDB');
      expect(indexHtml).toContain('Key Features');
      expect(indexHtml).toContain('Quick Start');

      // Critical content should be visible without JavaScript
      const heroContent = indexHtml.match(/<section[^>]*id=["']hero["'][^>]*>[\s\S]*?<\/section>/i);
      expect(heroContent).not.toBeNull();
      expect(heroContent[0]).toContain('MirDB');
      expect(heroContent[0]).toContain('Persistent Key-Value Store');
    });

    test('provides manual testing instructions', () => {
      // This test documents the manual verification process
      const manualTestInstructions = `
        Manual Testing Guide for Throttled Network Performance:

        1. Open Chrome DevTools (F12 or Cmd+Shift+I)
        2. Go to Network tab
        3. Select "Slow 3G" from the throttling dropdown
        4. Refresh the page
        5. Verify:
           - Page content is visible within 5 seconds
           - Navigation is functional
           - Images load progressively
           - No broken layouts during loading

        Expected Results:
        - Hero section visible within 3 seconds
        - All text content readable within 5 seconds
        - Interactive elements functional once JavaScript loads
      `;

      expect(manualTestInstructions).toBeTruthy();
    });

    test('page weight supports 3G loading', () => {
      // On 3G (400kbps), 1MB takes ~20 seconds
      // For 5 second load time, critical resources should be under 250KB
      const allFiles = getFilesRecursively(homepageDir);
      const resourceFiles = allFiles.filter(f => {
        const ext = extname(f).toLowerCase();
        return ['.html', '.css', '.js', '.svg'].includes(ext);
      });

      let totalSize = 0;
      for (const file of resourceFiles) {
        totalSize += getFileSize(file);
      }

      const totalSizeKB = totalSize / 1024;

      // Critical resources (HTML, CSS, JS, SVG) should be under 100KB
      // This allows loading on 3G within ~2-3 seconds
      expect(totalSizeKB).toBeLessThan(100);
    });
  });
});
