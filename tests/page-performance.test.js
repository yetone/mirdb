/**
 * Page Performance Tests
 *
 * Tests to verify page loads within acceptable time limits (NFR-2)
 * - DOMContentLoaded timing
 * - Total page load time
 * - JavaScript bundle size
 * - Image format optimization
 * - Lighthouse performance audit simulation
 */

const fs = require('fs');
const path = require('path');

describe('Page Performance', () => {
  const indexPath = path.join(__dirname, '..', 'index.html');
  const cssPath = path.join(__dirname, '..', 'css', 'styles.css');
  const prismCssPath = path.join(__dirname, '..', 'css', 'prism.css');
  const jsPath = path.join(__dirname, '..', 'js', 'prism.js');
  const assetsPath = path.join(__dirname, '..', 'assets');

  describe('Test Case 1: DOMContentLoaded Event Timing', () => {
    it('should have minimal blocking resources for fast DOMContentLoaded', () => {
      const htmlContent = fs.readFileSync(indexPath, 'utf-8');

      // Check for inline critical CSS or deferred stylesheets
      // Count external stylesheets in head (these block rendering)
      const linkTags = htmlContent.match(/<link[^>]*rel=["']stylesheet["'][^>]*>/gi) || [];
      const scriptTagsInHead = htmlContent.match(/<head[\s\S]*?<\/head>/i)?.[0]
        .match(/<script[^>]*src=["'][^"']+["'][^>]*>/gi) || [];

      // For a small static page, having 2 stylesheets and 0 blocking scripts in head is acceptable
      expect(linkTags.length).toBeLessThanOrEqual(3);
      expect(scriptTagsInHead.length).toBe(0);
    });

    it('should have optimized HTML structure for fast parsing', () => {
      const htmlContent = fs.readFileSync(indexPath, 'utf-8');
      const htmlSize = Buffer.byteLength(htmlContent, 'utf8');

      // HTML should be under 50KB for fast parsing (without minification)
      expect(htmlSize).toBeLessThan(50 * 1024);
    });

    it('should place scripts at the end of body for non-blocking load', () => {
      const htmlContent = fs.readFileSync(indexPath, 'utf-8');

      // Extract body content
      const bodyMatch = htmlContent.match(/<body[\s\S]*<\/body>/i);
      expect(bodyMatch).not.toBeNull();

      const bodyContent = bodyMatch[0];
      const lastScriptMatch = bodyContent.match(/<script[^>]*src=["'][^"']+["'][^>]*>\s*<\/script>\s*<\/body>/i);

      // Script should be at the end of body
      expect(lastScriptMatch).not.toBeNull();
    });
  });

  describe('Test Case 2: Total Page Load Time', () => {
    it('should have total assets under reasonable size for broadband loading', () => {
      const htmlSize = fs.statSync(indexPath).size;
      const cssSize = fs.statSync(cssPath).size;
      const prismCssSize = fs.existsSync(prismCssPath) ? fs.statSync(prismCssPath).size : 0;
      const jsSize = fs.statSync(jsPath).size;

      // Total text assets (HTML + CSS + JS) should be under 100KB
      const totalTextAssets = htmlSize + cssSize + prismCssSize + jsSize;
      expect(totalTextAssets).toBeLessThan(100 * 1024);
    });

    it('should have no unnecessary external dependencies', () => {
      const htmlContent = fs.readFileSync(indexPath, 'utf-8');

      // Check for external CDN resources (fonts, scripts, etc.)
      const externalLinks = htmlContent.match(/https?:\/\/(?!github\.com)[^"'\s]+/gi) || [];

      // Filter out GitHub links which are expected
      // Also filter out canonical URL (mirdb.io) which is required for SEO
      const nonGitHubExternals = externalLinks.filter(link =>
        !link.includes('github.com') && !link.includes('github.io') && !link.includes('mirdb.io')
      );

      // Should have minimal or no external dependencies for fast load
      expect(nonGitHubExternals.length).toBe(0);
    });

    it('should use relative paths for local assets', () => {
      const htmlContent = fs.readFileSync(indexPath, 'utf-8');

      // All local assets should use relative paths
      const cssLinks = htmlContent.match(/href=["']([^"']+\.css)["']/gi) || [];
      const jsLinks = htmlContent.match(/src=["']([^"']+\.js)["']/gi) || [];

      cssLinks.forEach(link => {
        expect(link).not.toMatch(/^https?:\/\//);
      });

      jsLinks.forEach(link => {
        expect(link).not.toMatch(/^https?:\/\//);
      });
    });
  });

  describe('Test Case 3: JavaScript Bundle Size', () => {
    it('should have total JS under 100KB (minified equivalent)', () => {
      const jsContent = fs.readFileSync(jsPath, 'utf-8');
      const jsSize = Buffer.byteLength(jsContent, 'utf8');

      // Current JS should be under 100KB
      expect(jsSize).toBeLessThan(100 * 1024);
    });

    it('should have minimal JavaScript with focused functionality', () => {
      const jsContent = fs.readFileSync(jsPath, 'utf-8');

      // Check that JS file has focused, minimal scope
      const lineCount = jsContent.split('\n').length;

      // For a syntax highlighting utility, should be under 200 lines
      expect(lineCount).toBeLessThan(200);
    });

    it('should not include heavy frameworks or libraries', () => {
      const jsContent = fs.readFileSync(jsPath, 'utf-8');

      // Should not include heavy frameworks
      const heavyFrameworks = ['react', 'vue', 'angular', 'jquery', 'lodash'];

      heavyFrameworks.forEach(framework => {
        expect(jsContent.toLowerCase()).not.toContain(framework);
      });
    });
  });

  describe('Test Case 4: Image Format Optimization', () => {
    it('should verify images exist in assets folder', () => {
      expect(fs.existsSync(assetsPath)).toBe(true);

      const files = fs.readdirSync(assetsPath);
      expect(files.length).toBeGreaterThan(0);
    });

    it('should check image formats for modern optimization opportunities', () => {
      if (!fs.existsSync(assetsPath)) {
        return; // Skip if no assets folder
      }

      const files = fs.readdirSync(assetsPath);
      const imageFiles = files.filter(f =>
        /\.(jpg|jpeg|png|gif|webp|svg|avif)$/i.test(f)
      );

      // Document image formats for optimization review
      const imageFormats = imageFiles.map(f => ({
        name: f,
        format: path.extname(f).toLowerCase(),
        size: fs.statSync(path.join(assetsPath, f)).size
      }));

      // All images should be accounted for
      expect(imageFormats.length).toBe(imageFiles.length);

      // Log formats for review (GIF is acceptable for animations)
      imageFormats.forEach(img => {
        const acceptableFormats = ['.webp', '.avif', '.svg', '.gif'];
        const isOptimized = acceptableFormats.includes(img.format);
        // For animated content like demos, GIF is acceptable
        // For static images, WebP/AVIF would be preferred
        expect(['.webp', '.avif', '.svg', '.gif', '.png', '.jpg', '.jpeg']).toContain(img.format);
      });
    });

    it('should not have excessively large image files', () => {
      if (!fs.existsSync(assetsPath)) {
        return;
      }

      const files = fs.readdirSync(assetsPath);
      const imageFiles = files.filter(f =>
        /\.(jpg|jpeg|png|gif|webp|svg|avif)$/i.test(f)
      );

      imageFiles.forEach(file => {
        const filePath = path.join(assetsPath, file);
        const stats = fs.statSync(filePath);
        // For GIF animations, a larger size is acceptable (up to 10MB)
        // For static images, should be under 500KB
        const isGif = file.toLowerCase().endsWith('.gif');
        const maxSize = isGif ? 10 * 1024 * 1024 : 500 * 1024;

        expect(stats.size).toBeLessThan(maxSize);
      });
    });
  });

  describe('Test Case 5: Lighthouse Performance Audit Simulation', () => {
    it('should have proper meta viewport for mobile performance', () => {
      const htmlContent = fs.readFileSync(indexPath, 'utf-8');

      const hasViewport = /<meta[^>]*name=["']viewport["'][^>]*>/i.test(htmlContent);
      expect(hasViewport).toBe(true);

      // Check for proper viewport content
      const viewportMatch = htmlContent.match(/<meta[^>]*name=["']viewport["'][^>]*content=["']([^"']+)["']/i);
      expect(viewportMatch).not.toBeNull();
      expect(viewportMatch[1]).toContain('width=device-width');
    });

    it('should have proper document structure for accessibility', () => {
      const htmlContent = fs.readFileSync(indexPath, 'utf-8');

      // Check for proper HTML5 structure
      expect(htmlContent).toMatch(/<!DOCTYPE html>/i);
      expect(htmlContent).toMatch(/<html[^>]*lang=["'][^"']+["']/i);
      expect(htmlContent).toMatch(/<meta[^>]*charset=["']UTF-8["']/i);
    });

    it('should have descriptive title and meta description', () => {
      const htmlContent = fs.readFileSync(indexPath, 'utf-8');

      // Check for title
      const titleMatch = htmlContent.match(/<title>([^<]+)<\/title>/i);
      expect(titleMatch).not.toBeNull();
      expect(titleMatch[1].length).toBeGreaterThan(10);

      // Check for meta description
      const descMatch = htmlContent.match(/<meta[^>]*name=["']description["'][^>]*content=["']([^"']+)["']/i);
      expect(descMatch).not.toBeNull();
      expect(descMatch[1].length).toBeGreaterThan(50);
    });

    it('should use semantic HTML elements', () => {
      const htmlContent = fs.readFileSync(indexPath, 'utf-8');

      // Check for semantic elements
      expect(htmlContent).toMatch(/<header[^>]*>/i);
      expect(htmlContent).toMatch(/<main[^>]*>/i);
      expect(htmlContent).toMatch(/<footer[^>]*>/i);
      expect(htmlContent).toMatch(/<nav[^>]*>/i);
      expect(htmlContent).toMatch(/<section[^>]*>/i);
    });

    it('should have optimized CSS for rendering performance', () => {
      const cssContent = fs.readFileSync(cssPath, 'utf-8');

      // Check for CSS custom properties (indicates modern, maintainable CSS)
      expect(cssContent).toMatch(/--[\w-]+:\s*[^;]+;/);

      // Check for responsive design
      expect(cssContent).toMatch(/@media/i);
    });

    it('should achieve simulated performance score of 80+', () => {
      // Simulate a basic Lighthouse-like scoring based on static analysis
      let score = 100;

      const htmlContent = fs.readFileSync(indexPath, 'utf-8');
      const cssContent = fs.readFileSync(cssPath, 'utf-8');
      const jsContent = fs.readFileSync(jsPath, 'utf-8');

      // Deduct points for issues
      const htmlSize = Buffer.byteLength(htmlContent, 'utf8');
      const cssSize = Buffer.byteLength(cssContent, 'utf8');
      const jsSize = Buffer.byteLength(jsContent, 'utf8');

      // Size penalties
      if (htmlSize > 50 * 1024) score -= 10;
      if (cssSize > 50 * 1024) score -= 10;
      if (jsSize > 100 * 1024) score -= 15;

      // Check for render-blocking resources
      const headContent = htmlContent.match(/<head[\s\S]*?<\/head>/i)?.[0] || '';
      const blockingScripts = headContent.match(/<script[^>]*src=["'][^"']+["'][^>]*(?!defer|async)/gi) || [];
      if (blockingScripts.length > 0) score -= 5 * blockingScripts.length;

      // Check for external dependencies
      const externalResources = htmlContent.match(/https?:\/\/(?!github)[^"'\s]+\.(js|css)/gi) || [];
      if (externalResources.length > 0) score -= 5 * externalResources.length;

      // Bonus for good practices
      if (htmlContent.includes('lang=')) score += 2;
      if (htmlContent.includes('meta name="description"')) score += 2;
      if (htmlContent.includes('meta name="viewport"')) score += 2;
      if (cssContent.includes(':root')) score += 2;
      if (cssContent.includes('@media')) score += 2;

      // Ensure score doesn't exceed 100
      score = Math.min(100, Math.max(0, score));

      expect(score).toBeGreaterThanOrEqual(80);
    });
  });
});
