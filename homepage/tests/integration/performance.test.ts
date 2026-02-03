/**
 * Performance and Loading integration tests.
 * Owner: Scenario 12 - Performance and Loading
 *
 * Tests validate:
 * - Build output structure for optimal performance
 * - Minimal JavaScript bundle size (islands architecture)
 * - CSS optimization patterns
 * - Performance-related code patterns (lazy loading, preconnect, etc.)
 */
import { describe, it, expect, beforeAll } from 'vitest';
import { readFileSync, readdirSync, existsSync, statSync } from 'fs';
import { join } from 'path';
import { execSync } from 'child_process';

const distDir = join(__dirname, '../../dist');
const srcDir = join(__dirname, '../../src');

describe('Performance and Loading', () => {
  describe('Test Case 1: Lighthouse Performance Optimization', () => {
    let htmlContent: string;
    let cssContent: string;

    beforeAll(() => {
      // Build if dist doesn't exist
      if (!existsSync(distDir)) {
        execSync('npm run build', { cwd: join(__dirname, '../..') });
      }
      htmlContent = readFileSync(join(distDir, 'index.html'), 'utf-8');
      const astroDir = join(distDir, '_astro');
      const cssFiles = readdirSync(astroDir).filter((f) => f.endsWith('.css'));
      cssContent = cssFiles
        .map((f) => readFileSync(join(astroDir, f), 'utf-8'))
        .join('\n');
    });

    it('should have proper meta viewport for mobile optimization', () => {
      expect(htmlContent).toContain(
        'name="viewport" content="width=device-width, initial-scale=1.0"'
      );
    });

    it('should have semantic HTML structure for SEO', () => {
      expect(htmlContent).toContain('<main');
      expect(htmlContent).toContain('</main>');
      expect(htmlContent).toContain('<footer');
      expect(htmlContent).toContain('</footer>');
    });

    it('should have proper document title', () => {
      expect(htmlContent).toContain('<title>');
      expect(htmlContent).toContain('</title>');
    });

    it('should have meta description for SEO', () => {
      expect(htmlContent).toContain('name="description"');
    });

    it('should have Open Graph meta tags', () => {
      expect(htmlContent).toContain('property="og:title"');
      expect(htmlContent).toContain('property="og:description"');
    });

    it('should use system fonts for fast rendering', () => {
      // Check for system font stack in CSS
      expect(cssContent).toMatch(/font-family.*system-ui|ui-sans-serif/);
    });
  });

  describe('Test Case 2: Page Load Time Optimization (NFR-1)', () => {
    let htmlContent: string;
    let htmlFileSize: number;
    let totalCssSize: number;
    let totalJsSize: number;

    beforeAll(() => {
      if (!existsSync(distDir)) {
        execSync('npm run build', { cwd: join(__dirname, '../..') });
      }

      htmlContent = readFileSync(join(distDir, 'index.html'), 'utf-8');
      htmlFileSize = statSync(join(distDir, 'index.html')).size;

      const astroDir = join(distDir, '_astro');
      const cssFiles = readdirSync(astroDir).filter((f) => f.endsWith('.css'));
      totalCssSize = cssFiles.reduce((sum, f) => {
        return sum + statSync(join(astroDir, f)).size;
      }, 0);

      const jsFiles = readdirSync(astroDir).filter((f) => f.endsWith('.js'));
      totalJsSize = jsFiles.reduce((sum, f) => {
        return sum + statSync(join(astroDir, f)).size;
      }, 0);
    });

    it('should have HTML file under 200KB for fast initial load', () => {
      const sizeInKB = htmlFileSize / 1024;
      expect(sizeInKB).toBeLessThan(200);
    });

    it('should have CSS under 50KB for fast styling', () => {
      const sizeInKB = totalCssSize / 1024;
      expect(sizeInKB).toBeLessThan(50);
    });

    it('should have minimal JavaScript (under 50KB or none)', () => {
      const sizeInKB = totalJsSize / 1024;
      expect(sizeInKB).toBeLessThan(50);
    });

    it('should use static HTML generation (no client-side rendering)', () => {
      // Verify the HTML contains actual content, not just a shell
      expect(htmlContent).toContain('MirDB');
      expect(htmlContent).toContain('Persistent Key-Value Store');
    });

    it('should inline critical dark mode script to prevent flash', () => {
      // Dark mode script should be inline in head for immediate execution
      expect(htmlContent).toContain("localStorage.getItem('theme')");
      expect(htmlContent).toContain('prefers-color-scheme');
    });
  });

  describe('Test Case 3: First Contentful Paint Optimization', () => {
    let htmlContent: string;
    let baseLayout: string;
    let globalCss: string;

    beforeAll(() => {
      if (!existsSync(distDir)) {
        execSync('npm run build', { cwd: join(__dirname, '../..') });
      }
      htmlContent = readFileSync(join(distDir, 'index.html'), 'utf-8');
      baseLayout = readFileSync(
        join(srcDir, 'layouts/BaseLayout.astro'),
        'utf-8'
      );
      globalCss = readFileSync(join(srcDir, 'styles/global.css'), 'utf-8');
    });

    it('should have CSS linked in head for early loading', () => {
      // CSS should be in the head, not at end of body
      const headMatch = htmlContent.match(/<head>[\s\S]*?<\/head>/i);
      expect(headMatch).toBeTruthy();
      const headContent = headMatch![0];
      expect(headContent).toMatch(/link.*rel="stylesheet"|<style/i);
    });

    it('should have above-the-fold content in initial HTML', () => {
      // Hero section content should be in initial HTML
      expect(htmlContent).toContain('Persistent Key-Value Store');
      expect(htmlContent).toContain('Get Started');
    });

    it('should have CSS custom properties for theme colors', () => {
      expect(globalCss).toContain(':root');
      expect(globalCss).toContain('--color-bg');
      expect(globalCss).toContain('--color-text');
    });

    it('should defer non-critical scripts', () => {
      // If there are any scripts, they should be deferred or inline
      // is:inline scripts in Astro are fine as they execute immediately
      // External scripts should have defer attribute
      const scriptMatches = htmlContent.match(
        /<script(?![^>]*is:inline)[^>]*src[^>]*>/gi
      );
      if (scriptMatches) {
        scriptMatches.forEach((script) => {
          expect(script).toMatch(/defer|async|type="module"/i);
        });
      }
    });

    it('should use efficient CSS selectors (no deep nesting)', () => {
      // Check that global CSS doesn't have overly complex selectors
      // Split by lines and check for deeply nested selectors
      const lines = globalCss.split('\n');
      lines.forEach((line) => {
        // Count spaces at start to check nesting (basic check)
        const selectorDepthIndicators = (line.match(/>/g) || []).length;
        expect(selectorDepthIndicators).toBeLessThan(4);
      });
    });
  });

  describe('Test Case 4: Cumulative Layout Shift Optimization', () => {
    let htmlContent: string;
    let globalCss: string;
    let heroComponent: string;

    beforeAll(() => {
      if (!existsSync(distDir)) {
        execSync('npm run build', { cwd: join(__dirname, '../..') });
      }
      htmlContent = readFileSync(join(distDir, 'index.html'), 'utf-8');
      globalCss = readFileSync(join(srcDir, 'styles/global.css'), 'utf-8');
      heroComponent = readFileSync(
        join(srcDir, 'components/Hero.astro'),
        'utf-8'
      );
    });

    it('should have fixed dimensions or aspect ratio for images', () => {
      // Check that img tags have width/height attributes or are SVG
      const imgTags = htmlContent.match(/<img[^>]*>/gi) || [];
      imgTags.forEach((img) => {
        const hasDimensions =
          (img.includes('width') && img.includes('height')) ||
          img.includes('.svg') ||
          img.includes('class=') ||
          img.includes('style=');
        expect(hasDimensions).toBe(true);
      });
    });

    it('should use flex or grid layout for stable layout', () => {
      expect(htmlContent).toMatch(/flex|grid/i);
    });

    it('should have min-height on body to prevent collapse', () => {
      // BaseLayout should set min-height
      expect(htmlContent).toContain('min-h-screen');
    });

    it('should not have layout-shifting animations without user preference', () => {
      // Check for prefers-reduced-motion support
      expect(globalCss).toContain('prefers-reduced-motion');
    });

    it('should have font-display or system fonts to prevent FOIT', () => {
      // Using system fonts eliminates font loading issues
      expect(globalCss).toMatch(/ui-sans-serif|system-ui|-apple-system/);
    });
  });

  describe('Test Case 5: JavaScript Bundle Size (Islands Architecture)', () => {
    let astroConfig: string;
    let jsFiles: string[];
    let totalJsSize: number;

    beforeAll(() => {
      if (!existsSync(distDir)) {
        execSync('npm run build', { cwd: join(__dirname, '../..') });
      }
      astroConfig = readFileSync(
        join(__dirname, '../../astro.config.mjs'),
        'utf-8'
      );

      const astroDir = join(distDir, '_astro');
      jsFiles = existsSync(astroDir)
        ? readdirSync(astroDir).filter((f) => f.endsWith('.js'))
        : [];

      totalJsSize = jsFiles.reduce((sum, f) => {
        return sum + statSync(join(astroDir, f)).size;
      }, 0);
    });

    it('should use Astro static site generator', () => {
      expect(astroConfig).toContain('defineConfig');
      expect(astroConfig).toContain('astro/config');
    });

    it('should produce zero or minimal JavaScript files', () => {
      // Astro's islands architecture should produce minimal JS
      // Only interactive islands should have JS
      expect(jsFiles.length).toBeLessThanOrEqual(3);
    });

    it('should have total JS bundle under 20KB (or none)', () => {
      const sizeInKB = totalJsSize / 1024;
      expect(sizeInKB).toBeLessThan(20);
    });

    it('should use Tailwind for styling instead of CSS-in-JS', () => {
      expect(astroConfig).toContain('tailwind');
    });

    it('should not include heavy frameworks in client bundle', () => {
      // Check that no large framework code is shipped
      const htmlContent = readFileSync(join(distDir, 'index.html'), 'utf-8');
      expect(htmlContent).not.toContain('react');
      expect(htmlContent).not.toContain('__NUXT__');
      expect(htmlContent).not.toContain('__NEXT_DATA__');
    });
  });
});
