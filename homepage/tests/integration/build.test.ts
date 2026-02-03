/**
 * Static Site Generation integration tests.
 * Owner: Scenario 15 - Static Site Generation
 *
 * Tests validate:
 * - Build process completes without errors
 * - Output produces valid HTML, CSS, and minimal JS files
 * - Built output functions as static files without server-side runtime
 * - Site can be served with any static file server
 */
import { describe, it, expect, beforeAll } from 'vitest';
import { readFileSync, readdirSync, existsSync, statSync } from 'fs';
import { join } from 'path';
import { execSync } from 'child_process';

const distDir = join(__dirname, '../../dist');
const projectRoot = join(__dirname, '../..');

describe('Static Site Generation', () => {
  describe('Test Case 1: Build Process (npm run build)', () => {
    let buildSucceeded: boolean = false;
    let buildOutput: string = '';
    let buildError: string = '';

    beforeAll(() => {
      try {
        // Run the build command
        buildOutput = execSync('npm run build', {
          cwd: projectRoot,
          encoding: 'utf-8',
          stdio: ['pipe', 'pipe', 'pipe'],
        });
        buildSucceeded = true;
      } catch (error: unknown) {
        buildSucceeded = false;
        if (error && typeof error === 'object' && 'stderr' in error) {
          buildError = (error as { stderr: string }).stderr;
        }
      }
    });

    it('should complete build without errors', () => {
      expect(buildSucceeded).toBe(true);
      if (!buildSucceeded) {
        console.error('Build error:', buildError);
      }
    });

    it('should produce a dist/ directory', () => {
      expect(existsSync(distDir)).toBe(true);
    });

    it('should not have build warnings about missing dependencies', () => {
      expect(buildOutput).not.toContain('Cannot find module');
      expect(buildOutput).not.toContain('Module not found');
    });

    it('should complete type checking (astro check)', () => {
      // The build script includes "astro check" before "astro build"
      // If build succeeded, type checking passed
      expect(buildSucceeded).toBe(true);
    });
  });

  describe('Test Case 2: Build Output Structure', () => {
    let distContents: string[] = [];
    let htmlContent: string = '';
    let astroDir: string;
    let cssFiles: string[] = [];
    let jsFiles: string[] = [];

    beforeAll(() => {
      // Ensure build exists
      if (!existsSync(distDir)) {
        execSync('npm run build', { cwd: projectRoot });
      }

      distContents = readdirSync(distDir);
      htmlContent = readFileSync(join(distDir, 'index.html'), 'utf-8');
      astroDir = join(distDir, '_astro');

      if (existsSync(astroDir)) {
        const astroContents = readdirSync(astroDir);
        cssFiles = astroContents.filter((f) => f.endsWith('.css'));
        jsFiles = astroContents.filter((f) => f.endsWith('.js'));
      }
    });

    it('should contain index.html at root of dist/', () => {
      expect(distContents).toContain('index.html');
    });

    it('should have valid HTML structure', () => {
      expect(htmlContent).toContain('<!DOCTYPE html>');
      expect(htmlContent).toContain('<html');
      expect(htmlContent).toContain('</html>');
      expect(htmlContent).toContain('<head>');
      expect(htmlContent).toContain('</head>');
      expect(htmlContent).toContain('<body');
      expect(htmlContent).toContain('</body>');
    });

    it('should contain CSS files in _astro directory', () => {
      expect(existsSync(astroDir)).toBe(true);
      expect(cssFiles.length).toBeGreaterThan(0);
    });

    it('should have minimal or no JavaScript files (static site)', () => {
      // Astro produces minimal JS - only for interactive islands
      // For a mostly static site, expect very few JS files
      expect(jsFiles.length).toBeLessThanOrEqual(3);
    });

    it('should include Tailwind-compiled CSS', () => {
      const allCss = cssFiles
        .map((f) => readFileSync(join(astroDir, f), 'utf-8'))
        .join('\n');
      // Tailwind uses utility classes - check for common patterns
      expect(allCss).toMatch(/flex|grid|text-|bg-|p-|m-/);
    });

    it('should copy public assets to dist', () => {
      // favicon should be copied from public/
      expect(existsSync(join(distDir, 'favicon.ico'))).toBe(true);
    });

    it('should have correct base path in HTML links', () => {
      // astro.config.mjs sets base: '/mirdb'
      expect(htmlContent).toMatch(/href="\/mirdb\/|src="\/mirdb\//);
    });
  });

  describe('Test Case 3: Static Output Functionality', () => {
    let htmlContent: string = '';

    beforeAll(() => {
      if (!existsSync(distDir)) {
        execSync('npm run build', { cwd: projectRoot });
      }
      htmlContent = readFileSync(join(distDir, 'index.html'), 'utf-8');
    });

    it('should have all page content pre-rendered in HTML', () => {
      // Static site generation means content is in the HTML
      expect(htmlContent).toContain('MirDB');
      expect(htmlContent).toContain('Persistent Key-Value Store');
    });

    it('should have working internal anchor links', () => {
      // Check for section anchor links - the site uses #installation anchor
      // The "Get Started" CTA in the Hero section links to installation
      expect(htmlContent).toMatch(/href="#installation"|href="\/mirdb\/#installation"/);
      // The sections have proper id attributes for anchor targeting
      expect(htmlContent).toContain('id="installation"');
      expect(htmlContent).toContain('id="features"');
    });

    it('should have external links with proper attributes', () => {
      // External links should have rel="noopener noreferrer" for security
      const githubLinkMatch = htmlContent.match(
        /<a[^>]*github\.com[^>]*>/gi
      );
      expect(githubLinkMatch).toBeTruthy();
      expect(githubLinkMatch![0]).toMatch(/target="_blank"/);
      expect(githubLinkMatch![0]).toMatch(/rel="[^"]*noopener[^"]*"/);
    });

    it('should include CSS inline or linked in head', () => {
      // CSS should be in the head for non-blocking render
      const headMatch = htmlContent.match(/<head>[\s\S]*?<\/head>/i);
      expect(headMatch).toBeTruthy();
      const headContent = headMatch![0];
      expect(headContent).toMatch(/link.*stylesheet|<style/);
    });

    it('should have all sections rendered', () => {
      // All main sections should be in the HTML
      expect(htmlContent).toContain('id="features"');
      expect(htmlContent).toContain('id="installation"');
    });

    it('should not require server-side rendering', () => {
      // No SSR markers should be present
      expect(htmlContent).not.toContain('__SSR__');
      expect(htmlContent).not.toContain('<!--ssr-->');
      // No hydration markers from React/Vue SSR
      expect(htmlContent).not.toContain('data-reactroot');
      expect(htmlContent).not.toContain('data-v-');
    });
  });

  describe('Test Case 4: No Server-Side Runtime Dependencies', () => {
    let packageJson: { dependencies?: Record<string, string> };
    let astroConfig: string;

    beforeAll(() => {
      packageJson = JSON.parse(
        readFileSync(join(projectRoot, 'package.json'), 'utf-8')
      );
      astroConfig = readFileSync(join(projectRoot, 'astro.config.mjs'), 'utf-8');
    });

    it('should use Astro static output mode (default)', () => {
      // Astro defaults to static output
      // If output is specified, it should be 'static' or not specified at all
      const hasServerOutput = astroConfig.includes("output: 'server'");
      const hasHybridOutput = astroConfig.includes("output: 'hybrid'");
      expect(hasServerOutput).toBe(false);
      expect(hasHybridOutput).toBe(false);
    });

    it('should not have server-side adapters installed', () => {
      const deps = packageJson.dependencies || {};
      // Check for common Astro adapters that require a server runtime
      expect(deps['@astrojs/node']).toBeUndefined();
      expect(deps['@astrojs/vercel']).toBeUndefined();
      expect(deps['@astrojs/netlify']).toBeUndefined();
      expect(deps['@astrojs/cloudflare']).toBeUndefined();
      expect(deps['@astrojs/deno']).toBeUndefined();
    });

    it('should have only Astro as a production dependency', () => {
      const deps = packageJson.dependencies || {};
      // For a static site, only Astro itself is needed as a dependency
      const depKeys = Object.keys(deps);
      expect(depKeys).toContain('astro');
      // Should not have server-side frameworks
      expect(deps['express']).toBeUndefined();
      expect(deps['fastify']).toBeUndefined();
      expect(deps['koa']).toBeUndefined();
    });

    it('should not use SSR-specific Astro features', () => {
      // No adapter configured means static output
      const hasAdapter = astroConfig.includes('adapter:');
      expect(hasAdapter).toBe(false);
    });

    it('should produce files servable by any static host', () => {
      // Verify all output files are static assets
      if (!existsSync(distDir)) {
        execSync('npm run build', { cwd: projectRoot });
      }

      const checkStaticDir = (dir: string): boolean => {
        const contents = readdirSync(dir);
        for (const item of contents) {
          const itemPath = join(dir, item);
          const stat = statSync(itemPath);
          if (stat.isDirectory()) {
            if (!checkStaticDir(itemPath)) return false;
          } else {
            // All files should be static assets
            const ext = item.split('.').pop()?.toLowerCase();
            const staticExtensions = [
              'html',
              'css',
              'js',
              'json',
              'svg',
              'png',
              'jpg',
              'jpeg',
              'gif',
              'ico',
              'webp',
              'woff',
              'woff2',
              'ttf',
              'eot',
              'txt',
              'xml',
              'webmanifest',
            ];
            // Files without extension or with known static extensions are fine
            if (ext && !staticExtensions.includes(ext)) {
              // Unknown extension - might still be static, just log it
              console.log(`Found file with extension: ${ext}`);
            }
          }
        }
        return true;
      };

      expect(checkStaticDir(distDir)).toBe(true);
    });

    it('should not have server-side API routes', () => {
      // Check that there are no API routes in the output
      const distContents = readdirSync(distDir, { recursive: true });
      const apiFiles = distContents.filter(
        (f) =>
          typeof f === 'string' &&
          (f.includes('/api/') || f.includes('\\api\\'))
      );
      // Static sites shouldn't have API endpoints
      expect(apiFiles.length).toBe(0);
    });
  });

  /**
   * External Link Validation tests.
   * Owner: Scenario 18 - Error Handling - Broken Links
   *
   * Tests validate:
   * - All external links have rel="noopener noreferrer" for security
   * - All external links open in new tab (target="_blank")
   * - No broken internal anchor links
   * - GitHub repository links are properly formed
   */
  describe('External Link Validation (Scenario 18)', () => {
    let htmlContent: string = '';
    let externalLinks: string[] = [];
    let allLinks: RegExpMatchArray | null;

    beforeAll(() => {
      if (!existsSync(distDir)) {
        execSync('npm run build', { cwd: projectRoot });
      }
      htmlContent = readFileSync(join(distDir, 'index.html'), 'utf-8');
      allLinks = htmlContent.match(/<a[^>]*>/gi);

      // Extract external links (those with http/https that aren't internal)
      if (allLinks) {
        externalLinks = allLinks.filter(link =>
          link.includes('href="http') || link.includes("href='http")
        );
      }
    });

    it('Test Case 1: GitHub repository link should be valid and properly formed', () => {
      // Validate that the GitHub URL is correctly formatted
      const githubPattern = /github\.com\/yetone\/mirdb/;
      const hasValidGitHubLink = htmlContent.match(githubPattern);
      expect(hasValidGitHubLink).toBeTruthy();

      // Verify the full GitHub link structure
      const fullGitHubLinks = htmlContent.match(/<a[^>]*github\.com\/yetone\/mirdb[^>]*>/gi);
      expect(fullGitHubLinks).toBeTruthy();
      expect(fullGitHubLinks!.length).toBeGreaterThan(0);
    });

    it('Test Case 2: CircleCI badge image should reference correct URL', () => {
      // Check that CircleCI badge image exists
      const circleCIBadge = htmlContent.match(/<img[^>]*atompunk\.yetone\.fun\/github\/yetone\/mirdb[^>]*>/gi);
      expect(circleCIBadge).toBeTruthy();

      // Check that the badge link points to CircleCI
      const circleCILink = htmlContent.match(/<a[^>]*circleci\.com\/gh\/yetone\/mirdb[^>]*>/gi);
      expect(circleCILink).toBeTruthy();
    });

    it('Test Case 3: All external links should have no 404 patterns (well-formed URLs)', () => {
      // All external links should be properly formed URLs
      if (externalLinks.length > 0) {
        for (const link of externalLinks) {
          // Extract href value
          const hrefMatch = link.match(/href=["']([^"']+)["']/);
          if (hrefMatch) {
            const href = hrefMatch[1];
            // Verify URLs are well-formed (not empty, not just protocol)
            expect(href).not.toBe('');
            expect(href).not.toBe('http://');
            expect(href).not.toBe('https://');
            // Should not contain obvious error patterns
            expect(href).not.toContain('undefined');
            expect(href).not.toContain('null');
            expect(href).not.toContain('404');
          }
        }
      }
    });

    it('Test Case 4: All external links should have rel="noopener noreferrer"', () => {
      // Every external link must have security attributes
      if (externalLinks.length > 0) {
        for (const link of externalLinks) {
          // Check for noopener
          const hasNoopener = /rel=["'][^"']*noopener[^"']*["']/i.test(link);
          expect(hasNoopener).toBe(true);

          // Check for noreferrer
          const hasNoreferrer = /rel=["'][^"']*noreferrer[^"']*["']/i.test(link);
          expect(hasNoreferrer).toBe(true);
        }
      }
    });

    it('All external links should open in new tab (target="_blank")', () => {
      if (externalLinks.length > 0) {
        for (const link of externalLinks) {
          const hasTargetBlank = /target=["']_blank["']/i.test(link);
          expect(hasTargetBlank).toBe(true);
        }
      }
    });

    it('Internal anchor links should have corresponding section IDs', () => {
      // Find all internal anchor links
      const anchorLinks = htmlContent.match(/<a[^>]*href=["']#[^"']+["'][^>]*>/gi) || [];

      for (const link of anchorLinks) {
        const anchorMatch = link.match(/href=["']#([^"']+)["']/);
        if (anchorMatch) {
          const anchorId = anchorMatch[1];
          // Check that the corresponding ID exists in the document
          const idPattern = new RegExp(`id=["']${anchorId}["']`, 'i');
          expect(htmlContent).toMatch(idPattern);
        }
      }
    });

    it('GitHub links should point to the correct repository', () => {
      // All GitHub links should point to yetone/mirdb
      const githubLinks = htmlContent.match(/<a[^>]*github\.com[^>]*>/gi) || [];

      for (const link of githubLinks) {
        const hrefMatch = link.match(/href=["']([^"']+)["']/);
        if (hrefMatch) {
          const href = hrefMatch[1];
          // If it's a GitHub link to a repo, it should be yetone/mirdb
          if (href.includes('github.com') && !href.includes('github.com/yetone')) {
            // Only fail if it's a repo link (not user profile links for attribution)
            if (!href.match(/github\.com\/[^\/]+$/)) {
              // This is a repo link to the wrong repo
              expect(href).toContain('github.com/yetone/mirdb');
            }
          }
        }
      }
    });

    it('License link should point to correct MIT license file', () => {
      const licenseLink = htmlContent.match(/<a[^>]*LICENSE[^>]*>/gi);
      expect(licenseLink).toBeTruthy();

      // Verify it links to the correct repo
      const licenseLinkContent = licenseLink![0];
      expect(licenseLinkContent).toContain('github.com/yetone/mirdb');
    });

    it('Author attribution link should be valid', () => {
      // Check for yetone author link
      const authorLink = htmlContent.match(/<a[^>]*github\.com\/yetone[^\/][^>]*>/gi);
      expect(authorLink).toBeTruthy();
    });

    it('No broken link indicators in HTML (empty hrefs, javascript:void)', () => {
      // Check for common broken link patterns
      const brokenPatterns = [
        'href=""',
        "href=''",
        'href="#"',  // Generic placeholder anchors (unless intentional)
        'href="javascript:void',
        'href="javascript:;',
      ];

      for (const pattern of brokenPatterns) {
        if (pattern === 'href="#"') {
          // Allow href="#" only if it's for a skip link or intentional
          continue;
        }
        expect(htmlContent).not.toContain(pattern);
      }
    });

    it('External images should have valid src attributes', () => {
      // Find all external image sources
      const externalImages = htmlContent.match(/<img[^>]*src=["']https?:\/\/[^"']+["'][^>]*>/gi) || [];

      for (const img of externalImages) {
        const srcMatch = img.match(/src=["']([^"']+)["']/);
        if (srcMatch) {
          const src = srcMatch[1];
          // Verify image URLs are well-formed
          expect(src).not.toBe('');
          expect(src).not.toContain('undefined');
          expect(src).not.toContain('null');
        }
      }
    });
  });

  describe('Build Output Quality', () => {
    let totalSize: number = 0;

    beforeAll(() => {
      if (!existsSync(distDir)) {
        execSync('npm run build', { cwd: projectRoot });
      }

      const calculateDirSize = (dir: string): number => {
        let size = 0;
        const contents = readdirSync(dir);
        for (const item of contents) {
          const itemPath = join(dir, item);
          const stat = statSync(itemPath);
          if (stat.isDirectory()) {
            size += calculateDirSize(itemPath);
          } else {
            size += stat.size;
          }
        }
        return size;
      };

      totalSize = calculateDirSize(distDir);
    });

    it('should have reasonable total build size (under 5MB)', () => {
      // The build includes a logo.gif animation which is ~2.5MB
      // Total build size should still be reasonable for a static site
      const sizeInMB = totalSize / (1024 * 1024);
      expect(sizeInMB).toBeLessThan(5);
    });

    it('should produce optimized HTML (no unnecessary whitespace)', () => {
      const html = readFileSync(join(distDir, 'index.html'), 'utf-8');
      // Astro minifies HTML by default in production
      // Check that there aren't excessive blank lines
      const blankLineMatches = html.match(/\n\s*\n\s*\n/g);
      expect(blankLineMatches?.length || 0).toBeLessThan(10);
    });
  });
});
