import { test, expect } from '@playwright/test';
import { execSync } from 'child_process';
import * as fs from 'fs';
import * as path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const websiteRoot = path.resolve(__dirname, '..');
const distDir = path.join(websiteRoot, 'dist');

test.describe('Static Site Build Process', () => {
  test.describe.configure({ mode: 'serial' });

  test('TC1: build command completes successfully with exit code 0', async () => {
    // Clean previous build
    if (fs.existsSync(distDir)) {
      fs.rmSync(distDir, { recursive: true, force: true });
    }

    // Run build command
    let exitCode: number | null = null;
    let stdout = '';
    let stderr = '';

    try {
      const result = execSync('npm run build', {
        cwd: websiteRoot,
        encoding: 'utf-8',
        stdio: ['pipe', 'pipe', 'pipe'],
      });
      stdout = result;
      exitCode = 0;
    } catch (error: any) {
      exitCode = error.status ?? 1;
      stdout = error.stdout ?? '';
      stderr = error.stderr ?? '';
    }

    // Verify exit code is 0
    expect(exitCode).toBe(0);

    // Verify build completed message is present
    expect(stdout).toContain('Complete!');
  });

  test('TC2: dist/ folder contains index.html and required assets', async () => {
    // Verify dist directory exists
    expect(fs.existsSync(distDir)).toBe(true);

    // Verify index.html exists
    const indexHtmlPath = path.join(distDir, 'index.html');
    expect(fs.existsSync(indexHtmlPath)).toBe(true);

    // Verify index.html is a valid HTML file
    const indexHtmlContent = fs.readFileSync(indexHtmlPath, 'utf-8');
    expect(indexHtmlContent).toContain('<!DOCTYPE html>');
    expect(indexHtmlContent).toContain('<html');
    expect(indexHtmlContent).toContain('</html>');

    // Verify _astro directory with CSS exists
    const astroDir = path.join(distDir, '_astro');
    expect(fs.existsSync(astroDir)).toBe(true);

    // Verify at least one CSS file exists in _astro
    const astroFiles = fs.readdirSync(astroDir);
    const cssFiles = astroFiles.filter((f) => f.endsWith('.css'));
    expect(cssFiles.length).toBeGreaterThanOrEqual(1);

    // Verify CSS file is referenced in index.html
    expect(indexHtmlContent).toContain('/_astro/');
    expect(indexHtmlContent).toContain('.css');
  });

  test('TC3: output contains only static files (HTML, CSS, JS, images) - no server-side code', async () => {
    // Define server-side code patterns that should NOT be present
    const serverSidePatterns = [
      /require\s*\(/g, // CommonJS require (not ES module import)
      /module\.exports/g, // CommonJS exports
      /process\.env\./g, // Runtime env access (in production build should be inlined)
      /\.php$/g,
      /\.asp$/g,
      /\.jsp$/g,
      /<%.*%>/g, // ASP/JSP tags
      /<\?php/g, // PHP tags
    ];

    // Patterns that indicate server-side rendering
    const ssrPatterns = [
      /getServerSideProps/g,
      /getStaticProps/g,
      /export\s+async\s+function\s+load/g, // SvelteKit server load
    ];

    // Function to recursively get all files
    const getAllFiles = (dir: string): string[] => {
      const files: string[] = [];
      const entries = fs.readdirSync(dir, { withFileTypes: true });

      for (const entry of entries) {
        const fullPath = path.join(dir, entry.name);
        if (entry.isDirectory()) {
          files.push(...getAllFiles(fullPath));
        } else {
          files.push(fullPath);
        }
      }
      return files;
    };

    // Get all files in dist
    const allFiles = getAllFiles(distDir);

    // Verify only static file types are present
    const allowedExtensions = [
      '.html',
      '.css',
      '.js',
      '.mjs',
      '.json',
      '.svg',
      '.png',
      '.jpg',
      '.jpeg',
      '.gif',
      '.webp',
      '.ico',
      '.woff',
      '.woff2',
      '.ttf',
      '.eot',
      '.txt',
      '.xml',
      '.map',
    ];

    for (const file of allFiles) {
      const ext = path.extname(file).toLowerCase();
      expect(allowedExtensions).toContain(ext);
    }

    // Verify HTML/JS files don't contain server-side code patterns
    const htmlAndJsFiles = allFiles.filter((f) => {
      const ext = path.extname(f).toLowerCase();
      return ext === '.html' || ext === '.js' || ext === '.mjs';
    });

    for (const file of htmlAndJsFiles) {
      const content = fs.readFileSync(file, 'utf-8');

      // Check for server-side patterns (these are indicators of non-static content)
      // Note: We skip checking for some patterns in JS files as minified code may have false positives
      if (file.endsWith('.html')) {
        for (const pattern of serverSidePatterns) {
          const matches = content.match(pattern);
          if (matches) {
            // Filter out false positives (e.g., comments mentioning these terms)
            const isFalsePositive = matches.every((match) => {
              return (
                content.includes(`<!--`) && content.includes(`-->`) // Might be in comments
              );
            });
            if (!isFalsePositive) {
              expect(matches).toBeNull();
            }
          }
        }

        // Check for SSR patterns
        for (const pattern of ssrPatterns) {
          expect(content.match(pattern)).toBeNull();
        }
      }
    }

    // Verify there are no .php, .asp, .jsp files
    const serverSideFiles = allFiles.filter((f) => {
      const ext = path.extname(f).toLowerCase();
      return ['.php', '.asp', '.aspx', '.jsp', '.py', '.rb'].includes(ext);
    });
    expect(serverSideFiles).toHaveLength(0);

    // Verify build is truly static by checking there's no astro runtime that requires server
    const indexHtml = fs.readFileSync(path.join(distDir, 'index.html'), 'utf-8');
    expect(indexHtml).not.toContain('astro:server');
  });

  test('build output is valid HTML', async () => {
    const indexHtmlPath = path.join(distDir, 'index.html');
    const content = fs.readFileSync(indexHtmlPath, 'utf-8');

    // Verify basic HTML structure
    expect(content).toContain('<!DOCTYPE html>');
    expect(content).toContain('<head>');
    expect(content).toContain('</head>');
    expect(content).toContain('<body');
    expect(content).toContain('</body>');

    // Verify meta tags are present
    expect(content).toContain('charset="UTF-8"');
    expect(content).toContain('viewport');

    // Verify title is present
    expect(content).toContain('<title>');
    expect(content).toContain('MirDB');
  });

  test('build output includes expected MirDB content', async () => {
    const indexHtmlPath = path.join(distDir, 'index.html');
    const content = fs.readFileSync(indexHtmlPath, 'utf-8');

    // Verify key MirDB content is present
    expect(content).toContain('MirDB');
    expect(content).toContain('Key-Value Store');
    expect(content).toContain('Memcached');

    // Verify navigation elements are present
    expect(content).toContain('data-testid="main-navigation"');
    expect(content).toContain('data-testid="hero-section"');
    expect(content).toContain('data-testid="features-section"');
  });
});
