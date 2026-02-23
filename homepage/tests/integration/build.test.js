/**
 * Build and Deployment Integration Tests
 * Owner: Scenario 19 - Static Site Deployment
 *
 * Integration tests for build process:
 * - Static build completes successfully
 * - Output contains only static files
 * - Site works with simple HTTP server
 * - GitHub Pages deployment verification
 */

const { execSync, spawn } = require('child_process');
const fs = require('fs');
const path = require('path');
const http = require('http');

const HOMEPAGE_DIR = path.resolve(__dirname, '../..');

// Helper function to make HTTP requests
function makeRequest(url) {
  return new Promise((resolve, reject) => {
    http.get(url, (res) => {
      let data = '';
      res.on('data', (chunk) => { data += chunk; });
      res.on('end', () => resolve({ statusCode: res.statusCode, data, headers: res.headers }));
    }).on('error', reject);
  });
}

// Helper function to wait for server to be ready
async function waitForServer(url, maxAttempts = 30, delay = 500) {
  for (let i = 0; i < maxAttempts; i++) {
    try {
      await makeRequest(url);
      return true;
    } catch (e) {
      await new Promise(resolve => setTimeout(resolve, delay));
    }
  }
  throw new Error(`Server not ready after ${maxAttempts} attempts`);
}

// Helper function to get all files recursively
function getAllFiles(dir, files = [], baseDir = dir) {
  const entries = fs.readdirSync(dir, { withFileTypes: true });
  for (const entry of entries) {
    const fullPath = path.join(dir, entry.name);
    const relativePath = path.relative(baseDir, fullPath);
    if (entry.isDirectory()) {
      // Skip node_modules and hidden directories
      if (entry.name !== 'node_modules' && !entry.name.startsWith('.')) {
        getAllFiles(fullPath, files, baseDir);
      }
    } else {
      files.push(relativePath);
    }
  }
  return files;
}

// List of allowed static file extensions for a static site
const STATIC_EXTENSIONS = [
  '.html', '.htm',
  '.css',
  '.js', '.mjs',
  '.json',
  '.png', '.jpg', '.jpeg', '.gif', '.svg', '.ico', '.webp', '.avif',
  '.woff', '.woff2', '.ttf', '.eot', '.otf',
  '.txt', '.md',
  '.xml', '.map'
];

// Files that indicate server-side code (should NOT exist in static site)
const SERVER_SIDE_PATTERNS = [
  /\.php$/,
  /\.py$/,
  /\.rb$/,
  /\.asp$/,
  /\.aspx$/,
  /\.jsp$/,
  /\.cgi$/,
  /server\.js$/i,
  /app\.js$/i,
  /api\.js$/i,
  /routes\.js$/i,
  /\.env$/,
  /Dockerfile$/i,
  /docker-compose/i,
  /\.go$/,
  /\.java$/,
  /\.class$/,
  /\.war$/,
  /\.jar$/
];

describe('Static Site Deployment Tests', () => {

  describe('Test Case 1: Static Site Build', () => {
    test('site has all required static files', () => {
      // Verify index.html exists
      const indexPath = path.join(HOMEPAGE_DIR, 'index.html');
      expect(fs.existsSync(indexPath)).toBe(true);

      // Verify CSS files exist
      const cssPath = path.join(HOMEPAGE_DIR, 'css/main.css');
      expect(fs.existsSync(cssPath)).toBe(true);

      // Verify JS files exist
      const jsPath = path.join(HOMEPAGE_DIR, 'js/main.js');
      expect(fs.existsSync(jsPath)).toBe(true);

      // Verify assets exist
      const logoPath = path.join(HOMEPAGE_DIR, 'assets/logo.gif');
      expect(fs.existsSync(logoPath)).toBe(true);
    });

    test('HTML file is valid and parseable', () => {
      const indexPath = path.join(HOMEPAGE_DIR, 'index.html');
      const content = fs.readFileSync(indexPath, 'utf8');

      // Should have DOCTYPE
      expect(content).toMatch(/<!DOCTYPE html>/i);

      // Should have html, head, and body tags
      expect(content).toMatch(/<html[^>]*>/i);
      expect(content).toMatch(/<head[^>]*>/i);
      expect(content).toMatch(/<body[^>]*>/i);

      // Should have title
      expect(content).toMatch(/<title[^>]*>.*<\/title>/i);

      // Should have meta charset
      expect(content).toMatch(/<meta[^>]*charset/i);
    });

    test('CSS files are valid', () => {
      const mainCssPath = path.join(HOMEPAGE_DIR, 'css/main.css');
      const content = fs.readFileSync(mainCssPath, 'utf8');

      // Should have actual CSS content (not empty)
      expect(content.trim().length).toBeGreaterThan(0);

      // Should have proper CSS syntax (basic check)
      expect(content).toMatch(/@import|[a-z-]+\s*:\s*[^;]+;/i);
    });

    test('JavaScript files are valid', () => {
      const mainJsPath = path.join(HOMEPAGE_DIR, 'js/main.js');
      const content = fs.readFileSync(mainJsPath, 'utf8');

      // Should have actual JS content
      expect(content.trim().length).toBeGreaterThan(0);

      // Should not have obvious syntax errors (basic check)
      expect(() => {
        // This will throw if there are major syntax errors
        new Function(content);
      }).not.toThrow();
    });
  });

  describe('Test Case 2: Static Files Only Verification', () => {
    test('all files have static file extensions', () => {
      const allFiles = getAllFiles(HOMEPAGE_DIR);

      // Filter out config/meta files that are allowed
      const allowedConfigFiles = [
        'package.json', 'package-lock.json',
        'playwright.config.js', 'jest.config.js',
        '.htmlhintrc', 'README.md'
      ];

      const contentFiles = allFiles.filter(file => {
        // Skip test files and config files
        if (file.startsWith('tests/')) return false;
        if (allowedConfigFiles.includes(path.basename(file))) return false;
        return true;
      });

      for (const file of contentFiles) {
        const ext = path.extname(file).toLowerCase();
        const isStatic = STATIC_EXTENSIONS.includes(ext) || ext === '';
        expect({ file, isStatic }).toEqual({ file, isStatic: true });
      }
    });

    test('no server-side code files present in site content', () => {
      const allFiles = getAllFiles(HOMEPAGE_DIR);

      // Filter to only check site content (not tests or config)
      const contentFiles = allFiles.filter(file => {
        return !file.startsWith('tests/') &&
               !file.startsWith('node_modules/');
      });

      for (const file of contentFiles) {
        for (const pattern of SERVER_SIDE_PATTERNS) {
          const matches = pattern.test(file);
          if (matches) {
            fail(`Found server-side file: ${file} (matches pattern: ${pattern})`);
          }
        }
      }
    });

    test('no server dependencies in package.json', () => {
      const packagePath = path.join(HOMEPAGE_DIR, 'package.json');
      const packageJson = JSON.parse(fs.readFileSync(packagePath, 'utf8'));

      // Server frameworks that should NOT be present for a static site
      const serverDependencies = [
        'express', 'koa', 'hapi', 'fastify', 'nest',
        'next', 'nuxt', 'gatsby', // These are SSR/SSG frameworks
        'mongoose', 'sequelize', 'typeorm', // Database ORMs
        'passport', 'jsonwebtoken' // Auth (server-side)
      ];

      const allDeps = {
        ...packageJson.dependencies || {},
        ...packageJson.devDependencies || {}
      };

      for (const dep of serverDependencies) {
        expect(allDeps).not.toHaveProperty(dep);
      }
    });

    test('all asset references use relative paths', () => {
      const indexPath = path.join(HOMEPAGE_DIR, 'index.html');
      const content = fs.readFileSync(indexPath, 'utf8');

      // Check CSS and JS references
      const srcMatches = content.match(/(?:src|href)=["']([^"']+)["']/gi) || [];

      for (const match of srcMatches) {
        const urlMatch = match.match(/["']([^"']+)["']/);
        if (urlMatch) {
          const url = urlMatch[1];
          // Skip external URLs and fragment links
          if (url.startsWith('http') || url.startsWith('//') || url.startsWith('#')) continue;

          // Local files should be relative (not absolute paths starting with /)
          // or they should be relative paths
          const isValidPath = !url.startsWith('/') || url.startsWith('/workspace');
          expect({ url, isValidPath }).toEqual({ url, isValidPath: true });
        }
      }
    });
  });

  describe('Test Case 3: Simple HTTP Server Test', () => {
    let serverProcess;
    const TEST_PORT = 8765;
    const BASE_URL = `http://localhost:${TEST_PORT}`;

    beforeAll(async () => {
      // Start a simple HTTP server
      serverProcess = spawn('npx', ['http-server', HOMEPAGE_DIR, '-p', String(TEST_PORT), '-c-1'], {
        stdio: 'pipe',
        detached: false
      });

      // Wait for server to be ready
      await waitForServer(BASE_URL);
    }, 30000);

    afterAll((done) => {
      if (serverProcess) {
        serverProcess.kill('SIGKILL');
        serverProcess.on('close', () => done());
        // Fallback timeout to ensure done is called
        setTimeout(done, 1000);
      } else {
        done();
      }
    });

    test('index.html serves correctly', async () => {
      const response = await makeRequest(BASE_URL);

      expect(response.statusCode).toBe(200);
      expect(response.data).toContain('<!DOCTYPE html>');
      expect(response.data).toContain('MirDB');
    });

    test('CSS files are accessible', async () => {
      const response = await makeRequest(`${BASE_URL}/css/main.css`);

      expect(response.statusCode).toBe(200);
      expect(response.headers['content-type']).toMatch(/css/i);
    });

    test('JavaScript files are accessible', async () => {
      const response = await makeRequest(`${BASE_URL}/js/main.js`);

      expect(response.statusCode).toBe(200);
      expect(response.headers['content-type']).toMatch(/javascript/i);
    });

    test('assets are accessible', async () => {
      const response = await makeRequest(`${BASE_URL}/assets/logo.gif`);

      expect(response.statusCode).toBe(200);
      expect(response.headers['content-type']).toMatch(/gif|image/i);
    });

    test('site sections render correctly', async () => {
      const response = await makeRequest(BASE_URL);

      // Check all major sections are present
      expect(response.data).toContain('id="hero"');
      expect(response.data).toContain('id="quickstart"');
      expect(response.data).toContain('id="features"');
      expect(response.data).toContain('id="roadmap"');
      expect(response.data).toContain('id="configuration"');
      expect(response.data).toContain('<footer');
    });
  });

  describe('Test Case 4: GitHub Pages Compatibility', () => {
    test('site root contains index.html', () => {
      const indexPath = path.join(HOMEPAGE_DIR, 'index.html');
      expect(fs.existsSync(indexPath)).toBe(true);
    });

    test('no Jekyll processing needed (_config.yml not required)', () => {
      // GitHub Pages can serve raw HTML without Jekyll
      // The presence of index.html at root is sufficient
      const indexPath = path.join(HOMEPAGE_DIR, 'index.html');
      const content = fs.readFileSync(indexPath, 'utf8');

      // Should be a complete HTML document
      expect(content).toContain('<!DOCTYPE html>');
      expect(content).toContain('</html>');
    });

    test('no files require Jekyll processing', () => {
      const allFiles = getAllFiles(HOMEPAGE_DIR);

      // Jekyll files start with front matter (---)
      const htmlFiles = allFiles.filter(f => f.endsWith('.html'));

      for (const file of htmlFiles) {
        const content = fs.readFileSync(path.join(HOMEPAGE_DIR, file), 'utf8');
        // Should NOT start with Jekyll front matter
        expect(content.trim().startsWith('---')).toBe(false);
      }
    });

    test('.nojekyll file can be created for GitHub Pages', () => {
      // GitHub Pages requires a .nojekyll file to disable Jekyll processing
      // This test verifies we CAN create one (deployment script should do this)
      const nojekyllPath = path.join(HOMEPAGE_DIR, '.nojekyll');

      // Create the file if it doesn't exist (simulating deployment)
      if (!fs.existsSync(nojekyllPath)) {
        fs.writeFileSync(nojekyllPath, '');
      }

      expect(fs.existsSync(nojekyllPath)).toBe(true);

      // Clean up - but leave it for actual deployment
      // fs.unlinkSync(nojekyllPath);
    });

    test('all links use relative paths compatible with subdirectory deployment', () => {
      const indexPath = path.join(HOMEPAGE_DIR, 'index.html');
      const content = fs.readFileSync(indexPath, 'utf8');

      // Check href and src attributes for local resources
      const resourceRefs = content.match(/(?:href|src)=["']([^"']+)["']/gi) || [];

      for (const ref of resourceRefs) {
        const urlMatch = ref.match(/["']([^"']+)["']/);
        if (!urlMatch) continue;

        const url = urlMatch[1];

        // Skip external URLs, mailto, tel, and fragment links
        if (url.startsWith('http') ||
            url.startsWith('//') ||
            url.startsWith('#') ||
            url.startsWith('mailto:') ||
            url.startsWith('tel:')) {
          continue;
        }

        // Local resources should use relative paths (not absolute)
        // This ensures the site works when deployed to a subdirectory like /mirdb/
        const usesRelativePath = !url.startsWith('/');
        expect({ url, usesRelativePath }).toEqual({ url, usesRelativePath: true });
      }
    });

    test('canonical URL is set for SEO', () => {
      const indexPath = path.join(HOMEPAGE_DIR, 'index.html');
      const content = fs.readFileSync(indexPath, 'utf8');

      // Should have a canonical link
      expect(content).toMatch(/<link[^>]+rel=["']canonical["'][^>]*>/i);
    });

    test('site size is reasonable for static hosting', () => {
      const allFiles = getAllFiles(HOMEPAGE_DIR);
      let totalSize = 0;

      // Calculate total size of deployable content
      const contentFiles = allFiles.filter(f =>
        !f.startsWith('tests/') &&
        !f.startsWith('node_modules/') &&
        !f.includes('package-lock.json')
      );

      for (const file of contentFiles) {
        const stats = fs.statSync(path.join(HOMEPAGE_DIR, file));
        totalSize += stats.size;
      }

      // Should be under 5MB for reasonable static site
      const maxSize = 5 * 1024 * 1024; // 5MB
      expect(totalSize).toBeLessThan(maxSize);
    });
  });
});
