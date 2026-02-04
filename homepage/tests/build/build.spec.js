/**
 * Build and Deployment Tests
 * Owner: Scenario 13 - Static Site Build and Deployment
 *
 * Test coverage:
 * - Zola build completes successfully
 * - Build output structure is correct
 * - Base URL configuration works for GitHub Pages
 * - Assets are correctly generated
 * - No build warnings
 */

const { test, expect } = require('@playwright/test');
const { execSync, spawn } = require('child_process');
const fs = require('fs');
const path = require('path');

const HOMEPAGE_DIR = path.join(__dirname, '../..');
const PUBLIC_DIR = path.join(HOMEPAGE_DIR, 'public');
const CONFIG_FILE = path.join(HOMEPAGE_DIR, 'config.toml');
const ZOLA_BIN = path.join(HOMEPAGE_DIR, 'zola');

test.describe('Zola Build Process', () => {
    test.beforeAll(async () => {
        // Clean up any existing build
        if (fs.existsSync(PUBLIC_DIR)) {
            fs.rmSync(PUBLIC_DIR, { recursive: true, force: true });
        }
    });

    test('Test Case 1: zola build completes successfully with no errors', async () => {
        // Run zola build and capture output
        let buildOutput;
        let buildError;
        let exitCode;

        try {
            buildOutput = execSync(`${ZOLA_BIN} build`, {
                cwd: HOMEPAGE_DIR,
                encoding: 'utf-8',
                stdio: ['pipe', 'pipe', 'pipe']
            });
            exitCode = 0;
        } catch (error) {
            buildOutput = error.stdout;
            buildError = error.stderr;
            exitCode = error.status;
        }

        // Verify build succeeded
        expect(exitCode).toBe(0);
        expect(buildOutput).toContain('Building site...');
        expect(buildOutput).toContain('Done in');

        // Verify no error messages
        expect(buildOutput).not.toContain('Error');
        expect(buildOutput).not.toContain('error');
    });

    test('Test Case 2: public/ directory has correct structure after build', async () => {
        // Build first if not already built
        if (!fs.existsSync(PUBLIC_DIR)) {
            execSync(`${ZOLA_BIN} build`, { cwd: HOMEPAGE_DIR });
        }

        // Check public directory exists
        expect(fs.existsSync(PUBLIC_DIR)).toBe(true);

        // Check index.html exists and has content
        const indexPath = path.join(PUBLIC_DIR, 'index.html');
        expect(fs.existsSync(indexPath)).toBe(true);

        const indexContent = fs.readFileSync(indexPath, 'utf-8');
        expect(indexContent.length).toBeGreaterThan(1000);
        expect(indexContent).toContain('<!doctype html>');
        expect(indexContent).toContain('MirDB');

        // Check CSS directory and files
        const cssDir = path.join(PUBLIC_DIR, 'css');
        expect(fs.existsSync(cssDir)).toBe(true);

        const stylesPath = path.join(cssDir, 'styles.css');
        expect(fs.existsSync(stylesPath)).toBe(true);

        const stylesContent = fs.readFileSync(stylesPath, 'utf-8');
        expect(stylesContent.length).toBeGreaterThan(100);

        // Check JS directory and files
        const jsDir = path.join(PUBLIC_DIR, 'js');
        expect(fs.existsSync(jsDir)).toBe(true);

        const mainJsPath = path.join(jsDir, 'main.js');
        expect(fs.existsSync(mainJsPath)).toBe(true);

        // Check images directory and files
        const imagesDir = path.join(PUBLIC_DIR, 'images');
        expect(fs.existsSync(imagesDir)).toBe(true);

        const logoPath = path.join(imagesDir, 'logo.gif');
        expect(fs.existsSync(logoPath)).toBe(true);

        const architecturePath = path.join(imagesDir, 'architecture.svg');
        expect(fs.existsSync(architecturePath)).toBe(true);

        // Check sitemap.xml
        const sitemapPath = path.join(PUBLIC_DIR, 'sitemap.xml');
        expect(fs.existsSync(sitemapPath)).toBe(true);

        // Check robots.txt
        const robotsPath = path.join(PUBLIC_DIR, 'robots.txt');
        expect(fs.existsSync(robotsPath)).toBe(true);
    });

    test('Test Case 3: base URL is correctly configured for GitHub Pages', async () => {
        // Verify config.toml has correct base_url
        const configContent = fs.readFileSync(CONFIG_FILE, 'utf-8');
        expect(configContent).toContain('base_url');
        expect(configContent).toContain('https://yetone.github.io/mirdb');

        // Build and check index.html uses correct base URL
        if (!fs.existsSync(PUBLIC_DIR)) {
            execSync(`${ZOLA_BIN} build`, { cwd: HOMEPAGE_DIR });
        }

        const indexPath = path.join(PUBLIC_DIR, 'index.html');
        const indexContent = fs.readFileSync(indexPath, 'utf-8');

        // Check that CSS link uses correct base URL
        expect(indexContent).toContain('href=https://yetone.github.io/mirdb/css/styles.css');

        // Check that JS link uses correct base URL
        expect(indexContent).toContain('src=https://yetone.github.io/mirdb/js/main.js');

        // Check that image links use correct base URL
        expect(indexContent).toContain('src=https://yetone.github.io/mirdb/images/logo.gif');

        // Check Open Graph URL
        expect(indexContent).toContain('content=https://yetone.github.io/mirdb');
    });

    test('Test Case 5: build has no warnings about broken links or missing files', async () => {
        // Clean and rebuild to capture fresh output
        if (fs.existsSync(PUBLIC_DIR)) {
            fs.rmSync(PUBLIC_DIR, { recursive: true, force: true });
        }

        let buildOutput;
        try {
            buildOutput = execSync(`${ZOLA_BIN} build`, {
                cwd: HOMEPAGE_DIR,
                encoding: 'utf-8',
                stdio: ['pipe', 'pipe', 'pipe']
            });
        } catch (error) {
            buildOutput = error.stdout + (error.stderr || '');
        }

        // Check for no warnings (case insensitive check)
        expect(buildOutput.toLowerCase()).not.toContain('warning');
        expect(buildOutput.toLowerCase()).not.toContain('broken link');
        expect(buildOutput.toLowerCase()).not.toContain('missing file');

        // "0 orphan" is acceptable - only fail if there are actual orphan pages
        // The pattern "X orphan" where X > 0 would indicate a problem
        const orphanMatch = buildOutput.match(/(\d+)\s+orphan/);
        if (orphanMatch) {
            const orphanCount = parseInt(orphanMatch[1], 10);
            expect(orphanCount).toBe(0);
        }

        // Verify link checking passed
        expect(buildOutput).toContain('Successfully checked');
    });
});

test.describe('Build Configuration', () => {
    test('config.toml has required settings', async () => {
        const configContent = fs.readFileSync(CONFIG_FILE, 'utf-8');

        // Required settings for GitHub Pages
        expect(configContent).toContain('base_url');
        expect(configContent).toContain('title');
        expect(configContent).toContain('description');

        // Build optimization settings
        expect(configContent).toContain('minify_html = true');
        expect(configContent).toContain('build_search_index = false');

        // Check title contains MirDB
        expect(configContent).toMatch(/title\s*=.*MirDB/);
    });

    test('all required template files exist', async () => {
        const templatesDir = path.join(HOMEPAGE_DIR, 'templates');

        // Check base template
        expect(fs.existsSync(path.join(templatesDir, 'base.html'))).toBe(true);

        // Check section template (used by _index.md)
        expect(fs.existsSync(path.join(templatesDir, 'section.html'))).toBe(true);

        // Check all required partials
        const partialsDir = path.join(templatesDir, 'partials');
        const requiredPartials = [
            'hero.html',
            'features.html',
            'code-examples.html',
            'architecture.html',
            'roadmap.html',
            'footer.html',
            'navigation.html'
        ];

        for (const partial of requiredPartials) {
            const partialPath = path.join(partialsDir, partial);
            expect(fs.existsSync(partialPath)).toBe(true);
        }
    });

    test('content/_index.md exists and has valid frontmatter', async () => {
        const indexMdPath = path.join(HOMEPAGE_DIR, 'content', '_index.md');
        expect(fs.existsSync(indexMdPath)).toBe(true);

        const content = fs.readFileSync(indexMdPath, 'utf-8');

        // Check frontmatter exists
        expect(content).toContain('+++');
        expect(content).toContain('title');
        expect(content).toContain('template');
    });
});

test.describe('Build Output Validation', () => {
    test('generated HTML is valid and contains all sections', async () => {
        // Ensure build exists
        if (!fs.existsSync(PUBLIC_DIR)) {
            execSync(`${ZOLA_BIN} build`, { cwd: HOMEPAGE_DIR });
        }

        const indexPath = path.join(PUBLIC_DIR, 'index.html');
        const content = fs.readFileSync(indexPath, 'utf-8');

        // Check HTML structure (minified HTML may not have closing head tag explicitly)
        expect(content).toContain('<!doctype html>');
        expect(content).toMatch(/<html\s+lang=["']?en["']?>/);
        expect(content).toMatch(/<head>/);
        expect(content).toMatch(/<body/);

        // Check all required sections are present (allowing quoted or unquoted attributes)
        expect(content).toMatch(/id=["']?hero["']?/);
        expect(content).toMatch(/id=["']?features["']?/);
        expect(content).toMatch(/id=["']?code-examples["']?/);
        expect(content).toMatch(/id=["']?architecture["']?/);
        expect(content).toMatch(/id=["']?roadmap["']?/);
        expect(content).toMatch(/role=["']?contentinfo["']?/); // footer

        // Check meta tags (allowing quoted or unquoted attributes)
        expect(content).toMatch(/name=["']?description["']?/);
        expect(content).toMatch(/name=["']?viewport["']?/);
        expect(content).toMatch(/property=["']?og:title["']?/);
        expect(content).toMatch(/property=["']?og:description["']?/);
    });

    test('static assets are copied correctly', async () => {
        if (!fs.existsSync(PUBLIC_DIR)) {
            execSync(`${ZOLA_BIN} build`, { cwd: HOMEPAGE_DIR });
        }

        // Compare source and destination file sizes
        const sourceStatic = path.join(HOMEPAGE_DIR, 'static');

        // CSS
        const sourceCss = fs.statSync(path.join(sourceStatic, 'css', 'styles.css'));
        const destCss = fs.statSync(path.join(PUBLIC_DIR, 'css', 'styles.css'));
        expect(destCss.size).toBe(sourceCss.size);

        // JS
        const sourceJs = fs.statSync(path.join(sourceStatic, 'js', 'main.js'));
        const destJs = fs.statSync(path.join(PUBLIC_DIR, 'js', 'main.js'));
        expect(destJs.size).toBe(sourceJs.size);

        // Images
        const sourceLogo = fs.statSync(path.join(sourceStatic, 'images', 'logo.gif'));
        const destLogo = fs.statSync(path.join(PUBLIC_DIR, 'images', 'logo.gif'));
        expect(destLogo.size).toBe(sourceLogo.size);

        const sourceArch = fs.statSync(path.join(sourceStatic, 'images', 'architecture.svg'));
        const destArch = fs.statSync(path.join(PUBLIC_DIR, 'images', 'architecture.svg'));
        expect(destArch.size).toBe(sourceArch.size);
    });

    test('HTML is minified', async () => {
        if (!fs.existsSync(PUBLIC_DIR)) {
            execSync(`${ZOLA_BIN} build`, { cwd: HOMEPAGE_DIR });
        }

        const indexPath = path.join(PUBLIC_DIR, 'index.html');
        const content = fs.readFileSync(indexPath, 'utf-8');

        // Minified HTML has minimal whitespace - the first line should contain the bulk of content
        const lines = content.split('\n').filter(line => line.trim().length > 0);
        const firstLine = lines[0];

        // First line should be very long (most content on one line is a sign of minification)
        expect(firstLine.length).toBeGreaterThan(5000);

        // HTML should not have multiple spaces or tabs between tags (sign of unminified)
        // However, code blocks in <pre> may have intentional whitespace
        const htmlWithoutPre = content.replace(/<pre[^>]*>[\s\S]*?<\/pre>/g, '');
        expect(htmlWithoutPre).not.toMatch(/>\s{3,}</);

        // Should not have newlines between adjacent elements (outside of code blocks)
        // We check that the structure is compact
        expect(content).toContain('<!doctype html><html');
    });
});

test.describe('Development Server', () => {
    test('Test Case 4: zola serve starts development server', async () => {
        // Skip if running in CI without proper ports
        test.skip(!!process.env.CI, 'Skipping server test in CI');

        let serverProcess;
        let serverOutput = '';
        let serverStarted = false;

        try {
            // Start zola serve
            serverProcess = spawn(ZOLA_BIN, ['serve', '--port', '1112'], {
                cwd: HOMEPAGE_DIR,
                stdio: ['pipe', 'pipe', 'pipe']
            });

            // Collect output
            serverProcess.stdout.on('data', (data) => {
                serverOutput += data.toString();
                if (serverOutput.includes('listening on')) {
                    serverStarted = true;
                }
            });

            serverProcess.stderr.on('data', (data) => {
                serverOutput += data.toString();
                if (serverOutput.includes('listening on')) {
                    serverStarted = true;
                }
            });

            // Wait for server to start (up to 10 seconds)
            const startTime = Date.now();
            while (!serverStarted && Date.now() - startTime < 10000) {
                await new Promise(resolve => setTimeout(resolve, 100));
            }

            // Verify server started
            expect(serverStarted || serverOutput.includes('Web server')).toBe(true);

        } finally {
            // Clean up
            if (serverProcess) {
                serverProcess.kill('SIGTERM');
            }
        }
    });
});

test.describe('GitHub Pages Readiness', () => {
    test('build output is ready for GitHub Pages deployment', async () => {
        if (!fs.existsSync(PUBLIC_DIR)) {
            execSync(`${ZOLA_BIN} build`, { cwd: HOMEPAGE_DIR });
        }

        // Check that index.html is at the root (required for GitHub Pages)
        expect(fs.existsSync(path.join(PUBLIC_DIR, 'index.html'))).toBe(true);

        // Check 404.html exists (optional but good for GitHub Pages)
        expect(fs.existsSync(path.join(PUBLIC_DIR, '404.html'))).toBe(true);

        // Check no server-side files that would cause issues
        expect(fs.existsSync(path.join(PUBLIC_DIR, '.htaccess'))).toBe(false);
        expect(fs.existsSync(path.join(PUBLIC_DIR, 'web.config'))).toBe(false);

        // Verify no files start with underscore in public (Zola convention)
        const publicFiles = fs.readdirSync(PUBLIC_DIR);
        for (const file of publicFiles) {
            expect(file.startsWith('_')).toBe(false);
        }
    });
});
