// @ts-check
const { test, expect } = require('@playwright/test');
const fs = require('fs');
const path = require('path');

/**
 * Tests for Static Site Requirements (NFR-6)
 * Verifies the site has no server-side dependencies and can be hosted statically
 */

// Helper function to recursively get all files in a directory
function getAllFiles(dirPath, arrayOfFiles = []) {
    const files = fs.readdirSync(dirPath);

    files.forEach(function(file) {
        const fullPath = path.join(dirPath, file);
        if (fs.statSync(fullPath).isDirectory()) {
            // Skip hidden directories and node_modules
            if (!file.startsWith('.') && file !== 'node_modules') {
                getAllFiles(fullPath, arrayOfFiles);
            }
        } else {
            arrayOfFiles.push(fullPath);
        }
    });

    return arrayOfFiles;
}

// Define static file extensions that are allowed
const STATIC_FILE_EXTENSIONS = [
    '.html',
    '.css',
    '.js',
    '.json',
    '.svg',
    '.png',
    '.jpg',
    '.jpeg',
    '.gif',
    '.ico',
    '.webp',
    '.woff',
    '.woff2',
    '.ttf',
    '.eot',
    '.txt',
    '.md',
    '.toml',
    '.yml',
    '.yaml',
    '.rs',        // Rust source (not server-side for homepage)
    '.lock',      // Lock files
    '.sample'     // Git hook samples
];

// Server-side file extensions that should NOT exist for a static site
const SERVER_SIDE_EXTENSIONS = [
    '.php',
    '.phtml',
    '.php3',
    '.php4',
    '.php5',
    '.php7',
    '.phps',
    '.cgi',
    '.asp',
    '.aspx',
    '.jsp',
    '.jspx',
    '.cfm',
    '.rb',        // Ruby server files (when used as CGI)
    '.erb',       // ERB templates
    '.ejs',       // EJS templates
    '.pug',       // Pug templates
    '.hbs',       // Handlebars templates
    '.twig',      // Twig templates
    '.blade.php', // Laravel Blade
];

// Server-side configuration files that indicate server-side processing
const SERVER_SIDE_CONFIG_FILES = [
    'server.js',
    'server.ts',
    'app.js',
    'app.ts',
    'index.js',  // When it's a Node.js server entry point - we'll check content
    '.htaccess',
    'web.config',
    'nginx.conf',
    'apache.conf',
    'wsgi.py',
    'asgi.py',
    'manage.py',
    'requirements.txt',  // Python server deps - we'll check if it's server-related
    'Gemfile',           // Ruby server deps
    'config.ru',
    'Procfile',          // Heroku server deployment
    'docker-compose.yml', // Could indicate server deployment
    'Dockerfile',
    'serverless.yml',
    'vercel.json',       // Could indicate serverless functions
    'netlify.toml',      // Could indicate serverless functions
];

test.describe('Static Site Requirements (NFR-6)', () => {

    test('Test Case 1: Site contains only static files (HTML, CSS, JS, images, SVGs)', async () => {
        const projectRoot = path.resolve(__dirname, '../..');
        const allFiles = getAllFiles(projectRoot);

        // Filter out test files, config files, and Rust source code
        // The main homepage should be index.html, styles.css, main.js, and assets
        const homepageFiles = allFiles.filter(file => {
            const relativePath = path.relative(projectRoot, file);
            // Skip test directories, node_modules, .git, .something, and Rust source
            return !relativePath.startsWith('tests') &&
                   !relativePath.startsWith('node_modules') &&
                   !relativePath.startsWith('.git') &&
                   !relativePath.startsWith('.something') &&
                   !relativePath.startsWith('.circleci') &&
                   !relativePath.startsWith('mirdb-server') &&
                   !relativePath.startsWith('skip-list') &&
                   !relativePath.startsWith('sstable');
        });

        // Verify each file has a static file extension
        const nonStaticFiles = [];
        homepageFiles.forEach(file => {
            const ext = path.extname(file).toLowerCase();
            const basename = path.basename(file);

            // Allow known static file extensions
            const isStaticExtension = STATIC_FILE_EXTENSIONS.includes(ext);
            // Allow files without extension (like Dockerfile, Makefile, etc.)
            const isConfigFile = basename === 'Cargo.toml' ||
                                 basename === 'Cargo.lock' ||
                                 basename === 'package.json' ||
                                 basename === 'package-lock.json' ||
                                 basename === 'playwright.config.js' ||
                                 basename === '.gitignore' ||
                                 basename === 'rust-toolchain' ||
                                 basename === 'README.md';

            if (!isStaticExtension && !isConfigFile) {
                nonStaticFiles.push(file);
            }
        });

        // Verify the core static files exist
        const requiredFiles = [
            path.join(projectRoot, 'index.html'),
            path.join(projectRoot, 'styles.css'),
            path.join(projectRoot, 'main.js'),
        ];

        for (const requiredFile of requiredFiles) {
            expect(fs.existsSync(requiredFile),
                `Required static file missing: ${requiredFile}`).toBeTruthy();
        }

        // Verify assets directory has static files
        const assetsDir = path.join(projectRoot, 'assets');
        if (fs.existsSync(assetsDir)) {
            const assetFiles = fs.readdirSync(assetsDir);
            assetFiles.forEach(file => {
                const ext = path.extname(file).toLowerCase();
                const isStaticAsset = ['.gif', '.png', '.jpg', '.jpeg', '.svg', '.webp', '.ico'].includes(ext);
                expect(isStaticAsset, `Asset ${file} should be a static image file`).toBeTruthy();
            });
        }

        expect(nonStaticFiles.length,
            `Found non-static files: ${nonStaticFiles.join(', ')}`).toBe(0);
    });

    test('Test Case 2: Verify no server-side code (No PHP, Python, Node.js server files, or server-side templates)', async () => {
        const projectRoot = path.resolve(__dirname, '../..');
        const allFiles = getAllFiles(projectRoot);

        // Filter to homepage-relevant files
        const homepageFiles = allFiles.filter(file => {
            const relativePath = path.relative(projectRoot, file);
            return !relativePath.startsWith('tests') &&
                   !relativePath.startsWith('node_modules') &&
                   !relativePath.startsWith('.git') &&
                   !relativePath.startsWith('.something') &&
                   !relativePath.startsWith('.circleci') &&
                   !relativePath.startsWith('mirdb-server') &&
                   !relativePath.startsWith('skip-list') &&
                   !relativePath.startsWith('sstable');
        });

        const serverSideFiles = [];

        homepageFiles.forEach(file => {
            const ext = path.extname(file).toLowerCase();
            const basename = path.basename(file);
            const relativePath = path.relative(projectRoot, file);

            // Check for server-side extensions
            if (SERVER_SIDE_EXTENSIONS.includes(ext)) {
                serverSideFiles.push({ file: relativePath, reason: 'Server-side extension' });
            }

            // Check for server-side config/entry files in the root
            if (relativePath.split(path.sep).length === 1) {
                // Files in root directory
                if (basename === 'server.js' || basename === 'server.ts' ||
                    basename === 'app.js' || basename === 'app.ts') {
                    serverSideFiles.push({ file: relativePath, reason: 'Node.js server file' });
                }
                if (basename === 'wsgi.py' || basename === 'asgi.py' || basename === 'manage.py') {
                    serverSideFiles.push({ file: relativePath, reason: 'Python server file' });
                }
                if (basename === 'config.ru' || basename === 'Gemfile') {
                    serverSideFiles.push({ file: relativePath, reason: 'Ruby server file' });
                }
            }
        });

        // Also verify main.js is NOT a Node.js server
        const mainJsPath = path.join(projectRoot, 'main.js');
        if (fs.existsSync(mainJsPath)) {
            const mainJsContent = fs.readFileSync(mainJsPath, 'utf-8');
            const serverIndicators = [
                'require(\'express\')',
                'require("express")',
                'require(\'http\')',
                'require("http")',
                'require(\'https\')',
                'require("https")',
                'createServer',
                '.listen(',
                'import express',
                'import http',
                'import https',
            ];

            serverIndicators.forEach(indicator => {
                if (mainJsContent.includes(indicator)) {
                    serverSideFiles.push({ file: 'main.js', reason: `Contains server code: ${indicator}` });
                }
            });
        }

        // Verify index.html doesn't have server-side template syntax
        const indexHtmlPath = path.join(projectRoot, 'index.html');
        if (fs.existsSync(indexHtmlPath)) {
            const htmlContent = fs.readFileSync(indexHtmlPath, 'utf-8');
            const templateIndicators = [
                '<?php',
                '<?=',
                '<%',
                '%>',
                '{{%',
                '{%',
                '#{',    // Ruby ERB
                '<%= ',  // ERB/EJS
            ];

            templateIndicators.forEach(indicator => {
                if (htmlContent.includes(indicator)) {
                    // Exclude Mustache-style {{ which is used in some static site generators
                    // and SVG template patterns
                    if (indicator !== '{{%' && indicator !== '{%') {
                        serverSideFiles.push({ file: 'index.html', reason: `Contains template syntax: ${indicator}` });
                    }
                }
            });
        }

        expect(serverSideFiles.length,
            `Found server-side files: ${JSON.stringify(serverSideFiles, null, 2)}`).toBe(0);
    });

    test('Test Case 3: Site loads correctly when served via simple HTTP server', async ({ page }) => {
        // The playwright config already starts http-server
        // Navigate to the homepage
        await page.goto('/');

        // Wait for the page to fully load
        await page.waitForLoadState('networkidle');

        // Verify the page loaded successfully
        const title = await page.title();
        expect(title).toContain('MirDB');

        // Verify key sections are visible
        const heroSection = page.locator('.hero');
        await expect(heroSection).toBeVisible();

        const featuresSection = page.locator('#features');
        await expect(featuresSection).toBeVisible();

        const codeExamplesSection = page.locator('#code-examples');
        await expect(codeExamplesSection).toBeVisible();

        // Verify CSS is loaded (check for styled elements)
        const heroTitle = page.locator('.hero-title');
        await expect(heroTitle).toBeVisible();
        const fontFamily = await heroTitle.evaluate(el =>
            window.getComputedStyle(el).fontFamily
        );
        expect(fontFamily).toBeTruthy();

        // Verify JavaScript is loaded (copy buttons should exist)
        const copyButtons = page.locator('.copy-btn');
        const copyButtonCount = await copyButtons.count();
        expect(copyButtonCount).toBeGreaterThan(0);

        // Verify no errors in console (basic check)
        const errors = [];
        page.on('pageerror', err => errors.push(err.message));

        // Give a moment for any delayed errors
        await page.waitForTimeout(500);

        // We allow some expected errors but no critical ones
        const criticalErrors = errors.filter(err =>
            !err.includes('net::') && // Network errors from external resources are OK
            !err.includes('favicon')   // Missing favicon is OK
        );

        expect(criticalErrors.length,
            `Critical JavaScript errors: ${criticalErrors.join(', ')}`).toBe(0);
    });

    test('Test Case 4: All asset paths are relative or use consistent base URL', async ({ page }) => {
        await page.goto('/');
        await page.waitForLoadState('domcontentloaded');

        // Get all resource paths from the HTML
        const resourcePaths = await page.evaluate(() => {
            const paths = {
                stylesheets: [],
                scripts: [],
                images: [],
                links: [],
            };

            // Stylesheets
            document.querySelectorAll('link[rel="stylesheet"]').forEach(link => {
                paths.stylesheets.push(link.getAttribute('href'));
            });

            // Scripts
            document.querySelectorAll('script[src]').forEach(script => {
                paths.scripts.push(script.getAttribute('src'));
            });

            // Images
            document.querySelectorAll('img[src]').forEach(img => {
                paths.images.push(img.getAttribute('src'));
            });

            // Links (internal only)
            document.querySelectorAll('a[href]').forEach(a => {
                const href = a.getAttribute('href');
                if (href && !href.startsWith('http') && !href.startsWith('#') && !href.startsWith('mailto:')) {
                    paths.links.push(href);
                }
            });

            return paths;
        });

        // Check local stylesheets are relative
        const localStylesheets = resourcePaths.stylesheets.filter(href =>
            href && !href.startsWith('http://') && !href.startsWith('https://')
        );
        localStylesheets.forEach(href => {
            expect(href.startsWith('/') || !href.includes('://'),
                `Stylesheet ${href} should be relative`).toBeTruthy();
            expect(!href.startsWith('C:') && !href.startsWith('/Users/') && !href.startsWith('/home/'),
                `Stylesheet ${href} should not be an absolute local path`).toBeTruthy();
        });

        // Check local scripts are relative
        const localScripts = resourcePaths.scripts.filter(src =>
            src && !src.startsWith('http://') && !src.startsWith('https://')
        );
        localScripts.forEach(src => {
            expect(src.startsWith('/') || !src.includes('://'),
                `Script ${src} should be relative`).toBeTruthy();
            expect(!src.startsWith('C:') && !src.startsWith('/Users/') && !src.startsWith('/home/'),
                `Script ${src} should not be an absolute local path`).toBeTruthy();
        });

        // Check images are relative
        resourcePaths.images.forEach(src => {
            if (src && !src.startsWith('http://') && !src.startsWith('https://') && !src.startsWith('data:')) {
                expect(src.startsWith('/') || src.startsWith('./') || src.startsWith('assets/') || !src.includes('://'),
                    `Image ${src} should be relative`).toBeTruthy();
                expect(!src.startsWith('C:') && !src.startsWith('/Users/') && !src.startsWith('/home/'),
                    `Image ${src} should not be an absolute local path`).toBeTruthy();
            }
        });

        // Verify that CSS file references internal assets correctly
        const styleResponse = await page.request.get('/styles.css');
        if (styleResponse.ok()) {
            const cssContent = await styleResponse.text();

            // Check for absolute local paths in CSS
            const absolutePathPatterns = [
                /url\s*\(\s*['"]?(C:|\/Users\/|\/home\/)/gi,
                /url\s*\(\s*['"]?file:\/\//gi,
            ];

            absolutePathPatterns.forEach(pattern => {
                expect(cssContent).not.toMatch(pattern);
            });
        }

        // Verify that JS file doesn't have hardcoded absolute paths
        const jsResponse = await page.request.get('/main.js');
        if (jsResponse.ok()) {
            const jsContent = await jsResponse.text();

            // Check for hardcoded absolute URLs that would break static hosting
            const hardcodedPaths = [
                /['"]C:\\.*['"]/gi,
                /['"]\/Users\/.*['"]/gi,
                /['"]\/home\/.*['"]/gi,
                /['"]file:\/\/.*['"]/gi,
            ];

            hardcodedPaths.forEach(pattern => {
                expect(jsContent).not.toMatch(pattern);
            });
        }

        // Verify local stylesheet actually loads
        expect(localStylesheets).toContain('styles.css');
    });
});
