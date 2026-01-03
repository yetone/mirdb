/**
 * Unit tests for validating asset paths in the HTML and CSS files
 * Test case ID: 4 - Check asset paths are correct
 */

const fs = require('fs');
const path = require('path');
const { JSDOM } = require('jsdom');

describe('Asset Path Validation', () => {
    let htmlContent;
    let cssContent;
    let dom;
    let document;

    beforeAll(() => {
        // Read HTML file
        htmlContent = fs.readFileSync(
            path.join(__dirname, '../../index.html'),
            'utf8'
        );

        // Read CSS file
        cssContent = fs.readFileSync(
            path.join(__dirname, '../../styles.css'),
            'utf8'
        );

        // Parse HTML
        dom = new JSDOM(htmlContent);
        document = dom.window.document;
    });

    describe('CSS file references', () => {
        test('local stylesheet uses relative path', () => {
            const localStylesheet = document.querySelector('link[href="styles.css"]');
            expect(localStylesheet).not.toBeNull();

            // Verify the CSS file exists
            const cssPath = path.join(__dirname, '../../styles.css');
            expect(fs.existsSync(cssPath)).toBe(true);
        });

        test('external stylesheet uses valid HTTPS URL', () => {
            const externalStylesheets = document.querySelectorAll('link[rel="stylesheet"]');

            externalStylesheets.forEach(link => {
                const href = link.getAttribute('href');
                if (href && href.startsWith('http')) {
                    // External URLs should use HTTPS
                    expect(href.startsWith('https://')).toBe(true);
                }
            });
        });

        test('Prism.js theme URL is valid', () => {
            const prismTheme = document.querySelector(
                'link[href*="prism"][href*=".css"]'
            );
            expect(prismTheme).not.toBeNull();

            const href = prismTheme.getAttribute('href');
            expect(href).toMatch(/^https:\/\/cdnjs\.cloudflare\.com/);
            expect(href).toContain('prism');
            expect(href).toContain('.css');
        });
    });

    describe('JavaScript file references', () => {
        test('Prism.js core script uses valid HTTPS URL', () => {
            const prismCore = document.querySelector(
                'script[src*="prism.min.js"]'
            );
            expect(prismCore).not.toBeNull();

            const src = prismCore.getAttribute('src');
            expect(src.startsWith('https://')).toBe(true);
            expect(src).toContain('cdnjs.cloudflare.com');
        });

        test('Prism.js bash component uses valid HTTPS URL', () => {
            const prismBash = document.querySelector(
                'script[src*="prism-bash"]'
            );
            expect(prismBash).not.toBeNull();

            const src = prismBash.getAttribute('src');
            expect(src.startsWith('https://')).toBe(true);
            expect(src).toContain('cdnjs.cloudflare.com');
        });

        test('all script sources use valid URLs or relative paths', () => {
            const scripts = document.querySelectorAll('script[src]');

            scripts.forEach(script => {
                const src = script.getAttribute('src');
                // Either relative path or valid HTTPS URL
                const isValidPath = !src.startsWith('http') ||
                                   src.startsWith('https://');
                expect(isValidPath).toBe(true);
            });
        });
    });

    describe('Image references', () => {
        test('OG image URL uses valid HTTPS path', () => {
            const ogImage = document.querySelector('meta[property="og:image"]');
            expect(ogImage).not.toBeNull();

            const content = ogImage.getAttribute('content');
            expect(content.startsWith('https://')).toBe(true);
            // GitHub raw content URL
            expect(content).toContain('raw.githubusercontent.com');
        });

        test('all image src attributes use valid paths', () => {
            const images = document.querySelectorAll('img[src]');

            images.forEach(img => {
                const src = img.getAttribute('src');
                // Should be either relative path or valid HTTPS URL
                const isValidPath = !src.startsWith('http') ||
                                   src.startsWith('https://');
                expect(isValidPath).toBe(true);

                // No empty src attributes
                expect(src.trim()).not.toBe('');
            });
        });
    });

    describe('CSS internal references', () => {
        test('CSS does not contain broken url() references', () => {
            // Find all url() references in CSS
            const urlMatches = cssContent.match(/url\([^)]+\)/g) || [];

            urlMatches.forEach(urlRef => {
                // Extract the URL from url()
                const url = urlRef.replace(/url\(['"]?|['"]?\)/g, '');

                // Skip data URIs
                if (url.startsWith('data:')) return;

                // Check for valid URL format
                if (url.startsWith('http')) {
                    expect(url.startsWith('https://')).toBe(true);
                }

                // URL should not be empty or just whitespace
                expect(url.trim()).not.toBe('');
            });
        });

        test('font-family declarations use valid system font stack', () => {
            // Check that var(--font-family) is used and defined
            expect(cssContent).toContain('--font-family:');
            expect(cssContent).toContain('system-ui');
            expect(cssContent).toContain('sans-serif');

            // Check monospace font stack
            expect(cssContent).toContain('--font-mono:');
            expect(cssContent).toContain('monospace');
        });
    });

    describe('Link href attributes', () => {
        test('canonical URL uses HTTPS', () => {
            const canonical = document.querySelector('link[rel="canonical"]');
            expect(canonical).not.toBeNull();

            const href = canonical.getAttribute('href');
            expect(href.startsWith('https://')).toBe(true);
        });

        test('all anchor hrefs use valid paths', () => {
            const anchors = document.querySelectorAll('a[href]');

            anchors.forEach(anchor => {
                const href = anchor.getAttribute('href');

                // Valid patterns: relative paths, hash links, https URLs
                const isValid = href.startsWith('#') ||
                               href.startsWith('/') ||
                               href.startsWith('./') ||
                               href.startsWith('../') ||
                               href.startsWith('https://') ||
                               href.startsWith('mailto:') ||
                               href.startsWith('tel:');

                // No HTTP (insecure) or javascript: URLs
                expect(href.startsWith('http://')).toBe(false);
                expect(href.startsWith('javascript:')).toBe(false);

                if (!isValid) {
                    throw new Error(`Invalid href: ${href}`);
                }
            });
        });
    });

    describe('Meta tag content URLs', () => {
        test('og:url uses HTTPS', () => {
            const ogUrl = document.querySelector('meta[property="og:url"]');
            expect(ogUrl).not.toBeNull();

            const content = ogUrl.getAttribute('content');
            expect(content.startsWith('https://')).toBe(true);
        });

        test('all meta content URLs use HTTPS when applicable', () => {
            const metaTags = document.querySelectorAll('meta[content]');

            metaTags.forEach(meta => {
                const content = meta.getAttribute('content');
                // If content looks like a URL, it should be HTTPS
                if (content && content.match(/^https?:\/\//)) {
                    expect(content.startsWith('https://')).toBe(true);
                }
            });
        });
    });

    describe('Asset file existence', () => {
        test('local CSS file exists', () => {
            const cssPath = path.join(__dirname, '../../styles.css');
            expect(fs.existsSync(cssPath)).toBe(true);

            // File should not be empty
            const stats = fs.statSync(cssPath);
            expect(stats.size).toBeGreaterThan(0);
        });

        test('HTML file exists and is valid', () => {
            const htmlPath = path.join(__dirname, '../../index.html');
            expect(fs.existsSync(htmlPath)).toBe(true);

            // File should not be empty
            const stats = fs.statSync(htmlPath);
            expect(stats.size).toBeGreaterThan(0);

            // Should be valid HTML (has doctype and html tag)
            expect(htmlContent).toContain('<!DOCTYPE html>');
            expect(htmlContent).toContain('<html');
        });

        test('assets directory exists', () => {
            const assetsPath = path.join(__dirname, '../../assets');
            // The assets directory is optional for this static site
            // but if it exists, it should be a directory
            if (fs.existsSync(assetsPath)) {
                const stats = fs.statSync(assetsPath);
                expect(stats.isDirectory()).toBe(true);
            }
        });
    });

    describe('CDN URL validation', () => {
        test('CDN URLs use recognized CDN providers', () => {
            const assetUrls = [];

            // Collect URLs from stylesheets (exclude canonical which is not a CDN asset)
            document.querySelectorAll('link[rel="stylesheet"][href]').forEach(link => {
                assetUrls.push(link.getAttribute('href'));
            });

            // Collect URLs from scripts
            document.querySelectorAll('script[src]').forEach(script => {
                assetUrls.push(script.getAttribute('src'));
            });

            // Filter to external URLs (only actual asset CDN URLs)
            const externalAssetUrls = assetUrls.filter(url => url && url.startsWith('https://'));

            // Known trusted CDN domains
            const trustedCdns = [
                'cdnjs.cloudflare.com',
                'cdn.jsdelivr.net',
                'unpkg.com',
                'raw.githubusercontent.com'
            ];

            externalAssetUrls.forEach(url => {
                const urlObj = new URL(url);
                const isTrustedCdn = trustedCdns.some(cdn =>
                    urlObj.hostname === cdn || urlObj.hostname.endsWith('.' + cdn)
                );
                if (!isTrustedCdn) {
                    throw new Error(`URL ${url} should use a trusted CDN provider`);
                }
            });
        });

        test('CDN URLs include version numbers for cache stability', () => {
            const cdnScripts = document.querySelectorAll(
                'script[src*="cdnjs.cloudflare.com"]'
            );

            cdnScripts.forEach(script => {
                const src = script.getAttribute('src');
                // CDN URLs should include version numbers
                expect(src).toMatch(/\/\d+\.\d+/);
            });

            const cdnStyles = document.querySelectorAll(
                'link[href*="cdnjs.cloudflare.com"]'
            );

            cdnStyles.forEach(link => {
                const href = link.getAttribute('href');
                // CDN URLs should include version numbers
                expect(href).toMatch(/\/\d+\.\d+/);
            });
        });
    });
});
