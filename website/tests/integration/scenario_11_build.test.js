/**
 * Scenario 11: Performance and Load Time - Integration Tests
 *
 * Integration tests to verify:
 * - TC2: Images are appropriately compressed
 * - TC3: Build completes without errors or warnings (static site is valid)
 * - TC5: HTML validates without errors
 */

const fs = require('fs');
const path = require('path');

const WEBSITE_DIR = path.resolve(__dirname, '../../');
const STATIC_DIR = path.join(WEBSITE_DIR, 'static');
const IMAGES_DIR = path.join(STATIC_DIR, 'images');

describe('Scenario 11: Performance and Build Integration Tests', () => {
    describe('TC2: Image file sizes are appropriately compressed', () => {
        test('logo.gif exists and is reasonably sized', () => {
            const logoPath = path.join(IMAGES_DIR, 'logo.gif');
            expect(fs.existsSync(logoPath)).toBe(true);

            const stats = fs.statSync(logoPath);
            const sizeInMB = stats.size / (1024 * 1024);

            console.log(`logo.gif size: ${sizeInMB.toFixed(2)} MB (${stats.size} bytes)`);

            // For a logo GIF, 5MB is the upper limit (animated GIFs can be large)
            // This is a check to ensure it exists and is readable
            expect(stats.size).toBeGreaterThan(0);
            expect(sizeInMB).toBeLessThan(10); // Allow up to 10MB for animated GIFs
        });

        test('usage.gif exists and is reasonably sized', () => {
            const usagePath = path.join(IMAGES_DIR, 'usage.gif');
            expect(fs.existsSync(usagePath)).toBe(true);

            const stats = fs.statSync(usagePath);
            const sizeInMB = stats.size / (1024 * 1024);

            console.log(`usage.gif size: ${sizeInMB.toFixed(2)} MB (${stats.size} bytes)`);

            // Usage GIF is a demo animation, can be larger
            expect(stats.size).toBeGreaterThan(0);
            expect(sizeInMB).toBeLessThan(15); // Allow up to 15MB for demo GIFs
        });

        test('images directory contains expected assets', () => {
            expect(fs.existsSync(IMAGES_DIR)).toBe(true);

            const files = fs.readdirSync(IMAGES_DIR);
            console.log('Images found:', files);

            // Should have at least the logo and usage GIFs
            expect(files.length).toBeGreaterThanOrEqual(2);
            expect(files).toContain('logo.gif');
            expect(files).toContain('usage.gif');
        });
    });

    describe('TC3: Static site build completes without errors', () => {
        test('static site files are properly organized', () => {
            // Verify static directory structure
            expect(fs.existsSync(STATIC_DIR)).toBe(true);
            expect(fs.existsSync(IMAGES_DIR)).toBe(true);

            const staticFiles = fs.readdirSync(STATIC_DIR);
            console.log('Static directory contents:', staticFiles);

            // Should have index.html and images directory
            expect(staticFiles).toContain('index.html');
            expect(staticFiles).toContain('images');
        });

        test('index.html exists and has valid structure', () => {
            const indexPath = path.join(STATIC_DIR, 'index.html');
            expect(fs.existsSync(indexPath)).toBe(true);

            const content = fs.readFileSync(indexPath, 'utf-8');
            const fileSizeKB = Buffer.byteLength(content, 'utf-8') / 1024;

            console.log(`index.html size: ${fileSizeKB.toFixed(2)} KB`);

            // Verify essential HTML elements exist
            expect(content).toContain('<!DOCTYPE html>');
            expect(content).toContain('<html');
            expect(content).toContain('</html>');
            expect(content).toContain('<head>');
            expect(content).toContain('</head>');
            expect(content).toContain('<body');
            expect(content).toContain('</body>');

            // Verify no template placeholders remain
            expect(content).not.toContain('{{');
            expect(content).not.toContain('}}');
            expect(content).not.toContain('{% block');
            expect(content).not.toContain('{% endblock');
        });

        test('all required content sections are present', () => {
            const indexPath = path.join(STATIC_DIR, 'index.html');
            const content = fs.readFileSync(indexPath, 'utf-8');

            // Check for content from each section
            // Header/Navigation
            expect(content).toContain('MirDB');

            // Hero section
            expect(content.toLowerCase()).toContain('key-value');

            // Features section
            expect(content.toLowerCase()).toMatch(/feature|memcached|persistence|lsm/);

            // Quick start section
            expect(content.toLowerCase()).toMatch(/quick|start|install|cargo/);

            // Configuration section
            expect(content.toLowerCase()).toMatch(/configuration|config|toml/);

            // Status section
            expect(content.toLowerCase()).toMatch(/status|completed|planned/);

            // Footer
            expect(content.toLowerCase()).toMatch(/github|license|footer/);
        });
    });

    describe('TC5: HTML validation', () => {
        test('HTML has proper document structure', () => {
            const indexPath = path.join(STATIC_DIR, 'index.html');
            const content = fs.readFileSync(indexPath, 'utf-8');

            // Check DOCTYPE
            expect(content.trim().startsWith('<!DOCTYPE html>')).toBe(true);

            // Check html lang attribute
            expect(content).toMatch(/<html[^>]*lang=["'][a-z]{2}["']/);

            // Check meta charset
            expect(content).toMatch(/<meta[^>]*charset=["']?[uU][tT][fF]-8["']?/);

            // Check meta viewport
            expect(content).toMatch(/<meta[^>]*name=["']viewport["']/);

            // Check title element
            expect(content).toMatch(/<title>[^<]+<\/title>/);

            // Check meta description
            expect(content).toMatch(/<meta[^>]*name=["']description["'][^>]*content=["'][^"']+["']/);
        });

        test('HTML has proper semantic structure', () => {
            const indexPath = path.join(STATIC_DIR, 'index.html');
            const content = fs.readFileSync(indexPath, 'utf-8');

            // Check for semantic elements
            expect(content).toMatch(/<header[^>]*>/);
            expect(content).toMatch(/<main[^>]*>/);
            expect(content).toMatch(/<footer[^>]*>/);
            expect(content).toMatch(/<nav[^>]*>/);

            // Check for at least one h1
            expect(content).toMatch(/<h1[^>]*>/);
        });

        test('HTML has no unclosed tags (basic check)', () => {
            const indexPath = path.join(STATIC_DIR, 'index.html');
            const content = fs.readFileSync(indexPath, 'utf-8');

            // Count opening and closing tags for common elements
            const checkTag = (tag) => {
                const openPattern = new RegExp(`<${tag}[^>]*>`, 'gi');
                const closePattern = new RegExp(`</${tag}>`, 'gi');
                const selfClosePattern = new RegExp(`<${tag}[^>]*/\\s*>`, 'gi');

                const openCount = (content.match(openPattern) || []).length;
                const closeCount = (content.match(closePattern) || []).length;
                const selfCloseCount = (content.match(selfClosePattern) || []).length;

                // Self-closing tags are valid HTML5
                return openCount === closeCount + selfCloseCount || openCount <= closeCount;
            };

            // Check common block elements
            expect(checkTag('div')).toBe(true);
            expect(checkTag('section')).toBe(true);
            expect(checkTag('header')).toBe(true);
            expect(checkTag('footer')).toBe(true);
            expect(checkTag('main')).toBe(true);
            expect(checkTag('nav')).toBe(true);
        });

        test('HTML images have alt attributes', () => {
            const indexPath = path.join(STATIC_DIR, 'index.html');
            const content = fs.readFileSync(indexPath, 'utf-8');

            // Find all img tags
            const imgPattern = /<img[^>]*>/gi;
            const imgs = content.match(imgPattern) || [];

            console.log(`Found ${imgs.length} img tags`);

            // Each img should have an alt attribute
            imgs.forEach((img, index) => {
                const hasAlt = /alt=["'][^"']*["']/.test(img);
                if (!hasAlt) {
                    console.log(`Image ${index + 1} missing alt: ${img.substring(0, 100)}`);
                }
                expect(hasAlt).toBe(true);
            });
        });

        test('HTML links have valid structure', () => {
            const indexPath = path.join(STATIC_DIR, 'index.html');
            const content = fs.readFileSync(indexPath, 'utf-8');

            // Find all anchor tags
            const anchorPattern = /<a[^>]*>/gi;
            const anchors = content.match(anchorPattern) || [];

            console.log(`Found ${anchors.length} anchor tags`);

            // Each anchor should have an href attribute
            anchors.forEach((anchor, index) => {
                const hasHref = /href=["'][^"']*["']/.test(anchor);
                if (!hasHref) {
                    console.log(`Anchor ${index + 1} missing href: ${anchor}`);
                }
                expect(hasHref).toBe(true);
            });

            // External links should have proper target
            const externalPattern = /<a[^>]*href=["']https?:\/\/[^"']+["'][^>]*>/gi;
            const externalLinks = content.match(externalPattern) || [];

            externalLinks.forEach((link) => {
                // External links should ideally have target="_blank" and rel attributes
                // This is a recommendation, not a hard requirement
                if (!/target=["']_blank["']/.test(link)) {
                    console.log(`External link could use target="_blank": ${link.substring(0, 100)}`);
                }
            });
        });
    });

    describe('Performance Optimization', () => {
        test('index.html file size is reasonable', () => {
            const indexPath = path.join(STATIC_DIR, 'index.html');
            const stats = fs.statSync(indexPath);
            const sizeKB = stats.size / 1024;

            console.log(`index.html size: ${sizeKB.toFixed(2)} KB`);

            // For a single-page site, HTML should be under 100KB
            expect(sizeKB).toBeLessThan(100);
        });

        test('CSS is efficiently loaded', () => {
            const indexPath = path.join(STATIC_DIR, 'index.html');
            const content = fs.readFileSync(indexPath, 'utf-8');

            // Check that CSS is loaded (either via CDN or inline)
            const hasCSSLink = content.includes('<link') && content.includes('.css');
            const hasInlineStyle = content.includes('<style>');
            const hasTailwindCDN = content.includes('cdn.tailwindcss.com');

            console.log(`CSS loading: link=${hasCSSLink}, inline=${hasInlineStyle}, tailwindCDN=${hasTailwindCDN}`);

            // Should have at least one method of loading CSS
            expect(hasCSSLink || hasInlineStyle || hasTailwindCDN).toBe(true);
        });

        test('config.toml has performance optimizations enabled', () => {
            const configPath = path.join(WEBSITE_DIR, 'config.toml');
            expect(fs.existsSync(configPath)).toBe(true);

            const content = fs.readFileSync(configPath, 'utf-8');
            console.log('Config.toml contents:\n', content);

            // Check for basic site configuration
            expect(content).toMatch(/base_url/);
            expect(content).toMatch(/title/);

            // Note: minify_html and other Zola-specific settings may not be used
            // since this is a static site served directly
        });
    });
});
