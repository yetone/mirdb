/**
 * HTML/CSS Validation Tests
 * Owner: Scenario 13 - HTML and CSS Validity
 *
 * Tests:
 * - HTML5 doctype
 * - Viewport meta tag
 * - HTML structure validation
 * - UTF-8 charset declaration
 */

const { test, describe } = require('node:test');
const assert = require('node:assert');
const fs = require('node:fs');
const path = require('node:path');

// Read the HTML file
const htmlPath = path.join(__dirname, '../../homepage/index.html');
const htmlContent = fs.readFileSync(htmlPath, 'utf-8');

describe('HTML and CSS Validity', () => {
    describe('Test Case 1: HTML Doctype', () => {
        test('Page has valid HTML5 doctype declaration', () => {
            // Check that the document starts with the HTML5 doctype
            const doctypeRegex = /^<!DOCTYPE\s+html\s*>/i;
            const trimmedContent = htmlContent.trim();

            assert.ok(
                doctypeRegex.test(trimmedContent),
                'HTML file should start with valid HTML5 doctype (<!DOCTYPE html>)'
            );
        });

        test('Doctype is the first line', () => {
            const lines = htmlContent.split('\n');
            const firstLine = lines[0].trim();

            assert.ok(
                firstLine.toLowerCase().startsWith('<!doctype html'),
                'DOCTYPE should be the first line of the HTML file'
            );
        });
    });

    describe('Test Case 2: Meta Tags', () => {
        test('Page has viewport meta tag for responsive design', () => {
            // Check for viewport meta tag with essential attributes
            const viewportRegex = /<meta\s+[^>]*name\s*=\s*["']viewport["'][^>]*>/i;

            assert.ok(
                viewportRegex.test(htmlContent),
                'HTML should contain a viewport meta tag'
            );
        });

        test('Viewport meta tag includes width=device-width', () => {
            const viewportContentRegex = /<meta\s+[^>]*name\s*=\s*["']viewport["'][^>]*content\s*=\s*["'][^"']*width\s*=\s*device-width[^"']*["'][^>]*>/i;

            assert.ok(
                viewportContentRegex.test(htmlContent),
                'Viewport meta tag should include width=device-width'
            );
        });

        test('Viewport meta tag includes initial-scale=1', () => {
            const viewportScaleRegex = /<meta\s+[^>]*name\s*=\s*["']viewport["'][^>]*content\s*=\s*["'][^"']*initial-scale\s*=\s*1[^"']*["'][^>]*>/i;

            assert.ok(
                viewportScaleRegex.test(htmlContent),
                'Viewport meta tag should include initial-scale=1.0'
            );
        });

        test('Page has description meta tag', () => {
            const descriptionRegex = /<meta\s+[^>]*name\s*=\s*["']description["'][^>]*>/i;

            assert.ok(
                descriptionRegex.test(htmlContent),
                'HTML should contain a description meta tag'
            );
        });
    });

    describe('Test Case 3: HTML Structure Validation', () => {
        test('HTML has valid html element with lang attribute', () => {
            const htmlLangRegex = /<html\s+[^>]*lang\s*=\s*["'][a-z]{2}(-[A-Z]{2})?["'][^>]*>/i;

            assert.ok(
                htmlLangRegex.test(htmlContent),
                'HTML element should have a lang attribute'
            );
        });

        test('HTML has head element', () => {
            assert.ok(
                /<head[^>]*>[\s\S]*<\/head>/i.test(htmlContent),
                'HTML should contain a head element'
            );
        });

        test('HTML has body element', () => {
            assert.ok(
                /<body[^>]*>[\s\S]*<\/body>/i.test(htmlContent),
                'HTML should contain a body element'
            );
        });

        test('HTML has title element', () => {
            const titleRegex = /<title[^>]*>[^<]+<\/title>/i;

            assert.ok(
                titleRegex.test(htmlContent),
                'HTML should contain a non-empty title element'
            );
        });

        test('HTML has proper heading hierarchy starting with h1', () => {
            const h1Regex = /<h1[^>]*>[^<]*<\/h1>/i;

            assert.ok(
                h1Regex.test(htmlContent),
                'HTML should contain an h1 element'
            );
        });

        test('HTML uses semantic elements', () => {
            // Check for common semantic HTML5 elements
            const semanticElements = ['header', 'nav', 'main', 'section', 'article', 'footer'];
            let foundSemanticElements = 0;

            for (const element of semanticElements) {
                const regex = new RegExp(`<${element}[^>]*>`, 'i');
                if (regex.test(htmlContent)) {
                    foundSemanticElements++;
                }
            }

            assert.ok(
                foundSemanticElements >= 3,
                `HTML should use at least 3 semantic elements (found: ${foundSemanticElements})`
            );
        });

        test('All opened tags have corresponding closing tags', () => {
            // Check for properly closed major structural elements
            const elementsToCheck = ['html', 'head', 'body', 'header', 'nav', 'section', 'footer'];

            for (const element of elementsToCheck) {
                // Use word boundary or > to avoid partial matches (e.g., <header matching <head)
                const openRegex = new RegExp(`<${element}(?:\\s[^>]*>|>)`, 'gi');
                const closeRegex = new RegExp(`</${element}>`, 'gi');
                const openMatches = htmlContent.match(openRegex) || [];
                const closeMatches = htmlContent.match(closeRegex) || [];

                assert.strictEqual(
                    openMatches.length,
                    closeMatches.length,
                    `Element <${element}> should have matching open and close tags`
                );
            }
        });

        test('No nested anchor tags', () => {
            // Anchor tags should not be nested
            const nestedAnchorRegex = /<a\s[^>]*>(?:[^<]*<(?!\/a>)[^>]*>)*[^<]*<a\s/i;

            assert.ok(
                !nestedAnchorRegex.test(htmlContent),
                'HTML should not contain nested anchor tags'
            );
        });
    });

    describe('Test Case 4: Charset Declaration', () => {
        test('Page declares UTF-8 charset', () => {
            const charsetRegex = /<meta\s+charset\s*=\s*["']UTF-8["']\s*\/?>/i;

            assert.ok(
                charsetRegex.test(htmlContent),
                'HTML should declare UTF-8 charset via <meta charset="UTF-8">'
            );
        });

        test('Charset is declared in the head element', () => {
            // Extract head content and check for charset
            const headMatch = htmlContent.match(/<head[^>]*>([\s\S]*?)<\/head>/i);

            assert.ok(headMatch, 'HTML should have a head element');

            const headContent = headMatch[1];
            const charsetRegex = /<meta\s+charset\s*=\s*["']UTF-8["']/i;

            assert.ok(
                charsetRegex.test(headContent),
                'Charset declaration should be in the head element'
            );
        });

        test('Charset is declared early in the head', () => {
            // Charset should be one of the first elements in head
            const headMatch = htmlContent.match(/<head[^>]*>([\s\S]*?)<\/head>/i);
            const headContent = headMatch[1];

            // Get position of charset meta tag
            const charsetPosition = headContent.search(/<meta\s+charset\s*=\s*["']UTF-8["']/i);

            // Get position of first non-meta, non-comment content
            const firstContentPosition = headContent.search(/<(?!meta|!--)[a-z]/i);

            if (firstContentPosition !== -1) {
                assert.ok(
                    charsetPosition < firstContentPosition,
                    'Charset should be declared before other head content'
                );
            }
        });
    });

    describe('Additional HTML Validity Checks', () => {
        test('Images have alt attributes', () => {
            // Find all img tags
            const imgRegex = /<img\s[^>]*>/gi;
            const imgTags = htmlContent.match(imgRegex) || [];

            for (const imgTag of imgTags) {
                const hasAlt = /\balt\s*=\s*["'][^"']*["']/i.test(imgTag);
                assert.ok(
                    hasAlt,
                    `All img elements should have alt attributes: ${imgTag.substring(0, 50)}...`
                );
            }
        });

        test('Links with target="_blank" have rel="noopener"', () => {
            // Find all anchor tags with target="_blank"
            const blankLinkRegex = /<a\s[^>]*target\s*=\s*["']_blank["'][^>]*>/gi;
            const blankLinks = htmlContent.match(blankLinkRegex) || [];

            for (const link of blankLinks) {
                const hasNoopener = /\brel\s*=\s*["'][^"']*noopener[^"']*["']/i.test(link);
                assert.ok(
                    hasNoopener,
                    'Links with target="_blank" should have rel="noopener": ' + link.substring(0, 80)
                );
            }
        });

        test('No duplicate id attributes', () => {
            const idRegex = /\bid\s*=\s*["']([^"']+)["']/gi;
            const ids = [];
            let match;

            while ((match = idRegex.exec(htmlContent)) !== null) {
                ids.push(match[1]);
            }

            const duplicates = ids.filter((id, index) => ids.indexOf(id) !== index);

            assert.strictEqual(
                duplicates.length,
                0,
                `HTML should not have duplicate IDs. Duplicates found: ${duplicates.join(', ')}`
            );
        });

        test('Required role attributes are present for landmark regions', () => {
            // Check that main landmark regions have appropriate roles
            const hasHeader = /<header[^>]*role\s*=\s*["']banner["'][^>]*>/i.test(htmlContent);
            const hasNav = /<nav[^>]*role\s*=\s*["']navigation["'][^>]*>/i.test(htmlContent);
            const hasFooter = /<footer[^>]*role\s*=\s*["']contentinfo["'][^>]*>/i.test(htmlContent);

            // At least some landmarks should have explicit roles for accessibility
            const landmarkCount = [hasHeader, hasNav, hasFooter].filter(Boolean).length;

            assert.ok(
                landmarkCount >= 2,
                'HTML should have explicit roles on landmark regions for accessibility'
            );
        });
    });
});
