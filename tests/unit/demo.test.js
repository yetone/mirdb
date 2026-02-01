/**
 * Demo Section Tests
 * Owner: Scenario 3 - Demo Section with Usage Examples
 *
 * Tests:
 * - Demo section exists
 * - Installation command present
 * - Server start command present
 * - Usage GIF present
 * - Copy button functionality
 * - Syntax highlighting (Scenario 14 shared)
 */

const { loadHTML, querySection, containsText } = require('../helpers/dom-utils');

describe('Demo Section with Usage Examples', () => {
    beforeEach(() => {
        loadHTML('index.html');
    });

    describe('Test Case 1: Demo section with code blocks exists', () => {
        test('demo section should exist', () => {
            const demoSection = querySection('demo');
            expect(demoSection).toBeInTheDocument();
        });

        test('demo section should have a heading', () => {
            const demoSection = querySection('demo');
            const heading = demoSection.querySelector('h2');
            expect(heading).toBeInTheDocument();
            expect(heading.textContent).toContain('Quick Start');
        });

        test('demo section should have code blocks', () => {
            const demoSection = querySection('demo');
            const codeBlocks = demoSection.querySelectorAll('pre code');
            expect(codeBlocks.length).toBeGreaterThanOrEqual(1);
        });
    });

    describe('Test Case 2: Installation command present', () => {
        test('should contain cargo install mirdb command', () => {
            const demoSection = querySection('demo');
            const installCode = document.getElementById('install-code');
            expect(installCode).toBeInTheDocument();
            expect(installCode.textContent).toContain('cargo install mirdb');
        });

        test('installation code block should have copy button', () => {
            const copyButton = document.querySelector('[data-target="install-code"]');
            expect(copyButton).toBeInTheDocument();
            expect(copyButton).toHaveClass('copy-button');
        });
    });

    describe('Test Case 3: Server start command present', () => {
        test('should contain server startup command', () => {
            const serverCode = document.getElementById('server-code');
            expect(serverCode).toBeInTheDocument();
            expect(serverCode.textContent).toContain('mirdb-server');
        });

        test('should contain port configuration', () => {
            const serverCode = document.getElementById('server-code');
            expect(serverCode.textContent).toContain('--port');
            expect(serverCode.textContent).toContain('11211');
        });

        test('server code block should have copy button', () => {
            const copyButton = document.querySelector('[data-target="server-code"]');
            expect(copyButton).toBeInTheDocument();
            expect(copyButton).toHaveClass('copy-button');
        });
    });

    describe('Test Case 4: Usage GIF present', () => {
        test('usage demonstration GIF should be present', () => {
            const usageGif = document.getElementById('usage-gif');
            expect(usageGif).toBeInTheDocument();
        });

        test('usage GIF should have correct source', () => {
            const usageGif = document.getElementById('usage-gif');
            expect(usageGif.getAttribute('src')).toContain('usage.gif');
        });

        test('usage GIF should have alt text for accessibility', () => {
            const usageGif = document.getElementById('usage-gif');
            expect(usageGif).toHaveAttribute('alt');
            expect(usageGif.getAttribute('alt')).toBeTruthy();
        });
    });

    describe('Test Case 5: Copy button functionality', () => {
        test('copy buttons should exist for each code block', () => {
            const copyButtons = document.querySelectorAll('.copy-button');
            expect(copyButtons.length).toBeGreaterThanOrEqual(3);
        });

        test('copy buttons should have data-target attributes', () => {
            const copyButtons = document.querySelectorAll('.copy-button');
            copyButtons.forEach(button => {
                expect(button).toHaveAttribute('data-target');
            });
        });

        test('copy buttons should have aria-label for accessibility', () => {
            const copyButtons = document.querySelectorAll('.copy-button');
            copyButtons.forEach(button => {
                expect(button).toHaveAttribute('aria-label');
            });
        });

        test('copy button should copy code content when clicked', async () => {
            // Load the copy-code.js functionality
            const { copyToClipboard } = require('../../assets/js/copy-code.js');

            const testCode = 'cargo install mirdb';
            const result = await copyToClipboard(testCode);

            expect(result).toBe(true);
            expect(navigator.clipboard.writeText).toHaveBeenCalledWith(testCode);
        });
    });

    describe('Additional Demo Section Tests', () => {
        test('code blocks should have language class for syntax highlighting', () => {
            const codeElements = document.querySelectorAll('#demo pre code');
            codeElements.forEach(code => {
                expect(code.className).toMatch(/language-/);
            });
        });

        test('demo section should be properly styled', () => {
            const demoSection = querySection('demo');
            expect(demoSection.className).toContain('py-20');
        });

        test('code block wrappers should have proper structure', () => {
            const wrappers = document.querySelectorAll('.code-block-wrapper');
            expect(wrappers.length).toBeGreaterThanOrEqual(3);

            wrappers.forEach(wrapper => {
                const header = wrapper.querySelector('.code-block-header');
                const pre = wrapper.querySelector('pre');
                expect(header).toBeInTheDocument();
                expect(pre).toBeInTheDocument();
            });
        });
    });
});
