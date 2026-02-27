/**
 * Unit Tests for Architecture Diagram CSS (Scenario 4)
 *
 * Tests that the SVG diagram uses CSS custom properties for theming.
 */
const fs = require('fs');
const path = require('path');

describe('Architecture Diagram CSS', () => {
    let cssContent;
    let htmlContent;

    beforeAll(() => {
        // Read the CSS file
        const cssPath = path.join(__dirname, '../../css/styles.css');
        cssContent = fs.readFileSync(cssPath, 'utf-8');

        // Read the HTML file
        const htmlPath = path.join(__dirname, '../../index.html');
        htmlContent = fs.readFileSync(htmlPath, 'utf-8');
    });

    // Test Case 7: Check SVG uses CSS custom properties
    describe('CSS custom properties usage', () => {
        test('CSS file contains var(--color-*) properties for architecture diagram', () => {
            // Check for CSS variable references in architecture diagram styles
            const architectureSection = cssContent.substring(
                cssContent.indexOf('/* ===== ARCHITECTURE DIAGRAM'),
                cssContent.indexOf('/* ===== BENCHMARKS TABLE')
            );

            // Check for various CSS variable usages
            expect(architectureSection).toMatch(/var\(--color-[a-z-]+/);
            expect(architectureSection).toMatch(/var\(--color-text\)/);
            expect(architectureSection).toMatch(/var\(--color-primary\)/);
            expect(architectureSection).toMatch(/var\(--color-bg\)/);
        });

        test('Architecture diagram wrapper uses CSS custom properties', () => {
            // Check for var() usage in diagram wrapper styles
            expect(cssContent).toMatch(/\.architecture-diagram-wrapper/);
            expect(cssContent).toMatch(/var\(--color-bg\)/);
        });

        test('SVG text elements use CSS custom properties', () => {
            // Check for CSS variable usage for text fills
            expect(cssContent).toMatch(/\.diagram-text\s*\{[\s\S]*?fill:\s*var\(--color-text\)/);
        });

        test('SVG box elements use CSS custom properties', () => {
            // Check for box styling with CSS variables
            expect(cssContent).toMatch(/\.box-client[\s\S]*?fill:\s*var\(--color-diagram-client-bg/);
            expect(cssContent).toMatch(/\.box-memtable[\s\S]*?fill:\s*var\(--color-diagram-memtable-bg/);
            expect(cssContent).toMatch(/\.box-sstable[\s\S]*?fill:\s*var\(--color-diagram-sstable-bg/);
        });

        test('Arrow elements use CSS custom properties', () => {
            // Check for arrow styling with CSS variables
            expect(cssContent).toMatch(/\.arrow-write[\s\S]*?stroke:\s*var\(--color-primary\)/);
            expect(cssContent).toMatch(/\.arrow-read[\s\S]*?stroke:\s*var\(--color-diagram-arrow-read/);
        });

        test('Arrow markers use CSS custom properties', () => {
            // Check for arrow marker fills with CSS variables
            expect(cssContent).toMatch(/\.arrow-marker[\s\S]*?fill:\s*var\(--color-primary\)/);
        });
    });

    describe('Dark theme support', () => {
        test('CSS contains dark theme overrides for diagram colors', () => {
            // Check for dark theme specific variables
            expect(cssContent).toMatch(/\[data-theme="dark"\][\s\S]*?--color-diagram-bg/);
            expect(cssContent).toMatch(/\[data-theme="dark"\][\s\S]*?--color-diagram-client-bg/);
            expect(cssContent).toMatch(/\[data-theme="dark"\][\s\S]*?--color-diagram-memtable-bg/);
            expect(cssContent).toMatch(/\[data-theme="dark"\][\s\S]*?--color-diagram-sstable-bg/);
        });

        test('Dark theme defines diagram arrow colors', () => {
            // Check for dark theme arrow colors
            expect(cssContent).toMatch(/\[data-theme="dark"\][\s\S]*?--color-diagram-arrow-read/);
            expect(cssContent).toMatch(/\[data-theme="dark"\][\s\S]*?--color-diagram-arrow-compact/);
        });
    });

    describe('SVG structure in HTML', () => {
        test('SVG has class architecture-diagram', () => {
            expect(htmlContent).toMatch(/class="architecture-diagram"/);
        });

        test('SVG uses CSS classes for styling boxes', () => {
            expect(htmlContent).toMatch(/class="box-client"/);
            expect(htmlContent).toMatch(/class="box-memtable"/);
            expect(htmlContent).toMatch(/class="box-sstable"/);
            expect(htmlContent).toMatch(/class="box-wal"/);
            expect(htmlContent).toMatch(/class="box-compaction"/);
        });

        test('SVG uses CSS classes for styling arrows', () => {
            expect(htmlContent).toMatch(/class="arrow-write"/);
            expect(htmlContent).toMatch(/class="arrow-read"/);
            expect(htmlContent).toMatch(/class="arrow-compact"/);
        });

        test('SVG uses CSS classes for text elements', () => {
            expect(htmlContent).toMatch(/class="diagram-text/);
            expect(htmlContent).toMatch(/class="diagram-text-secondary/);
        });
    });
});
