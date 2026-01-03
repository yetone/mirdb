/**
 * Unit tests for print stylesheet functionality
 * Scenario: Print Stylesheet - Verify page prints correctly with appropriate print styles
 */

const fs = require('fs');
const path = require('path');
const { JSDOM } = require('jsdom');

describe('Print Stylesheet', () => {
  let cssContent;
  let document;

  beforeAll(() => {
    const cssPath = path.join(__dirname, '../../styles.css');
    cssContent = fs.readFileSync(cssPath, 'utf-8');

    const htmlPath = path.join(__dirname, '../../index.html');
    const html = fs.readFileSync(htmlPath, 'utf-8');
    const dom = new JSDOM(html);
    document = dom.window.document;
  });

  describe('TC1: Check print media query exists', () => {
    test('CSS includes print-specific styles via @media print', () => {
      // Check that @media print block exists in the CSS
      const hasPrintMediaQuery = /@media\s+print\s*\{/.test(cssContent);
      expect(hasPrintMediaQuery).toBe(true);
    });

    test('Print styles define page layout adjustments', () => {
      // Extract print media query content (greedy to capture nested blocks)
      const printMediaMatch = cssContent.match(/@media\s+print\s*\{([\s\S]*)\}[\s\n]*$/);
      expect(printMediaMatch).not.toBeNull();

      const printStyles = printMediaMatch[1];

      // Print styles should exist and contain meaningful CSS rules
      expect(printStyles.length).toBeGreaterThan(0);

      // Should have some CSS declarations (property: value pairs)
      const hasDeclarations = /\w+\s*:\s*[^;]+;/.test(printStyles);
      expect(hasDeclarations).toBe(true);
    });
  });

  describe('TC2: Non-essential elements hidden in print', () => {
    test('Print styles hide interactive/non-essential elements', () => {
      // Extract print media query content (greedy to capture nested blocks)
      const printMediaMatch = cssContent.match(/@media\s+print\s*\{([\s\S]*)\}[\s\n]*$/);
      expect(printMediaMatch).not.toBeNull();

      const printStyles = printMediaMatch[1];

      // CTA buttons should be hidden in print (interactive elements)
      const hidesCTAButtons = printStyles.includes('.cta-buttons') &&
                              printStyles.includes('display') &&
                              printStyles.includes('none');
      expect(hidesCTAButtons).toBe(true);
    });

    test('Print styles should address background and colors for readability', () => {
      const printMediaMatch = cssContent.match(/@media\s+print\s*\{([\s\S]*)\}[\s\n]*$/);
      expect(printMediaMatch).not.toBeNull();

      const printStyles = printMediaMatch[1];

      // Should modify background or colors for print readability
      const addressesBackground = printStyles.includes('background') ||
                                  printStyles.includes('color');
      expect(addressesBackground).toBe(true);
    });
  });

  describe('TC3: Link URLs visible in print (enhancement)', () => {
    test('Print styles include link URL display enhancement', () => {
      // Use a more comprehensive regex to extract the entire @media print block
      // including nested @page rules
      const printMediaMatch = cssContent.match(/@media\s+print\s*\{([\s\S]*)\}[\s\n]*$/);
      expect(printMediaMatch).not.toBeNull();

      const printStyles = printMediaMatch[1];

      // Check for link URL display using ::after pseudo-element with attr(href)
      // The CSS may use a[href^="http"] or a[href] selector
      const hasLinkSelector = printStyles.includes('a[href') && printStyles.includes('::after');
      const hasAttrHref = printStyles.includes('attr(href)');
      const hasLinkURLDisplay = hasLinkSelector && hasAttrHref;
      expect(hasLinkURLDisplay).toBe(true);
    });
  });

  describe('Main content accessibility in print', () => {
    test('Main content elements exist and are not hidden by default', () => {
      // Verify main content areas exist in the HTML
      const main = document.querySelector('main');
      expect(main).not.toBeNull();

      const features = document.querySelector('#features');
      expect(features).not.toBeNull();

      const quickstart = document.querySelector('#quickstart');
      expect(quickstart).not.toBeNull();
    });

    test('Print styles ensure content readability', () => {
      const printMediaMatch = cssContent.match(/@media\s+print\s*\{([\s\S]*)\}[\s\n]*$/);
      expect(printMediaMatch).not.toBeNull();

      const printStyles = printMediaMatch[1];

      // Print styles should set text to a readable color (black or dark)
      const hasReadableTextColor = printStyles.includes('color:') &&
                                   (printStyles.includes('#000') ||
                                    printStyles.includes('black') ||
                                    printStyles.includes('#333') ||
                                    printStyles.includes('#111'));
      expect(hasReadableTextColor).toBe(true);
    });
  });
});
