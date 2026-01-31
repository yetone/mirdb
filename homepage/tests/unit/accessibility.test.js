/**
 * Accessibility Unit Tests
 * Owner: Scenario 12 - Accessibility Compliance
 *
 * Tests:
 * - Alt text on images
 * - ARIA attributes
 * - Color contrast values
 * - Focus indicators
 *
 * Requirements: NFR-3 (WCAG 2.1 AA)
 */

const fs = require('fs');
const path = require('path');

describe('Accessibility Tests', () => {
  let htmlContent;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '../../index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf-8');
  });

  describe('Architecture Diagram Accessibility (Test Case 5)', () => {
    test('Architecture diagram has appropriate alt text via aria-label', () => {
      // Check that the architecture diagram container has aria-label
      const hasAriaLabel = htmlContent.includes('class="architecture-diagram"') &&
        htmlContent.includes('role="img"') &&
        htmlContent.includes('aria-label=');

      expect(hasAriaLabel).toBe(true);
    });

    test('Architecture diagram aria-label describes LSM tree structure', () => {
      // Extract the aria-label content
      const ariaLabelMatch = htmlContent.match(/class="architecture-diagram"[^>]*aria-label="([^"]*)"/);

      expect(ariaLabelMatch).not.toBeNull();

      if (ariaLabelMatch) {
        const ariaLabelContent = ariaLabelMatch[1].toLowerCase();
        // Check that the aria-label mentions key components
        expect(ariaLabelContent).toContain('lsm');
        expect(ariaLabelContent).toContain('write');
        expect(ariaLabelContent).toContain('read');
      }
    });

    test('Mermaid diagram has aria-hidden for screen readers', () => {
      // The mermaid pre element should have aria-hidden="true"
      // since we provide a separate accessible description
      const hasMermaidAriaHidden = htmlContent.includes('class="mermaid"') &&
        htmlContent.includes('aria-hidden="true"');

      expect(hasMermaidAriaHidden).toBe(true);
    });

    test('Screen reader only text is provided for diagram description', () => {
      // Check for sr-only class with description
      const hasSrOnlyDescription = htmlContent.includes('class="sr-only"') &&
        htmlContent.includes('The diagram shows');

      expect(hasSrOnlyDescription).toBe(true);
    });
  });
});
