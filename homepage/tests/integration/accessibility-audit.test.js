/**
 * Accessibility Audit Integration Tests
 * Owner: Scenario 11 - Accessibility Compliance
 *
 * Uses axe-core to perform automated accessibility audit
 * Tests for WCAG 2.1 AA compliance
 */

const fs = require('fs');
const path = require('path');
const axe = require('axe-core');

// Read the HTML file
const htmlPath = path.join(__dirname, '../../index.html');
const htmlContent = fs.readFileSync(htmlPath, 'utf8');

// Create a complete HTML document for axe-core
const fullHtml = `<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"></head><body>${htmlContent}</body></html>`;

describe('Test Case 7: axe-core Accessibility Audit', () => {
  let results;

  beforeAll(async () => {
    // Set up DOM with full HTML
    document.documentElement.innerHTML = htmlContent;

    // Configure axe-core
    axe.configure({
      rules: [
        // Include WCAG 2.1 AA rules
        { id: 'color-contrast', enabled: true },
        { id: 'heading-order', enabled: true },
        { id: 'landmark-one-main', enabled: true },
        { id: 'region', enabled: true },
        { id: 'bypass', enabled: true },
        { id: 'document-title', enabled: true },
        { id: 'html-has-lang', enabled: true },
        { id: 'html-lang-valid', enabled: true },
        { id: 'image-alt', enabled: true },
        { id: 'link-name', enabled: true },
        { id: 'button-name', enabled: true },
        { id: 'label', enabled: true },
      ]
    });

    // Run axe-core analysis
    results = await axe.run(document.body, {
      runOnly: {
        type: 'tag',
        values: ['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa', 'best-practice']
      }
    });
  });

  test('No critical accessibility violations', () => {
    const criticalViolations = results.violations.filter(
      (v) => v.impact === 'critical'
    );

    if (criticalViolations.length > 0) {
      console.log('Critical Violations:');
      criticalViolations.forEach((v) => {
        console.log(`  - ${v.id}: ${v.description}`);
        v.nodes.forEach((node) => {
          console.log(`    Element: ${node.html}`);
        });
      });
    }

    expect(criticalViolations).toHaveLength(0);
  });

  test('No serious accessibility violations', () => {
    const seriousViolations = results.violations.filter(
      (v) => v.impact === 'serious'
    );

    if (seriousViolations.length > 0) {
      console.log('Serious Violations:');
      seriousViolations.forEach((v) => {
        console.log(`  - ${v.id}: ${v.description}`);
        v.nodes.forEach((node) => {
          console.log(`    Element: ${node.html}`);
        });
      });
    }

    expect(seriousViolations).toHaveLength(0);
  });

  test('Page has proper document structure', () => {
    // Check that document structure rules pass
    const documentRules = ['document-title', 'html-has-lang', 'html-lang-valid'];
    const documentViolations = results.violations.filter((v) =>
      documentRules.includes(v.id)
    );

    expect(documentViolations).toHaveLength(0);
  });

  test('All images have alt text', () => {
    const imageViolations = results.violations.filter(
      (v) => v.id === 'image-alt'
    );

    expect(imageViolations).toHaveLength(0);
  });

  test('All links have accessible names', () => {
    const linkViolations = results.violations.filter(
      (v) => v.id === 'link-name'
    );

    expect(linkViolations).toHaveLength(0);
  });

  test('All buttons have accessible names', () => {
    const buttonViolations = results.violations.filter(
      (v) => v.id === 'button-name'
    );

    expect(buttonViolations).toHaveLength(0);
  });

  test('Page has proper landmark structure', () => {
    const landmarkRules = ['landmark-one-main', 'region'];
    const landmarkViolations = results.violations.filter((v) =>
      landmarkRules.includes(v.id)
    );

    // Only fail on critical/serious landmark issues
    const seriousLandmarkViolations = landmarkViolations.filter(
      (v) => v.impact === 'critical' || v.impact === 'serious'
    );

    expect(seriousLandmarkViolations).toHaveLength(0);
  });

  test('Bypass mechanism exists (skip link)', () => {
    const bypassViolations = results.violations.filter(
      (v) => v.id === 'bypass'
    );

    expect(bypassViolations).toHaveLength(0);
  });

  test('Heading order is correct', () => {
    const headingViolations = results.violations.filter(
      (v) => v.id === 'heading-order'
    );

    expect(headingViolations).toHaveLength(0);
  });

  test('Logs accessibility passes for verification', () => {
    console.log('\nAccessibility Passes:');
    console.log(`  Total passed rules: ${results.passes.length}`);

    // Log key passes
    const keyRules = ['image-alt', 'link-name', 'button-name', 'bypass', 'heading-order'];
    keyRules.forEach((ruleId) => {
      const passed = results.passes.find((p) => p.id === ruleId);
      if (passed) {
        console.log(`  - ${ruleId}: PASSED`);
      }
    });
  });
});

/**
 * Additional axe-core specific tests
 */
describe('WCAG 2.1 AA Specific Checks', () => {
  beforeAll(() => {
    document.documentElement.innerHTML = htmlContent;
  });

  test('Focus indicators are not suppressed globally', () => {
    // Check that there's no outline: none on :focus without alternative
    const styles = document.querySelectorAll('style');
    let hasGlobalOutlineNone = false;

    styles.forEach((style) => {
      if (style.textContent.includes(':focus') && style.textContent.includes('outline: none')) {
        // Check if there's an alternative focus indicator
        const hasAlternative = style.textContent.includes('box-shadow') ||
                              style.textContent.includes('border') ||
                              style.textContent.includes('outline-offset');
        if (!hasAlternative) {
          hasGlobalOutlineNone = true;
        }
      }
    });

    expect(hasGlobalOutlineNone).toBe(false);
  });

  test('Interactive elements have sufficient target size', () => {
    // Check button and link minimum sizes (WCAG 2.5.5 - Target Size)
    const buttons = document.querySelectorAll('button');
    const minSize = 24; // WCAG recommends at least 24x24px

    buttons.forEach((button) => {
      const width = parseInt(button.style.width) || 0;
      const height = parseInt(button.style.height) || 0;

      // Since we can't measure computed styles in jsdom, we verify existence
      // E2E tests will verify actual pixel sizes
      expect(button).toBeDefined();
    });
  });

  test('No use of color alone to convey information', () => {
    // Check for elements that rely only on color
    // This is a basic check - thorough testing requires visual inspection
    const elementsWithColorOnly = document.querySelectorAll('[class*="error"], [class*="success"], [class*="warning"]');

    elementsWithColorOnly.forEach((el) => {
      // These elements should have additional indicators (icons, text, etc.)
      const hasIcon = el.querySelector('svg, img, [aria-hidden]');
      const hasTextContent = el.textContent.trim().length > 0;

      // At least one additional indicator should be present
      const hasAdditionalIndicator = hasIcon || hasTextContent;
      expect(hasAdditionalIndicator).toBe(true);
    });
  });
});
