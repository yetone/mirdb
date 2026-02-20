/**
 * Visual Design Validation Tests
 * Owner: Scenario 10 - Visual Design Validation
 *
 * Tests for:
 * - Background colors (dark mode/neutral dev tool aesthetic)
 * - Typography (sans-serif for body, monospace for code)
 * - Spacing and alignment consistency
 * - Code block styling (terminal-style with distinct background)
 * - Heading hierarchy
 * - Link and button styling
 */

const { loadHTML } = require('./test-utils');

describe('Visual Design Validation', () => {
  let document;
  let cssText;

  beforeAll(async () => {
    document = await loadHTML();
    // Collect all CSS text from embedded styles
    const styleElements = document.querySelectorAll('style');
    cssText = Array.from(styleElements).map(s => s.textContent).join('\n');
  });

  // Test Case 1: Check body background color
  describe('Test Case 1: Body Background Color', () => {
    test('Body uses dark or neutral background color appropriate for dev tools', () => {
      // Check CSS variables for dark theme colors
      const hasDarkBgVariable = cssText.includes('--bg-primary') &&
        (cssText.includes('--bg-primary:') &&
         /--bg-primary:\s*#[0-9a-fA-F]{3,6}/.test(cssText));

      // Check for actual dark color values in background
      const darkColorPatterns = [
        /#1[0-9a-fA-F]{5}/,  // Colors starting with #1 (dark)
        /#0[0-9a-fA-F]{5}/,  // Colors starting with #0 (very dark)
        /#2[0-3][0-9a-fA-F]{4}/,  // Colors starting with #20-23 (dark)
        /rgb\s*\(\s*[0-4]\d\s*,/,  // RGB with low red values
      ];

      const hasActualDarkColor = darkColorPatterns.some(pattern => pattern.test(cssText));

      // Also check if body uses the CSS variable
      const bodyUsesVariable = cssText.includes('var(--bg-primary)');

      expect(hasDarkBgVariable || hasActualDarkColor).toBe(true);
      expect(bodyUsesVariable).toBe(true);
    });

    test('Background color is defined in :root or body styles', () => {
      // Look for background color declaration
      const hasRootBg = cssText.includes(':root') && cssText.includes('--bg-primary');
      const hasBodyBg = /body[^{]*\{[^}]*background(-color)?:/i.test(cssText);

      expect(hasRootBg || hasBodyBg).toBe(true);
    });
  });

  // Test Case 2: Check body font-family
  describe('Test Case 2: Body Font Family', () => {
    test('Body text uses sans-serif font family', () => {
      // Check for sans-serif in body font-family
      const bodyFontMatch = cssText.match(/body[^{]*\{[^}]*font-family:\s*([^;]+)/);

      expect(bodyFontMatch).not.toBeNull();

      const fontFamily = bodyFontMatch[1].toLowerCase();
      const hasSansSerif = fontFamily.includes('sans-serif') ||
                           fontFamily.includes('segoe ui') ||
                           fontFamily.includes('roboto') ||
                           fontFamily.includes('-apple-system') ||
                           fontFamily.includes('blinkmacsystemfont') ||
                           fontFamily.includes('arial') ||
                           fontFamily.includes('helvetica');

      expect(hasSansSerif).toBe(true);
    });

    test('Sans-serif is included as fallback font', () => {
      const bodyFontMatch = cssText.match(/body[^{]*\{[^}]*font-family:\s*([^;]+)/);
      expect(bodyFontMatch).not.toBeNull();

      const fontFamily = bodyFontMatch[1].toLowerCase();
      expect(fontFamily).toContain('sans-serif');
    });
  });

  // Test Case 3: Check code element font-family
  describe('Test Case 3: Code Element Font Family', () => {
    test('Code elements use monospace font family', () => {
      // Check for monospace in code/pre font-family
      const codeMonospace = cssText.includes('monospace') &&
        (cssText.includes('pre') || cssText.includes('code'));

      expect(codeMonospace).toBe(true);
    });

    test('Code font-family includes appropriate monospace fonts', () => {
      // Look for font-family declaration with code elements
      const codePattern = /(pre|code)[^{]*\{[^}]*font-family:\s*([^;]+)/;
      const match = cssText.match(codePattern);

      expect(match).not.toBeNull();

      const fontFamily = match[2].toLowerCase();
      const hasMonospace = fontFamily.includes('monospace') ||
                           fontFamily.includes('monaco') ||
                           fontFamily.includes('consolas') ||
                           fontFamily.includes('courier') ||
                           fontFamily.includes('sf mono') ||
                           fontFamily.includes('fira code') ||
                           fontFamily.includes('inconsolata');

      expect(hasMonospace).toBe(true);
    });
  });

  // Test Case 4: Check pre/code background color
  describe('Test Case 4: Code Block Background Color', () => {
    test('Code blocks have distinct background color from main content', () => {
      // Check for --bg-code variable
      const hasCodeBgVariable = cssText.includes('--bg-code');

      // Check for pre background styling
      const preBackground = /pre[^{]*\{[^}]*background(-color)?:/i.test(cssText);

      expect(hasCodeBgVariable || preBackground).toBe(true);
    });

    test('Code background uses different CSS variable than body', () => {
      // Verify --bg-code is different from --bg-primary
      const bgPrimaryMatch = cssText.match(/--bg-primary:\s*([^;]+)/);
      const bgCodeMatch = cssText.match(/--bg-code:\s*([^;]+)/);

      expect(bgPrimaryMatch).not.toBeNull();
      expect(bgCodeMatch).not.toBeNull();

      const bgPrimary = bgPrimaryMatch[1].trim();
      const bgCode = bgCodeMatch[1].trim();

      expect(bgPrimary).not.toBe(bgCode);
    });

    test('Code blocks have terminal-style presentation', () => {
      // Check for terminal-style attributes: border-radius, padding, border
      const hasTerminalStyle = cssText.includes('border-radius') &&
                               /pre[^{]*\{[^}]*padding:/i.test(cssText);

      expect(hasTerminalStyle).toBe(true);
    });
  });

  // Test Case 5: Verify consistent padding/margin on sections
  describe('Test Case 5: Section Spacing Consistency', () => {
    test('Sections have consistent vertical and horizontal spacing', () => {
      // Check for section padding declaration
      const sectionPadding = /section[^{]*\{[^}]*padding:/i.test(cssText);

      expect(sectionPadding).toBe(true);
    });

    test('Multiple sections exist with proper structure', () => {
      const sections = document.querySelectorAll('section');
      expect(sections.length).toBeGreaterThanOrEqual(3);
    });

    test('Container class provides horizontal padding', () => {
      // Check for container padding
      const containerPadding = /\.container[^{]*\{[^}]*padding:/i.test(cssText);

      expect(containerPadding).toBe(true);
    });

    test('All sections use container class for consistent width', () => {
      const sections = document.querySelectorAll('section');
      let sectionsWithContainer = 0;

      for (const section of sections) {
        if (section.querySelector('.container')) {
          sectionsWithContainer++;
        }
      }

      // All sections should have container class
      expect(sectionsWithContainer).toBe(sections.length);
    });
  });

  // Test Case 6: Check heading styles
  describe('Test Case 6: Heading Visual Hierarchy', () => {
    test('Headings have clear visual hierarchy with distinct sizes', () => {
      // Check for different heading sizes in CSS
      const hasH1Style = /h1[^,{]*\{[^}]*font-size:/i.test(cssText) ||
                         /#hero h1[^{]*\{[^}]*font-size:/i.test(cssText);
      const hasH2Style = /h2[^,{]*\{[^}]*font-size:/i.test(cssText);

      expect(hasH1Style).toBe(true);
      expect(hasH2Style).toBe(true);
    });

    test('H1 has larger font size than H2', () => {
      // Extract font sizes
      const h1Match = cssText.match(/#hero h1[^{]*\{[^}]*font-size:\s*([^;]+)/i) ||
                      cssText.match(/h1[^,{]*\{[^}]*font-size:\s*([^;]+)/i);
      const h2Match = cssText.match(/h2[^,{]*\{[^}]*font-size:\s*([^;]+)/i);

      expect(h1Match).not.toBeNull();
      expect(h2Match).not.toBeNull();

      // Parse rem values
      const h1Size = parseFloat(h1Match[1]);
      const h2Size = parseFloat(h2Match[1]);

      expect(h1Size).toBeGreaterThan(h2Size);
    });

    test('Headings use primary text color', () => {
      const headingColor = /h[1-3][^{]*\{[^}]*color:/i.test(cssText) ||
                           cssText.includes('var(--text-primary)');

      expect(headingColor).toBe(true);
    });
  });

  // Test Case 7: Verify link styling
  describe('Test Case 7: Link Styling', () => {
    test('Links are visually distinct from regular text', () => {
      // Check for link color styling
      const linkStyle = /^[^@]*a[^{]*\{[^}]*color:/im.test(cssText);

      expect(linkStyle).toBe(true);
    });

    test('Links have hover state defined', () => {
      // Check for a:hover styling
      const hoverStyle = /a:hover[^{]*\{[^}]*color:/i.test(cssText);

      expect(hoverStyle).toBe(true);
    });

    test('Link color uses accent color or is visually prominent', () => {
      // Check for accent variable in link color
      const linkAccent = /^[^@]*a[^{:]*\{[^}]*color:\s*var\(--accent\)/im.test(cssText) ||
                         /^[^@]*a[^{:]*\{[^}]*color:\s*#00[a-fA-F0-9]{4}/im.test(cssText);

      expect(linkAccent).toBe(true);
    });

    test('Hover color is different from default link color', () => {
      // Extract default link color and hover color
      const linkMatch = cssText.match(/^[^@]*a\s*\{[^}]*color:\s*([^;]+)/m);
      const hoverMatch = cssText.match(/a:hover[^{]*\{[^}]*color:\s*([^;]+)/);

      expect(linkMatch).not.toBeNull();
      expect(hoverMatch).not.toBeNull();

      const defaultColor = linkMatch[1].trim();
      const hoverColor = hoverMatch[1].trim();

      expect(defaultColor).not.toBe(hoverColor);
    });
  });

  // Test Case 8: Check button styling
  describe('Test Case 8: CTA Button Styling', () => {
    test('CTA buttons have clear styling distinct from regular links', () => {
      // Check for .btn class styling
      const btnStyle = /\.btn[^{]*\{[^}]+}/i.test(cssText);

      expect(btnStyle).toBe(true);
    });

    test('Buttons have background color', () => {
      const btnBackground = /\.btn[^{]*\{[^}]*background(-color)?:/i.test(cssText);

      expect(btnBackground).toBe(true);
    });

    test('Buttons have padding for proper sizing', () => {
      const btnPadding = /\.btn[^{]*\{[^}]*padding:/i.test(cssText);

      expect(btnPadding).toBe(true);
    });

    test('Buttons have border-radius for rounded appearance', () => {
      const btnRadius = /\.btn[^{]*\{[^}]*border-radius:/i.test(cssText);

      expect(btnRadius).toBe(true);
    });

    test('Buttons have hover state', () => {
      const btnHover = /\.btn:hover[^{]*\{/i.test(cssText);

      expect(btnHover).toBe(true);
    });

    test('Button exists in hero section with proper class', () => {
      const heroSection = document.querySelector('#hero');
      expect(heroSection).not.toBeNull();

      const ctaButton = heroSection.querySelector('.btn');
      expect(ctaButton).not.toBeNull();
    });

    test('CTA button has "Get Started" text', () => {
      const heroSection = document.querySelector('#hero');
      const ctaButton = heroSection.querySelector('.btn');

      expect(ctaButton).not.toBeNull();
      expect(ctaButton.textContent).toContain('Get Started');
    });
  });
});
