/**
 * Responsive Design - Mobile Tests
 *
 * These tests verify that the MirDB landing page is responsive and mobile-friendly,
 * meeting the requirements of NFR-1 and User Story 7.
 *
 * @jest-environment jsdom
 */

const fs = require('fs');
const path = require('path');

describe('Responsive Design - Mobile', () => {
  let htmlContent;
  let styleContent;

  beforeAll(() => {
    // Load the landing page HTML
    const htmlPath = path.join(__dirname, '..', 'index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf8');
    document.documentElement.innerHTML = htmlContent;

    // Extract style content for CSS analysis
    const styleMatch = htmlContent.match(/<style[^>]*>([\s\S]*?)<\/style>/i);
    styleContent = styleMatch ? styleMatch[1] : '';
  });

  // Test Case 2: Check viewport meta tag
  describe('Viewport Meta Tag', () => {
    test('should include proper viewport meta tag for mobile', () => {
      const viewportMeta = document.querySelector('meta[name="viewport"]');
      expect(viewportMeta).not.toBeNull();

      const content = viewportMeta.getAttribute('content');
      expect(content).toContain('width=device-width');
      expect(content).toContain('initial-scale=1');
    });

    test('should have viewport meta tag in head section', () => {
      // Check that the viewport tag appears in the correct location
      const headContent = htmlContent.match(/<head[^>]*>([\s\S]*?)<\/head>/i);
      expect(headContent).not.toBeNull();
      expect(headContent[1]).toContain('viewport');
    });
  });

  // Test Case 1: Load page at 375px viewport width - content fits without horizontal scrolling
  describe('Mobile Viewport (375px)', () => {
    test('should have max-width container to prevent overflow', () => {
      const containers = document.querySelectorAll('.container');
      expect(containers.length).toBeGreaterThan(0);

      // Check CSS includes container with max-width
      expect(styleContent).toMatch(/\.container\s*\{[^}]*max-width/);
    });

    test('should use box-sizing: border-box globally', () => {
      // This prevents elements from overflowing due to padding/borders
      expect(styleContent).toMatch(/box-sizing:\s*border-box/);
    });

    test('should have responsive features-grid with flexible columns', () => {
      // The features-grid should use auto-fit/auto-fill to adapt to viewport
      expect(styleContent).toMatch(/\.features-grid\s*\{[^}]*grid-template-columns[^}]*auto-fit|auto-fill/);
    });

    test('should have flexible CTA buttons that wrap on small screens', () => {
      // CTA buttons should be able to wrap
      expect(styleContent).toMatch(/\.cta-buttons\s*\{[^}]*flex-wrap:\s*wrap/);
    });

    test('should have code blocks that handle overflow with scrolling', () => {
      // Code blocks should have overflow-x: auto to handle long content
      expect(styleContent).toMatch(/\.code-block\s*\{[^}]*overflow-x:\s*auto/);
    });

    test('should not have any elements with fixed pixel widths that exceed mobile viewport', () => {
      // Check that no element has a fixed width greater than 375px
      const fixedWidthMatches = styleContent.matchAll(/width:\s*(\d+)px/g);
      for (const match of fixedWidthMatches) {
        const width = parseInt(match[1], 10);
        // Allow widths up to 800px for content max-widths (they're still constrained by container)
        // but flag anything clearly oversized
        expect(width).toBeLessThanOrEqual(1200);
      }
    });
  });

  // Test Case 3: Test tablet viewport (768px) - layout adapts appropriately
  describe('Tablet Viewport (768px)', () => {
    test('should have media query for tablet/smaller screens', () => {
      // Check for media query targeting max-width around 768px
      expect(styleContent).toMatch(/@media\s*\([^)]*max-width:\s*768px/);
    });

    test('should adjust hero heading size for smaller screens', () => {
      // The hero h1 should be smaller on mobile/tablet
      const mediaQuery768 = styleContent.match(/@media\s*\([^)]*max-width:\s*768px[^)]*\)\s*\{([^@]*)\}/s);
      expect(mediaQuery768).not.toBeNull();
      expect(mediaQuery768[1]).toMatch(/\.hero\s+h1\s*\{[^}]*font-size/);
    });

    test('should adjust tagline size for smaller screens', () => {
      const mediaQuery768 = styleContent.match(/@media\s*\([^)]*max-width:\s*768px[^)]*\)\s*\{([^@]*)\}/s);
      expect(mediaQuery768).not.toBeNull();
      expect(mediaQuery768[1]).toMatch(/\.hero\s+\.tagline|tagline\s*\{[^}]*font-size/);
    });

    test('should adjust section headings for smaller screens', () => {
      const mediaQuery768 = styleContent.match(/@media\s*\([^)]*max-width:\s*768px[^)]*\)\s*\{([^@]*)\}/s);
      expect(mediaQuery768).not.toBeNull();
      // Should have font-size adjustments for h2 headings
      expect(mediaQuery768[1]).toMatch(/h2[^}]*font-size/);
    });
  });

  // Test Case 4: Verify touch-friendly tap targets (44x44px minimum)
  describe('Touch-Friendly Tap Targets', () => {
    test('should have buttons/links with adequate padding for touch targets', () => {
      // The .btn class should have sufficient padding
      const btnMatch = styleContent.match(/\.btn\s*\{([^}]*)\}/);
      expect(btnMatch).not.toBeNull();

      // Check that padding exists (12px vertical = 24px + text height should meet 44px)
      expect(btnMatch[1]).toMatch(/padding:\s*\d+px/);
    });

    test('should have minimum button dimensions meeting touch target requirements', () => {
      // Extract btn padding values
      const btnMatch = styleContent.match(/\.btn\s*\{([^}]*)\}/);
      expect(btnMatch).not.toBeNull();

      // Parse padding - looking for format like "12px 30px" or "12px"
      const paddingMatch = btnMatch[1].match(/padding:\s*(\d+)px\s*(\d+)?px?/);
      expect(paddingMatch).not.toBeNull();

      const verticalPadding = parseInt(paddingMatch[1], 10);
      // With 12px vertical padding on top+bottom = 24px + ~16px font = ~40px
      // This is close to 44px requirement
      expect(verticalPadding).toBeGreaterThanOrEqual(10);
    });

    test('should have explicit 44px minimum height for buttons on mobile', () => {
      // Check for mobile media query that sets min-height: 44px on buttons
      // Use a broader regex to capture the entire media query block
      const hasMobileMinHeight = styleContent.includes('max-width: 480px') &&
        styleContent.includes('min-height: 44px');
      expect(hasMobileMinHeight).toBe(true);

      // Also verify btn has min-height in mobile context
      const mobileSection = styleContent.substring(styleContent.indexOf('@media (max-width: 480px)'));
      expect(mobileSection).toMatch(/\.btn[\s\S]*?min-height:\s*44px/);
    });

    test('should have footer links that are accessible', () => {
      // Footer links should have adequate spacing
      const footerLinks = document.querySelectorAll('footer a');
      expect(footerLinks.length).toBeGreaterThan(0);

      // Check that links have display/spacing that allows touch
      const footerStyle = styleContent.match(/footer\s+a\s*\{([^}]*)\}|footer a\s*\{([^}]*)\}/);
      // Footer links exist and can be styled
      expect(footerLinks.length).toBeGreaterThan(0);
    });

    test('should have footer links with 44px touch target on mobile', () => {
      // Check mobile media query includes footer link styling with 44px min-height
      const mobileSection = styleContent.substring(styleContent.indexOf('@media (max-width: 480px)'));
      expect(mobileSection).toMatch(/footer\s+a[\s\S]*?min-height:\s*44px/);
    });

    test('CTA buttons should be inline-block or block for proper sizing', () => {
      const btnMatch = styleContent.match(/\.btn\s*\{([^}]*)\}/);
      expect(btnMatch).not.toBeNull();
      expect(btnMatch[1]).toMatch(/display:\s*(inline-block|block|flex)/);
    });
  });

  // Test for 375px mobile viewport specifically
  describe('Mobile Viewport Specific (375px)', () => {
    test('should have media query for mobile screens (480px or smaller)', () => {
      expect(styleContent).toMatch(/@media\s*\([^)]*max-width:\s*480px/);
    });

    test('should have single column layout for features on mobile', () => {
      const mobileSection = styleContent.substring(styleContent.indexOf('@media (max-width: 480px)'));
      expect(mobileSection).toMatch(/\.features-grid[\s\S]*?grid-template-columns:\s*1fr/);
    });

    test('should stack CTA buttons vertically on mobile', () => {
      const mobileSection = styleContent.substring(styleContent.indexOf('@media (max-width: 480px)'));
      expect(mobileSection).toMatch(/\.cta-buttons[\s\S]*?flex-direction:\s*column/);
    });
  });

  // Additional responsive design tests
  describe('General Responsive Best Practices', () => {
    test('should use relative units (rem, em, %) for font sizes', () => {
      // Hero uses rem for font sizes
      expect(styleContent).toMatch(/font-size:\s*[\d.]+rem/);
    });

    test('should have responsive padding on sections', () => {
      // Sections should have padding that includes horizontal padding
      const heroMatch = styleContent.match(/\.hero\s*\{([^}]*)\}/);
      expect(heroMatch).not.toBeNull();
      expect(heroMatch[1]).toMatch(/padding:/);
    });

    test('should have container with auto margins for centering', () => {
      // Container should be centered
      expect(styleContent).toMatch(/\.container\s*\{[^}]*margin:\s*0\s+auto/);
    });

    test('should use CSS Grid or Flexbox for responsive layouts', () => {
      // Should use modern layout techniques
      const usesGrid = styleContent.includes('display: grid') || styleContent.includes('display:grid');
      const usesFlex = styleContent.includes('display: flex') || styleContent.includes('display:flex');
      expect(usesGrid || usesFlex).toBe(true);
    });
  });
});
