import { describe, it, expect, beforeEach } from 'vitest';
import { JSDOM } from 'jsdom';
import fs from 'fs';
import path from 'path';

describe('Error Handling and Graceful Degradation', () => {
  let document;
  let css;
  let html;

  beforeEach(() => {
    const htmlPath = path.resolve(__dirname, '../index.html');
    const cssPath = path.resolve(__dirname, '../styles.css');
    html = fs.readFileSync(htmlPath, 'utf8');
    css = fs.readFileSync(cssPath, 'utf8');
    const dom = new JSDOM(html);
    document = dom.window.document;
  });

  // Test Case 1: All core content visible without JavaScript
  describe('Test Case 1: Core content visible without JavaScript', () => {
    it('should have hero section with no JavaScript dependencies', () => {
      // Hero section should be pure HTML/CSS
      const hero = document.querySelector('.hero, header[role="banner"]');
      expect(hero).not.toBeNull();

      // Product name should be visible
      const productName = document.querySelector('.product-name, h1');
      expect(productName).not.toBeNull();
      expect(productName.textContent).toContain('MirDB');
    });

    it('should have tagline visible without JavaScript', () => {
      const tagline = document.querySelector('.tagline');
      expect(tagline).not.toBeNull();
      expect(tagline.textContent.length).toBeGreaterThan(0);
    });

    it('should have CTA buttons visible without JavaScript', () => {
      const ctaButtons = document.querySelectorAll('.btn, .cta-buttons a');
      expect(ctaButtons.length).toBeGreaterThanOrEqual(2);

      // Primary CTA
      const primaryBtn = document.querySelector('.btn-primary');
      expect(primaryBtn).not.toBeNull();
      expect(primaryBtn.getAttribute('href')).toBeTruthy();
    });

    it('should have features/get-started section without JavaScript', () => {
      const getStarted = document.querySelector('#get-started, .get-started-section');
      expect(getStarted).not.toBeNull();

      // Should contain installation instructions
      const codeBlocks = document.querySelectorAll('.code-block, pre');
      expect(codeBlocks.length).toBeGreaterThan(0);
    });

    it('should have footer visible without JavaScript', () => {
      const footer = document.querySelector('footer');
      expect(footer).not.toBeNull();

      // Footer links should work without JavaScript
      const footerLinks = footer.querySelectorAll('a');
      expect(footerLinks.length).toBeGreaterThan(0);
      footerLinks.forEach(link => {
        expect(link.getAttribute('href')).toBeTruthy();
      });
    });

    it('should not have any onclick handlers that would break without JS', () => {
      // All interactive elements should use href for navigation
      const allLinks = document.querySelectorAll('a');
      allLinks.forEach(link => {
        const href = link.getAttribute('href');
        // Links should have valid hrefs, not just onclick handlers
        expect(href).toBeTruthy();
        expect(href).not.toBe('javascript:void(0)');
        expect(href).not.toBe('#');
      });
    });

    it('should not rely on JavaScript for layout', () => {
      // No display:none elements that would need JS to show
      const inlineStyles = document.querySelectorAll('[style*="display: none"], [style*="display:none"]');
      // If there are hidden elements, they should be intentionally hidden (like skip-link)
      inlineStyles.forEach(el => {
        // Skip link and similar accessibility elements are OK
        const isAccessibilityElement = el.classList.contains('skip-link') ||
                                       el.classList.contains('sr-only') ||
                                       el.getAttribute('aria-hidden') === 'true';
        if (!isAccessibilityElement) {
          // Main content should not be hidden
          const isMainContent = el.closest('.hero, .get-started-section, footer, main');
          expect(isMainContent).toBeNull();
        }
      });
    });
  });

  // Test Case 2: Page navigable without CSS
  describe('Test Case 2: HTML structure accessible without CSS', () => {
    it('should have semantic HTML structure', () => {
      // Check for semantic landmarks
      expect(document.querySelector('header, [role="banner"]')).not.toBeNull();
      expect(document.querySelector('main, [role="main"]')).not.toBeNull();
      expect(document.querySelector('footer, [role="contentinfo"]')).not.toBeNull();
    });

    it('should have proper heading hierarchy', () => {
      const headings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
      expect(headings.length).toBeGreaterThan(0);

      // Should have exactly one h1
      const h1s = document.querySelectorAll('h1');
      expect(h1s.length).toBe(1);

      // Should have h2s for sections
      const h2s = document.querySelectorAll('h2');
      expect(h2s.length).toBeGreaterThan(0);
    });

    it('should have descriptive link text (not just "click here")', () => {
      const links = document.querySelectorAll('a');
      links.forEach(link => {
        const text = link.textContent.trim().toLowerCase();
        expect(text).not.toBe('click here');
        expect(text).not.toBe('here');
        expect(text).not.toBe('link');
        expect(text.length).toBeGreaterThan(0);
      });
    });

    it('should have skip link for keyboard navigation', () => {
      const skipLink = document.querySelector('.skip-link, [href="#main"], [href="#content"], a[href^="#get"]');
      expect(skipLink).not.toBeNull();
    });

    it('should have navigation structure in footer', () => {
      const footerNav = document.querySelector('footer nav, footer .footer-links');
      expect(footerNav).not.toBeNull();
    });

    it('should have content in logical reading order', () => {
      // Content order: hero (h1) -> main content (h2) -> footer
      const allElements = document.querySelectorAll('h1, h2, h3, main, footer');
      let foundH1 = false;
      let foundMainContent = false;
      let foundFooter = false;

      allElements.forEach(el => {
        if (el.tagName === 'H1') {
          expect(foundH1).toBe(false); // h1 should come first
          foundH1 = true;
        }
        if (el.tagName === 'MAIN') {
          expect(foundH1).toBe(true); // main should come after h1
          foundMainContent = true;
        }
        if (el.tagName === 'FOOTER') {
          expect(foundMainContent).toBe(true); // footer should come last
          foundFooter = true;
        }
      });

      expect(foundH1).toBe(true);
      expect(foundFooter).toBe(true);
    });
  });

  // Test Case 3: Image loading failure handling
  describe('Test Case 3: Image loading failure handling', () => {
    it('should have alt text on all images', () => {
      const images = document.querySelectorAll('img');
      images.forEach(img => {
        const alt = img.getAttribute('alt');
        // Alt can be empty for decorative images, but should be present
        expect(img.hasAttribute('alt')).toBe(true);
      });
    });

    it('should not have images critical to layout understanding', () => {
      // If images exist, they should be decorative or have text alternatives
      const images = document.querySelectorAll('img');
      // For a text-heavy landing page, minimal images is OK
      // Key information should be in text, not images
      images.forEach(img => {
        // Check that image is not the only way to convey information
        const parent = img.parentElement;
        if (parent) {
          // Parent should have text content or aria-label
          const hasTextContent = parent.textContent.replace(img.alt || '', '').trim().length > 0;
          const hasAriaLabel = parent.getAttribute('aria-label');
          // Either has surrounding text or is explicitly decorative
          expect(hasTextContent || hasAriaLabel || img.getAttribute('alt') === '').toBe(true);
        }
      });
    });

    it('should use CSS for decorative elements instead of images where possible', () => {
      // Check that gradients and backgrounds are CSS-based
      expect(css).toContain('linear-gradient');
      expect(css).toContain('background');
    });

    it('should have icon alternatives in CSS or Unicode', () => {
      // Feature icons should use CSS or Unicode, not image files
      // Check for text-based icons (Unicode checkmarks, etc.)
      const featureIcons = document.querySelectorAll('.feature-icon');
      featureIcons.forEach(icon => {
        // Icon should have text content (Unicode character)
        expect(icon.textContent.trim().length).toBeGreaterThan(0);
      });
    });
  });

  // Test Case 4: System font fallbacks
  describe('Test Case 4: Font stack with system font fallbacks', () => {
    it('should have system font stack for body text', () => {
      // Should include system fonts as fallbacks
      expect(css).toMatch(/font-family:.*(-apple-system|BlinkMacSystemFont|Segoe UI|Roboto|system-ui|sans-serif)/i);
    });

    it('should have sans-serif as final fallback', () => {
      // The font stack should end with generic sans-serif
      const fontFamilyMatch = css.match(/font-family:[^;]+sans-serif/);
      expect(fontFamilyMatch).not.toBeNull();
    });

    it('should include cross-platform system fonts', () => {
      // macOS
      expect(css).toContain('-apple-system');
      // Windows
      expect(css).toContain('Segoe UI');
      // Android/Linux
      expect(css).toMatch(/Roboto|Ubuntu|sans-serif/);
    });

    it('should have monospace fallback for code blocks', () => {
      // Code blocks should have monospace font stack
      expect(css).toMatch(/font-family:.*monospace/);
      // Should include popular monospace fonts
      expect(css).toMatch(/Monaco|Menlo|Consolas|Courier|monospace/i);
    });

    it('should not use web fonts as the only font option', () => {
      // Font stack should not start with a custom web font without fallbacks
      const bodyFontStack = css.match(/body\s*\{[^}]*font-family:\s*([^;]+)/);
      if (bodyFontStack) {
        const fonts = bodyFontStack[1];
        // Should contain at least one system font
        const hasSystemFont = /-apple-system|BlinkMacSystemFont|Segoe UI|Roboto|system-ui|sans-serif/i.test(fonts);
        expect(hasSystemFont).toBe(true);
      }
    });

    it('should define readable font sizes', () => {
      // Base font size should be reasonable (16px is standard)
      expect(css).toMatch(/font-size:\s*(16px|1rem|100%)/);

      // Line height should be set for readability
      expect(css).toContain('line-height');
    });
  });

  // Test Case 5: Font loading blocked - text readability
  describe('Test Case 5: Text readable with fallback fonts', () => {
    it('should have explicit font-size on key elements', () => {
      // Product name
      expect(css).toMatch(/\.product-name\s*\{[^}]*font-size/);

      // Tagline
      expect(css).toMatch(/\.tagline\s*\{[^}]*font-size/);
    });

    it('should not use icon fonts that would break without font loading', () => {
      // Should not rely on Font Awesome or similar icon fonts
      // Check that icons use Unicode or inline SVG
      const iconElements = document.querySelectorAll('.feature-icon, [class*="icon"]');
      iconElements.forEach(icon => {
        // Should not have empty content that relies on :before/:after from icon font
        const textContent = icon.textContent.trim();
        if (textContent.length === 0) {
          // If empty, should have aria-label or be hidden
          const hasAriaLabel = icon.getAttribute('aria-label') || icon.getAttribute('aria-hidden');
          expect(hasAriaLabel).toBeTruthy();
        }
      });
    });

    it('should have proper text contrast that works with any font', () => {
      // Color values should ensure readability
      expect(css).toMatch(/color:\s*#[0-9a-fA-F]{3,6}/);
      expect(css).toMatch(/background(-color)?:\s*#[0-9a-fA-F]{3,6}/);
    });

    it('should not have font-display that would cause invisible text', () => {
      // If @font-face is used, should have font-display: swap or fallback
      const hasFontFace = css.includes('@font-face');
      if (hasFontFace) {
        // Should use font-display: swap, optional, or fallback
        expect(css).toMatch(/font-display:\s*(swap|optional|fallback)/);
      }
    });

    it('should use relative units for font sizes where appropriate', () => {
      // rem units for scalable text
      expect(css).toMatch(/font-size:\s*\d+(\.\d+)?rem/);
    });
  });

  // Additional graceful degradation tests
  describe('Progressive Enhancement Best Practices', () => {
    it('should work without JavaScript entirely', () => {
      // No <noscript> with "JavaScript required" message for core content
      const noscript = document.querySelector('noscript');
      if (noscript) {
        const text = noscript.textContent.toLowerCase();
        // If there's a noscript, it should be for enhancement notices, not blocking
        expect(text).not.toContain('javascript required');
        expect(text).not.toContain('please enable javascript');
      }
    });

    it('should have no inline event handlers', () => {
      // onclick, onload, etc. are not progressive enhancement
      const elementsWithHandlers = document.querySelectorAll('[onclick], [onload], [onmouseover], [onsubmit]');
      expect(elementsWithHandlers.length).toBe(0);
    });

    it('should use data attributes for JS hooks instead of class names', () => {
      // If JS is used, it should use data attributes
      // The page should not break if those data attributes are ignored
      const allElements = document.querySelectorAll('*');
      let hasDataAttributes = false;
      allElements.forEach(el => {
        if (Object.keys(el.dataset || {}).length > 0) {
          hasDataAttributes = true;
        }
      });
      // Data attributes are optional but good practice if JS is used
      // Main assertion is that no core functionality is hidden behind JS
    });

    it('should have CSS transitions that do not affect usability when disabled', () => {
      // Should respect prefers-reduced-motion
      expect(css).toContain('@media (prefers-reduced-motion');
    });

    it('should have anchor links that work without JavaScript', () => {
      // Internal links should be proper anchor links
      const internalLinks = document.querySelectorAll('a[href^="#"]');
      internalLinks.forEach(link => {
        const targetId = link.getAttribute('href').slice(1);
        if (targetId) {
          const target = document.getElementById(targetId);
          expect(target).not.toBeNull();
        }
      });
    });
  });

  // CSS resilience tests
  describe('CSS Resilience', () => {
    it('should have default/fallback colors', () => {
      // Should define base colors that work without CSS variables
      expect(css).toMatch(/color:\s*#[0-9a-fA-F]{3,6}/);
    });

    it('should use box-sizing border-box for predictable layouts', () => {
      expect(css).toContain('box-sizing: border-box');
    });

    it('should have margin/padding reset for consistent cross-browser rendering', () => {
      expect(css).toMatch(/margin:\s*0/);
      expect(css).toMatch(/padding:\s*0/);
    });

    it('should use flexbox with fallbacks where critical', () => {
      expect(css).toContain('display: flex');
      // Flexbox is well-supported, but layout should be linear without it
    });
  });
});
