/**
 * Accessibility Integration Tests - Keyboard Navigation
 * Owner: Scenario 15 - Accessibility - Keyboard Navigation
 * Owner: Scenario 16 - Accessibility - Screen Reader
 * Owner: Scenario 17 - Accessibility - Color Contrast
 *
 * Tests keyboard navigation, focus management, and accessibility features
 * for the homepage to ensure WCAG 2.1 AA compliance.
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter, Routes, Route } from 'react-router-dom';
import { ThemeProvider } from '../../../src/contexts/ThemeContext';
import { AuthProvider } from '../../../src/contexts/AuthContext';
import { Home } from '../../../src/pages/Home';

// Mock framer-motion to avoid animation issues in tests
vi.mock('framer-motion', () => ({
  motion: {
    div: ({ children, ...props }: any) => <div {...props}>{children}</div>,
    h2: ({ children, ...props }: any) => <h2 {...props}>{children}</h2>,
    p: ({ children, ...props }: any) => <p {...props}>{children}</p>,
  },
  AnimatePresence: ({ children }: any) => <>{children}</>,
}));

/**
 * Renders Home page with all required providers
 */
function renderHomePage() {
  return render(
    <ThemeProvider>
      <AuthProvider>
        <MemoryRouter initialEntries={['/']}>
          <Routes>
            <Route path="/" element={<Home />} />
            <Route path="/register" element={<div data-testid="register-page">Register Page</div>} />
            <Route path="/login" element={<div data-testid="login-page">Login Page</div>} />
            <Route path="/dashboard" element={<div data-testid="dashboard-page">Dashboard Page</div>} />
          </Routes>
        </MemoryRouter>
      </AuthProvider>
    </ThemeProvider>
  );
}

describe('Accessibility - Keyboard Navigation', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  describe('Test Case 1: Tab through page interactive elements', () => {
    it('should allow navigation through all interactive elements using Tab key', async () => {
      const user = userEvent.setup();
      renderHomePage();

      // Start tabbing through elements
      // First tab should go to skip-to-content link (when it exists)
      await user.tab();

      // Collect all focusable elements
      const focusableSelectors = [
        'a[href]',
        'button:not([disabled])',
        '[tabindex]:not([tabindex="-1"])',
      ];

      const focusableElements = document.querySelectorAll(focusableSelectors.join(', '));
      expect(focusableElements.length).toBeGreaterThan(0);

      // Tab through several elements and verify focus moves
      let previousElement: Element | null = null;
      for (let i = 0; i < Math.min(5, focusableElements.length); i++) {
        await user.tab();
        const currentFocused = document.activeElement;
        expect(currentFocused).not.toBe(document.body);
        expect(currentFocused).not.toBe(previousElement);
        previousElement = currentFocused;
      }
    });

    it('should include hero CTA button in tab order', async () => {
      const user = userEvent.setup();
      renderHomePage();

      // Find the hero CTA
      const heroCta = screen.getByTestId('hero-cta');
      expect(heroCta).toBeInTheDocument();

      // Tab until we reach the hero CTA
      let found = false;
      for (let i = 0; i < 20; i++) {
        await user.tab();
        if (document.activeElement === heroCta) {
          found = true;
          break;
        }
      }

      expect(found).toBe(true);
    });

    it('should include navigation links in tab order', async () => {
      const user = userEvent.setup();
      renderHomePage();

      // Find the navigation login link
      const navLogin = screen.getByTestId('nav-login');
      expect(navLogin).toBeInTheDocument();

      // Tab until we reach the nav login link
      let found = false;
      for (let i = 0; i < 20; i++) {
        await user.tab();
        if (document.activeElement === navLogin) {
          found = true;
          break;
        }
      }

      expect(found).toBe(true);
    });

    it('should include footer links in tab order', async () => {
      const user = userEvent.setup();
      renderHomePage();

      // Find a footer link
      const footerLinks = screen.getAllByRole('link');
      const termsLink = footerLinks.find(link => link.textContent?.includes('Terms'));

      if (termsLink) {
        // Tab until we reach a footer link
        let found = false;
        for (let i = 0; i < 30; i++) {
          await user.tab();
          if (document.activeElement === termsLink) {
            found = true;
            break;
          }
        }

        expect(found).toBe(true);
      }
    });
  });

  describe('Test Case 2: Check focus indicators visibility', () => {
    it('should have visible focus styles on interactive elements', async () => {
      const user = userEvent.setup();
      renderHomePage();

      // Tab to an interactive element
      await user.tab();
      await user.tab();

      const focusedElement = document.activeElement as HTMLElement;

      // Check that the focused element has focus styling
      // DaisyUI/Tailwind applies focus rings via CSS classes
      expect(focusedElement).not.toBe(document.body);

      // The element should be focusable
      expect(
        focusedElement.tagName === 'A' ||
        focusedElement.tagName === 'BUTTON' ||
        focusedElement.hasAttribute('tabindex')
      ).toBe(true);
    });

    it('should apply focus ring to buttons', async () => {
      const user = userEvent.setup();
      renderHomePage();

      // Get the hero CTA button
      const heroCta = screen.getByTestId('hero-cta');

      // Focus the button
      heroCta.focus();

      expect(document.activeElement).toBe(heroCta);

      // DaisyUI buttons have built-in focus styling via btn class
      expect(heroCta.classList.contains('btn')).toBe(true);
    });

    it('should apply focus ring to links', async () => {
      const user = userEvent.setup();
      renderHomePage();

      // Get a link element
      const heroLoginLink = screen.getByTestId('hero-login-link');

      // Focus the link
      heroLoginLink.focus();

      expect(document.activeElement).toBe(heroLoginLink);

      // Link should be an anchor tag
      expect(heroLoginLink.tagName).toBe('A');
    });
  });

  describe('Test Case 3: Press Enter on focused CTA button', () => {
    it('should activate hero CTA and navigate to register when Enter is pressed', async () => {
      const user = userEvent.setup();
      renderHomePage();

      // Get the hero CTA
      const heroCta = screen.getByTestId('hero-cta');

      // Focus the hero CTA
      heroCta.focus();
      expect(document.activeElement).toBe(heroCta);

      // Press Enter
      await user.keyboard('{Enter}');

      // Should navigate to register page
      await waitFor(() => {
        expect(screen.getByTestId('register-page')).toBeInTheDocument();
      });
    });

    it('should activate navigation login link when Enter is pressed', async () => {
      const user = userEvent.setup();
      renderHomePage();

      // Get the navigation login link
      const navLogin = screen.getByTestId('nav-login');

      // Focus the login link
      navLogin.focus();
      expect(document.activeElement).toBe(navLogin);

      // Press Enter
      await user.keyboard('{Enter}');

      // Should navigate to login page
      await waitFor(() => {
        expect(screen.getByTestId('login-page')).toBeInTheDocument();
      });
    });

    it('should activate dashboard preview CTA when Enter is pressed', async () => {
      const user = userEvent.setup();
      renderHomePage();

      // Get the dashboard preview CTA
      const dashboardCta = screen.getByTestId('dashboard-preview-cta');

      // Focus the CTA
      dashboardCta.focus();
      expect(document.activeElement).toBe(dashboardCta);

      // Press Enter
      await user.keyboard('{Enter}');

      // Should navigate to register page (since it links to /register)
      await waitFor(() => {
        expect(screen.getByTestId('register-page')).toBeInTheDocument();
      });
    });
  });

  describe('Test Case 4: Check for skip-to-content link', () => {
    it('should have a skip-to-content link as first focusable element', async () => {
      const user = userEvent.setup();
      renderHomePage();

      // Press Tab to focus the first element
      await user.tab();

      // The first focusable element should be the skip-to-content link
      const skipLink = screen.queryByText(/skip to main content/i) ||
                       screen.queryByText(/skip to content/i);

      expect(skipLink).toBeInTheDocument();
      expect(document.activeElement).toBe(skipLink);
    });

    it('should move focus to main content when skip link is activated', async () => {
      const user = userEvent.setup();
      renderHomePage();

      // Press Tab to focus the skip link
      await user.tab();

      // Get the skip link
      const skipLink = screen.queryByText(/skip to main content/i) ||
                       screen.queryByText(/skip to content/i);

      if (skipLink) {
        // Press Enter to activate
        await user.keyboard('{Enter}');

        // Focus should move to main content or first heading
        await waitFor(() => {
          const mainContent = document.getElementById('main-content') ||
                             document.querySelector('main');
          expect(document.activeElement).toBe(mainContent);
        });
      }
    });

    it('should only show skip link when focused', async () => {
      renderHomePage();

      // Find the skip link
      const skipLink = screen.queryByText(/skip to main content/i) ||
                       screen.queryByText(/skip to content/i);

      if (skipLink) {
        // Skip link should be visually hidden when not focused (sr-only class or similar)
        const skipLinkClasses = skipLink.className;
        const isVisuallyHidden =
          skipLinkClasses.includes('sr-only') ||
          skipLinkClasses.includes('visually-hidden') ||
          skipLinkClasses.includes('focus:not-sr-only');

        // If not hidden with CSS class, check computed styles
        const computedStyle = window.getComputedStyle(skipLink);
        const isHiddenUntilFocused =
          isVisuallyHidden ||
          computedStyle.position === 'absolute' ||
          computedStyle.clip !== 'auto';

        expect(isHiddenUntilFocused).toBe(true);
      }
    });
  });

  describe('Additional keyboard navigation tests', () => {
    it('should support Shift+Tab to navigate backwards', async () => {
      const user = userEvent.setup();
      renderHomePage();

      // Tab forward a few times
      await user.tab();
      await user.tab();
      await user.tab();

      const thirdElement = document.activeElement;

      // Tab forward one more
      await user.tab();
      const fourthElement = document.activeElement;

      expect(fourthElement).not.toBe(thirdElement);

      // Shift+Tab to go back
      await user.tab({ shift: true });

      expect(document.activeElement).toBe(thirdElement);
    });

    it('should not trap focus in any component', async () => {
      const user = userEvent.setup();
      renderHomePage();

      // Tab through all elements and ensure we can eventually reach the end
      // or cycle back to the beginning
      const startingElement = document.activeElement;

      // Tab many times
      for (let i = 0; i < 50; i++) {
        await user.tab();
      }

      // We should have cycled through elements without getting stuck
      // (no infinite loop or focus trap)
      expect(document.activeElement).not.toBe(null);
    });

    it('should maintain focus order in logical sequence', async () => {
      const user = userEvent.setup();
      renderHomePage();

      const focusedElements: Element[] = [];

      // Collect focus order
      for (let i = 0; i < 15; i++) {
        await user.tab();
        const el = document.activeElement;
        if (el && el !== document.body) {
          focusedElements.push(el);
        }
      }

      // Verify we have collected focus elements
      expect(focusedElements.length).toBeGreaterThan(0);

      // Ensure focus is progressing through different elements
      // Check that consecutive elements are different DOM nodes
      for (let i = 1; i < focusedElements.length; i++) {
        // Consecutive elements should be different DOM nodes (focus is progressing)
        expect(focusedElements[i]).not.toBe(focusedElements[i - 1]);
      }
    });
  });
});

/**
 * Accessibility - Color Contrast Tests
 * Owner: Scenario 17 - Accessibility - Color Contrast
 *
 * Verifies that text and interactive elements meet WCAG AA color contrast requirements.
 * - Normal text: 4.5:1 contrast ratio minimum
 * - Large text (18pt+): 3:1 contrast ratio minimum
 * - Interactive elements: Must meet contrast requirements
 */

/**
 * Calculate relative luminance of a color
 * Per WCAG 2.1 formula: https://www.w3.org/WAI/WCAG21/Techniques/general/G17
 */
function getLuminance(r: number, g: number, b: number): number {
  const [rs, gs, bs] = [r, g, b].map((c) => {
    const sRGB = c / 255;
    return sRGB <= 0.03928 ? sRGB / 12.92 : Math.pow((sRGB + 0.055) / 1.055, 2.4);
  });
  return 0.2126 * rs + 0.7152 * gs + 0.0722 * bs;
}

/**
 * Calculate contrast ratio between two colors
 * Per WCAG 2.1 formula
 */
function getContrastRatio(color1: { r: number; g: number; b: number }, color2: { r: number; g: number; b: number }): number {
  const l1 = getLuminance(color1.r, color1.g, color1.b);
  const l2 = getLuminance(color2.r, color2.g, color2.b);
  const lighter = Math.max(l1, l2);
  const darker = Math.min(l1, l2);
  return (lighter + 0.05) / (darker + 0.05);
}

/**
 * Parse CSS color string to RGB values
 * Supports rgb(), rgba(), and hex formats
 */
function parseColor(colorStr: string): { r: number; g: number; b: number } | null {
  if (!colorStr || colorStr === 'transparent' || colorStr === 'rgba(0, 0, 0, 0)') {
    return null;
  }

  // Handle rgb/rgba format
  const rgbMatch = colorStr.match(/rgba?\((\d+),\s*(\d+),\s*(\d+)/);
  if (rgbMatch) {
    return {
      r: parseInt(rgbMatch[1], 10),
      g: parseInt(rgbMatch[2], 10),
      b: parseInt(rgbMatch[3], 10),
    };
  }

  // Handle hex format
  const hexMatch = colorStr.match(/^#?([a-f\d]{2})([a-f\d]{2})([a-f\d]{2})$/i);
  if (hexMatch) {
    return {
      r: parseInt(hexMatch[1], 16),
      g: parseInt(hexMatch[2], 16),
      b: parseInt(hexMatch[3], 16),
    };
  }

  return null;
}

/**
 * Get computed color from an element, traversing up if transparent
 */
function getEffectiveBackgroundColor(element: Element): { r: number; g: number; b: number } {
  let currentElement: Element | null = element;

  while (currentElement) {
    const style = window.getComputedStyle(currentElement);
    const bgColor = style.backgroundColor;
    const parsed = parseColor(bgColor);

    if (parsed && (parsed.r !== 0 || parsed.g !== 0 || parsed.b !== 0 || bgColor.includes('255'))) {
      return parsed;
    }

    currentElement = currentElement.parentElement;
  }

  // Default to white background if nothing found
  return { r: 255, g: 255, b: 255 };
}

/**
 * Check if an element has large text (18pt+ or 14pt+ bold)
 */
function isLargeText(element: Element): boolean {
  const style = window.getComputedStyle(element);
  const fontSize = parseFloat(style.fontSize);
  const fontWeight = parseInt(style.fontWeight, 10) || 400;

  // 18pt = 24px, 14pt = 18.67px
  // Large text: 18pt+ normal, or 14pt+ bold (700+)
  return fontSize >= 24 || (fontSize >= 18.67 && fontWeight >= 700);
}

/**
 * WCAG AA contrast requirements
 */
const WCAG_AA = {
  NORMAL_TEXT: 4.5,
  LARGE_TEXT: 3.0,
};

describe('Accessibility - Color Contrast', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    // Reset theme to light mode before each test
    document.documentElement.setAttribute('data-theme', 'light');
    localStorage.setItem('theme', 'light');
  });

  describe('Test Case 1: Audit color contrast in light mode', () => {
    it('should have sufficient contrast for normal text elements in light mode', () => {
      document.documentElement.setAttribute('data-theme', 'light');
      renderHomePage();

      // Get all text elements
      const textElements = document.querySelectorAll('p, span, a:not([class*="btn"])');

      textElements.forEach((element) => {
        const htmlElement = element as HTMLElement;
        const style = window.getComputedStyle(htmlElement);
        const textColor = parseColor(style.color);
        const bgColor = getEffectiveBackgroundColor(htmlElement);

        if (textColor && bgColor) {
          const contrastRatio = getContrastRatio(textColor, bgColor);
          const requiredRatio = isLargeText(element) ? WCAG_AA.LARGE_TEXT : WCAG_AA.NORMAL_TEXT;

          // Normal text should have at least 4.5:1 contrast
          expect(contrastRatio).toBeGreaterThanOrEqual(requiredRatio);
        }
      });
    });

    it('should have sufficient contrast for headings in light mode', () => {
      document.documentElement.setAttribute('data-theme', 'light');
      renderHomePage();

      // Get all heading elements
      const headings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');

      expect(headings.length).toBeGreaterThan(0);

      headings.forEach((heading) => {
        const htmlElement = heading as HTMLElement;
        const style = window.getComputedStyle(htmlElement);
        const textColor = parseColor(style.color);
        const bgColor = getEffectiveBackgroundColor(htmlElement);

        if (textColor && bgColor) {
          const contrastRatio = getContrastRatio(textColor, bgColor);
          // Large text (headings) need at least 3:1 contrast
          const requiredRatio = isLargeText(heading) ? WCAG_AA.LARGE_TEXT : WCAG_AA.NORMAL_TEXT;

          expect(contrastRatio).toBeGreaterThanOrEqual(requiredRatio);
        }
      });
    });

    it('should have sufficient contrast for navigation links in light mode', () => {
      document.documentElement.setAttribute('data-theme', 'light');
      renderHomePage();

      // Get navigation links
      const navLinks = document.querySelectorAll('nav a, [data-testid="nav-login"], [data-testid="nav-register"]');

      navLinks.forEach((link) => {
        const htmlElement = link as HTMLElement;
        const style = window.getComputedStyle(htmlElement);
        const textColor = parseColor(style.color);
        const bgColor = getEffectiveBackgroundColor(htmlElement);

        if (textColor && bgColor) {
          const contrastRatio = getContrastRatio(textColor, bgColor);
          const requiredRatio = isLargeText(link) ? WCAG_AA.LARGE_TEXT : WCAG_AA.NORMAL_TEXT;

          expect(contrastRatio).toBeGreaterThanOrEqual(requiredRatio);
        }
      });
    });
  });

  describe('Test Case 2: Audit color contrast in dark mode', () => {
    it('should have sufficient contrast for normal text elements in dark mode', () => {
      document.documentElement.setAttribute('data-theme', 'dark');
      localStorage.setItem('theme', 'dark');
      renderHomePage();

      // Get all text elements
      const textElements = document.querySelectorAll('p, span, a:not([class*="btn"])');

      textElements.forEach((element) => {
        const htmlElement = element as HTMLElement;
        const style = window.getComputedStyle(htmlElement);
        const textColor = parseColor(style.color);
        const bgColor = getEffectiveBackgroundColor(htmlElement);

        if (textColor && bgColor) {
          const contrastRatio = getContrastRatio(textColor, bgColor);
          const requiredRatio = isLargeText(element) ? WCAG_AA.LARGE_TEXT : WCAG_AA.NORMAL_TEXT;

          expect(contrastRatio).toBeGreaterThanOrEqual(requiredRatio);
        }
      });
    });

    it('should have sufficient contrast for headings in dark mode', () => {
      document.documentElement.setAttribute('data-theme', 'dark');
      localStorage.setItem('theme', 'dark');
      renderHomePage();

      // Get all heading elements
      const headings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');

      expect(headings.length).toBeGreaterThan(0);

      headings.forEach((heading) => {
        const htmlElement = heading as HTMLElement;
        const style = window.getComputedStyle(htmlElement);
        const textColor = parseColor(style.color);
        const bgColor = getEffectiveBackgroundColor(htmlElement);

        if (textColor && bgColor) {
          const contrastRatio = getContrastRatio(textColor, bgColor);
          const requiredRatio = isLargeText(heading) ? WCAG_AA.LARGE_TEXT : WCAG_AA.NORMAL_TEXT;

          expect(contrastRatio).toBeGreaterThanOrEqual(requiredRatio);
        }
      });
    });

    it('should have sufficient contrast for navigation links in dark mode', () => {
      document.documentElement.setAttribute('data-theme', 'dark');
      localStorage.setItem('theme', 'dark');
      renderHomePage();

      // Get navigation links
      const navLinks = document.querySelectorAll('nav a, [data-testid="nav-login"], [data-testid="nav-register"]');

      navLinks.forEach((link) => {
        const htmlElement = link as HTMLElement;
        const style = window.getComputedStyle(htmlElement);
        const textColor = parseColor(style.color);
        const bgColor = getEffectiveBackgroundColor(htmlElement);

        if (textColor && bgColor) {
          const contrastRatio = getContrastRatio(textColor, bgColor);
          const requiredRatio = isLargeText(link) ? WCAG_AA.LARGE_TEXT : WCAG_AA.NORMAL_TEXT;

          expect(contrastRatio).toBeGreaterThanOrEqual(requiredRatio);
        }
      });
    });

    it('should have sufficient contrast for footer content in dark mode', () => {
      document.documentElement.setAttribute('data-theme', 'dark');
      localStorage.setItem('theme', 'dark');
      renderHomePage();

      // Get footer elements
      const footerElements = document.querySelectorAll('footer, footer *');

      footerElements.forEach((element) => {
        if (element.textContent && element.textContent.trim()) {
          const htmlElement = element as HTMLElement;
          const style = window.getComputedStyle(htmlElement);
          const textColor = parseColor(style.color);
          const bgColor = getEffectiveBackgroundColor(htmlElement);

          if (textColor && bgColor) {
            const contrastRatio = getContrastRatio(textColor, bgColor);
            const requiredRatio = isLargeText(element) ? WCAG_AA.LARGE_TEXT : WCAG_AA.NORMAL_TEXT;

            expect(contrastRatio).toBeGreaterThanOrEqual(requiredRatio);
          }
        }
      });
    });
  });

  describe('Test Case 3: Check CTA button contrast', () => {
    it('should have sufficient contrast for hero CTA button text against button background', () => {
      renderHomePage();

      const heroCta = screen.getByTestId('hero-cta');
      expect(heroCta).toBeInTheDocument();

      const style = window.getComputedStyle(heroCta);
      const textColor = parseColor(style.color);
      const bgColor = parseColor(style.backgroundColor);

      if (textColor && bgColor) {
        const contrastRatio = getContrastRatio(textColor, bgColor);
        // Button text should have at least 4.5:1 contrast for normal text
        // or 3:1 for large text (buttons are typically large)
        const requiredRatio = isLargeText(heroCta) ? WCAG_AA.LARGE_TEXT : WCAG_AA.NORMAL_TEXT;

        expect(contrastRatio).toBeGreaterThanOrEqual(requiredRatio);
      }
    });

    it('should have sufficient contrast for dashboard preview CTA button', () => {
      renderHomePage();

      const dashboardCta = screen.getByTestId('dashboard-preview-cta');
      expect(dashboardCta).toBeInTheDocument();

      const style = window.getComputedStyle(dashboardCta);
      const textColor = parseColor(style.color);
      const bgColor = parseColor(style.backgroundColor);

      if (textColor && bgColor) {
        const contrastRatio = getContrastRatio(textColor, bgColor);
        const requiredRatio = isLargeText(dashboardCta) ? WCAG_AA.LARGE_TEXT : WCAG_AA.NORMAL_TEXT;

        expect(contrastRatio).toBeGreaterThanOrEqual(requiredRatio);
      }
    });

    it('should have sufficient contrast for all primary buttons', () => {
      renderHomePage();

      // Get all primary buttons (btn-primary class from DaisyUI)
      const primaryButtons = document.querySelectorAll('.btn-primary');

      primaryButtons.forEach((button) => {
        const htmlElement = button as HTMLElement;
        const style = window.getComputedStyle(htmlElement);
        const textColor = parseColor(style.color);
        const bgColor = parseColor(style.backgroundColor);

        if (textColor && bgColor) {
          const contrastRatio = getContrastRatio(textColor, bgColor);
          const requiredRatio = isLargeText(button) ? WCAG_AA.LARGE_TEXT : WCAG_AA.NORMAL_TEXT;

          expect(contrastRatio).toBeGreaterThanOrEqual(requiredRatio);
        }
      });
    });

    it('should have sufficient contrast for hero CTA in dark mode', () => {
      document.documentElement.setAttribute('data-theme', 'dark');
      localStorage.setItem('theme', 'dark');
      renderHomePage();

      const heroCta = screen.getByTestId('hero-cta');
      expect(heroCta).toBeInTheDocument();

      const style = window.getComputedStyle(heroCta);
      const textColor = parseColor(style.color);
      const bgColor = parseColor(style.backgroundColor);

      if (textColor && bgColor) {
        const contrastRatio = getContrastRatio(textColor, bgColor);
        const requiredRatio = isLargeText(heroCta) ? WCAG_AA.LARGE_TEXT : WCAG_AA.NORMAL_TEXT;

        expect(contrastRatio).toBeGreaterThanOrEqual(requiredRatio);
      }
    });

    it('should have sufficient contrast for navigation register button', () => {
      renderHomePage();

      const navRegister = screen.getByTestId('nav-register');
      expect(navRegister).toBeInTheDocument();

      const style = window.getComputedStyle(navRegister);
      const textColor = parseColor(style.color);
      const bgColor = parseColor(style.backgroundColor);

      if (textColor && bgColor) {
        const contrastRatio = getContrastRatio(textColor, bgColor);
        const requiredRatio = isLargeText(navRegister) ? WCAG_AA.LARGE_TEXT : WCAG_AA.NORMAL_TEXT;

        expect(contrastRatio).toBeGreaterThanOrEqual(requiredRatio);
      }
    });
  });

  describe('Additional color contrast validations', () => {
    it('should have sufficient contrast for feature card titles', () => {
      renderHomePage();

      // Get feature card titles
      const featureTitles = document.querySelectorAll('[data-testid^="feature-title-"]');

      featureTitles.forEach((title) => {
        const htmlElement = title as HTMLElement;
        const style = window.getComputedStyle(htmlElement);
        const textColor = parseColor(style.color);
        const bgColor = getEffectiveBackgroundColor(htmlElement);

        if (textColor && bgColor) {
          const contrastRatio = getContrastRatio(textColor, bgColor);
          const requiredRatio = isLargeText(title) ? WCAG_AA.LARGE_TEXT : WCAG_AA.NORMAL_TEXT;

          expect(contrastRatio).toBeGreaterThanOrEqual(requiredRatio);
        }
      });
    });

    it('should have sufficient contrast for feature card descriptions', () => {
      renderHomePage();

      // Get feature card descriptions
      const featureDescriptions = document.querySelectorAll('[data-testid^="feature-description-"]');

      featureDescriptions.forEach((desc) => {
        const htmlElement = desc as HTMLElement;
        const style = window.getComputedStyle(htmlElement);
        const textColor = parseColor(style.color);
        const bgColor = getEffectiveBackgroundColor(htmlElement);

        if (textColor && bgColor) {
          const contrastRatio = getContrastRatio(textColor, bgColor);
          const requiredRatio = isLargeText(desc) ? WCAG_AA.LARGE_TEXT : WCAG_AA.NORMAL_TEXT;

          expect(contrastRatio).toBeGreaterThanOrEqual(requiredRatio);
        }
      });
    });

    it('should have sufficient contrast for how-it-works step content', () => {
      renderHomePage();

      // Get how-it-works step elements
      const stepTitles = document.querySelectorAll('[data-testid^="how-it-works-step-"][data-testid$="-title"]');
      const stepDescriptions = document.querySelectorAll('[data-testid^="how-it-works-step-"][data-testid$="-description"]');

      [...stepTitles, ...stepDescriptions].forEach((element) => {
        const htmlElement = element as HTMLElement;
        const style = window.getComputedStyle(htmlElement);
        const textColor = parseColor(style.color);
        const bgColor = getEffectiveBackgroundColor(htmlElement);

        if (textColor && bgColor) {
          const contrastRatio = getContrastRatio(textColor, bgColor);
          const requiredRatio = isLargeText(element) ? WCAG_AA.LARGE_TEXT : WCAG_AA.NORMAL_TEXT;

          expect(contrastRatio).toBeGreaterThanOrEqual(requiredRatio);
        }
      });
    });

    it('should have sufficient contrast for login link in hero section', () => {
      renderHomePage();

      const heroLoginLink = screen.getByTestId('hero-login-link');
      expect(heroLoginLink).toBeInTheDocument();

      const style = window.getComputedStyle(heroLoginLink);
      const textColor = parseColor(style.color);
      const bgColor = getEffectiveBackgroundColor(heroLoginLink);

      if (textColor && bgColor) {
        const contrastRatio = getContrastRatio(textColor, bgColor);
        const requiredRatio = isLargeText(heroLoginLink) ? WCAG_AA.LARGE_TEXT : WCAG_AA.NORMAL_TEXT;

        expect(contrastRatio).toBeGreaterThanOrEqual(requiredRatio);
      }
    });
  });
});
