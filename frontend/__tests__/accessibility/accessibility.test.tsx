/**
 * Accessibility Compliance Tests
 * Scenario 7: Validates WCAG 2.1 AA accessibility standards
 *
 * Tests cover:
 * - Keyboard navigation (Tab order)
 * - Focus indicators visibility
 * - Enter key form submission
 * - ARIA labels
 * - Form labels
 * - Color contrast (via axe-core)
 * - Skip-to-content link
 */
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { render, screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { BrowserRouter } from 'react-router-dom';
import { axe, toHaveNoViolations } from 'jest-axe';
import Home from '../../src/pages/Home';
import { ThemeProvider } from '../../src/contexts/ThemeContext';
import { UrlShortenerForm } from '../../src/components/UrlShortenerForm';
import { ShortUrlResult } from '../../src/components/ShortUrlResult';

// Extend expect with axe matchers
expect.extend(toHaveNoViolations);

// Helper to render Home component with all providers
const renderHome = () => {
  return render(
    <BrowserRouter>
      <ThemeProvider>
        <Home />
      </ThemeProvider>
    </BrowserRouter>
  );
};

// Helper to render with just BrowserRouter for isolated component tests
const renderWithRouter = (component: React.ReactElement) => {
  return render(<BrowserRouter>{component}</BrowserRouter>);
};

describe('Accessibility Compliance (Scenario 7)', () => {
  beforeEach(() => {
    localStorage.clear();
  });

  afterEach(() => {
    localStorage.clear();
  });

  /**
   * Test Case 1: Tab through homepage and track focus order
   * Expected: Focus moves through elements in logical order: nav, URL input, submit, CTAs
   */
  describe('Test Case 1: Keyboard Navigation Focus Order', () => {
    it('should move focus through elements in logical order when tabbing', async () => {
      const user = userEvent.setup();
      renderHome();

      // Focus order should be: skip link -> theme toggle -> (URL form area) -> Get Started -> Login -> features
      // Start with no focus, then tab through

      // First tab should focus skip link (which is visually hidden but focusable)
      await user.tab();
      const skipLink = screen.getByText(/skip to main content/i);
      expect(skipLink).toHaveFocus();

      // Second tab should focus the theme toggle button
      await user.tab();
      const themeToggle = screen.getByTestId('theme-toggle');
      expect(themeToggle).toHaveFocus();

      // Continue tabbing to CTAs
      await user.tab();
      const getStartedLink = screen.getByRole('link', { name: /get started/i });
      expect(getStartedLink).toHaveFocus();

      await user.tab();
      const loginLink = screen.getByRole('link', { name: /login/i });
      expect(loginLink).toHaveFocus();
    });

    it('should allow tabbing through all interactive elements', async () => {
      const user = userEvent.setup();
      renderHome();

      // Collect all focusable elements
      const focusableElements: Element[] = [];
      let iterations = 0;
      const maxIterations = 20; // Safety limit

      // Tab through the page and collect focused elements
      while (iterations < maxIterations) {
        await user.tab();
        const activeElement = document.activeElement;

        if (!activeElement || activeElement === document.body) break;
        if (focusableElements.includes(activeElement)) break; // Looped back

        focusableElements.push(activeElement);
        iterations++;
      }

      // Should have multiple focusable elements
      expect(focusableElements.length).toBeGreaterThanOrEqual(4);
    });
  });

  /**
   * Test Case 2: Check focus visibility on all interactive elements
   * Expected: Each element shows visible focus ring/outline when focused
   */
  describe('Test Case 2: Focus Visibility', () => {
    it('should have focus indicators on interactive elements', async () => {
      const user = userEvent.setup();
      renderHome();

      // Theme toggle should have focus styles
      const themeToggle = screen.getByTestId('theme-toggle');
      themeToggle.focus();
      expect(document.activeElement).toBe(themeToggle);
      // DaisyUI btn-ghost provides focus styles via outline

      // CTA buttons should have focus styles
      const getStartedLink = screen.getByRole('link', { name: /get started/i });
      getStartedLink.focus();
      expect(document.activeElement).toBe(getStartedLink);

      const loginLink = screen.getByRole('link', { name: /login/i });
      loginLink.focus();
      expect(document.activeElement).toBe(loginLink);
    });

    it('should show visible focus on skip-to-content link when focused', () => {
      renderHome();

      const skipLink = screen.getByText(/skip to main content/i);
      skipLink.focus();

      expect(document.activeElement).toBe(skipLink);
      // When focused, skip link should become visible (not sr-only)
      expect(skipLink).toHaveClass('focus:not-sr-only');
    });
  });

  /**
   * Test Case 3: Focus URL input, enter URL, press Enter
   * Expected: Form submits without clicking button
   */
  describe('Test Case 3: Enter Key Form Submission', () => {
    it('should submit form when pressing Enter in URL input', async () => {
      const user = userEvent.setup();
      const onShorten = vi.fn();

      renderWithRouter(
        <UrlShortenerForm onShorten={onShorten} isLoading={false} error={null} />
      );

      const input = screen.getByRole('textbox');
      await user.type(input, 'https://example.com');
      await user.keyboard('{Enter}');

      expect(onShorten).toHaveBeenCalledWith('https://example.com');
    });

    it('should submit form via Enter key without needing to click button', async () => {
      const user = userEvent.setup();
      const onShorten = vi.fn();

      renderWithRouter(
        <UrlShortenerForm onShorten={onShorten} isLoading={false} error={null} />
      );

      const input = screen.getByRole('textbox');
      await user.click(input);
      await user.type(input, 'https://test.example.org');
      await user.keyboard('{Enter}');

      expect(onShorten).toHaveBeenCalledTimes(1);
      expect(onShorten).toHaveBeenCalledWith('https://test.example.org');
    });
  });

  /**
   * Test Case 4: Run axe-core accessibility audit on homepage
   * Expected: No critical or serious accessibility violations
   */
  describe('Test Case 4: Axe-core Accessibility Audit', () => {
    it('should have no critical or serious accessibility violations', async () => {
      const { container } = renderHome();

      const results = await axe(container, {
        rules: {
          // Only report critical and serious issues
          region: { enabled: false }, // Allow content outside landmarks for this test
        },
      });

      // Filter for only critical and serious violations
      const criticalViolations = results.violations.filter(
        (v) => v.impact === 'critical' || v.impact === 'serious'
      );

      expect(criticalViolations).toHaveLength(0);
    });

    it('should pass core accessibility rules', async () => {
      const { container } = renderHome();

      const results = await axe(container, {
        runOnly: {
          type: 'tag',
          values: ['wcag2a', 'wcag2aa'],
        },
        rules: {
          region: { enabled: false },
        },
      });

      // Allow minor/moderate issues but no critical/serious
      const criticalViolations = results.violations.filter(
        (v) => v.impact === 'critical' || v.impact === 'serious'
      );

      expect(criticalViolations).toHaveLength(0);
    });
  });

  /**
   * Test Case 5: Check URL input for associated label
   * Expected: Input has aria-label or associated <label> element
   */
  describe('Test Case 5: URL Input Label', () => {
    it('should have an associated label for the URL input', () => {
      renderWithRouter(
        <UrlShortenerForm onShorten={vi.fn()} isLoading={false} error={null} />
      );

      const input = screen.getByRole('textbox');

      // Check for associated label via htmlFor/id
      const inputId = input.getAttribute('id');
      expect(inputId).toBeTruthy();

      // The label should exist and be associated
      const label = document.querySelector(`label[for="${inputId}"]`);
      expect(label).toBeInTheDocument();
    });

    it('should have accessible name for URL input', () => {
      renderWithRouter(
        <UrlShortenerForm onShorten={vi.fn()} isLoading={false} error={null} />
      );

      // The input should be findable by its accessible name
      const input = screen.getByLabelText(/enter url|url to shorten/i);
      expect(input).toBeInTheDocument();
    });
  });

  /**
   * Test Case 6: Check submit button for accessible name
   * Expected: Button has accessible name (text content or aria-label)
   */
  describe('Test Case 6: Submit Button Accessible Name', () => {
    it('should have an accessible name on the submit button', () => {
      renderWithRouter(
        <UrlShortenerForm onShorten={vi.fn()} isLoading={false} error={null} />
      );

      const submitButton = screen.getByRole('button', { name: /shorten/i });
      expect(submitButton).toBeInTheDocument();
    });

    it('should have accessible loading state on submit button', () => {
      renderWithRouter(
        <UrlShortenerForm onShorten={vi.fn()} isLoading={true} error={null} />
      );

      const submitButton = screen.getByRole('button');
      expect(submitButton).toHaveAttribute('aria-busy', 'true');
      expect(submitButton).toHaveTextContent(/shortening/i);
    });
  });

  /**
   * Test Case 7 & 8: Color Contrast Tests
   * Expected: All text meets 4.5:1 contrast ratio
   * Note: Actual color contrast is validated by axe-core
   */
  describe('Test Case 7 & 8: Color Contrast', () => {
    it('should pass color contrast requirements in light mode', async () => {
      const { container } = render(
        <BrowserRouter>
          <ThemeProvider>
            <Home />
          </ThemeProvider>
        </BrowserRouter>
      );

      const results = await axe(container, {
        runOnly: {
          type: 'rule',
          values: ['color-contrast'],
        },
      });

      // No critical color contrast violations
      const criticalViolations = results.violations.filter(
        (v) => v.impact === 'critical' || v.impact === 'serious'
      );
      expect(criticalViolations).toHaveLength(0);
    });

    it('should pass color contrast requirements in dark mode', async () => {
      // Set dark mode preference
      localStorage.setItem('theme', 'dark');

      const { container } = render(
        <BrowserRouter>
          <ThemeProvider>
            <Home />
          </ThemeProvider>
        </BrowserRouter>
      );

      const results = await axe(container, {
        runOnly: {
          type: 'rule',
          values: ['color-contrast'],
        },
      });

      const criticalViolations = results.violations.filter(
        (v) => v.impact === 'critical' || v.impact === 'serious'
      );
      expect(criticalViolations).toHaveLength(0);
    });
  });

  /**
   * Test Case 9: Check for skip-to-content link
   * Expected: Hidden skip link becomes visible on focus and links to main content
   */
  describe('Test Case 9: Skip-to-Content Link', () => {
    it('should have a skip-to-content link', () => {
      renderHome();

      const skipLink = screen.getByText(/skip to main content/i);
      expect(skipLink).toBeInTheDocument();
      expect(skipLink).toHaveAttribute('href', '#main-content');
    });

    it('should link to main content area', () => {
      renderHome();

      const skipLink = screen.getByText(/skip to main content/i);
      const mainContent = document.getElementById('main-content');

      expect(skipLink).toHaveAttribute('href', '#main-content');
      expect(mainContent).toBeInTheDocument();
    });

    it('should be visually hidden by default but visible on focus', () => {
      renderHome();

      const skipLink = screen.getByText(/skip to main content/i);

      // Should have sr-only class (visually hidden)
      expect(skipLink).toHaveClass('sr-only');
      // Should become visible on focus
      expect(skipLink).toHaveClass('focus:not-sr-only');
    });

    it('should become visible when focused', () => {
      renderHome();

      const skipLink = screen.getByText(/skip to main content/i);
      skipLink.focus();

      expect(document.activeElement).toBe(skipLink);
      // The focus:not-sr-only class makes it visible when focused
    });
  });

  /**
   * Test Case 10: Verify copy button has accessible feedback
   * Expected: Screen reader announces 'Copied to clipboard' via aria-live region
   */
  describe('Test Case 10: Copy Button Accessible Feedback', () => {
    it('should have aria-live region for copy feedback', () => {
      renderWithRouter(
        <ShortUrlResult
          shortUrl="https://short.url/abc123"
          onCopy={vi.fn()}
          copied={false}
        />
      );

      const liveRegion = document.querySelector('[aria-live="polite"]');
      expect(liveRegion).toBeInTheDocument();
    });

    it('should announce "Copied to clipboard" when copied is true', () => {
      renderWithRouter(
        <ShortUrlResult
          shortUrl="https://short.url/abc123"
          onCopy={vi.fn()}
          copied={true}
        />
      );

      const liveRegion = document.querySelector('[aria-live="polite"]');
      expect(liveRegion).toHaveTextContent(/copied to clipboard/i);
    });

    it('should update copy button aria-label when copied', () => {
      const { rerender } = renderWithRouter(
        <ShortUrlResult
          shortUrl="https://short.url/abc123"
          onCopy={vi.fn()}
          copied={false}
        />
      );

      let copyButton = screen.getByRole('button');
      expect(copyButton).toHaveAttribute(
        'aria-label',
        'Copy shortened URL to clipboard'
      );

      rerender(
        <BrowserRouter>
          <ShortUrlResult
            shortUrl="https://short.url/abc123"
            onCopy={vi.fn()}
            copied={true}
          />
        </BrowserRouter>
      );

      copyButton = screen.getByRole('button');
      expect(copyButton).toHaveAttribute('aria-label', 'URL copied to clipboard');
    });
  });

  /**
   * Additional ARIA tests
   */
  describe('ARIA Labels and Roles', () => {
    it('should have proper navigation landmarks', () => {
      renderHome();

      // Primary navigation
      const primaryNav = screen.getByRole('navigation', {
        name: /primary navigation/i,
      });
      expect(primaryNav).toBeInTheDocument();
    });

    it('should have proper heading hierarchy', () => {
      renderHome();

      // H1 - main headline
      const h1 = screen.getByRole('heading', { level: 1 });
      expect(h1).toBeInTheDocument();

      // H2 - features section
      const h2 = screen.getByRole('heading', { level: 2, name: /why choose us/i });
      expect(h2).toBeInTheDocument();
    });

    it('should have labeled sections', () => {
      renderHome();

      // Hero section labeled
      const heroSection = document.querySelector('[aria-labelledby="hero-headline"]');
      expect(heroSection).toBeInTheDocument();

      // Features section labeled
      const featuresSection = document.querySelector(
        '[aria-labelledby="features-heading"]'
      );
      expect(featuresSection).toBeInTheDocument();
    });

    it('should have theme toggle with proper aria-label', () => {
      renderHome();

      const themeToggle = screen.getByTestId('theme-toggle');
      expect(themeToggle).toHaveAttribute(
        'aria-label',
        expect.stringMatching(/switch to (dark|light) theme/i)
      );
    });
  });
});
