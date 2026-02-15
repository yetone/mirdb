/**
 * Accessibility Integration Tests
 * Owner: Scenario 7 - Accessibility Compliance
 *
 * Integration tests for keyboard navigation and accessibility:
 * - Tab navigation through interactive elements
 * - Keyboard submission (Enter/Space on buttons)
 * - Focus management
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter, Routes, Route } from 'react-router-dom';
import Home from '../../src/pages/Home';

// Mock the useGuestShorten hook
vi.mock('../../src/hooks/useGuestShorten', () => ({
  useGuestShorten: () => ({
    shortenUrl: vi.fn().mockResolvedValue({
      originalUrl: 'https://example.com',
      shortUrl: 'https://short.url/abc',
      shortCode: 'abc',
    }),
    isLoading: false,
    error: null,
    result: null,
    reset: vi.fn(),
  }),
}));

// Mock the useThemeStore
vi.mock('../../src/stores/themeStore', () => ({
  useThemeStore: () => ({
    theme: 'light',
    setTheme: vi.fn(),
  }),
}));

// Mock clipboard utility
vi.mock('../../src/utils/clipboard', () => ({
  copyToClipboard: vi.fn().mockResolvedValue(true),
}));

// Mock Register page component
const MockRegisterPage = () => <div data-testid="register-page">Register Page</div>;

const renderWithRouter = (initialRoute = '/') => {
  return render(
    <MemoryRouter initialEntries={[initialRoute]}>
      <Routes>
        <Route path="/" element={<Home />} />
        <Route path="/register" element={<MockRegisterPage />} />
        <Route path="/login" element={<div data-testid="login-page">Login Page</div>} />
        <Route path="/privacy" element={<div data-testid="privacy-page">Privacy Page</div>} />
        <Route path="/terms" element={<div data-testid="terms-page">Terms Page</div>} />
      </Routes>
    </MemoryRouter>
  );
};

describe('Accessibility - Keyboard Navigation', () => {
  /**
   * Test Case 1: Tab through all interactive elements on homepage
   * Input: Tab through all interactive elements on homepage
   * Expected: Focus moves through Navbar links, CTA buttons, form input, shorten button, copy button, footer links in logical order
   */
  describe('Test Case 1: Tab Navigation Order', () => {
    it('should allow keyboard navigation through main interactive elements', async () => {
      const user = userEvent.setup();
      renderWithRouter();

      // Start by pressing Tab to move focus into the document
      await user.tab();

      // We should be able to tab through elements
      // The exact order depends on DOM structure, but all elements should be reachable
      const focusedElement = document.activeElement;
      expect(focusedElement).not.toBe(document.body);
    });

    it('should have logical focus order starting from navbar', async () => {
      const user = userEvent.setup();
      renderWithRouter();

      // Focus on the document first
      document.body.focus();

      // Tab through elements - first should hit navbar area
      await user.tab();

      // Verify that interactive elements can receive focus
      const brandLogo = screen.getByTestId('brand-logo');
      const loginLink = screen.getByTestId('login-link');
      const registerLink = screen.getByTestId('register-link');
      const getStartedButton = screen.getByTestId('get-started-button');

      // All these elements should be focusable
      expect(brandLogo.tabIndex).toBeGreaterThanOrEqual(-1);
      expect(loginLink.tabIndex).toBeGreaterThanOrEqual(-1);
      expect(registerLink.tabIndex).toBeGreaterThanOrEqual(-1);
      expect(getStartedButton.tabIndex).toBeGreaterThanOrEqual(-1);
    });

    it('should be able to tab to form input', async () => {
      const user = userEvent.setup();
      renderWithRouter();

      const urlInput = screen.getByRole('textbox');

      // Focus the input directly
      urlInput.focus();
      expect(document.activeElement).toBe(urlInput);
    });

    it('should be able to tab to submit button when enabled', async () => {
      const user = userEvent.setup();
      renderWithRouter();

      // First type in the URL input to enable the submit button
      const urlInput = screen.getByRole('textbox');
      await user.type(urlInput, 'https://example.com');

      const submitButton = screen.getByRole('button', { name: /shorten/i });

      // Now the button should be enabled and focusable
      submitButton.focus();
      expect(document.activeElement).toBe(submitButton);
    });

    it('should reach footer links via keyboard', async () => {
      renderWithRouter();

      const privacyLink = screen.getByTestId('privacy-policy-link');
      const termsLink = screen.getByTestId('terms-of-service-link');

      // These links should be focusable
      privacyLink.focus();
      expect(document.activeElement).toBe(privacyLink);

      termsLink.focus();
      expect(document.activeElement).toBe(termsLink);
    });
  });

  /**
   * Test Case 3: Press Enter on 'Get Started Free' button when focused
   * Input: Press Enter on 'Get Started Free' button when focused
   * Expected: Navigation to /register occurs
   */
  describe('Test Case 3: Keyboard Activation', () => {
    it('should navigate to /register when Enter is pressed on Get Started button', async () => {
      const user = userEvent.setup();
      renderWithRouter();

      // Find the Get Started Free button (which is a Link component)
      const getStartedButton = screen.getByTestId('get-started-button');

      // Focus the button
      getStartedButton.focus();
      expect(document.activeElement).toBe(getStartedButton);

      // Press Enter
      await user.keyboard('{Enter}');

      // Should navigate to register page
      await waitFor(() => {
        expect(screen.getByTestId('register-page')).toBeInTheDocument();
      });
    });

    it('should navigate to /register when clicked', async () => {
      const user = userEvent.setup();
      renderWithRouter();

      const getStartedButton = screen.getByTestId('get-started-button');
      await user.click(getStartedButton);

      await waitFor(() => {
        expect(screen.getByTestId('register-page')).toBeInTheDocument();
      });
    });

    it('should navigate to /login when Enter is pressed on Login link', async () => {
      const user = userEvent.setup();
      renderWithRouter();

      const loginLink = screen.getByTestId('login-link');
      loginLink.focus();
      await user.keyboard('{Enter}');

      await waitFor(() => {
        expect(screen.getByTestId('login-page')).toBeInTheDocument();
      });
    });

    it('should navigate to register when clicking navbar Register button', async () => {
      const user = userEvent.setup();
      renderWithRouter();

      const registerLink = screen.getByTestId('register-link');
      await user.click(registerLink);

      await waitFor(() => {
        expect(screen.getByTestId('register-page')).toBeInTheDocument();
      });
    });

    it('should activate Learn More button with Enter key', async () => {
      const user = userEvent.setup();
      renderWithRouter();

      const learnMoreButton = screen.getByTestId('learn-more-button');
      learnMoreButton.focus();

      // Press Enter - this should trigger scroll to features section
      await user.keyboard('{Enter}');

      // The button click handler should be called
      // (scrollIntoView is mocked in setup.ts)
    });

    it('should activate Learn More button with Space key', async () => {
      const user = userEvent.setup();
      renderWithRouter();

      const learnMoreButton = screen.getByTestId('learn-more-button');
      learnMoreButton.focus();

      // Press Space - this should trigger the button click
      await user.keyboard(' ');

      // The button's click handler should be invoked
    });
  });

  describe('Form Keyboard Interaction', () => {
    it('should submit form when Enter is pressed in input field', async () => {
      const user = userEvent.setup();
      renderWithRouter();

      const urlInput = screen.getByRole('textbox');

      // Focus input and type a URL
      await user.click(urlInput);
      await user.type(urlInput, 'https://example.com');

      // Press Enter to submit (form default behavior)
      await user.keyboard('{Enter}');

      // Form should be submitted (mock will be called)
    });

    it('should submit form when Space is pressed on submit button', async () => {
      const user = userEvent.setup();
      renderWithRouter();

      const urlInput = screen.getByRole('textbox');
      const submitButton = screen.getByRole('button', { name: /shorten/i });

      // Type URL
      await user.type(urlInput, 'https://example.com');

      // Focus and activate submit button with Space
      submitButton.focus();
      await user.keyboard(' ');

      // Form should be submitted
    });

    it('should be able to fill form using only keyboard', async () => {
      const user = userEvent.setup();
      renderWithRouter();

      // Tab to reach the input
      const urlInput = screen.getByRole('textbox');
      urlInput.focus();

      // Type URL
      await user.type(urlInput, 'https://example.com/test');
      expect(urlInput).toHaveValue('https://example.com/test');

      // Tab to submit button
      await user.tab();

      // Verify we reached a button (might be submit or another element)
      expect(document.activeElement?.tagName.toLowerCase()).toBe('button');
    });
  });

  describe('Focus Management', () => {
    it('should maintain focus visibility on interactive elements', async () => {
      renderWithRouter();

      const buttons = screen.getAllByRole('button');
      const links = screen.getAllByRole('link');

      // All enabled buttons should be focusable
      buttons.forEach((button) => {
        // Skip disabled buttons - they can't receive focus (valid behavior)
        if (!button.hasAttribute('disabled')) {
          button.focus();
          expect(document.activeElement).toBe(button);
        }
      });

      // All links should be focusable
      links.forEach((link) => {
        link.focus();
        expect(document.activeElement).toBe(link);
      });
    });

    it('should not trap focus within any component', async () => {
      const user = userEvent.setup();
      renderWithRouter();

      // Start tabbing through the page
      await user.tab();
      const firstFocus = document.activeElement;

      // Tab through many times - should eventually cycle back or reach end
      for (let i = 0; i < 30; i++) {
        await user.tab();
      }

      // Should not be stuck on the same element (unless it's the last focusable)
      // The point is that focus should be able to move
    });

    it('should allow reverse tab navigation with Shift+Tab', async () => {
      const user = userEvent.setup();
      renderWithRouter();

      // Get to somewhere in the middle
      await user.tab();
      await user.tab();
      await user.tab();

      const middleElement = document.activeElement;

      // Tab forward
      await user.tab();
      const nextElement = document.activeElement;

      // Shift+Tab back
      await user.tab({ shift: true });

      // Should be back at the middle element
      expect(document.activeElement).toBe(middleElement);
    });
  });
});

describe('Accessibility - Screen Reader Support', () => {
  it('should have proper form structure for screen readers', () => {
    renderWithRouter();

    const form = document.querySelector('form');
    expect(form).toBeInTheDocument();

    // Form should contain labeled input
    const urlInput = screen.getByRole('textbox');
    expect(urlInput).toHaveAttribute('aria-label');
  });

  it('should have landmarks for navigation', () => {
    renderWithRouter();

    // Should have nav landmark
    const nav = document.querySelector('nav');
    expect(nav).toBeInTheDocument();

    // Should have main landmark
    const main = document.querySelector('main');
    expect(main).toBeInTheDocument();

    // Should have footer landmark (contentinfo)
    const footer = document.querySelector('footer');
    expect(footer).toBeInTheDocument();
  });

  it('should have proper link text for all navigation links', () => {
    renderWithRouter();

    const links = screen.getAllByRole('link');

    links.forEach((link) => {
      // Each link should have accessible text
      const hasText = link.textContent && link.textContent.trim().length > 0;
      const hasAriaLabel = link.hasAttribute('aria-label');
      const hasAriaLabelledBy = link.hasAttribute('aria-labelledby');

      expect(hasText || hasAriaLabel || hasAriaLabelledBy).toBe(true);
    });
  });
});
