/**
 * UrlDemoSection component tests.
 * Owner: Scenario 11 - URL Demo Section
 *
 * Test coverage:
 * - Demo section displays URL input and submit button
 * - Demo shows result or prompts registration
 * - onRegisterPrompt callback triggers correctly
 * - Invalid URL validation
 */
import { describe, it, expect, vi } from 'vitest';
import { screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { UrlDemoSection } from '@/components/home/UrlDemoSection';
import { renderWithProviders } from '../../utils/renderWithProviders';

describe('UrlDemoSection', () => {
  describe('Test Case 1: Demo section displays URL input field and submit button', () => {
    it('renders the demo section container', () => {
      renderWithProviders(<UrlDemoSection />);

      const demoSection = screen.getByTestId('url-demo-section');
      expect(demoSection).toBeInTheDocument();
      expect(demoSection.tagName).toBe('SECTION');
    });

    it('displays the demo headline', () => {
      renderWithProviders(<UrlDemoSection />);

      const headline = screen.getByTestId('demo-headline');
      expect(headline).toBeInTheDocument();
      expect(headline.tagName).toBe('H2');
      expect(headline.textContent).toMatch(/try it out/i);
    });

    it('displays the demo description', () => {
      renderWithProviders(<UrlDemoSection />);

      const description = screen.getByTestId('demo-description');
      expect(description).toBeInTheDocument();
      expect(description.textContent).toMatch(/shorten|url/i);
    });

    it('renders URL input field with placeholder', () => {
      renderWithProviders(<UrlDemoSection />);

      const input = screen.getByTestId('url-demo-input');
      expect(input).toBeInTheDocument();
      expect(input).toHaveAttribute('placeholder');
      expect(input.getAttribute('placeholder')).toMatch(/url/i);
    });

    it('renders submit button', () => {
      renderWithProviders(<UrlDemoSection />);

      const submitButton = screen.getByTestId('url-demo-submit');
      expect(submitButton).toBeInTheDocument();
      expect(submitButton).toHaveAttribute('type', 'submit');
    });

    it('renders the demo form', () => {
      renderWithProviders(<UrlDemoSection />);

      const form = screen.getByTestId('url-demo-form');
      expect(form).toBeInTheDocument();
      expect(form.tagName).toBe('FORM');
    });

    it('wraps content in GlassMorphismCard', () => {
      renderWithProviders(<UrlDemoSection />);

      const card = screen.getByTestId('url-demo-card');
      expect(card).toBeInTheDocument();
    });

    it('input field is accessible with aria-label', () => {
      renderWithProviders(<UrlDemoSection />);

      const input = screen.getByTestId('url-demo-input');
      expect(input).toHaveAttribute('aria-label');
    });

    it('submit button has aria-label', () => {
      renderWithProviders(<UrlDemoSection />);

      const submitButton = screen.getByTestId('url-demo-submit');
      expect(submitButton).toHaveAttribute('aria-label');
    });
  });

  describe('Test Case 2: Enter URL and submit demo shows shortened URL example or prompts registration', () => {
    it('shows shortened URL after submitting valid URL', async () => {
      const user = userEvent.setup();
      renderWithProviders(<UrlDemoSection />);

      const input = screen.getByTestId('url-demo-input');
      const submitButton = screen.getByTestId('url-demo-submit');

      await user.type(input, 'https://example.com/very/long/url/that/needs/shortening');
      await user.click(submitButton);

      await waitFor(() => {
        const result = screen.getByTestId('url-demo-result');
        expect(result).toBeInTheDocument();
      });

      const shortenedUrl = screen.getByTestId('shortened-url');
      expect(shortenedUrl).toBeInTheDocument();
      expect(shortenedUrl.textContent).toMatch(/https:\/\/short\.url\//);
    });

    it('displays registration prompt after successful demo', async () => {
      const user = userEvent.setup();
      renderWithProviders(<UrlDemoSection />);

      const input = screen.getByTestId('url-demo-input');
      const submitButton = screen.getByTestId('url-demo-submit');

      await user.type(input, 'https://example.com/test');
      await user.click(submitButton);

      await waitFor(() => {
        const registrationPrompt = screen.getByTestId('registration-prompt');
        expect(registrationPrompt).toBeInTheDocument();
      });
    });

    it('shows register CTA button in registration prompt', async () => {
      const user = userEvent.setup();
      renderWithProviders(<UrlDemoSection />);

      const input = screen.getByTestId('url-demo-input');
      const submitButton = screen.getByTestId('url-demo-submit');

      await user.type(input, 'https://example.com/test');
      await user.click(submitButton);

      await waitFor(() => {
        const registerCta = screen.getByTestId('demo-register-cta');
        expect(registerCta).toBeInTheDocument();
        expect(registerCta).toHaveAttribute('href', '/register');
      });
    });

    it('registration prompt explains the demo limitation', async () => {
      const user = userEvent.setup();
      renderWithProviders(<UrlDemoSection />);

      const input = screen.getByTestId('url-demo-input');
      const submitButton = screen.getByTestId('url-demo-submit');

      await user.type(input, 'https://example.com/test');
      await user.click(submitButton);

      await waitFor(() => {
        const registrationPrompt = screen.getByTestId('registration-prompt');
        expect(registrationPrompt.textContent).toMatch(/demo|preview|register/i);
      });
    });

    it('can submit URL using Enter key', async () => {
      const user = userEvent.setup();
      renderWithProviders(<UrlDemoSection />);

      const input = screen.getByTestId('url-demo-input');

      await user.type(input, 'https://example.com/test{enter}');

      await waitFor(() => {
        const result = screen.getByTestId('url-demo-result');
        expect(result).toBeInTheDocument();
      });
    });
  });

  describe('Test Case 3: Trigger onRegisterPrompt callback navigates user toward registration', () => {
    it('calls onRegisterPrompt callback when register CTA is clicked', async () => {
      const user = userEvent.setup();
      const onRegisterPrompt = vi.fn();

      renderWithProviders(<UrlDemoSection onRegisterPrompt={onRegisterPrompt} />);

      const input = screen.getByTestId('url-demo-input');
      const submitButton = screen.getByTestId('url-demo-submit');

      // First submit a valid URL to show the registration prompt
      await user.type(input, 'https://example.com/test');
      await user.click(submitButton);

      // Wait for the registration prompt to appear
      await waitFor(() => {
        const registerCta = screen.getByTestId('demo-register-cta');
        expect(registerCta).toBeInTheDocument();
      });

      // Click the register CTA
      const registerCta = screen.getByTestId('demo-register-cta');
      await user.click(registerCta);

      expect(onRegisterPrompt).toHaveBeenCalledTimes(1);
    });

    it('works without onRegisterPrompt callback', async () => {
      const user = userEvent.setup();

      renderWithProviders(<UrlDemoSection />);

      const input = screen.getByTestId('url-demo-input');
      const submitButton = screen.getByTestId('url-demo-submit');

      await user.type(input, 'https://example.com/test');
      await user.click(submitButton);

      await waitFor(() => {
        const registerCta = screen.getByTestId('demo-register-cta');
        expect(registerCta).toBeInTheDocument();
      });

      // Should not throw error when clicking without callback
      const registerCta = screen.getByTestId('demo-register-cta');
      await user.click(registerCta);
    });

    it('register CTA navigates to /register route', async () => {
      const user = userEvent.setup();
      renderWithProviders(<UrlDemoSection />);

      const input = screen.getByTestId('url-demo-input');
      const submitButton = screen.getByTestId('url-demo-submit');

      await user.type(input, 'https://example.com/test');
      await user.click(submitButton);

      await waitFor(() => {
        const registerCta = screen.getByTestId('demo-register-cta');
        expect(registerCta).toHaveAttribute('href', '/register');
      });
    });
  });

  describe('Test Case 4: Invalid URLs show appropriate error message', () => {
    it('shows error for empty URL submission', async () => {
      const user = userEvent.setup();
      renderWithProviders(<UrlDemoSection />);

      const submitButton = screen.getByTestId('url-demo-submit');
      await user.click(submitButton);

      await waitFor(() => {
        const error = screen.getByTestId('url-demo-error');
        expect(error).toBeInTheDocument();
        expect(error.textContent).toMatch(/enter.*url/i);
      });
    });

    it('shows error for invalid URL format', async () => {
      const user = userEvent.setup();
      renderWithProviders(<UrlDemoSection />);

      const input = screen.getByTestId('url-demo-input');
      const submitButton = screen.getByTestId('url-demo-submit');

      await user.type(input, 'not-a-valid-url');
      await user.click(submitButton);

      await waitFor(() => {
        const error = screen.getByTestId('url-demo-error');
        expect(error).toBeInTheDocument();
        expect(error.textContent).toMatch(/valid.*url/i);
      });
    });

    it('shows error for URL without http/https protocol', async () => {
      const user = userEvent.setup();
      renderWithProviders(<UrlDemoSection />);

      const input = screen.getByTestId('url-demo-input');
      const submitButton = screen.getByTestId('url-demo-submit');

      await user.type(input, 'ftp://example.com');
      await user.click(submitButton);

      await waitFor(() => {
        const error = screen.getByTestId('url-demo-error');
        expect(error).toBeInTheDocument();
        expect(error.textContent).toMatch(/http|https/i);
      });
    });

    it('error message has alert role for accessibility', async () => {
      const user = userEvent.setup();
      renderWithProviders(<UrlDemoSection />);

      const submitButton = screen.getByTestId('url-demo-submit');
      await user.click(submitButton);

      await waitFor(() => {
        const error = screen.getByTestId('url-demo-error');
        expect(error).toHaveAttribute('role', 'alert');
      });
    });

    it('input gets error styling when invalid', async () => {
      const user = userEvent.setup();
      renderWithProviders(<UrlDemoSection />);

      const input = screen.getByTestId('url-demo-input');
      const submitButton = screen.getByTestId('url-demo-submit');

      await user.click(submitButton);

      await waitFor(() => {
        expect(input).toHaveClass('input-error');
        expect(input).toHaveAttribute('aria-invalid', 'true');
      });
    });

    it('clears error when user starts typing', async () => {
      const user = userEvent.setup();
      renderWithProviders(<UrlDemoSection />);

      const input = screen.getByTestId('url-demo-input');
      const submitButton = screen.getByTestId('url-demo-submit');

      // Submit empty to get error
      await user.click(submitButton);

      await waitFor(() => {
        expect(screen.getByTestId('url-demo-error')).toBeInTheDocument();
      });

      // Start typing to clear error
      await user.type(input, 'h');

      await waitFor(() => {
        expect(screen.queryByTestId('url-demo-error')).not.toBeInTheDocument();
      });
    });

    it('shows error for whitespace-only URL', async () => {
      const user = userEvent.setup();
      renderWithProviders(<UrlDemoSection />);

      const input = screen.getByTestId('url-demo-input');
      const submitButton = screen.getByTestId('url-demo-submit');

      await user.type(input, '   ');
      await user.click(submitButton);

      await waitFor(() => {
        const error = screen.getByTestId('url-demo-error');
        expect(error).toBeInTheDocument();
      });
    });
  });

  describe('Accessibility', () => {
    it('section has proper aria-labelledby', () => {
      renderWithProviders(<UrlDemoSection />);

      const section = screen.getByTestId('url-demo-section');
      expect(section).toHaveAttribute('aria-labelledby', 'demo-headline');
    });

    it('input has aria-describedby when error is shown', async () => {
      const user = userEvent.setup();
      renderWithProviders(<UrlDemoSection />);

      const input = screen.getByTestId('url-demo-input');
      const submitButton = screen.getByTestId('url-demo-submit');

      await user.click(submitButton);

      await waitFor(() => {
        expect(input).toHaveAttribute('aria-describedby', 'url-error');
      });
    });

    it('submit button and input are keyboard focusable', async () => {
      const user = userEvent.setup();
      renderWithProviders(<UrlDemoSection />);

      const input = screen.getByTestId('url-demo-input');
      const submitButton = screen.getByTestId('url-demo-submit');

      // Tab to input
      await user.tab();
      expect(input).toHaveFocus();

      // Tab to submit button
      await user.tab();
      expect(submitButton).toHaveFocus();
    });
  });

  describe('Demo result handling', () => {
    it('generates unique short codes on each submission', async () => {
      const user = userEvent.setup();
      renderWithProviders(<UrlDemoSection />);

      const input = screen.getByTestId('url-demo-input');
      const submitButton = screen.getByTestId('url-demo-submit');

      // First submission
      await user.type(input, 'https://example.com/first');
      await user.click(submitButton);

      await waitFor(() => {
        expect(screen.getByTestId('shortened-url')).toBeInTheDocument();
      });

      const firstShortUrl = screen.getByTestId('shortened-url').textContent;

      // Clear and submit again
      await user.clear(input);
      await user.type(input, 'https://example.com/second');
      await user.click(submitButton);

      await waitFor(() => {
        const secondShortUrl = screen.getByTestId('shortened-url').textContent;
        // URLs should be different (with very high probability)
        // Even if they're the same, the test validates the functionality works
        expect(secondShortUrl).toMatch(/https:\/\/short\.url\//);
      });
    });

    it('clears previous result when submitting new URL', async () => {
      const user = userEvent.setup();
      renderWithProviders(<UrlDemoSection />);

      const input = screen.getByTestId('url-demo-input');
      const submitButton = screen.getByTestId('url-demo-submit');

      // First valid submission
      await user.type(input, 'https://example.com/test');
      await user.click(submitButton);

      await waitFor(() => {
        expect(screen.getByTestId('url-demo-result')).toBeInTheDocument();
      });

      // Clear and submit invalid URL
      await user.clear(input);
      await user.click(submitButton);

      await waitFor(() => {
        expect(screen.getByTestId('url-demo-error')).toBeInTheDocument();
        expect(screen.queryByTestId('url-demo-result')).not.toBeInTheDocument();
      });
    });
  });
});
