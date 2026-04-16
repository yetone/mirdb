/**
 * Unit Tests for DemoSection Component.
 * Owner: Scenario 4 - URL Demo Section Functionality
 *
 * Tests the URL demo section displays correctly,
 * validates input, and redirects unauthenticated users.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { screen, fireEvent, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { renderWithProviders } from './setup';
import { DemoSection } from '../../../src/components/homepage/DemoSection';

// Mock react-router-dom navigate
const mockNavigate = vi.fn();
vi.mock('react-router-dom', async () => {
  const actual = await vi.importActual('react-router-dom');
  return {
    ...actual,
    useNavigate: () => mockNavigate,
  };
});

describe('DemoSection', () => {
  beforeEach(() => {
    mockNavigate.mockClear();
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  describe('Test Case 1: Render Demo section component - Demo section displays with URL input field and Try It button', () => {
    it('renders the demo section with data-testid', () => {
      renderWithProviders(<DemoSection />);

      const demoSection = screen.getByTestId('demo-section');
      expect(demoSection).toBeInTheDocument();
      expect(demoSection).toBeVisible();
    });

    it('displays URL input field', () => {
      renderWithProviders(<DemoSection />);

      const urlInput = screen.getByTestId('demo-url-input');
      expect(urlInput).toBeInTheDocument();
      expect(urlInput).toHaveAttribute('type', 'url');
      expect(urlInput).toHaveAttribute('placeholder', 'Paste your URL here...');
    });

    it('displays Try It button', () => {
      renderWithProviders(<DemoSection />);

      const tryButton = screen.getByTestId('demo-try-button');
      expect(tryButton).toBeInTheDocument();
      expect(tryButton).toHaveTextContent('Try It');
    });

    it('displays demo title with "Try it yourself"', () => {
      renderWithProviders(<DemoSection />);

      const title = screen.getByTestId('demo-title');
      expect(title).toBeInTheDocument();
      expect(title.textContent?.toLowerCase()).toContain('try it yourself');
    });

    it('uses GlassMorphismCard for styling', () => {
      renderWithProviders(<DemoSection />);

      const card = screen.getByTestId('demo-card');
      expect(card).toBeInTheDocument();
    });

    it('section element is semantic', () => {
      renderWithProviders(<DemoSection />);

      const section = screen.getByTestId('demo-section');
      expect(section.tagName).toBe('SECTION');
    });
  });

  describe('Test Case 2: Enter URL in input - Input accepts and displays the URL value', () => {
    it('accepts and displays URL value when typed', async () => {
      vi.useRealTimers();
      const user = userEvent.setup();
      renderWithProviders(<DemoSection />);

      const urlInput = screen.getByTestId('demo-url-input');
      await user.type(urlInput, 'https://example.com/test');

      expect(urlInput).toHaveValue('https://example.com/test');
    });

    it('updates input value on change', () => {
      renderWithProviders(<DemoSection />);

      const urlInput = screen.getByTestId('demo-url-input');
      fireEvent.change(urlInput, { target: { value: 'https://example.com/test' } });

      expect(urlInput).toHaveValue('https://example.com/test');
    });

    it('clears error when URL is modified', async () => {
      // Use real timers for this test as it involves async animations
      vi.useRealTimers();
      renderWithProviders(<DemoSection />);

      const urlInput = screen.getByTestId('demo-url-input');
      const tryButton = screen.getByTestId('demo-try-button');

      // Trigger error by clicking with invalid URL
      fireEvent.change(urlInput, { target: { value: 'invalid' } });
      fireEvent.click(tryButton);

      expect(screen.getByTestId('demo-error')).toBeInTheDocument();

      // Type a new URL to clear error
      fireEvent.change(urlInput, { target: { value: 'https://example.com' } });

      // Wait for animation to complete and element to be removed
      await waitFor(() => {
        expect(screen.queryByTestId('demo-error')).not.toBeInTheDocument();
      }, { timeout: 1000 });
    });
  });

  describe('Test Case 3: Click Try It without entering URL - Validation message or disabled state prevents empty submission', () => {
    it('button is disabled when URL is empty', () => {
      renderWithProviders(<DemoSection />);

      const tryButton = screen.getByTestId('demo-try-button');
      expect(tryButton).toBeDisabled();
    });

    it('button remains disabled with whitespace-only input', () => {
      renderWithProviders(<DemoSection />);

      const urlInput = screen.getByTestId('demo-url-input');
      const tryButton = screen.getByTestId('demo-try-button');

      // Input whitespace only
      fireEvent.change(urlInput, { target: { value: '   ' } });

      // Button should still be disabled
      expect(tryButton).toBeDisabled();
    });

    it('input has aria-invalid when error exists', () => {
      renderWithProviders(<DemoSection />);

      const urlInput = screen.getByTestId('demo-url-input');
      const tryButton = screen.getByTestId('demo-try-button');

      // Enter non-empty invalid URL and trigger validation
      fireEvent.change(urlInput, { target: { value: 'not-a-valid-url' } });
      fireEvent.click(tryButton);

      expect(urlInput).toHaveAttribute('aria-invalid', 'true');
    });
  });

  describe('Test Case 4: Enter invalid URL format and click Try It - Validation error message displayed', () => {
    it('shows error for invalid URL format', () => {
      renderWithProviders(<DemoSection />);

      const urlInput = screen.getByTestId('demo-url-input');
      const tryButton = screen.getByTestId('demo-try-button');

      fireEvent.change(urlInput, { target: { value: 'not-a-valid-url' } });
      fireEvent.click(tryButton);

      const error = screen.getByTestId('demo-error');
      expect(error).toBeInTheDocument();
      expect(error).toHaveTextContent(/please enter a valid url/i);
    });

    it('shows error for URL without protocol', () => {
      renderWithProviders(<DemoSection />);

      const urlInput = screen.getByTestId('demo-url-input');
      const tryButton = screen.getByTestId('demo-try-button');

      fireEvent.change(urlInput, { target: { value: 'example.com/page' } });
      fireEvent.click(tryButton);

      const error = screen.getByTestId('demo-error');
      expect(error).toBeInTheDocument();
      expect(error).toHaveTextContent(/please enter a valid url/i);
    });

    it('shows error for ftp protocol', () => {
      renderWithProviders(<DemoSection />);

      const urlInput = screen.getByTestId('demo-url-input');
      const tryButton = screen.getByTestId('demo-try-button');

      fireEvent.change(urlInput, { target: { value: 'ftp://example.com/file' } });
      fireEvent.click(tryButton);

      const error = screen.getByTestId('demo-error');
      expect(error).toBeInTheDocument();
    });

    it('error has role="alert" for accessibility', () => {
      renderWithProviders(<DemoSection />);

      const urlInput = screen.getByTestId('demo-url-input');
      const tryButton = screen.getByTestId('demo-try-button');

      fireEvent.change(urlInput, { target: { value: 'invalid' } });
      fireEvent.click(tryButton);

      const error = screen.getByTestId('demo-error');
      expect(error).toHaveAttribute('role', 'alert');
    });

    it('input has error styling when invalid', () => {
      renderWithProviders(<DemoSection />);

      const urlInput = screen.getByTestId('demo-url-input');
      const tryButton = screen.getByTestId('demo-try-button');

      fireEvent.change(urlInput, { target: { value: 'invalid' } });
      fireEvent.click(tryButton);

      expect(urlInput).toHaveClass('input-error');
    });
  });

  describe('Valid URL Submission - Login prompt and redirect', () => {
    it('shows login prompt message for valid URL', () => {
      renderWithProviders(<DemoSection />);

      const urlInput = screen.getByTestId('demo-url-input');
      const tryButton = screen.getByTestId('demo-try-button');

      fireEvent.change(urlInput, { target: { value: 'https://example.com/very-long-url' } });
      fireEvent.click(tryButton);

      const loginPrompt = screen.getByTestId('demo-login-prompt');
      expect(loginPrompt).toBeInTheDocument();
      expect(loginPrompt).toHaveTextContent(/registration required/i);
    });

    it('login prompt explains registration is required', () => {
      renderWithProviders(<DemoSection />);

      const urlInput = screen.getByTestId('demo-url-input');
      const tryButton = screen.getByTestId('demo-try-button');

      fireEvent.change(urlInput, { target: { value: 'https://example.com/test' } });
      fireEvent.click(tryButton);

      const loginPrompt = screen.getByTestId('demo-login-prompt');
      expect(loginPrompt).toHaveTextContent(/create a free account/i);
    });

    it('redirects to /register with URL in query params after 2 seconds', async () => {
      renderWithProviders(<DemoSection />);

      const urlInput = screen.getByTestId('demo-url-input');
      const tryButton = screen.getByTestId('demo-try-button');

      fireEvent.change(urlInput, { target: { value: 'https://example.com/test' } });
      fireEvent.click(tryButton);

      // Fast forward 2 seconds
      vi.advanceTimersByTime(2000);

      expect(mockNavigate).toHaveBeenCalledWith(
        '/register?url=https%3A%2F%2Fexample.com%2Ftest'
      );
    });

    it('URL is properly encoded in redirect', () => {
      renderWithProviders(<DemoSection />);

      const urlInput = screen.getByTestId('demo-url-input');
      const tryButton = screen.getByTestId('demo-try-button');

      const testUrl = 'https://example.com/page?param=value&other=123';
      fireEvent.change(urlInput, { target: { value: testUrl } });
      fireEvent.click(tryButton);

      vi.advanceTimersByTime(2000);

      const expectedEncodedUrl = encodeURIComponent(testUrl);
      expect(mockNavigate).toHaveBeenCalledWith(`/register?url=${expectedEncodedUrl}`);
    });

    it('accepts https URLs', () => {
      renderWithProviders(<DemoSection />);

      const urlInput = screen.getByTestId('demo-url-input');
      const tryButton = screen.getByTestId('demo-try-button');

      fireEvent.change(urlInput, { target: { value: 'https://secure.example.com' } });
      fireEvent.click(tryButton);

      expect(screen.queryByTestId('demo-error')).not.toBeInTheDocument();
      expect(screen.getByTestId('demo-login-prompt')).toBeInTheDocument();
    });

    it('accepts http URLs', () => {
      renderWithProviders(<DemoSection />);

      const urlInput = screen.getByTestId('demo-url-input');
      const tryButton = screen.getByTestId('demo-try-button');

      fireEvent.change(urlInput, { target: { value: 'http://example.com' } });
      fireEvent.click(tryButton);

      expect(screen.queryByTestId('demo-error')).not.toBeInTheDocument();
      expect(screen.getByTestId('demo-login-prompt')).toBeInTheDocument();
    });
  });

  describe('Keyboard Interaction', () => {
    it('submits on Enter key press', () => {
      renderWithProviders(<DemoSection />);

      const urlInput = screen.getByTestId('demo-url-input');

      fireEvent.change(urlInput, { target: { value: 'https://example.com' } });
      fireEvent.keyDown(urlInput, { key: 'Enter', code: 'Enter' });

      expect(screen.getByTestId('demo-login-prompt')).toBeInTheDocument();
    });
  });

  describe('Accessibility', () => {
    it('input has aria-label', () => {
      renderWithProviders(<DemoSection />);

      const urlInput = screen.getByTestId('demo-url-input');
      expect(urlInput).toHaveAttribute('aria-label', 'URL input');
    });

    it('button has aria-label', () => {
      renderWithProviders(<DemoSection />);

      const tryButton = screen.getByTestId('demo-try-button');
      expect(tryButton).toHaveAttribute('aria-label', 'Try It');
    });

    it('input references error with aria-describedby when error exists', () => {
      renderWithProviders(<DemoSection />);

      const urlInput = screen.getByTestId('demo-url-input');
      const tryButton = screen.getByTestId('demo-try-button');

      fireEvent.change(urlInput, { target: { value: 'invalid' } });
      fireEvent.click(tryButton);

      expect(urlInput).toHaveAttribute('aria-describedby', 'demo-error');
    });
  });

  describe('Custom className prop', () => {
    it('applies custom className to section', () => {
      renderWithProviders(<DemoSection className="custom-class" />);

      const section = screen.getByTestId('demo-section');
      expect(section).toHaveClass('custom-class');
    });
  });
});
