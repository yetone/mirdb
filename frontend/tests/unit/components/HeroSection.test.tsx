/**
 * Unit tests for HeroSection component
 * Owner: Scenario 1 - Hero Section URL Shortening
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { BrowserRouter } from 'react-router-dom';
import { HeroSection } from '../../../src/components/landing/HeroSection';
import { HERO_CONTENT } from '../../../src/constants/landingContent';

// Wrapper component for router context (needed for RegistrationPrompt Link component)
function renderWithRouter(ui: React.ReactElement) {
  return render(<BrowserRouter>{ui}</BrowserRouter>);
}

// Mock clipboard API
const mockClipboard = {
  writeText: vi.fn().mockResolvedValue(undefined),
};
Object.assign(navigator, { clipboard: mockClipboard });

describe('HeroSection', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockClipboard.writeText.mockResolvedValue(undefined);
  });

  describe('Initial render', () => {
    it('should render headline and tagline', () => {
      renderWithRouter(<HeroSection />);

      expect(screen.getByRole('heading', { level: 1 })).toHaveTextContent(
        HERO_CONTENT.headline
      );
      expect(screen.getByText(HERO_CONTENT.tagline)).toBeInTheDocument();
    });

    it('should render URL input with proper placeholder and accessible attributes', () => {
      renderWithRouter(<HeroSection />);

      const input = screen.getByTestId('url-input');
      expect(input).toBeInTheDocument();
      expect(input).toHaveAttribute('placeholder', HERO_CONTENT.inputPlaceholder);
      expect(input).toHaveAttribute('aria-label', 'URL to shorten');
    });

    it('should render shorten button', () => {
      renderWithRouter(<HeroSection />);

      const button = screen.getByTestId('shorten-button');
      expect(button).toBeInTheDocument();
      expect(button).toHaveTextContent(HERO_CONTENT.submitButton);
    });

    it('should not show result container initially', () => {
      renderWithRouter(<HeroSection />);

      expect(screen.queryByTestId('result-container')).not.toBeInTheDocument();
    });
  });

  describe('Form submission', () => {
    it('should submit form and show short URL on success', async () => {
      const user = userEvent.setup();
      renderWithRouter(<HeroSection />);

      const input = screen.getByTestId('url-input');
      const button = screen.getByTestId('shorten-button');

      await user.type(input, 'https://example.com/very/long/path');
      await user.click(button);

      await waitFor(() => {
        expect(screen.getByTestId('result-container')).toBeInTheDocument();
      });

      expect(screen.getByTestId('short-url-display')).toHaveTextContent(
        'http://localhost:8000/abc123'
      );
    });

    it('should submit form when Enter key is pressed', async () => {
      const user = userEvent.setup();
      renderWithRouter(<HeroSection />);

      const input = screen.getByTestId('url-input');

      await user.type(input, 'https://example.com/path{enter}');

      await waitFor(() => {
        expect(screen.getByTestId('result-container')).toBeInTheDocument();
      });
    });

    it('should show loading state during submission', async () => {
      const user = userEvent.setup();
      renderWithRouter(<HeroSection />);

      const input = screen.getByTestId('url-input');
      const button = screen.getByTestId('shorten-button');

      await user.type(input, 'https://example.com/path');

      // Click and immediately check for loading state
      fireEvent.click(button);

      // Check for loading indicator
      expect(screen.getByText('Shortening...')).toBeInTheDocument();

      // Wait for completion
      await waitFor(() => {
        expect(screen.getByTestId('result-container')).toBeInTheDocument();
      });
    });

    it('should show error when submitting empty URL', async () => {
      const user = userEvent.setup();
      renderWithRouter(<HeroSection />);

      const button = screen.getByTestId('shorten-button');

      await user.click(button);

      expect(screen.getByTestId('error-message')).toBeInTheDocument();
      expect(screen.getByTestId('error-message')).toHaveTextContent(
        'Please enter a URL'
      );
    });
  });

  describe('Copy functionality', () => {
    it('should copy URL to clipboard when copy button is clicked', async () => {
      const user = userEvent.setup();
      renderWithRouter(<HeroSection />);

      const input = screen.getByTestId('url-input');
      const submitButton = screen.getByTestId('shorten-button');

      // Submit URL
      await user.type(input, 'https://example.com/path');
      await user.click(submitButton);

      // Wait for result
      await waitFor(() => {
        expect(screen.getByTestId('result-container')).toBeInTheDocument();
      });

      // Verify short URL is displayed correctly
      const shortUrlDisplay = screen.getByTestId('short-url-display');
      expect(shortUrlDisplay).toHaveTextContent('http://localhost:8000/abc123');

      // Verify copy button exists with initial state
      const copyButton = screen.getByTestId('copy-button');
      expect(copyButton).toHaveTextContent('Copy');
      expect(copyButton).toHaveAttribute('aria-label', 'Copy short URL to clipboard');

      // Click copy button and verify state changes
      await user.click(copyButton);

      // After clicking, button should show success state
      await waitFor(() => {
        expect(copyButton).toHaveTextContent('Copied!');
        expect(copyButton).toHaveAttribute('aria-label', 'Copied to clipboard');
      });
    });

    it('should show visual confirmation after copying', async () => {
      const user = userEvent.setup();
      renderWithRouter(<HeroSection />);

      const input = screen.getByTestId('url-input');
      const submitButton = screen.getByTestId('shorten-button');

      // Submit URL
      await user.type(input, 'https://example.com/path');
      await user.click(submitButton);

      // Wait for result
      await waitFor(() => {
        expect(screen.getByTestId('result-container')).toBeInTheDocument();
      });

      // Click copy button
      const copyButton = screen.getByTestId('copy-button');
      await user.click(copyButton);

      // Check for visual confirmation
      await waitFor(() => {
        expect(screen.getByText('Copied!')).toBeInTheDocument();
      });
    });
  });

  describe('Accessibility', () => {
    it('should have proper ARIA labels on interactive elements', () => {
      renderWithRouter(<HeroSection />);

      const input = screen.getByTestId('url-input');
      expect(input).toHaveAttribute('aria-label');

      const button = screen.getByTestId('shorten-button');
      expect(button).toHaveAttribute('aria-label');
    });

    it('should have proper section labeling', () => {
      renderWithRouter(<HeroSection />);

      // Section is labeled by the headline via aria-labelledby
      const section = screen.getByRole('region', { name: /shorten urls/i });
      expect(section).toBeInTheDocument();
    });

    it('should mark error state with aria-invalid', async () => {
      const user = userEvent.setup();
      renderWithRouter(<HeroSection />);

      const button = screen.getByTestId('shorten-button');
      await user.click(button);

      const input = screen.getByTestId('url-input');
      expect(input).toHaveAttribute('aria-invalid', 'true');
    });
  });
});
