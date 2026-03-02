/**
 * Unit Tests for MobileMenu Component.
 * Owner: Scenario 9 - Mobile Responsive Design
 *
 * Tests:
 * - Menu renders correctly when open
 * - Menu is hidden when closed
 * - Close button triggers onClose callback
 * - Contains Login and Register links
 * - Has theme toggle
 * - Escape key closes the menu
 * - Click on backdrop closes the menu
 * - Touch-friendly elements (min 44px height)
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { BrowserRouter } from 'react-router-dom';
import { ThemeProvider } from '../../../../src/contexts/ThemeContext';
import { MobileMenu } from '../../../../src/components/layout/MobileMenu';

function renderWithProviders(ui: React.ReactElement) {
  return render(
    <BrowserRouter>
      <ThemeProvider>
        {ui}
      </ThemeProvider>
    </BrowserRouter>
  );
}

describe('MobileMenu', () => {
  const mockOnClose = vi.fn();

  beforeEach(() => {
    mockOnClose.mockClear();
    document.body.style.overflow = '';
  });

  // Test Case 2: Click hamburger menu on mobile - Slide-in navigation appears
  describe('when open', () => {
    it('renders the slide-in menu panel', () => {
      renderWithProviders(<MobileMenu isOpen={true} onClose={mockOnClose} />);

      const menu = screen.getByTestId('mobile-menu');
      expect(menu).toBeInTheDocument();
      expect(menu).toHaveAttribute('aria-hidden', 'false');
    });

    it('displays Login link', () => {
      renderWithProviders(<MobileMenu isOpen={true} onClose={mockOnClose} />);

      const loginLink = screen.getByTestId('mobile-menu-login');
      expect(loginLink).toBeInTheDocument();
      expect(loginLink).toHaveTextContent('Login');
      expect(loginLink).toHaveAttribute('href', '/login');
    });

    it('displays Register link', () => {
      renderWithProviders(<MobileMenu isOpen={true} onClose={mockOnClose} />);

      const registerLink = screen.getByTestId('mobile-menu-register');
      expect(registerLink).toBeInTheDocument();
      expect(registerLink).toHaveTextContent('Register');
      expect(registerLink).toHaveAttribute('href', '/register');
    });

    it('displays theme toggle', () => {
      renderWithProviders(<MobileMenu isOpen={true} onClose={mockOnClose} />);

      const themeToggle = screen.getByTestId('theme-toggle');
      expect(themeToggle).toBeInTheDocument();
    });

    it('displays close button', () => {
      renderWithProviders(<MobileMenu isOpen={true} onClose={mockOnClose} />);

      const closeButton = screen.getByTestId('mobile-menu-close');
      expect(closeButton).toBeInTheDocument();
      expect(closeButton).toHaveAttribute('aria-label', 'Close menu');
    });

    it('has visible backdrop overlay', () => {
      renderWithProviders(<MobileMenu isOpen={true} onClose={mockOnClose} />);

      const backdrop = screen.getByTestId('mobile-menu-backdrop');
      expect(backdrop).toBeInTheDocument();
      expect(backdrop).toHaveClass('opacity-100');
      expect(backdrop).not.toHaveClass('pointer-events-none');
    });

    it('prevents body scroll', () => {
      renderWithProviders(<MobileMenu isOpen={true} onClose={mockOnClose} />);

      expect(document.body.style.overflow).toBe('hidden');
    });

    it('has proper accessibility attributes', () => {
      renderWithProviders(<MobileMenu isOpen={true} onClose={mockOnClose} />);

      const menu = screen.getByRole('dialog');
      expect(menu).toHaveAttribute('aria-modal', 'true');
      expect(menu).toHaveAttribute('aria-label', 'Mobile navigation menu');
    });
  });

  describe('when closed', () => {
    it('does not render the menu panel', () => {
      renderWithProviders(<MobileMenu isOpen={false} onClose={mockOnClose} />);

      const menu = screen.queryByTestId('mobile-menu');
      expect(menu).not.toBeInTheDocument();
    });

    it('does not render backdrop overlay', () => {
      renderWithProviders(<MobileMenu isOpen={false} onClose={mockOnClose} />);

      const backdrop = screen.queryByTestId('mobile-menu-backdrop');
      expect(backdrop).not.toBeInTheDocument();
    });

    it('restores body scroll when closed', () => {
      const { rerender } = renderWithProviders(
        <MobileMenu isOpen={true} onClose={mockOnClose} />
      );
      expect(document.body.style.overflow).toBe('hidden');

      rerender(
        <BrowserRouter>
          <ThemeProvider>
            <MobileMenu isOpen={false} onClose={mockOnClose} />
          </ThemeProvider>
        </BrowserRouter>
      );
      expect(document.body.style.overflow).toBe('');
    });
  });

  describe('interactions', () => {
    it('calls onClose when close button is clicked', async () => {
      const user = userEvent.setup();
      renderWithProviders(<MobileMenu isOpen={true} onClose={mockOnClose} />);

      const closeButton = screen.getByTestId('mobile-menu-close');
      await user.click(closeButton);

      expect(mockOnClose).toHaveBeenCalledTimes(1);
    });

    it('calls onClose when backdrop is clicked', async () => {
      const user = userEvent.setup();
      renderWithProviders(<MobileMenu isOpen={true} onClose={mockOnClose} />);

      const backdrop = screen.getByTestId('mobile-menu-backdrop');
      await user.click(backdrop);

      expect(mockOnClose).toHaveBeenCalledTimes(1);
    });

    it('calls onClose when Escape key is pressed', () => {
      renderWithProviders(<MobileMenu isOpen={true} onClose={mockOnClose} />);

      fireEvent.keyDown(document, { key: 'Escape' });

      expect(mockOnClose).toHaveBeenCalledTimes(1);
    });

    it('calls onClose when Login link is clicked', async () => {
      const user = userEvent.setup();
      renderWithProviders(<MobileMenu isOpen={true} onClose={mockOnClose} />);

      const loginLink = screen.getByTestId('mobile-menu-login');
      await user.click(loginLink);

      expect(mockOnClose).toHaveBeenCalledTimes(1);
    });

    it('calls onClose when Register link is clicked', async () => {
      const user = userEvent.setup();
      renderWithProviders(<MobileMenu isOpen={true} onClose={mockOnClose} />);

      const registerLink = screen.getByTestId('mobile-menu-register');
      await user.click(registerLink);

      expect(mockOnClose).toHaveBeenCalledTimes(1);
    });
  });

  // Test Case 3 & 4: Touch-friendly elements - minimum 44px height
  describe('touch accessibility', () => {
    it('has Login link with minimum 44px height for touch accessibility', () => {
      renderWithProviders(<MobileMenu isOpen={true} onClose={mockOnClose} />);

      const loginLink = screen.getByTestId('mobile-menu-login');
      expect(loginLink).toHaveClass('min-h-[44px]');
    });

    it('has Register link with minimum 44px height for touch accessibility', () => {
      renderWithProviders(<MobileMenu isOpen={true} onClose={mockOnClose} />);

      const registerLink = screen.getByTestId('mobile-menu-register');
      expect(registerLink).toHaveClass('min-h-[44px]');
    });
  });

  describe('focus management', () => {
    it('focuses close button when menu opens', () => {
      renderWithProviders(<MobileMenu isOpen={true} onClose={mockOnClose} />);

      const closeButton = screen.getByTestId('mobile-menu-close');
      expect(document.activeElement).toBe(closeButton);
    });
  });
});
