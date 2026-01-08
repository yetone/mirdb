import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, within } from '@testing-library/react';
import { BrowserRouter } from 'react-router-dom';
import Home from './Home';
import { ThemeProvider } from '../contexts/ThemeContext';

// Mock useNavigate
const mockNavigate = vi.fn();
vi.mock('react-router-dom', async () => {
  const actual = await vi.importActual('react-router-dom');
  return {
    ...actual,
    useNavigate: () => mockNavigate,
  };
});

const renderHome = () => {
  return render(
    <ThemeProvider>
      <BrowserRouter>
        <Home />
      </BrowserRouter>
    </ThemeProvider>
  );
};

// Footer Section Tests (Scenario: Footer Section - REQ-8)
// Verify footer displays with relevant links and theme toggle
describe('Home - Footer Section (REQ-8)', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  // Test Case 1: Footer element is present at page bottom
  describe('Test Case 1: Footer element is present at page bottom', () => {
    it('renders a footer element on the homepage', () => {
      renderHome();

      const footer = screen.getByTestId('homepage-footer');
      expect(footer).toBeInTheDocument();
    });

    it('footer uses semantic HTML footer element', () => {
      renderHome();

      const footer = screen.getByTestId('homepage-footer');
      expect(footer.tagName.toLowerCase()).toBe('footer');
    });

    it('footer is visible on the page', () => {
      renderHome();

      const footer = screen.getByTestId('homepage-footer');
      expect(footer).toBeVisible();
    });

    it('footer is positioned at the end of the page content', () => {
      renderHome();

      // Footer should be the last major section in the page container
      const footer = screen.getByTestId('homepage-footer');
      const parent = footer.parentElement;

      // Footer should be the last child of its parent
      if (parent) {
        const lastChild = parent.lastElementChild;
        expect(lastChild).toBe(footer);
      }
    });

    it('footer has appropriate styling classes for centering', () => {
      renderHome();

      const footer = screen.getByTestId('homepage-footer');
      expect(footer).toHaveClass('footer-center');
    });
  });

  // Test Case 2: Copyright or brand text is displayed
  describe('Test Case 2: Copyright or brand text is displayed', () => {
    it('displays the brand name "URLShort" in the footer', () => {
      renderHome();

      const footer = screen.getByTestId('homepage-footer');
      const brandName = within(footer).getByText('URLShort');
      expect(brandName).toBeInTheDocument();
    });

    it('displays the brand tagline "Shorten, Share, Track"', () => {
      renderHome();

      const footer = screen.getByTestId('homepage-footer');
      const tagline = within(footer).getByText('Shorten, Share, Track');
      expect(tagline).toBeInTheDocument();
    });

    it('displays copyright text with current year', () => {
      renderHome();

      const footer = screen.getByTestId('homepage-footer');
      const currentYear = new Date().getFullYear().toString();

      // Look for copyright text containing the current year
      const copyrightText = within(footer).getByText((content) => {
        return content.includes('©') && content.includes(currentYear);
      });
      expect(copyrightText).toBeInTheDocument();
    });

    it('copyright text includes "All rights reserved"', () => {
      renderHome();

      const footer = screen.getByTestId('homepage-footer');
      const copyrightText = within(footer).getByText(/All rights reserved/i);
      expect(copyrightText).toBeInTheDocument();
    });

    it('brand name has bold styling', () => {
      renderHome();

      const footer = screen.getByTestId('homepage-footer');
      const brandName = within(footer).getByText('URLShort');
      expect(brandName).toHaveClass('font-bold');
    });
  });

  // Test Case 3: Privacy Policy link/placeholder is present
  describe('Test Case 3: Privacy Policy link/placeholder is present', () => {
    it('renders a Privacy Policy link in the footer', () => {
      renderHome();

      const footer = screen.getByTestId('homepage-footer');
      const privacyLink = within(footer).getByRole('link', { name: /privacy policy/i });
      expect(privacyLink).toBeInTheDocument();
    });

    it('Privacy Policy link points to correct href', () => {
      renderHome();

      const footer = screen.getByTestId('homepage-footer');
      const privacyLink = within(footer).getByRole('link', { name: /privacy policy/i });
      expect(privacyLink).toHaveAttribute('href', '/privacy');
    });

    it('Privacy Policy link is visible and clickable', () => {
      renderHome();

      const footer = screen.getByTestId('homepage-footer');
      const privacyLink = within(footer).getByRole('link', { name: /privacy policy/i });
      expect(privacyLink).toBeVisible();
    });

    it('Privacy Policy link has appropriate hover styling class', () => {
      renderHome();

      const footer = screen.getByTestId('homepage-footer');
      const privacyLink = within(footer).getByRole('link', { name: /privacy policy/i });
      expect(privacyLink).toHaveClass('link-hover');
    });
  });

  // Test Case 4: Terms of Service link/placeholder is present
  describe('Test Case 4: Terms of Service link/placeholder is present', () => {
    it('renders a Terms of Service link in the footer', () => {
      renderHome();

      const footer = screen.getByTestId('homepage-footer');
      const termsLink = within(footer).getByRole('link', { name: /terms of service/i });
      expect(termsLink).toBeInTheDocument();
    });

    it('Terms of Service link points to correct href', () => {
      renderHome();

      const footer = screen.getByTestId('homepage-footer');
      const termsLink = within(footer).getByRole('link', { name: /terms of service/i });
      expect(termsLink).toHaveAttribute('href', '/terms');
    });

    it('Terms of Service link is visible and clickable', () => {
      renderHome();

      const footer = screen.getByTestId('homepage-footer');
      const termsLink = within(footer).getByRole('link', { name: /terms of service/i });
      expect(termsLink).toBeVisible();
    });

    it('Terms of Service link has appropriate hover styling class', () => {
      renderHome();

      const footer = screen.getByTestId('homepage-footer');
      const termsLink = within(footer).getByRole('link', { name: /terms of service/i });
      expect(termsLink).toHaveClass('link-hover');
    });
  });

  // Test Case 5: ThemeToggle component is accessible in footer or header
  describe('Test Case 5: ThemeToggle component is accessible in footer or header', () => {
    it('ThemeToggle component is rendered on the homepage', () => {
      renderHome();

      const themeToggle = screen.getByTestId('theme-toggle');
      expect(themeToggle).toBeInTheDocument();
    });

    it('ThemeToggle is located within the footer section', () => {
      renderHome();

      const footer = screen.getByTestId('homepage-footer');
      const themeToggle = within(footer).getByTestId('theme-toggle');
      expect(themeToggle).toBeInTheDocument();
    });

    it('ThemeToggle has accessible label', () => {
      renderHome();

      const toggleButton = screen.getByLabelText(/change theme/i);
      expect(toggleButton).toBeInTheDocument();
    });

    it('ThemeToggle is visible and interactive', () => {
      renderHome();

      const themeToggle = screen.getByTestId('theme-toggle');
      expect(themeToggle).toBeVisible();
    });

    it('footer displays "Theme:" label next to ThemeToggle', () => {
      renderHome();

      const footer = screen.getByTestId('homepage-footer');
      const themeLabel = within(footer).getByText('Theme:');
      expect(themeLabel).toBeInTheDocument();
    });

    it('ThemeToggle button is keyboard accessible', () => {
      renderHome();

      const toggleButton = screen.getByLabelText(/change theme/i);
      expect(toggleButton).not.toHaveAttribute('tabindex', '-1');
    });
  });

  // Additional footer integration tests
  describe('Footer - Additional Integration Tests', () => {
    it('footer contains Contact link', () => {
      renderHome();

      const footer = screen.getByTestId('homepage-footer');
      const contactLink = within(footer).getByRole('link', { name: /contact/i });
      expect(contactLink).toBeInTheDocument();
      expect(contactLink).toHaveAttribute('href', '/contact');
    });

    it('footer has all three navigation links in a row', () => {
      renderHome();

      const footer = screen.getByTestId('homepage-footer');
      const links = within(footer).getAllByRole('link');

      // Should have at least Privacy, Terms, and Contact links
      const linkTexts = links.map(link => link.textContent);
      expect(linkTexts).toContain('Privacy Policy');
      expect(linkTexts).toContain('Terms of Service');
      expect(linkTexts).toContain('Contact');
    });

    it('footer has proper background styling', () => {
      renderHome();

      const footer = screen.getByTestId('homepage-footer');
      expect(footer).toHaveClass('bg-base-300');
    });

    it('footer has proper text color styling', () => {
      renderHome();

      const footer = screen.getByTestId('homepage-footer');
      expect(footer).toHaveClass('text-base-content');
    });

    it('footer has appropriate padding', () => {
      renderHome();

      const footer = screen.getByTestId('homepage-footer');
      expect(footer).toHaveClass('p-10');
    });
  });
});
