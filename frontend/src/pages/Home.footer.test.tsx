import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, within } from '@testing-library/react';
import { BrowserRouter } from 'react-router-dom';
import Home from './Home';
import { ThemeProvider } from '../contexts/ThemeContext';
import { useThemeStore } from '../store/themeStore';

// Mock useNavigate
const mockNavigate = vi.fn();
vi.mock('react-router-dom', async () => {
  const actual = await vi.importActual('react-router-dom');
  return {
    ...actual,
    useNavigate: () => mockNavigate,
  };
});

// Helper to render Home with required providers
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
    localStorage.clear();
    useThemeStore.setState({ theme: 'light' });
  });

  afterEach(() => {
    localStorage.clear();
  });

  // Test Case 1: Footer element is present at page bottom
  describe('Test Case 1: Footer element is present at page bottom', () => {
    it('renders the footer element', () => {
      renderHome();

      const footer = screen.getByTestId('homepage-footer');
      expect(footer).toBeInTheDocument();
    });

    it('footer has the correct HTML element tag', () => {
      renderHome();

      const footer = screen.getByTestId('homepage-footer');
      expect(footer.tagName.toLowerCase()).toBe('footer');
    });

    it('footer has footer-center class for proper layout', () => {
      renderHome();

      const footer = screen.getByTestId('homepage-footer');
      expect(footer).toHaveClass('footer-center');
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
  });

  // Test Case 2: Copyright or brand text is displayed
  describe('Test Case 2: Copyright or brand text is displayed', () => {
    it('displays the brand name "URLShort"', () => {
      renderHome();

      const footer = screen.getByTestId('homepage-footer');
      expect(footer).toHaveTextContent('URLShort');
    });

    it('displays brand tagline "Shorten, Share, Track"', () => {
      renderHome();

      const footer = screen.getByTestId('homepage-footer');
      expect(footer).toHaveTextContent('Shorten, Share, Track');
    });

    it('displays copyright notice with current year', () => {
      renderHome();

      const footer = screen.getByTestId('homepage-footer');
      const currentYear = new Date().getFullYear().toString();
      expect(footer).toHaveTextContent(currentYear);
      expect(footer).toHaveTextContent('All rights reserved');
    });

    it('copyright symbol is present', () => {
      renderHome();

      const footer = screen.getByTestId('homepage-footer');
      expect(footer).toHaveTextContent('©');
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
    it('displays Privacy Policy link', () => {
      renderHome();

      const footer = screen.getByTestId('homepage-footer');
      const privacyLink = within(footer).getByRole('link', { name: /privacy policy/i });
      expect(privacyLink).toBeInTheDocument();
    });

    it('Privacy Policy link has correct href', () => {
      renderHome();

      const footer = screen.getByTestId('homepage-footer');
      const privacyLink = within(footer).getByRole('link', { name: /privacy policy/i });
      expect(privacyLink).toHaveAttribute('href', '/privacy');
    });

    it('Privacy Policy link is visible', () => {
      renderHome();

      const footer = screen.getByTestId('homepage-footer');
      const privacyLink = within(footer).getByRole('link', { name: /privacy policy/i });
      expect(privacyLink).toBeVisible();
    });

    it('Privacy Policy link has hover styling class', () => {
      renderHome();

      const footer = screen.getByTestId('homepage-footer');
      const privacyLink = within(footer).getByRole('link', { name: /privacy policy/i });
      expect(privacyLink).toHaveClass('link-hover');
    });
  });

  // Test Case 4: Terms of Service link/placeholder is present
  describe('Test Case 4: Terms of Service link/placeholder is present', () => {
    it('displays Terms of Service link', () => {
      renderHome();

      const footer = screen.getByTestId('homepage-footer');
      const termsLink = within(footer).getByRole('link', { name: /terms of service/i });
      expect(termsLink).toBeInTheDocument();
    });

    it('Terms of Service link has correct href', () => {
      renderHome();

      const footer = screen.getByTestId('homepage-footer');
      const termsLink = within(footer).getByRole('link', { name: /terms of service/i });
      expect(termsLink).toHaveAttribute('href', '/terms');
    });

    it('Terms of Service link is visible', () => {
      renderHome();

      const footer = screen.getByTestId('homepage-footer');
      const termsLink = within(footer).getByRole('link', { name: /terms of service/i });
      expect(termsLink).toBeVisible();
    });

    it('Terms of Service link has hover styling class', () => {
      renderHome();

      const footer = screen.getByTestId('homepage-footer');
      const termsLink = within(footer).getByRole('link', { name: /terms of service/i });
      expect(termsLink).toHaveClass('link-hover');
    });
  });

  // Test Case 5: ThemeToggle component is accessible in footer or header
  describe('Test Case 5: ThemeToggle component is accessible in footer or header', () => {
    it('ThemeToggle component is present in footer', () => {
      renderHome();

      const footer = screen.getByTestId('homepage-footer');
      const themeToggle = within(footer).getByTestId('theme-toggle');
      expect(themeToggle).toBeInTheDocument();
    });

    it('ThemeToggle has accessible label', () => {
      renderHome();

      const footer = screen.getByTestId('homepage-footer');
      const themeToggleButton = within(footer).getByLabelText(/change theme/i);
      expect(themeToggleButton).toBeInTheDocument();
    });

    it('ThemeToggle is visible and interactive', () => {
      renderHome();

      const footer = screen.getByTestId('homepage-footer');
      const themeToggle = within(footer).getByTestId('theme-toggle');
      expect(themeToggle).toBeVisible();
    });

    it('Footer displays "Theme:" label next to ThemeToggle', () => {
      renderHome();

      const footer = screen.getByTestId('homepage-footer');
      expect(footer).toHaveTextContent('Theme:');
    });

    it('ThemeToggle in footer uses dropdown variant by default', () => {
      renderHome();

      const footer = screen.getByTestId('homepage-footer');
      const themeToggle = within(footer).getByTestId('theme-toggle');
      // Dropdown variant has the dropdown class
      expect(themeToggle).toHaveClass('dropdown');
    });

    it('ThemeToggle button is keyboard accessible', () => {
      renderHome();

      const toggleButton = screen.getByLabelText(/change theme/i);
      expect(toggleButton).not.toHaveAttribute('tabindex', '-1');
    });
  });

  // Additional footer structure tests
  describe('Footer structure and layout', () => {
    it('footer contains Contact link', () => {
      renderHome();

      const footer = screen.getByTestId('homepage-footer');
      const contactLink = within(footer).getByRole('link', { name: /contact/i });
      expect(contactLink).toBeInTheDocument();
      expect(contactLink).toHaveAttribute('href', '/contact');
    });

    it('footer links are grouped together', () => {
      renderHome();

      const footer = screen.getByTestId('homepage-footer');
      const links = within(footer).getAllByRole('link');
      // Should have at least 3 links: Privacy, Terms, Contact
      expect(links.length).toBeGreaterThanOrEqual(3);
    });

    it('footer has all three navigation links', () => {
      renderHome();

      const footer = screen.getByTestId('homepage-footer');
      const links = within(footer).getAllByRole('link');

      // Should have Privacy, Terms, and Contact links
      const linkTexts = links.map(link => link.textContent);
      expect(linkTexts).toContain('Privacy Policy');
      expect(linkTexts).toContain('Terms of Service');
      expect(linkTexts).toContain('Contact');
    });

    it('footer has appropriate background styling', () => {
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
