/**
 * Navbar component tests.
 * Owner: Scenario 6 - Navbar Component Integration
 *
 * Test coverage:
 * - Navbar renders with logo and navigation links
 * - Home, Login, Register links navigate correctly
 * - Theme selector displays 6 theme options
 * - Unauthenticated state shows login/register
 */

import { describe, it, expect, vi } from 'vitest';
import { screen, fireEvent, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { Navbar } from '@/components/layout/Navbar';
import { renderWithProviders } from '../../utils/renderWithProviders';
import { getAvailableThemes } from '../../utils/mockTheme';

// Mock useNavigate for navigation tests
const mockNavigate = vi.fn();
vi.mock('react-router-dom', async () => {
  const actual = await vi.importActual('react-router-dom');
  return {
    ...actual,
    useNavigate: () => mockNavigate,
  };
});

describe('Navbar Component Integration', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  describe('Test Case 1: Navbar renders with logo, navigation links, and theme selector', () => {
    it('should render Navbar component with all required elements', () => {
      renderWithProviders(<Navbar />);

      // Verify Navbar is present
      const navbar = screen.getByTestId('navbar');
      expect(navbar).toBeInTheDocument();

      // Verify logo/brand link
      const logo = screen.getByTestId('navbar-logo');
      expect(logo).toBeInTheDocument();
      expect(logo).toHaveTextContent('URLShort');

      // Verify navigation links
      expect(screen.getByTestId('navbar-login')).toBeInTheDocument();
      expect(screen.getByTestId('navbar-register')).toBeInTheDocument();

      // Verify theme toggle
      expect(screen.getByTestId('theme-toggle')).toBeInTheDocument();
    });

    it('should have sticky positioning with z-50', () => {
      renderWithProviders(<Navbar />);

      const navbar = screen.getByTestId('navbar');
      expect(navbar).toHaveClass('sticky');
      expect(navbar).toHaveClass('top-0');
      expect(navbar).toHaveClass('z-50');
    });

    it('should have proper backdrop blur styling', () => {
      renderWithProviders(<Navbar />);

      const navbar = screen.getByTestId('navbar');
      expect(navbar).toHaveClass('backdrop-blur-md');
    });
  });

  describe('Test Case 2: Click Home link in Navbar navigates to / route', () => {
    it('should have Home link (logo) that navigates to root route', () => {
      renderWithProviders(<Navbar />, { initialEntries: ['/other'] });

      const homeLink = screen.getByTestId('navbar-logo');
      expect(homeLink).toHaveAttribute('href', '/');
    });

    it('should display the brand name as the home link', () => {
      renderWithProviders(<Navbar />);

      const homeLink = screen.getByTestId('navbar-logo');
      expect(homeLink.tagName).toBe('A');
      expect(homeLink).toHaveTextContent('URLShort');
    });
  });

  describe('Test Case 3: Click Login link in Navbar navigates to /login route', () => {
    it('should have Login link with correct href', () => {
      renderWithProviders(<Navbar />);

      const loginLink = screen.getByTestId('navbar-login');
      expect(loginLink).toBeInTheDocument();
      expect(loginLink).toHaveAttribute('href', '/login');
    });

    it('should display Login text on the login link', () => {
      renderWithProviders(<Navbar />);

      const loginLink = screen.getByTestId('navbar-login');
      expect(loginLink).toHaveTextContent('Login');
    });

    it('should have button styling on login link', () => {
      renderWithProviders(<Navbar />);

      const loginLink = screen.getByTestId('navbar-login');
      expect(loginLink).toHaveClass('btn');
      expect(loginLink).toHaveClass('btn-ghost');
    });
  });

  describe('Test Case 4: Click Register link in Navbar navigates to /register route', () => {
    it('should have Register link with correct href', () => {
      renderWithProviders(<Navbar />);

      const registerLink = screen.getByTestId('navbar-register');
      expect(registerLink).toBeInTheDocument();
      expect(registerLink).toHaveAttribute('href', '/register');
    });

    it('should display Register text on the register link', () => {
      renderWithProviders(<Navbar />);

      const registerLink = screen.getByTestId('navbar-register');
      expect(registerLink).toHaveTextContent('Register');
    });

    it('should have primary button styling on register link', () => {
      renderWithProviders(<Navbar />);

      const registerLink = screen.getByTestId('navbar-register');
      expect(registerLink).toHaveClass('btn');
      expect(registerLink).toHaveClass('btn-primary');
    });
  });

  describe('Test Case 5: Theme selector dropdown displays 6 theme options', () => {
    it('should display theme toggle button', () => {
      renderWithProviders(<Navbar />);

      const themeToggle = screen.getByTestId('theme-toggle');
      expect(themeToggle).toBeInTheDocument();
      expect(themeToggle).toHaveTextContent('Theme');
    });

    it('should display all 6 theme options in dropdown', () => {
      renderWithProviders(<Navbar />);

      const expectedThemes = ['light', 'dark', 'cyberpunk', 'synthwave', 'retro', 'valentine'];

      // Verify all theme options are present
      expectedThemes.forEach((theme) => {
        const themeOption = screen.getByTestId(`theme-option-${theme}`);
        expect(themeOption).toBeInTheDocument();
        expect(themeOption).toHaveTextContent(theme.charAt(0).toUpperCase() + theme.slice(1));
      });
    });

    it('should match the available themes from mock utility', () => {
      renderWithProviders(<Navbar />);

      const availableThemes = getAvailableThemes();
      expect(availableThemes).toHaveLength(6);

      availableThemes.forEach((theme) => {
        expect(screen.getByTestId(`theme-option-${theme}`)).toBeInTheDocument();
      });
    });

    it('should have dropdown styling on theme menu', () => {
      renderWithProviders(<Navbar />);

      const themeToggle = screen.getByTestId('theme-toggle');
      const dropdown = themeToggle.closest('.dropdown');
      expect(dropdown).toBeInTheDocument();
      expect(dropdown).toHaveClass('dropdown-end');
    });

    it('should have clickable theme option buttons', async () => {
      renderWithProviders(<Navbar />);

      const darkThemeOption = screen.getByTestId('theme-option-dark');
      expect(darkThemeOption.tagName).toBe('BUTTON');

      // Verify the button is clickable
      await userEvent.click(darkThemeOption);
    });
  });

  describe('Unauthenticated State', () => {
    it('should show login and register links when not authenticated', () => {
      renderWithProviders(<Navbar />, { withAuth: true });

      expect(screen.getByTestId('navbar-login')).toBeInTheDocument();
      expect(screen.getByTestId('navbar-register')).toBeInTheDocument();
    });

    it('should have visible navigation links in unauthenticated state', () => {
      renderWithProviders(<Navbar />);

      const loginLink = screen.getByTestId('navbar-login');
      const registerLink = screen.getByTestId('navbar-register');

      expect(loginLink).toBeVisible();
      expect(registerLink).toBeVisible();
    });
  });

  describe('Accessibility', () => {
    it('should have proper semantic header element', () => {
      renderWithProviders(<Navbar />);

      const navbar = screen.getByTestId('navbar');
      expect(navbar.tagName).toBe('HEADER');
    });

    it('should have focusable interactive elements', () => {
      renderWithProviders(<Navbar />);

      const logo = screen.getByTestId('navbar-logo');
      const loginLink = screen.getByTestId('navbar-login');
      const registerLink = screen.getByTestId('navbar-register');
      const themeToggle = screen.getByTestId('theme-toggle');

      // All interactive elements should be focusable
      expect(logo).not.toHaveAttribute('tabindex', '-1');
      expect(loginLink).not.toHaveAttribute('tabindex', '-1');
      expect(registerLink).not.toHaveAttribute('tabindex', '-1');
      expect(themeToggle.getAttribute('tabIndex')).toBe('0');
    });
  });
});
