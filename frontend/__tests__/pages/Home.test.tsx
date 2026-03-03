/**
 * Homepage Integration Tests
 * Owner: Scenario 2 - Navigation to Registration
 *
 * Tests the navigation functionality from homepage to registration page.
 */
import { describe, it, expect, beforeEach, vi } from 'vitest';
import { render, screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { BrowserRouter, MemoryRouter, Routes, Route } from 'react-router-dom';
import { ThemeProvider } from '../../src/contexts/ThemeContext';
import { AuthProvider } from '../../src/contexts/AuthContext';
import { Home } from '../../src/pages/Home';
import { HeroSection } from '../../src/components/HeroSection';

// Wrapper component with all providers for testing
const TestWrapper = ({ children }: { children: React.ReactNode }) => (
  <ThemeProvider>
    <AuthProvider>
      <BrowserRouter>{children}</BrowserRouter>
    </AuthProvider>
  </ThemeProvider>
);

describe('Navigation to Registration - Scenario 2', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    // Reset localStorage theme
    window.localStorage.getItem = vi.fn((key) => {
      if (key === 'app-theme') return 'dark';
      return null;
    });
  });

  describe('Test Case 1: Get Started button navigates to /register', () => {
    it('should render Get Started button in hero section as a link to /register', () => {
      render(
        <TestWrapper>
          <Home />
        </TestWrapper>
      );

      const getStartedButton = screen.getByTestId('get-started-button');
      expect(getStartedButton).toBeInTheDocument();
      expect(getStartedButton).toHaveAttribute('href', '/register');
    });

    it('should navigate to register page when clicking Get Started in hero section', async () => {
      const user = userEvent.setup();

      // Render the app with memory router at root
      render(
        <MemoryRouter initialEntries={['/']}>
          <ThemeProvider>
            <AuthProvider>
              <Routes>
                <Route path="/" element={<Home />} />
                <Route path="/register" element={<div data-testid="register-page">Register Page</div>} />
              </Routes>
            </AuthProvider>
          </ThemeProvider>
        </MemoryRouter>
      );

      const getStartedButton = screen.getByTestId('get-started-button');
      await user.click(getStartedButton);

      // After clicking, we should be on the register page
      expect(screen.getByTestId('register-page')).toBeInTheDocument();
    });

    it('should have clickable Get Started button that triggers navigation', async () => {
      const user = userEvent.setup();

      render(
        <MemoryRouter initialEntries={['/']}>
          <ThemeProvider>
            <AuthProvider>
              <Routes>
                <Route path="/" element={<Home />} />
                <Route path="/register" element={<div data-testid="register-page">Register</div>} />
              </Routes>
            </AuthProvider>
          </ThemeProvider>
        </MemoryRouter>
      );

      // Verify homepage is displayed first
      expect(screen.getByTestId('home-page')).toBeInTheDocument();

      // Click the Get Started button
      const getStartedButton = screen.getByTestId('get-started-button');
      await user.click(getStartedButton);

      // Verify navigation to register page
      expect(screen.getByTestId('register-page')).toBeInTheDocument();
    });
  });

  describe('Test Case 2: Navigation preserves theme selection', () => {
    it('should preserve cyberpunk theme when navigating to /register', async () => {
      const user = userEvent.setup();

      // Mock localStorage to return cyberpunk theme
      window.localStorage.getItem = vi.fn((key) => {
        if (key === 'app-theme') return 'cyberpunk';
        return null;
      });

      render(
        <MemoryRouter initialEntries={['/']}>
          <ThemeProvider>
            <AuthProvider>
              <Routes>
                <Route path="/" element={<Home />} />
                <Route path="/register" element={
                  <div data-testid="register-page">
                    <span data-testid="theme-indicator">{document.documentElement.getAttribute('data-theme')}</span>
                  </div>
                } />
              </Routes>
            </AuthProvider>
          </ThemeProvider>
        </MemoryRouter>
      );

      // Verify theme is applied initially
      expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk');

      // Navigate to register
      const getStartedButton = screen.getByTestId('get-started-button');
      await user.click(getStartedButton);

      // Theme should still be cyberpunk after navigation
      expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk');
    });

    it('should maintain theme context across navigation', () => {
      window.localStorage.getItem = vi.fn((key) => {
        if (key === 'app-theme') return 'synthwave';
        return null;
      });

      render(
        <TestWrapper>
          <Home />
        </TestWrapper>
      );

      // Theme should be applied to document
      expect(document.documentElement.getAttribute('data-theme')).toBe('synthwave');
    });
  });

  describe('Test Case 3: Get Started button has correct href', () => {
    it('should have href attribute pointing to /register', () => {
      render(
        <TestWrapper>
          <HeroSection />
        </TestWrapper>
      );

      const getStartedButton = screen.getByTestId('get-started-button');
      expect(getStartedButton.tagName.toLowerCase()).toBe('a');
      expect(getStartedButton).toHaveAttribute('href', '/register');
    });

    it('should render as a Link component (anchor tag)', () => {
      render(
        <TestWrapper>
          <Home />
        </TestWrapper>
      );

      const getStartedButton = screen.getByTestId('get-started-button');
      expect(getStartedButton.tagName.toLowerCase()).toBe('a');
    });

    it('should have accessible text content in hero section', () => {
      render(
        <TestWrapper>
          <Home />
        </TestWrapper>
      );

      // Use getByTestId to get the specific hero section button
      const heroSection = screen.getByTestId('hero-section');
      const getStartedButton = within(heroSection).getByRole('link', { name: /get started/i });
      expect(getStartedButton).toBeInTheDocument();
      expect(getStartedButton).toHaveAttribute('href', '/register');
    });
  });

  describe('Hero Section displays correctly', () => {
    it('should display the hero section on the homepage', () => {
      render(
        <TestWrapper>
          <Home />
        </TestWrapper>
      );

      expect(screen.getByTestId('hero-section')).toBeInTheDocument();
    });

    it('should display both Get Started and Sign In buttons in hero', () => {
      render(
        <TestWrapper>
          <Home />
        </TestWrapper>
      );

      expect(screen.getByTestId('get-started-button')).toBeInTheDocument();
      expect(screen.getByTestId('sign-in-button')).toBeInTheDocument();
    });
  });
});
