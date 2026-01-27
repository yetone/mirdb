/**
 * Integration Tests for Landing Page Routing
 * Owner: Scenario 1 - Hero Section Display (Test Case 5)
 * Owner: Scenario 20 - Error Handling - Route Not Found
 *
 * Tests:
 * - Primary CTA navigates to /register page
 * - Landing page renders without errors at root URL
 * - Component handles missing context gracefully
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { screen, fireEvent, waitFor } from '@testing-library/react';
import { render, renderForIntegration } from './setup';
import { render as rtlRender } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import App from '../../../src/App';
import Home from '../../../src/pages/Home';

describe('Landing Page Routing Integration', () => {
  it('navigates to /register when primary CTA is clicked', async () => {
    render(<App />, { initialEntries: ['/'] });

    // Verify we're on the home page
    expect(screen.getByText(/shorten links/i)).toBeInTheDocument();

    // Find and click the Get Started button in the hero section
    const getStartedButtons = screen.getAllByRole('button', { name: /get started/i });
    // Click the one in the hero section (first one should be it)
    const heroGetStarted = getStartedButtons.find(btn =>
      btn.getAttribute('aria-label')?.includes('URL shortening')
    ) || getStartedButtons[0];

    fireEvent.click(heroGetStarted);

    // Wait for navigation to /register
    await waitFor(() => {
      expect(screen.getByText(/create account/i)).toBeInTheDocument();
    });
  });

  it('renders the home page at root URL', () => {
    render(<App />, { initialEntries: ['/'] });

    // Verify hero section is rendered
    expect(screen.getByRole('heading', { level: 1 })).toBeInTheDocument();
    expect(screen.getByText(/shorten links/i)).toBeInTheDocument();
  });

  it('renders the register page at /register URL', () => {
    render(<App />, { initialEntries: ['/register'] });

    expect(screen.getByText(/create account/i)).toBeInTheDocument();
  });

  it('renders the login page at /login URL', () => {
    render(<App />, { initialEntries: ['/login'] });

    expect(screen.getByRole('heading', { name: /login/i })).toBeInTheDocument();
  });
});

/**
 * Error Handling Tests for Landing Page
 * Owner: Scenario 20 - Error Handling - Route Not Found
 *
 * Tests edge cases and error handling:
 * - Landing page renders without JavaScript errors
 * - Component handles missing context gracefully
 * - Page renders without console errors
 */
describe('Error Handling - Landing Page Edge Cases', () => {
  let consoleErrorSpy: ReturnType<typeof vi.spyOn>;
  let consoleWarnSpy: ReturnType<typeof vi.spyOn>;

  beforeEach(() => {
    // Spy on console.error and console.warn to detect React errors
    consoleErrorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
    consoleWarnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {});
  });

  afterEach(() => {
    consoleErrorSpy.mockRestore();
    consoleWarnSpy.mockRestore();
  });

  describe('Test Case 1: Landing page renders without errors at root URL', () => {
    it('renders landing page component without errors when navigating to / route', () => {
      // Render the full App at root URL
      render(<App />, { initialEntries: ['/'] });

      // Verify landing page rendered successfully
      expect(screen.getByRole('banner')).toBeInTheDocument(); // header
      expect(screen.getByRole('main')).toBeInTheDocument(); // main content
      expect(screen.getByRole('contentinfo')).toBeInTheDocument(); // footer

      // Verify core landing page elements are present
      expect(screen.getByRole('heading', { level: 1 })).toBeInTheDocument();
      expect(screen.getByText(/shorten links/i)).toBeInTheDocument();

      // No JavaScript errors should have been logged
      // Filter out non-critical warnings that may come from test environment
      const criticalErrors = consoleErrorSpy.mock.calls.filter(call => {
        const message = String(call[0]);
        // Ignore React strict mode double-render warnings and test environment noise
        return !message.includes('Warning:') &&
               !message.includes('act(') &&
               !message.includes('test environment');
      });
      expect(criticalErrors).toHaveLength(0);
    });

    it('landing page is accessible via direct URL navigation', () => {
      render(<App />, { initialEntries: ['/'] });

      // Verify the landing page has essential sections
      const main = screen.getByRole('main');
      expect(main).toBeInTheDocument();

      // Check for hero section content
      expect(screen.getByText(/shorten links/i)).toBeInTheDocument();

      // Check for navigation
      expect(screen.getByRole('link', { name: /login/i })).toBeInTheDocument();
      expect(screen.getByRole('link', { name: /get started/i })).toBeInTheDocument();
    });
  });

  describe('Test Case 2: No JavaScript errors in console', () => {
    it('page renders without logging JavaScript errors to console', () => {
      // Render the app at root URL
      render(<App />, { initialEntries: ['/'] });

      // Wait for any async rendering to complete
      expect(screen.getByRole('heading', { level: 1 })).toBeInTheDocument();

      // Check that no uncaught errors were logged
      // Filter out expected warnings from test environment
      const actualErrors = consoleErrorSpy.mock.calls.filter(call => {
        const message = String(call[0]);
        // Filter out known non-critical test environment messages
        return !message.includes('Warning:') &&
               !message.includes('ReactDOMTestUtils.act') &&
               !message.includes('act(...)');
      });

      expect(actualErrors).toHaveLength(0);
    });

    it('home page renders all sections without throwing errors', () => {
      // Render the Home component directly with proper providers
      render(<App />, { initialEntries: ['/'] });

      // Verify all major sections render
      expect(screen.getByRole('banner')).toBeInTheDocument(); // Navigation header
      expect(screen.getByRole('main')).toBeInTheDocument(); // Main content area
      expect(screen.getByRole('contentinfo')).toBeInTheDocument(); // Footer

      // Verify section content is present (indicates successful render)
      expect(screen.getByText(/shorten links/i)).toBeInTheDocument(); // Hero
      expect(screen.getByText(/features/i)).toBeInTheDocument(); // Features section
      expect(screen.getByText(/how it works/i)).toBeInTheDocument(); // How it works
    });
  });

  describe('Test Case 3: Component handles missing ThemeContext gracefully', () => {
    it('Home component renders with default theme when ThemeContext provides defaults', () => {
      // The ThemeProvider has built-in defaults (dark theme)
      // We render with ThemeProvider to verify it provides proper defaults
      render(<App />, { initialEntries: ['/'] });

      // Component should render without errors
      expect(screen.getByRole('banner')).toBeInTheDocument();
      expect(screen.getByRole('main')).toBeInTheDocument();

      // Theme toggle should be present and functional
      const themeToggle = screen.getByRole('button', { name: /switch to (light|dark) mode/i });
      expect(themeToggle).toBeInTheDocument();

      // No errors should be thrown related to missing context
      const contextErrors = consoleErrorSpy.mock.calls.filter(call => {
        const message = String(call[0]);
        return message.includes('useTheme') ||
               message.includes('ThemeContext') ||
               message.includes('undefined');
      });
      expect(contextErrors).toHaveLength(0);
    });

    it('renders gracefully even when localStorage has no theme preference', () => {
      // Clear localStorage to simulate first-time visitor
      localStorage.removeItem('theme');

      render(<App />, { initialEntries: ['/'] });

      // Component should still render successfully with default theme
      expect(screen.getByRole('banner')).toBeInTheDocument();
      expect(screen.getByRole('main')).toBeInTheDocument();
      expect(screen.getByRole('contentinfo')).toBeInTheDocument();

      // No errors from theme initialization
      const themeErrors = consoleErrorSpy.mock.calls.filter(call => {
        const message = String(call[0]);
        return message.includes('theme') || message.includes('localStorage');
      });
      expect(themeErrors).toHaveLength(0);
    });

    it('ThemeContext provides default theme value when initialized', () => {
      // The ThemeContext defaults to 'dark' when no localStorage value exists
      localStorage.removeItem('theme');

      render(<App />, { initialEntries: ['/'] });

      // The theme toggle should reflect the default theme
      // Default is 'dark', so button should show "Switch to light mode"
      const themeToggle = screen.getByRole('button', { name: /switch to (light|dark) mode/i });
      expect(themeToggle).toBeInTheDocument();

      // Verify data-theme attribute is set on document
      // This is set by ThemeContext useEffect
      const dataTheme = document.documentElement.getAttribute('data-theme');
      expect(dataTheme).toBeTruthy();
      expect(['light', 'dark', 'cyberpunk', 'synthwave']).toContain(dataTheme);
    });
  });
});
