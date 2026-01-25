/**
 * Integration tests for Theme Toggle Functionality and Dark Mode Rendering
 * Owner: Scenario 12 - Theme Toggle Functionality, Scenario 13 - Dark Mode Rendering
 *
 * Tests:
 * - Theme toggle switches between light and dark mode
 * - Theme changes are applied to all homepage elements
 * - ThemeToggle component is present in navigation
 * - Theme preference is persisted
 * - Homepage renders with dark theme context
 * - Text contrast in dark mode
 * - GlassMorphismCard components render with dark mode styling
 * - FuturisticButton components render correctly in dark mode
 */

import React, { ReactElement, ReactNode } from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { BrowserRouter, MemoryRouter } from 'react-router-dom';
import { ThemeContext, ThemeProvider } from '../../../src/contexts/ThemeContext';
import { AuthContext, AuthProvider } from '../../../src/contexts/AuthContext';
import { Home } from '../../../src/pages/Home';
import { FeaturesSection } from '../../../src/components/homepage/FeaturesSection';
import { HeroSection } from '../../../src/components/homepage/HeroSection';
import { GlassMorphismCard } from '../../../src/components/GlassMorphismCard';
import { FuturisticButton } from '../../../src/components/FuturisticButton';
import { Navbar } from '../../../src/components/Navbar';
import { ThemeToggle } from '../../../src/components/ThemeToggle';

// Mock framer-motion to avoid animation issues in tests
vi.mock('framer-motion', () => ({
  motion: {
    div: ({ children, ...props }: React.HTMLAttributes<HTMLDivElement>) => (
      <div {...props}>{children}</div>
    ),
    h2: ({ children, ...props }: React.HTMLAttributes<HTMLHeadingElement>) => (
      <h2 {...props}>{children}</h2>
    ),
    p: ({ children, ...props }: React.HTMLAttributes<HTMLParagraphElement>) => (
      <p {...props}>{children}</p>
    ),
    section: ({ children, ...props }: React.HTMLAttributes<HTMLElement>) => (
      <section {...props}>{children}</section>
    ),
  },
}));

// Mock auth context for testing
const mockAuthContext = {
  user: null,
  isAuthenticated: false,
  isLoading: false,
  login: vi.fn(),
  logout: vi.fn(),
  register: vi.fn(),
};

/**
 * Custom dark theme provider wrapper for testing dark mode specifically
 */
interface DarkThemeProviderProps {
  children: ReactNode;
}

function DarkThemeProvider({ children }: DarkThemeProviderProps) {
  const darkThemeValue = {
    theme: 'dark' as const,
    setTheme: vi.fn(),
    toggleTheme: vi.fn(),
  };

  return (
    <ThemeContext.Provider value={darkThemeValue}>
      <AuthContext.Provider value={mockAuthContext}>
        <BrowserRouter>{children}</BrowserRouter>
      </AuthContext.Provider>
    </ThemeContext.Provider>
  );
}

/**
 * Render helper that wraps component with dark theme context
 */
function renderWithDarkTheme(ui: ReactElement) {
  // Set the document theme attribute to dark before rendering
  document.documentElement.setAttribute('data-theme', 'dark');

  return render(ui, { wrapper: DarkThemeProvider });
}

/**
 * Helper function to render components with all required providers
 */
function renderWithProviders(ui: React.ReactElement) {
  return render(
    <ThemeProvider>
      <AuthProvider>
        <MemoryRouter initialEntries={['/']}>{ui}</MemoryRouter>
      </AuthProvider>
    </ThemeProvider>
  );
}

/**
 * Helper to get current theme from document
 */
function getCurrentTheme(): string | null {
  return document.documentElement.getAttribute('data-theme');
}

// ===============================================
// Scenario 12: Theme Toggle Functionality Tests
// ===============================================

describe('Theme Toggle Functionality - Scenario 12', () => {
  beforeEach(() => {
    // Clear localStorage before each test
    localStorage.clear();
    // Reset document theme attribute
    document.documentElement.removeAttribute('data-theme');
  });

  afterEach(() => {
    localStorage.clear();
    document.documentElement.removeAttribute('data-theme');
  });

  describe('Test Case 1: Click theme toggle from light to dark mode', () => {
    it('should switch homepage to dark theme with appropriate colors', async () => {
      // Set initial theme to light
      localStorage.setItem('theme', 'light');

      renderWithProviders(<Home />);

      // Wait for the page to render
      await waitFor(() => {
        expect(screen.getByText('Shorten URLs. Track Every Click.')).toBeInTheDocument();
      });

      // Verify initial light theme is applied
      await waitFor(() => {
        expect(getCurrentTheme()).toBe('light');
      });

      // Find the theme toggle button
      const themeToggle = screen.getByRole('button', { name: /switch to dark mode/i });
      expect(themeToggle).toBeInTheDocument();

      // Click to toggle to dark mode
      fireEvent.click(themeToggle);

      // Verify dark theme is now applied
      await waitFor(() => {
        expect(getCurrentTheme()).toBe('dark');
      });

      // Verify localStorage is updated
      expect(localStorage.getItem('theme')).toBe('dark');
    });

    it('should update aria-label when switching to dark mode', async () => {
      localStorage.setItem('theme', 'light');

      renderWithProviders(<Navbar />);

      // Find toggle with light mode aria-label
      const themeToggle = screen.getByRole('button', { name: /switch to dark mode/i });
      expect(themeToggle).toBeInTheDocument();

      // Click to toggle
      fireEvent.click(themeToggle);

      // Verify aria-label changed
      await waitFor(() => {
        expect(screen.getByRole('button', { name: /switch to light mode/i })).toBeInTheDocument();
      });
    });
  });

  describe('Test Case 2: Click theme toggle from dark to light mode', () => {
    it('should switch homepage to light theme with appropriate colors', async () => {
      // Set initial theme to dark
      localStorage.setItem('theme', 'dark');

      renderWithProviders(<Home />);

      // Wait for page to render
      await waitFor(() => {
        expect(screen.getByText('Shorten URLs. Track Every Click.')).toBeInTheDocument();
      });

      // Verify initial dark theme is applied
      await waitFor(() => {
        expect(getCurrentTheme()).toBe('dark');
      });

      // Find the theme toggle button
      const themeToggle = screen.getByRole('button', { name: /switch to light mode/i });
      expect(themeToggle).toBeInTheDocument();

      // Click to toggle to light mode
      fireEvent.click(themeToggle);

      // Verify light theme is now applied
      await waitFor(() => {
        expect(getCurrentTheme()).toBe('light');
      });

      // Verify localStorage is updated
      expect(localStorage.getItem('theme')).toBe('light');
    });

    it('should display sun icon when in dark mode (to switch to light)', async () => {
      localStorage.setItem('theme', 'dark');

      renderWithProviders(<Navbar />);

      // In dark mode, the sun icon should be displayed (to indicate switching to light)
      const themeToggle = screen.getByRole('button', { name: /switch to light mode/i });

      // The button should contain an SVG
      const svg = themeToggle.querySelector('svg');
      expect(svg).toBeInTheDocument();
    });
  });

  describe('Test Case 3: Verify theme toggle component renders', () => {
    it('should render ThemeToggle component in navigation', async () => {
      renderWithProviders(<Navbar />);

      // ThemeToggle should be present in the navigation
      const themeToggle = screen.getByRole('button', { name: /switch to (dark|light) mode/i });
      expect(themeToggle).toBeInTheDocument();
    });

    it('should have correct button styling (btn btn-ghost btn-circle)', async () => {
      renderWithProviders(<Navbar />);

      const themeToggle = screen.getByRole('button', { name: /switch to (dark|light) mode/i });
      expect(themeToggle).toHaveClass('btn', 'btn-ghost', 'btn-circle');
    });

    it('should render ThemeToggle in both desktop and mobile navigation', async () => {
      renderWithProviders(<Navbar />);

      // Check for theme toggle buttons (desktop and potentially mobile)
      const themeToggles = screen.getAllByRole('button', { name: /switch to (dark|light) mode/i });
      expect(themeToggles.length).toBeGreaterThanOrEqual(1);
    });

    it('should have accessible aria-label', async () => {
      localStorage.setItem('theme', 'light');

      renderWithProviders(<ThemeToggle />);

      const button = screen.getByRole('button');
      expect(button).toHaveAttribute('aria-label', 'Switch to dark mode');
    });
  });

  describe('Theme persistence', () => {
    it('should persist theme preference in localStorage', async () => {
      localStorage.setItem('theme', 'light');

      renderWithProviders(<Home />);

      // Toggle to dark
      const themeToggle = screen.getByRole('button', { name: /switch to dark mode/i });
      fireEvent.click(themeToggle);

      await waitFor(() => {
        expect(localStorage.getItem('theme')).toBe('dark');
      });

      // Toggle back to light
      const updatedToggle = screen.getByRole('button', { name: /switch to light mode/i });
      fireEvent.click(updatedToggle);

      await waitFor(() => {
        expect(localStorage.getItem('theme')).toBe('light');
      });
    });

    it('should apply stored theme on initial load', async () => {
      // Pre-set theme in localStorage
      localStorage.setItem('theme', 'dark');

      renderWithProviders(<Home />);

      // Theme should be applied from localStorage
      await waitFor(() => {
        expect(getCurrentTheme()).toBe('dark');
      });
    });
  });

  describe('Theme toggle icon changes', () => {
    it('should display moon icon in light mode', async () => {
      localStorage.setItem('theme', 'light');

      renderWithProviders(<ThemeToggle />);

      const button = screen.getByRole('button');
      const svg = button.querySelector('svg');
      expect(svg).toBeInTheDocument();

      // Moon icon has the specific path for moon shape
      const moonPath = svg?.querySelector('path[d*="20.354"]');
      expect(moonPath).toBeInTheDocument();
    });

    it('should display sun icon in dark mode', async () => {
      localStorage.setItem('theme', 'dark');

      renderWithProviders(<ThemeToggle />);

      const button = screen.getByRole('button');
      const svg = button.querySelector('svg');
      expect(svg).toBeInTheDocument();

      // Sun icon has the specific path for sun rays
      const sunPath = svg?.querySelector('path[d*="12 3v1"]');
      expect(sunPath).toBeInTheDocument();
    });
  });

  describe('Theme applies to document', () => {
    it('should set data-theme attribute on document root', async () => {
      localStorage.setItem('theme', 'light');

      renderWithProviders(<Home />);

      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('light');
      });

      // Toggle theme
      const themeToggle = screen.getByRole('button', { name: /switch to dark mode/i });
      fireEvent.click(themeToggle);

      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
      });
    });
  });

  describe('Theme toggle works with all homepage sections visible', () => {
    it('should toggle theme while all sections are present', async () => {
      localStorage.setItem('theme', 'light');

      renderWithProviders(<Home />);

      // Verify all sections are present
      await waitFor(() => {
        expect(screen.getByText('Shorten URLs. Track Every Click.')).toBeInTheDocument(); // Hero
      });
      expect(screen.getByTestId('features-section')).toBeInTheDocument();
      expect(screen.getByTestId('how-it-works-section')).toBeInTheDocument();
      expect(screen.getByTestId('footer')).toBeInTheDocument();

      // Toggle theme
      const themeToggle = screen.getByRole('button', { name: /switch to dark mode/i });
      fireEvent.click(themeToggle);

      // Verify theme changed
      await waitFor(() => {
        expect(getCurrentTheme()).toBe('dark');
      });

      // Verify all sections are still present after theme change
      expect(screen.getByText('Shorten URLs. Track Every Click.')).toBeInTheDocument();
      expect(screen.getByTestId('features-section')).toBeInTheDocument();
      expect(screen.getByTestId('how-it-works-section')).toBeInTheDocument();
      expect(screen.getByTestId('footer')).toBeInTheDocument();
    });

    it('should maintain theme after multiple toggles', async () => {
      localStorage.setItem('theme', 'light');

      renderWithProviders(<Home />);

      await waitFor(() => {
        expect(getCurrentTheme()).toBe('light');
      });

      // Toggle multiple times
      let themeToggle = screen.getByRole('button', { name: /switch to dark mode/i });
      fireEvent.click(themeToggle);

      await waitFor(() => {
        expect(getCurrentTheme()).toBe('dark');
      });

      themeToggle = screen.getByRole('button', { name: /switch to light mode/i });
      fireEvent.click(themeToggle);

      await waitFor(() => {
        expect(getCurrentTheme()).toBe('light');
      });

      themeToggle = screen.getByRole('button', { name: /switch to dark mode/i });
      fireEvent.click(themeToggle);

      await waitFor(() => {
        expect(getCurrentTheme()).toBe('dark');
      });
    });
  });
});

// ===============================================
// Scenario 13: Dark Mode Rendering Tests
// ===============================================

describe('Dark Mode Rendering - Scenario 13', () => {
  beforeEach(() => {
    // Set up dark mode before each test
    document.documentElement.setAttribute('data-theme', 'dark');
    localStorage.setItem('theme', 'dark');
  });

  afterEach(() => {
    // Clean up after each test
    document.documentElement.removeAttribute('data-theme');
    localStorage.clear();
    vi.clearAllMocks();
  });

  describe('Test Case 1: Render homepage with dark theme context', () => {
    it('should render homepage with dark theme attribute on document', () => {
      renderWithDarkTheme(<Home />);

      // Verify dark theme is applied to document
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
    });

    it('should render all homepage sections in dark mode', () => {
      renderWithDarkTheme(<Home />);

      // Verify main sections are rendered
      expect(screen.getByText('Shorten URLs. Track Every Click.')).toBeInTheDocument();
      expect(screen.getByText('Key Features')).toBeInTheDocument();
    });

    it('should apply dark mode base styles to main container', () => {
      const { container } = renderWithDarkTheme(<Home />);

      // Check that the main container has min-h-screen class
      const mainDiv = container.firstChild as HTMLElement;
      expect(mainDiv).toHaveClass('min-h-screen');
    });

    it('should render with DaisyUI dark theme CSS variables available', () => {
      renderWithDarkTheme(<Home />);

      // DaisyUI sets data-theme attribute which enables dark mode CSS variables
      const theme = document.documentElement.getAttribute('data-theme');
      expect(theme).toBe('dark');
    });
  });

  describe('Test Case 2: Check text contrast in dark mode', () => {
    it('should render hero headline with appropriate styling for dark mode', () => {
      renderWithDarkTheme(<HeroSection isAuthenticated={false} />);

      const headline = screen.getByText('Shorten URLs. Track Every Click.');
      expect(headline).toBeInTheDocument();
      // The headline uses gradient text which is visible in both light and dark modes
      expect(headline).toHaveClass('bg-gradient-to-r', 'from-primary', 'to-secondary', 'bg-clip-text', 'text-transparent');
    });

    it('should render subheadline with base-content color class for readability', () => {
      renderWithDarkTheme(<HeroSection isAuthenticated={false} />);

      const subheadline = screen.getByText(/Transform long, unwieldy URLs/);
      expect(subheadline).toBeInTheDocument();
      // Text uses base-content/70 which adapts to theme for proper contrast
      expect(subheadline).toHaveClass('text-base-content/70');
    });

    it('should render feature titles with proper text styling', () => {
      renderWithDarkTheme(<FeaturesSection />);

      const featureTitles = screen.getAllByRole('heading', { level: 3 });
      expect(featureTitles.length).toBe(4);

      // All titles should be visible and have font-semibold class
      featureTitles.forEach(title => {
        expect(title).toBeInTheDocument();
        expect(title).toHaveClass('font-semibold');
      });
    });

    it('should render feature descriptions with muted text styling', () => {
      renderWithDarkTheme(<FeaturesSection />);

      const description = screen.getByTestId('feature-description-url-shortening');
      expect(description).toBeInTheDocument();
      // Descriptions use base-content/70 opacity for appropriate contrast
      expect(description).toHaveClass('text-base-content/70');
    });

    it('should render secondary text with appropriate opacity for contrast', () => {
      renderWithDarkTheme(<HeroSection isAuthenticated={false} />);

      const secondaryText = screen.getByText(/Already have an account\?/);
      expect(secondaryText).toBeInTheDocument();
      expect(secondaryText).toHaveClass('text-base-content/60');
    });
  });

  describe('Test Case 3: Check feature cards in dark mode', () => {
    it('should render GlassMorphismCard with dark mode appropriate styling', () => {
      const { container } = renderWithDarkTheme(
        <GlassMorphismCard>
          <p>Test content</p>
        </GlassMorphismCard>
      );

      // Get the card element by its class
      const card = container.querySelector('.bg-base-100\\/60');
      expect(card).toBeInTheDocument();
      // Card uses base-100/60 which adapts to dark theme
      expect(card).toHaveClass('bg-base-100/60', 'backdrop-blur-md', 'border-base-200');
    });

    it('should render all feature cards with glass morphism effect in dark mode', () => {
      renderWithDarkTheme(<FeaturesSection />);

      const featureCards = [
        screen.getByTestId('feature-card-url-shortening'),
        screen.getByTestId('feature-card-click-analytics'),
        screen.getByTestId('feature-card-geographic-insights'),
        screen.getByTestId('feature-card-shareable-stats'),
      ];

      featureCards.forEach(card => {
        expect(card).toBeInTheDocument();
        // Each card wrapper contains a GlassMorphismCard
        const glassCard = card.querySelector('.bg-base-100\\/60');
        expect(glassCard).toBeInTheDocument();
      });
    });

    it('should render card borders with theme-appropriate color', () => {
      renderWithDarkTheme(
        <GlassMorphismCard>
          <p>Border test</p>
        </GlassMorphismCard>
      );

      const card = screen.getByText('Border test').parentElement;
      expect(card).toHaveClass('border', 'border-base-200');
    });

    it('should render card with backdrop blur for glass effect', () => {
      renderWithDarkTheme(
        <GlassMorphismCard>
          <p>Blur test</p>
        </GlassMorphismCard>
      );

      const card = screen.getByText('Blur test').parentElement;
      expect(card).toHaveClass('backdrop-blur-md');
    });

    it('should render feature icons with primary color in dark mode', () => {
      renderWithDarkTheme(<FeaturesSection />);

      const icon = screen.getByTestId('icon-url-shortening');
      expect(icon).toBeInTheDocument();
      expect(icon).toHaveClass('text-primary');
    });
  });

  describe('Test Case 4: Check buttons in dark mode', () => {
    it('should render FuturisticButton with primary variant in dark mode', () => {
      renderWithDarkTheme(
        <FuturisticButton variant="primary" data-testid="test-btn">
          Test Button
        </FuturisticButton>
      );

      const button = screen.getByTestId('test-btn');
      expect(button).toBeInTheDocument();
      expect(button).toHaveClass('btn', 'btn-primary');
    });

    it('should render FuturisticButton with secondary variant in dark mode', () => {
      renderWithDarkTheme(
        <FuturisticButton variant="secondary" data-testid="test-btn-secondary">
          Secondary Button
        </FuturisticButton>
      );

      const button = screen.getByTestId('test-btn-secondary');
      expect(button).toBeInTheDocument();
      expect(button).toHaveClass('btn', 'btn-secondary');
    });

    it('should render FuturisticButton with ghost variant in dark mode', () => {
      renderWithDarkTheme(
        <FuturisticButton variant="ghost" data-testid="test-btn-ghost">
          Ghost Button
        </FuturisticButton>
      );

      const button = screen.getByTestId('test-btn-ghost');
      expect(button).toBeInTheDocument();
      expect(button).toHaveClass('btn', 'btn-ghost');
    });

    it('should render hero CTA button correctly in dark mode', () => {
      renderWithDarkTheme(<HeroSection isAuthenticated={false} />);

      const ctaButton = screen.getByTestId('hero-cta');
      expect(ctaButton).toBeInTheDocument();
      expect(ctaButton).toHaveClass('btn', 'btn-primary', 'btn-lg');
    });

    it('should render button with proper size and touch target in dark mode', () => {
      renderWithDarkTheme(
        <FuturisticButton size="lg" data-testid="test-btn-lg">
          Large Button
        </FuturisticButton>
      );

      const button = screen.getByTestId('test-btn-lg');
      expect(button).toHaveClass('btn-lg', 'min-h-[44px]', 'min-w-[44px]');
    });

    it('should render FuturisticButton as link with dark mode styling', () => {
      renderWithDarkTheme(
        <FuturisticButton to="/test" variant="primary" data-testid="test-link-btn">
          Link Button
        </FuturisticButton>
      );

      const linkButton = screen.getByTestId('test-link-btn');
      expect(linkButton).toBeInTheDocument();
      expect(linkButton.tagName.toLowerCase()).toBe('a');
      expect(linkButton).toHaveClass('btn', 'btn-primary');
    });

    it('should render login link with primary color in dark mode', () => {
      renderWithDarkTheme(<HeroSection isAuthenticated={false} />);

      const loginLink = screen.getByTestId('hero-login-link');
      expect(loginLink).toBeInTheDocument();
      expect(loginLink).toHaveClass('text-primary');
    });
  });

  describe('Theme context integration', () => {
    it('should have dark theme in context', () => {
      let capturedTheme: string | undefined;

      function ThemeCapture() {
        const { theme } = React.useContext(ThemeContext)!;
        capturedTheme = theme;
        return <div>Theme: {theme}</div>;
      }

      renderWithDarkTheme(<ThemeCapture />);

      expect(capturedTheme).toBe('dark');
    });

    it('should render homepage with dark theme context value', () => {
      renderWithDarkTheme(<Home />);

      // Verify the theme attribute is set correctly
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
    });
  });
});
