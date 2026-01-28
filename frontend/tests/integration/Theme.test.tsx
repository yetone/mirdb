/**
 * Theme Integration Tests
 * Owner: Scenario 7 - Theme Support
 *
 * Test coverage:
 * - Render homepage with light theme
 * - Render homepage with dark theme
 * - Render homepage with cyberpunk theme
 * - Render homepage with synthwave theme
 * - Toggle theme from light to dark
 * - Feature cards use theme-appropriate colors
 * - CTAs use DaisyUI theme colors
 * - Gradient text uses theme primary/secondary colors
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import { render, screen, act, within } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { Home } from '../../src/pages/Home';
import { ThemeProvider, useTheme, Theme, AVAILABLE_THEMES } from '../../src/contexts';

// Helper to render Home with ThemeProvider and specific theme
const renderHomeWithTheme = (initialTheme: Theme) => {
  return render(
    <ThemeProvider initialTheme={initialTheme}>
      <MemoryRouter>
        <Home />
      </MemoryRouter>
    </ThemeProvider>
  );
};

// Helper component to access theme context for testing toggle
function ThemeToggler({ targetTheme }: { targetTheme: Theme }) {
  const { setTheme } = useTheme();
  return (
    <button data-testid="theme-toggler" onClick={() => setTheme(targetTheme)}>
      Toggle
    </button>
  );
}

const renderHomeWithToggler = (initialTheme: Theme, targetTheme: Theme) => {
  return render(
    <ThemeProvider initialTheme={initialTheme}>
      <MemoryRouter>
        <Home />
        <ThemeToggler targetTheme={targetTheme} />
      </MemoryRouter>
    </ThemeProvider>
  );
};

describe('Theme Support - Integration Tests', () => {
  beforeEach(() => {
    // Clear localStorage before each test
    localStorage.clear();
    // Reset document theme
    document.documentElement.removeAttribute('data-theme');
  });

  // Test Case 1: Render homepage with ThemeContext value='light'
  describe('TC1: Light Theme Rendering', () => {
    it('applies light theme styles when ThemeContext value is light', () => {
      renderHomeWithTheme('light');

      // Verify data-theme attribute is set on document
      expect(document.documentElement.getAttribute('data-theme')).toBe('light');

      // Verify homepage renders
      expect(screen.getByRole('heading', { level: 1 })).toBeInTheDocument();
      expect(screen.getByRole('main')).toBeInTheDocument();
    });

    it('renders all sections with light theme', () => {
      renderHomeWithTheme('light');

      // Hero section
      expect(screen.getByText(/shorten urls/i)).toBeInTheDocument();

      // Features section
      expect(screen.getByRole('heading', { name: /powerful features/i })).toBeInTheDocument();

      // How It Works section
      expect(screen.getByRole('heading', { name: /how it works/i })).toBeInTheDocument();

      // Footer
      expect(screen.getByRole('contentinfo')).toBeInTheDocument();
    });
  });

  // Test Case 2: Render homepage with ThemeContext value='dark'
  describe('TC2: Dark Theme Rendering', () => {
    it('applies dark theme styles when ThemeContext value is dark', () => {
      renderHomeWithTheme('dark');

      // Verify data-theme attribute is set on document
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark');

      // Verify homepage renders
      expect(screen.getByRole('heading', { level: 1 })).toBeInTheDocument();
      expect(screen.getByRole('main')).toBeInTheDocument();
    });

    it('renders all sections with dark theme', () => {
      renderHomeWithTheme('dark');

      // Hero section
      expect(screen.getByText(/shorten urls/i)).toBeInTheDocument();

      // Features section
      expect(screen.getByRole('heading', { name: /powerful features/i })).toBeInTheDocument();

      // How It Works section
      expect(screen.getByRole('heading', { name: /how it works/i })).toBeInTheDocument();

      // Footer
      expect(screen.getByRole('contentinfo')).toBeInTheDocument();
    });
  });

  // Test Case 3: Render homepage with ThemeContext value='cyberpunk'
  describe('TC3: Cyberpunk Theme Rendering', () => {
    it('applies cyberpunk theme colors consistently', () => {
      renderHomeWithTheme('cyberpunk');

      // Verify data-theme attribute is set on document
      expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk');

      // Verify homepage renders correctly
      expect(screen.getByRole('heading', { level: 1 })).toBeInTheDocument();

      // All sections should render
      expect(screen.getByRole('heading', { name: /powerful features/i })).toBeInTheDocument();
      expect(screen.getByRole('heading', { name: /how it works/i })).toBeInTheDocument();
      expect(screen.getByRole('contentinfo')).toBeInTheDocument();
    });
  });

  // Test Case 4: Render homepage with ThemeContext value='synthwave'
  describe('TC4: Synthwave Theme Rendering', () => {
    it('applies synthwave theme colors consistently', () => {
      renderHomeWithTheme('synthwave');

      // Verify data-theme attribute is set on document
      expect(document.documentElement.getAttribute('data-theme')).toBe('synthwave');

      // Verify homepage renders correctly
      expect(screen.getByRole('heading', { level: 1 })).toBeInTheDocument();

      // All sections should render
      expect(screen.getByRole('heading', { name: /powerful features/i })).toBeInTheDocument();
      expect(screen.getByRole('heading', { name: /how it works/i })).toBeInTheDocument();
      expect(screen.getByRole('contentinfo')).toBeInTheDocument();
    });
  });

  // Test Case 5: Toggle theme from light to dark
  describe('TC5: Theme Toggle', () => {
    it('updates all homepage sections when theme is toggled from light to dark', () => {
      renderHomeWithToggler('light', 'dark');

      // Initially light theme
      expect(document.documentElement.getAttribute('data-theme')).toBe('light');

      // Toggle to dark
      const toggler = screen.getByTestId('theme-toggler');
      act(() => {
        toggler.click();
      });

      // After toggle, should be dark theme
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark');

      // All sections should still render correctly
      expect(screen.getByRole('heading', { level: 1 })).toBeInTheDocument();
      expect(screen.getByRole('heading', { name: /powerful features/i })).toBeInTheDocument();
      expect(screen.getByRole('heading', { name: /how it works/i })).toBeInTheDocument();
      expect(screen.getByRole('contentinfo')).toBeInTheDocument();
    });

    it('persists theme preference to localStorage', () => {
      renderHomeWithToggler('light', 'dark');

      // Toggle to dark
      const toggler = screen.getByTestId('theme-toggler');
      act(() => {
        toggler.click();
      });

      // Should be stored in localStorage
      expect(localStorage.getItem('url-shortener-theme')).toBe('dark');
    });
  });

  // Test Case 6: Check feature cards theme integration
  describe('TC6: Feature Cards Theme Integration', () => {
    it('feature cards use theme-appropriate glass effect styling', () => {
      renderHomeWithTheme('dark');

      const featureCards = screen.getAllByTestId('feature-card');
      expect(featureCards).toHaveLength(3);

      // Check that cards have the glassmorphism styling classes
      featureCards.forEach((card) => {
        expect(card).toHaveClass('backdrop-blur-md');
        expect(card).toHaveClass('bg-base-100/30');
        expect(card).toHaveClass('border');
      });
    });

    it('feature card icons use theme primary color', () => {
      renderHomeWithTheme('dark');

      const featureIcons = screen.getAllByTestId('feature-icon');
      expect(featureIcons).toHaveLength(3);

      // Icons should have text-primary class for theme color
      featureIcons.forEach((icon) => {
        expect(icon).toHaveClass('text-primary');
      });
    });

    it('feature cards render consistently across all themes', () => {
      AVAILABLE_THEMES.forEach((theme) => {
        const { unmount } = renderHomeWithTheme(theme);

        // Each theme should render feature cards
        const featureCards = screen.getAllByTestId('feature-card');
        expect(featureCards).toHaveLength(3);

        // Cards should have proper structure
        expect(screen.getByText(/url shortening/i)).toBeInTheDocument();
        expect(screen.getByText(/analytics dashboard/i)).toBeInTheDocument();
        expect(screen.getByText(/link management/i)).toBeInTheDocument();

        unmount();
        document.documentElement.removeAttribute('data-theme');
      });
    });
  });

  // Test Case 7: Check CTAs theme integration
  describe('TC7: CTAs Theme Integration', () => {
    it('primary CTA button uses DaisyUI btn-primary class', () => {
      renderHomeWithTheme('dark');

      // Hero section uses role="button" on Link elements
      const getStartedBtn = screen.getByRole('button', { name: /get started/i });
      expect(getStartedBtn).toHaveClass('btn');
      expect(getStartedBtn).toHaveClass('btn-primary');
    });

    it('secondary CTA button uses DaisyUI btn-outline class', () => {
      renderHomeWithTheme('dark');

      // Hero section uses role="button" on Link elements
      const loginBtn = screen.getByRole('button', { name: /log in/i });
      expect(loginBtn).toHaveClass('btn');
      expect(loginBtn).toHaveClass('btn-outline');
    });

    it('CTA buttons render correctly across all themes', () => {
      AVAILABLE_THEMES.forEach((theme) => {
        const { unmount } = renderHomeWithTheme(theme);

        // Hero section uses role="button" on Link elements
        const getStartedBtn = screen.getByRole('button', { name: /get started/i });
        const loginBtn = screen.getByRole('button', { name: /log in/i });

        expect(getStartedBtn).toBeInTheDocument();
        expect(loginBtn).toBeInTheDocument();
        expect(getStartedBtn).toHaveClass('btn-primary');
        expect(loginBtn).toHaveClass('btn-outline');

        unmount();
        document.documentElement.removeAttribute('data-theme');
      });
    });
  });

  // Test Case 8: Check gradient text theme integration
  describe('TC8: Gradient Text Theme Integration', () => {
    it('headline uses gradient with theme primary/secondary colors', () => {
      renderHomeWithTheme('dark');

      const headline = screen.getByRole('heading', { level: 1 });

      // Check for gradient classes
      expect(headline).toHaveClass('bg-gradient-to-r');
      expect(headline).toHaveClass('from-primary');
      expect(headline).toHaveClass('to-secondary');
      expect(headline).toHaveClass('bg-clip-text');
      expect(headline).toHaveClass('text-transparent');
    });

    it('headline gradient renders correctly across all themes', () => {
      AVAILABLE_THEMES.forEach((theme) => {
        const { unmount } = renderHomeWithTheme(theme);

        const headline = screen.getByRole('heading', { level: 1 });

        // Gradient should use theme-aware colors
        expect(headline).toHaveClass('from-primary');
        expect(headline).toHaveClass('to-secondary');

        unmount();
        document.documentElement.removeAttribute('data-theme');
      });
    });
  });
});

describe('ThemeProvider - Unit Tests', () => {
  beforeEach(() => {
    localStorage.clear();
    document.documentElement.removeAttribute('data-theme');
  });

  it('provides default theme when no initialTheme specified', () => {
    function ThemeConsumer() {
      const { theme } = useTheme();
      return <div data-testid="current-theme">{theme}</div>;
    }

    render(
      <ThemeProvider>
        <ThemeConsumer />
      </ThemeProvider>
    );

    // Default theme is 'dark'
    expect(screen.getByTestId('current-theme')).toHaveTextContent('dark');
  });

  it('uses initialTheme when provided', () => {
    function ThemeConsumer() {
      const { theme } = useTheme();
      return <div data-testid="current-theme">{theme}</div>;
    }

    render(
      <ThemeProvider initialTheme="cyberpunk">
        <ThemeConsumer />
      </ThemeProvider>
    );

    expect(screen.getByTestId('current-theme')).toHaveTextContent('cyberpunk');
  });

  it('throws error when useTheme is used outside ThemeProvider', () => {
    function BadComponent() {
      useTheme();
      return <div>Bad</div>;
    }

    // Suppress console.error for this test
    const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {});

    expect(() => render(<BadComponent />)).toThrow(
      'useTheme must be used within a ThemeProvider'
    );

    consoleSpy.mockRestore();
  });

  it('reads theme from localStorage if available', () => {
    localStorage.setItem('url-shortener-theme', 'synthwave');

    function ThemeConsumer() {
      const { theme } = useTheme();
      return <div data-testid="current-theme">{theme}</div>;
    }

    render(
      <ThemeProvider>
        <ThemeConsumer />
      </ThemeProvider>
    );

    expect(screen.getByTestId('current-theme')).toHaveTextContent('synthwave');
  });

  it('ignores invalid theme from localStorage and uses default', () => {
    localStorage.setItem('url-shortener-theme', 'invalid-theme');

    function ThemeConsumer() {
      const { theme } = useTheme();
      return <div data-testid="current-theme">{theme}</div>;
    }

    render(
      <ThemeProvider>
        <ThemeConsumer />
      </ThemeProvider>
    );

    // Should fall back to default 'dark'
    expect(screen.getByTestId('current-theme')).toHaveTextContent('dark');
  });
});

describe('Theme Consistency Across Sections', () => {
  beforeEach(() => {
    localStorage.clear();
    document.documentElement.removeAttribute('data-theme');
  });

  it('all sections use consistent DaisyUI theme classes', () => {
    renderHomeWithTheme('dark');

    // Hero uses base-content for text
    const heroDescription = screen.getByText(/transform long, unwieldy links/i);
    expect(heroDescription).toHaveClass('text-base-content/80');

    // Features section uses base-100 and base-content
    const featureCards = screen.getAllByTestId('feature-card');
    featureCards.forEach((card) => {
      expect(card.className).toMatch(/bg-base-100/);
      expect(card.className).toMatch(/border-base-content/);
    });

    // How It Works uses bg-base-200 for background
    const howItWorksSection = document.querySelector('[aria-labelledby="how-it-works-heading"]');
    expect(howItWorksSection).toHaveClass('bg-base-200/50');

    // Footer uses base-content for border
    const footer = screen.getByRole('contentinfo');
    expect(footer.className).toMatch(/border-base-content/);
  });

  it('step indicators use theme primary color', () => {
    renderHomeWithTheme('dark');

    const steps = screen.getAllByTestId('how-it-works-step');
    steps.forEach((step) => {
      // Each step has a numbered indicator with bg-primary
      const numberIndicator = step.querySelector('.rounded-full');
      expect(numberIndicator).toHaveClass('bg-primary');
      expect(numberIndicator).toHaveClass('text-primary-content');
    });
  });
});
