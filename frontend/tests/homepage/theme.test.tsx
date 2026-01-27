/**
 * Theme Switching Tests
 * Owner: Scenario 9
 *
 * Tests for theme toggle functionality:
 * - Light to dark theme switching
 * - Theme application to all components
 * - Theme persistence
 * - Support for all theme variants (cyberpunk, synthwave, etc.)
 */
import { describe, it, expect, beforeEach, vi } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { ThemeProvider } from '../../src/contexts/ThemeContext';
import { AuthProvider } from '../../src/contexts/AuthContext';
import ThemeToggle from '../../src/components/ThemeToggle';
import GlassMorphismCard from '../../src/components/GlassMorphismCard';
import FuturisticButton from '../../src/components/FuturisticButton';
import Home from '../../src/pages/Home';

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: false,
    },
  },
});

function renderWithProviders(ui: React.ReactElement) {
  return render(
    <MemoryRouter>
      <QueryClientProvider client={queryClient}>
        <ThemeProvider>
          <AuthProvider>
            {ui}
          </AuthProvider>
        </ThemeProvider>
      </QueryClientProvider>
    </MemoryRouter>
  );
}

function renderWithThemeOnly(ui: React.ReactElement) {
  return render(
    <ThemeProvider>
      {ui}
    </ThemeProvider>
  );
}

describe('Theme Switching Functionality', () => {
  beforeEach(() => {
    localStorage.clear();
    document.documentElement.removeAttribute('data-theme');
  });

  describe('Test Case 1: ThemeToggle component presence and functionality', () => {
    it('renders ThemeToggle component in HomePage', () => {
      renderWithProviders(<Home />);

      const themeSelect = screen.getByRole('combobox', { name: /select theme/i });
      expect(themeSelect).toBeInTheDocument();
    });

    it('ThemeToggle displays all available themes', () => {
      renderWithThemeOnly(<ThemeToggle />);

      const themeSelect = screen.getByRole('combobox', { name: /select theme/i });
      expect(themeSelect).toBeInTheDocument();

      const options = screen.getAllByRole('option');
      const themeNames = options.map(opt => opt.textContent?.toLowerCase());

      expect(themeNames).toContain('light');
      expect(themeNames).toContain('dark');
      expect(themeNames).toContain('cyberpunk');
      expect(themeNames).toContain('synthwave');
      expect(themeNames).toContain('retro');
      expect(themeNames).toContain('valentine');
      expect(themeNames).toContain('night');
    });

    it('ThemeToggle is functional and responds to user interaction', async () => {
      const user = userEvent.setup();
      renderWithThemeOnly(<ThemeToggle />);

      const themeSelect = screen.getByRole('combobox', { name: /select theme/i });

      await user.selectOptions(themeSelect, 'cyberpunk');
      expect(themeSelect).toHaveValue('cyberpunk');
    });
  });

  describe('Test Case 2: Toggle theme from light to dark', () => {
    it('changes data-theme attribute on document when theme is toggled to dark', async () => {
      const user = userEvent.setup();
      localStorage.setItem('theme', 'light');

      renderWithThemeOnly(<ThemeToggle />);

      const themeSelect = screen.getByRole('combobox', { name: /select theme/i });

      await user.selectOptions(themeSelect, 'dark');

      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
      });
    });

    it('changes data-theme attribute when switching from dark to light', async () => {
      const user = userEvent.setup();
      localStorage.setItem('theme', 'dark');

      renderWithThemeOnly(<ThemeToggle />);

      const themeSelect = screen.getByRole('combobox', { name: /select theme/i });

      await user.selectOptions(themeSelect, 'light');

      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('light');
      });
    });

    it('correctly updates theme on the homepage', async () => {
      const user = userEvent.setup();
      localStorage.setItem('theme', 'light');

      renderWithProviders(<Home />);

      const themeSelect = screen.getByRole('combobox', { name: /select theme/i });

      await user.selectOptions(themeSelect, 'dark');

      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
      });
    });
  });

  describe('Test Case 3: GlassMorphismCard in dark theme', () => {
    it('GlassMorphismCard renders with theme-aware base classes', () => {
      renderWithThemeOnly(
        <GlassMorphismCard>
          <p>Card content</p>
        </GlassMorphismCard>
      );

      const card = screen.getByText('Card content').closest('div');
      expect(card).toBeInTheDocument();
      expect(card).toHaveClass('bg-base-100/70');
      expect(card).toHaveClass('border-base-content/10');
    });

    it('GlassMorphismCard maintains styling when theme changes to dark', async () => {
      const user = userEvent.setup();
      localStorage.setItem('theme', 'light');

      render(
        <ThemeProvider>
          <ThemeToggle />
          <GlassMorphismCard>
            <p>Card content</p>
          </GlassMorphismCard>
        </ThemeProvider>
      );

      const themeSelect = screen.getByRole('combobox', { name: /select theme/i });
      await user.selectOptions(themeSelect, 'dark');

      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
      });

      const card = screen.getByText('Card content').closest('div');
      expect(card).toHaveClass('bg-base-100/70');
    });

    it('GlassMorphismCard uses DaisyUI theme-aware classes for dark theme compatibility', () => {
      localStorage.setItem('theme', 'dark');

      renderWithThemeOnly(
        <GlassMorphismCard className="p-4">
          <span>Test content</span>
        </GlassMorphismCard>
      );

      const card = screen.getByText('Test content').closest('div');
      expect(card).toHaveClass('backdrop-blur-lg');
      expect(card).toHaveClass('rounded-2xl');
      expect(card).toHaveClass('shadow-xl');
    });
  });

  describe('Test Case 4: FuturisticButton in cyberpunk theme', () => {
    it('FuturisticButton renders with theme-aware classes', () => {
      renderWithThemeOnly(
        <FuturisticButton variant="primary">Click me</FuturisticButton>
      );

      const button = screen.getByRole('button', { name: /click me/i });
      expect(button).toBeInTheDocument();
      expect(button).toHaveClass('btn-primary');
    });

    it('FuturisticButton styling reflects theme changes to cyberpunk', async () => {
      const user = userEvent.setup();
      localStorage.setItem('theme', 'light');

      render(
        <ThemeProvider>
          <ThemeToggle />
          <FuturisticButton variant="primary">Action</FuturisticButton>
        </ThemeProvider>
      );

      const themeSelect = screen.getByRole('combobox', { name: /select theme/i });
      await user.selectOptions(themeSelect, 'cyberpunk');

      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk');
      });

      const button = screen.getByRole('button', { name: /action/i });
      expect(button).toHaveClass('btn');
      expect(button).toHaveClass('btn-primary');
    });

    it('FuturisticButton secondary variant works with cyberpunk theme', async () => {
      const user = userEvent.setup();

      render(
        <ThemeProvider>
          <ThemeToggle />
          <FuturisticButton variant="secondary">Secondary</FuturisticButton>
        </ThemeProvider>
      );

      const themeSelect = screen.getByRole('combobox', { name: /select theme/i });
      await user.selectOptions(themeSelect, 'cyberpunk');

      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk');
      });

      const button = screen.getByRole('button', { name: /secondary/i });
      expect(button).toHaveClass('btn-secondary');
    });

    it('FuturisticButton outline variant works with cyberpunk theme', async () => {
      const user = userEvent.setup();

      render(
        <ThemeProvider>
          <ThemeToggle />
          <FuturisticButton variant="outline">Outline</FuturisticButton>
        </ThemeProvider>
      );

      const themeSelect = screen.getByRole('combobox', { name: /select theme/i });
      await user.selectOptions(themeSelect, 'cyberpunk');

      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk');
      });

      const button = screen.getByRole('button', { name: /outline/i });
      expect(button).toHaveClass('btn-outline');
    });
  });

  describe('Test Case 5: Theme persistence after toggle', () => {
    it('saves theme preference to localStorage when changed', async () => {
      const user = userEvent.setup();

      renderWithThemeOnly(<ThemeToggle />);

      const themeSelect = screen.getByRole('combobox', { name: /select theme/i });
      await user.selectOptions(themeSelect, 'synthwave');

      await waitFor(() => {
        expect(localStorage.getItem('theme')).toBe('synthwave');
      });
    });

    it('loads theme from localStorage on initial render', () => {
      localStorage.setItem('theme', 'retro');

      renderWithThemeOnly(<ThemeToggle />);

      const themeSelect = screen.getByRole('combobox', { name: /select theme/i });
      expect(themeSelect).toHaveValue('retro');
      expect(document.documentElement.getAttribute('data-theme')).toBe('retro');
    });

    it('persists theme across multiple selections', async () => {
      const user = userEvent.setup();

      renderWithThemeOnly(<ThemeToggle />);

      const themeSelect = screen.getByRole('combobox', { name: /select theme/i });

      await user.selectOptions(themeSelect, 'valentine');
      await waitFor(() => {
        expect(localStorage.getItem('theme')).toBe('valentine');
      });

      await user.selectOptions(themeSelect, 'night');
      await waitFor(() => {
        expect(localStorage.getItem('theme')).toBe('night');
      });

      await user.selectOptions(themeSelect, 'light');
      await waitFor(() => {
        expect(localStorage.getItem('theme')).toBe('light');
      });
    });

    it('defaults to dark theme when no localStorage value exists', () => {
      localStorage.clear();

      renderWithThemeOnly(<ThemeToggle />);

      const themeSelect = screen.getByRole('combobox', { name: /select theme/i });
      expect(themeSelect).toHaveValue('dark');
    });
  });

  describe('Additional theme coverage', () => {
    it('supports all theme variants: light, dark, cyberpunk, synthwave, retro, valentine, night', async () => {
      const user = userEvent.setup();
      const themes = ['light', 'dark', 'cyberpunk', 'synthwave', 'retro', 'valentine', 'night'];

      renderWithThemeOnly(<ThemeToggle />);

      const themeSelect = screen.getByRole('combobox', { name: /select theme/i });

      for (const theme of themes) {
        await user.selectOptions(themeSelect, theme);
        await waitFor(() => {
          expect(document.documentElement.getAttribute('data-theme')).toBe(theme);
          expect(localStorage.getItem('theme')).toBe(theme);
        });
      }
    });

    it('theme changes are immediately visible on homepage components', async () => {
      const user = userEvent.setup();
      localStorage.setItem('theme', 'light');

      renderWithProviders(<Home />);

      const themeSelect = screen.getByRole('combobox', { name: /select theme/i });

      await user.selectOptions(themeSelect, 'synthwave');

      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('synthwave');
      });

      const mainContainer = document.querySelector('.min-h-screen');
      expect(mainContainer).toHaveClass('bg-base-100');
    });
  });
});
