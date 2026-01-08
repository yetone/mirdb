import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import ThemeToggle from './ThemeToggle';
import { ThemeProvider } from '../contexts/ThemeContext';
import { useThemeStore, AVAILABLE_THEMES, Theme } from '../store/themeStore';

// Helper to render ThemeToggle with ThemeProvider
const renderThemeToggle = (initialTheme: Theme = 'light', variant: 'dropdown' | 'buttons' = 'dropdown') => {
  useThemeStore.setState({ theme: initialTheme });

  return render(
    <ThemeProvider>
      <ThemeToggle variant={variant} />
    </ThemeProvider>
  );
};

describe('ThemeToggle Component', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    localStorage.clear();
  });

  afterEach(() => {
    localStorage.clear();
  });

  describe('Dropdown Variant', () => {
    it('renders the theme toggle component', () => {
      renderThemeToggle('light', 'dropdown');

      const toggle = screen.getByTestId('theme-toggle');
      expect(toggle).toBeInTheDocument();
    });

    it('shows toggle button with aria-label', () => {
      renderThemeToggle('light', 'dropdown');

      const button = screen.getByLabelText(/change theme/i);
      expect(button).toBeInTheDocument();
    });

    it('opens dropdown menu on click', async () => {
      const user = userEvent.setup();
      renderThemeToggle('light', 'dropdown');

      const button = screen.getByLabelText(/change theme/i);
      await user.click(button);

      // Should show all theme options
      for (const theme of AVAILABLE_THEMES) {
        expect(screen.getByTestId(`theme-option-${theme}`)).toBeInTheDocument();
      }
    });

    it('changes theme when option is clicked', async () => {
      const user = userEvent.setup();
      renderThemeToggle('light', 'dropdown');

      const button = screen.getByLabelText(/change theme/i);
      await user.click(button);

      const darkOption = screen.getByTestId('theme-option-dark');
      await user.click(darkOption);

      expect(useThemeStore.getState().theme).toBe('dark');
    });

    it('marks current theme as active', async () => {
      const user = userEvent.setup();
      renderThemeToggle('cyberpunk', 'dropdown');

      const button = screen.getByLabelText(/change theme/i);
      await user.click(button);

      const cyberpunkOption = screen.getByTestId('theme-option-cyberpunk');
      expect(cyberpunkOption).toHaveAttribute('aria-checked', 'true');
      expect(within(cyberpunkOption).getByText('Active')).toBeInTheDocument();
    });

    it('has menu role for accessibility', async () => {
      const user = userEvent.setup();
      renderThemeToggle('light', 'dropdown');

      const button = screen.getByLabelText(/change theme/i);
      await user.click(button);

      const menu = screen.getByRole('menu');
      expect(menu).toBeInTheDocument();
    });

    it('persists theme choice to localStorage', async () => {
      const user = userEvent.setup();
      renderThemeToggle('light', 'dropdown');

      const button = screen.getByLabelText(/change theme/i);
      await user.click(button);

      const darkOption = screen.getByTestId('theme-option-dark');
      await user.click(darkOption);

      expect(localStorage.getItem('theme')).toBe('dark');
    });
  });

  describe('Buttons Variant', () => {
    it('renders buttons for all themes', () => {
      renderThemeToggle('light', 'buttons');

      for (const theme of AVAILABLE_THEMES) {
        expect(screen.getByTestId(`theme-button-${theme}`)).toBeInTheDocument();
      }
    });

    it('marks current theme button as primary', () => {
      renderThemeToggle('dark', 'buttons');

      const darkButton = screen.getByTestId('theme-button-dark');
      expect(darkButton).toHaveClass('btn-primary');

      const lightButton = screen.getByTestId('theme-button-light');
      expect(lightButton).toHaveClass('btn-ghost');
    });

    it('changes theme when button is clicked', async () => {
      const user = userEvent.setup();
      renderThemeToggle('light', 'buttons');

      const synthwaveButton = screen.getByTestId('theme-button-synthwave');
      await user.click(synthwaveButton);

      expect(useThemeStore.getState().theme).toBe('synthwave');
    });

    it('buttons have proper aria-pressed attribute', () => {
      renderThemeToggle('cyberpunk', 'buttons');

      const cyberpunkButton = screen.getByTestId('theme-button-cyberpunk');
      expect(cyberpunkButton).toHaveAttribute('aria-pressed', 'true');

      const lightButton = screen.getByTestId('theme-button-light');
      expect(lightButton).toHaveAttribute('aria-pressed', 'false');
    });

    it('buttons have accessible labels', () => {
      renderThemeToggle('light', 'buttons');

      for (const theme of AVAILABLE_THEMES) {
        const button = screen.getByLabelText(new RegExp(`switch to ${theme} theme`, 'i'));
        expect(button).toBeInTheDocument();
      }
    });
  });

  describe('Theme Icons', () => {
    it('displays sun icon for light theme in dropdown', () => {
      renderThemeToggle('light', 'dropdown');

      const toggle = screen.getByTestId('theme-toggle');
      const svg = toggle.querySelector('svg');
      expect(svg).toBeInTheDocument();
    });

    it('displays moon icon for dark theme in dropdown', () => {
      renderThemeToggle('dark', 'dropdown');

      const toggle = screen.getByTestId('theme-toggle');
      const svg = toggle.querySelector('svg');
      expect(svg).toBeInTheDocument();
    });
  });
});
