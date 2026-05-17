import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { render, screen, cleanup, act } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter } from 'react-router-dom';
import { ReactNode } from 'react';
import Home from '../../../src/pages/Home';
import ThemeToggle from '../../../src/components/shared/ThemeToggle';
import {
  ThemeProvider,
  useTheme,
  THEME_STORAGE_KEY,
  DEFAULT_THEME,
  isValidTheme,
} from '../../../src/contexts/ThemeContext';
import { THEMES } from '../../../src/utils/constants';

function HomeWithProvider({ children }: { children?: ReactNode }) {
  return (
    <ThemeProvider>
      <MemoryRouter initialEntries={['/']}>
        <Home />
        {children}
      </MemoryRouter>
    </ThemeProvider>
  );
}

function resetThemeEnvironment() {
  try {
    window.localStorage.clear();
  } catch {
    // ignore
  }
  document.documentElement.removeAttribute('data-theme');
}

beforeEach(() => {
  resetThemeEnvironment();
});

afterEach(() => {
  cleanup();
  resetThemeEnvironment();
  vi.restoreAllMocks();
});

describe('Theme Switching and Persistence (Scenario 4)', () => {
  describe('test case 1: stored theme is applied on render', () => {
    it("applies data-theme='dark' to documentElement when localStorage has 'dark'", () => {
      window.localStorage.setItem(THEME_STORAGE_KEY, 'dark');

      render(<HomeWithProvider />);

      expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
    });

    it("renders the toggle in its 'dark' state (aria-pressed=true) when stored theme is 'dark'", () => {
      window.localStorage.setItem(THEME_STORAGE_KEY, 'dark');

      render(<HomeWithProvider />);

      const toggle = screen.getByTestId('theme-toggle');
      expect(toggle).toHaveAttribute('aria-pressed', 'true');
      expect(toggle).toHaveAttribute('data-active-theme', 'dark');
      expect(toggle).toHaveAccessibleName(/light/i);
    });
  });

  describe('test case 2: clicking the toggle switches theme and updates storage', () => {
    it('changes data-theme on documentElement and writes the new value to localStorage', async () => {
      const user = userEvent.setup();
      render(<HomeWithProvider />);

      expect(document.documentElement.getAttribute('data-theme')).toBe(
        DEFAULT_THEME,
      );

      const toggle = screen.getByTestId('theme-toggle');
      await user.click(toggle);

      const appliedTheme = document.documentElement.getAttribute('data-theme');
      expect(appliedTheme).not.toBe(DEFAULT_THEME);
      expect(isValidTheme(appliedTheme)).toBe(true);
      expect(window.localStorage.getItem(THEME_STORAGE_KEY)).toBe(appliedTheme);
    });

    it("flips aria-pressed and aria-label after the toggle is clicked", async () => {
      const user = userEvent.setup();
      render(<HomeWithProvider />);

      const toggle = screen.getByTestId('theme-toggle');
      expect(toggle).toHaveAttribute('aria-pressed', 'false');
      expect(toggle).toHaveAccessibleName(/dark/i);

      await user.click(toggle);

      expect(toggle).toHaveAttribute('aria-pressed', 'true');
      expect(toggle).toHaveAccessibleName(/light/i);
    });
  });

  describe('test case 3: ThemeToggle renders safely without a provider', () => {
    it('does not throw when rendered without ThemeProvider', () => {
      expect(() => render(<ThemeToggle />)).not.toThrow();
    });

    it('falls back to the default theme state and silently no-ops on click', async () => {
      const user = userEvent.setup();
      render(<ThemeToggle />);

      const toggle = screen.getByTestId('theme-toggle');
      expect(toggle).toBeInTheDocument();
      expect(toggle).toHaveAttribute('data-active-theme', DEFAULT_THEME);

      const beforeDataTheme =
        document.documentElement.getAttribute('data-theme');
      await user.click(toggle);
      const afterDataTheme =
        document.documentElement.getAttribute('data-theme');

      expect(toggle).toHaveAttribute('data-active-theme', DEFAULT_THEME);
      expect(afterDataTheme).toBe(beforeDataTheme);
      expect(window.localStorage.getItem(THEME_STORAGE_KEY)).toBeNull();
    });
  });

  describe('test case 4: invalid stored theme falls back to a valid default', () => {
    it("does not apply 'invalid-theme-name' to documentElement", () => {
      window.localStorage.setItem(THEME_STORAGE_KEY, 'invalid-theme-name');

      render(<HomeWithProvider />);

      const applied = document.documentElement.getAttribute('data-theme');
      expect(applied).not.toBe('invalid-theme-name');
      expect(applied).toBeTruthy();
      expect((THEMES as readonly string[]).includes(applied as string)).toBe(
        true,
      );
    });

    it('overwrites the invalid stored value with a valid theme', () => {
      window.localStorage.setItem(THEME_STORAGE_KEY, 'invalid-theme-name');

      render(<HomeWithProvider />);

      const stored = window.localStorage.getItem(THEME_STORAGE_KEY);
      expect(stored).not.toBe('invalid-theme-name');
      expect(isValidTheme(stored)).toBe(true);
    });
  });

  describe('test case 5: theme persists across unmount and remount', () => {
    it('restores the previously selected theme after remount without further interaction', async () => {
      const user = userEvent.setup();

      const { unmount } = render(<HomeWithProvider />);

      const toggle = screen.getByTestId('theme-toggle');
      await user.click(toggle);

      const chosen = document.documentElement.getAttribute('data-theme');
      expect(chosen).not.toBe(DEFAULT_THEME);
      expect(window.localStorage.getItem(THEME_STORAGE_KEY)).toBe(chosen);

      unmount();
      document.documentElement.removeAttribute('data-theme');
      expect(document.documentElement.getAttribute('data-theme')).toBeNull();

      render(<HomeWithProvider />);

      expect(document.documentElement.getAttribute('data-theme')).toBe(chosen);
      const remountedToggle = screen.getByTestId('theme-toggle');
      expect(remountedToggle).toHaveAttribute('data-active-theme', chosen!);
    });
  });

  describe('ThemeContext API surface', () => {
    it('useTheme returns default values when used outside a provider', () => {
      let captured: ReturnType<typeof useTheme> | undefined;

      function Probe() {
        captured = useTheme();
        return null;
      }

      render(<Probe />);

      expect(captured?.theme).toBe(DEFAULT_THEME);
      expect(typeof captured?.setTheme).toBe('function');
      expect(typeof captured?.toggleTheme).toBe('function');
    });

    it('setTheme accepts a valid theme and ignores an invalid one', () => {
      let api: ReturnType<typeof useTheme> | undefined;

      function Probe() {
        api = useTheme();
        return <span data-testid="probe-theme">{api.theme}</span>;
      }

      render(
        <ThemeProvider>
          <Probe />
        </ThemeProvider>,
      );

      act(() => {
        api?.setTheme('cyberpunk');
      });
      expect(screen.getByTestId('probe-theme').textContent).toBe('cyberpunk');
      expect(document.documentElement.getAttribute('data-theme')).toBe(
        'cyberpunk',
      );

      const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
      act(() => {
        api?.setTheme('not-a-real-theme' as never);
      });
      expect(screen.getByTestId('probe-theme').textContent).toBe('cyberpunk');
      expect(document.documentElement.getAttribute('data-theme')).toBe(
        'cyberpunk',
      );
      warn.mockRestore();
    });
  });
});
