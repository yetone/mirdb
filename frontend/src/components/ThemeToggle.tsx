/**
 * ThemeToggle Component
 * Owner: Scenario 4 - Theme Switching
 *
 * A dropdown component for switching between available themes.
 * Uses DaisyUI theming system and persists selection to localStorage.
 */

import { Sun, Moon, Palette } from 'lucide-react';
import { useThemeStore } from '../stores/themeStore';
import { AVAILABLE_THEMES, type Theme, type ThemeToggleProps } from '../types/home';

/**
 * Get the appropriate icon for a theme
 */
const getThemeIcon = (theme: Theme, className: string = 'w-5 h-5') => {
  switch (theme) {
    case 'light':
      return <Sun className={className} />;
    case 'dark':
      return <Moon className={className} />;
    default:
      return <Palette className={className} />;
  }
};

/**
 * Get the display name for a theme
 */
const getThemeDisplayName = (theme: Theme): string => {
  return theme.charAt(0).toUpperCase() + theme.slice(1);
};

export default function ThemeToggle({ className = '' }: ThemeToggleProps) {
  const { theme, setTheme } = useThemeStore();

  return (
    <div className={`dropdown dropdown-end ${className}`} data-testid="theme-toggle">
      <button
        tabIndex={0}
        role="button"
        className="btn btn-ghost btn-circle"
        aria-label={`Current theme: ${theme}. Click to change theme.`}
        data-testid="theme-toggle-button"
      >
        {getThemeIcon(theme)}
      </button>
      <ul
        tabIndex={0}
        className="dropdown-content z-[1] menu p-2 shadow-lg bg-base-200 rounded-box w-52"
        data-testid="theme-dropdown"
      >
        {AVAILABLE_THEMES.map((themeOption) => (
          <li key={themeOption}>
            <button
              onClick={() => setTheme(themeOption)}
              className={`flex items-center gap-2 ${theme === themeOption ? 'active' : ''}`}
              aria-current={theme === themeOption ? 'true' : 'false'}
              data-testid={`theme-option-${themeOption}`}
            >
              {getThemeIcon(themeOption, 'w-4 h-4')}
              <span>{getThemeDisplayName(themeOption)}</span>
              {theme === themeOption && (
                <span className="badge badge-primary badge-sm ml-auto">Active</span>
              )}
            </button>
          </li>
        ))}
      </ul>
    </div>
  );
}
