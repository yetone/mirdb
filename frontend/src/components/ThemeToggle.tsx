import { Sun, Moon, Palette } from 'lucide-react';
import { useTheme } from '../contexts/ThemeContext';
import { Theme } from '../store/themeStore';

interface ThemeToggleProps {
  variant?: 'dropdown' | 'buttons';
  className?: string;
}

const themeIcons: Record<Theme, JSX.Element> = {
  light: <Sun className="w-4 h-4" />,
  dark: <Moon className="w-4 h-4" />,
  cyberpunk: <Palette className="w-4 h-4" />,
  synthwave: <Palette className="w-4 h-4" />,
  retro: <Palette className="w-4 h-4" />,
  valentine: <Palette className="w-4 h-4" />,
  night: <Moon className="w-4 h-4" />,
};

const themeLabels: Record<Theme, string> = {
  light: 'Light',
  dark: 'Dark',
  cyberpunk: 'Cyberpunk',
  synthwave: 'Synthwave',
  retro: 'Retro',
  valentine: 'Valentine',
  night: 'Night',
};

export default function ThemeToggle({ variant = 'dropdown', className = '' }: ThemeToggleProps) {
  const { theme, setTheme, availableThemes } = useTheme();

  if (variant === 'buttons') {
    return (
      <div className={`flex flex-wrap gap-2 ${className}`} data-testid="theme-toggle">
        {availableThemes.map((t) => (
          <button
            key={t}
            onClick={() => setTheme(t)}
            className={`btn btn-sm ${theme === t ? 'btn-primary' : 'btn-ghost'}`}
            aria-label={`Switch to ${themeLabels[t]} theme`}
            aria-pressed={theme === t}
            data-testid={`theme-button-${t}`}
          >
            {themeIcons[t]}
            <span className="ml-1">{themeLabels[t]}</span>
          </button>
        ))}
      </div>
    );
  }

  return (
    <div className={`dropdown dropdown-end ${className}`} data-testid="theme-toggle">
      <label
        tabIndex={0}
        className="btn btn-ghost btn-circle"
        aria-label="Change theme"
        role="button"
      >
        {themeIcons[theme]}
      </label>
      <ul
        tabIndex={0}
        className="dropdown-content z-[1] menu p-2 shadow-lg bg-base-200 rounded-box w-52"
        role="listbox"
        aria-label="Theme options"
      >
        {availableThemes.map((t) => (
          <li key={t}>
            <button
              onClick={() => setTheme(t)}
              className={theme === t ? 'active' : ''}
              role="option"
              aria-selected={theme === t}
              aria-label={`${themeLabels[t]} theme`}
              data-testid={`theme-option-${t}`}
            >
              {themeIcons[t]}
              <span>{themeLabels[t]}</span>
              {theme === t && <span className="badge badge-primary badge-sm">Active</span>}
            </button>
          </li>
        ))}
      </ul>
    </div>
  );
}
