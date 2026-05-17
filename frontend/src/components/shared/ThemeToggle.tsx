import { useTheme } from '../../contexts/ThemeContext';

const SUN_ICON = '☀';
const MOON_ICON = '☾';

export default function ThemeToggle() {
  const { theme, toggleTheme } = useTheme();
  const isDark = theme === 'dark';
  const nextLabel = isDark ? 'Switch to light theme' : 'Switch to dark theme';

  return (
    <button
      type="button"
      onClick={toggleTheme}
      className="btn btn-ghost btn-circle"
      aria-label={nextLabel}
      aria-pressed={isDark}
      title={nextLabel}
      data-testid="theme-toggle"
      data-active-theme={theme}
    >
      <span aria-hidden="true" data-testid="theme-toggle-icon">
        {isDark ? SUN_ICON : MOON_ICON}
      </span>
    </button>
  );
}
