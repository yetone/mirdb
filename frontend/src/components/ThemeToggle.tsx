import { useTheme } from '../contexts/ThemeContext';

export default function ThemeToggle() {
  const { theme, toggleTheme, isDark } = useTheme();

  return (
    <button
      data-testid="theme-toggle"
      onClick={toggleTheme}
      aria-label={`Current theme: ${theme}. Click to toggle theme.`}
      className="theme-toggle px-3 py-2 rounded-lg border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 hover:bg-gray-100 dark:hover:bg-gray-700 transition-colors min-h-[44px] min-w-[44px] flex items-center justify-center"
    >
      <span aria-hidden="true" className="text-lg">
        {isDark ? '🌙' : '☀️'}
      </span>
    </button>
  );
}
