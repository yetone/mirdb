import { Sun, Moon } from 'lucide-react';
import { useTheme } from '../contexts/ThemeContext';

interface ThemeToggleProps {
  className?: string;
}

export function ThemeToggle({ className = '' }: ThemeToggleProps) {
  const { theme, toggleTheme } = useTheme();

  return (
    <button
      onClick={toggleTheme}
      className={`btn btn-ghost btn-circle ${className}`}
      aria-label={`Switch to ${theme === 'light' ? 'dark' : 'light'} theme`}
      data-testid="theme-toggle"
    >
      {theme === 'light' ? (
        <Moon className="h-5 w-5" aria-hidden="true" />
      ) : (
        <Sun className="h-5 w-5" aria-hidden="true" />
      )}
    </button>
  );
}
