import React from 'react';
import { useTheme, Theme } from '../contexts/ThemeContext';
import { Sun, Moon } from 'lucide-react';

const themes: Theme[] = ['light', 'dark', 'system', 'cyberpunk', 'synthwave', 'retro', 'valentine', 'night'];

export function ThemeToggle() {
  const { theme, setTheme } = useTheme();

  const handleThemeChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    setTheme(e.target.value as Theme);
  };

  return (
    <div className="flex items-center gap-2" data-testid="theme-toggle">
      {theme === 'dark' || theme === 'night' ? (
        <Moon className="w-4 h-4" aria-hidden="true" />
      ) : (
        <Sun className="w-4 h-4" aria-hidden="true" />
      )}
      <select
        value={theme}
        onChange={handleThemeChange}
        className="select select-sm select-bordered"
        aria-label="Select theme"
      >
        {themes.map((t) => (
          <option key={t} value={t}>
            {t.charAt(0).toUpperCase() + t.slice(1)}
          </option>
        ))}
      </select>
    </div>
  );
}
