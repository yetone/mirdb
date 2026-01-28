import { Sun, Moon, Palette } from 'lucide-react';
import { useThemeStore } from '../store/uiStore';
import { Theme } from '../types/custom';

const themes: { value: Theme; label: string }[] = [
  { value: 'light', label: 'Light' },
  { value: 'dark', label: 'Dark' },
  { value: 'cyberpunk', label: 'Cyberpunk' },
  { value: 'synthwave', label: 'Synthwave' },
  { value: 'retro', label: 'Retro' },
  { value: 'valentine', label: 'Valentine' },
  { value: 'night', label: 'Night' },
];

export const ThemeToggle = () => {
  const { theme, setTheme } = useThemeStore();

  const getThemeIcon = () => {
    if (theme === 'light') return <Sun className="h-5 w-5" />;
    if (theme === 'dark' || theme === 'night') return <Moon className="h-5 w-5" />;
    return <Palette className="h-5 w-5" />;
  };

  return (
    <div className="dropdown dropdown-end" data-testid="theme-toggle">
      <label tabIndex={0} className="btn btn-ghost btn-circle">
        {getThemeIcon()}
      </label>
      <ul
        tabIndex={0}
        className="dropdown-content menu p-2 shadow-lg bg-base-200 rounded-box w-52 z-50"
      >
        {themes.map((t) => (
          <li key={t.value}>
            <button
              onClick={() => setTheme(t.value)}
              className={theme === t.value ? 'active' : ''}
            >
              {t.label}
            </button>
          </li>
        ))}
      </ul>
    </div>
  );
};

export default ThemeToggle;
