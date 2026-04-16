import { useTheme } from '../contexts/ThemeContext';

export default function ThemeToggle() {
  const { theme, setTheme } = useTheme();

  const themes = ['light', 'dark', 'cyberpunk', 'synthwave'] as const;

  return (
    <select
      className="select select-bordered select-sm"
      value={theme}
      onChange={(e) => setTheme(e.target.value as typeof themes[number])}
      aria-label="Select theme"
    >
      {themes.map((t) => (
        <option key={t} value={t}>
          {t.charAt(0).toUpperCase() + t.slice(1)}
        </option>
      ))}
    </select>
  );
}
