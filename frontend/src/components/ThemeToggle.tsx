import { useTheme } from '../contexts/ThemeContext';

const themes = ['light', 'dark', 'cyberpunk', 'synthwave', 'retro', 'valentine', 'night'] as const;

export default function ThemeToggle() {
  const { theme, setTheme } = useTheme();

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
