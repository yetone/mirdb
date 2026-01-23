import React from 'react';
import { useTheme } from '../contexts/ThemeContext';

const themes = ['light', 'dark', 'cyberpunk', 'synthwave', 'retro', 'valentine', 'night'] as const;

const ThemeToggle: React.FC = () => {
  const { theme, setTheme } = useTheme();

  return (
    <div className="dropdown dropdown-end">
      <label tabIndex={0} className="btn btn-ghost m-1">
        Theme
        <svg
          xmlns="http://www.w3.org/2000/svg"
          className="h-4 w-4"
          fill="none"
          viewBox="0 0 24 24"
          stroke="currentColor"
        >
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
        </svg>
      </label>
      <ul
        tabIndex={0}
        className="dropdown-content z-[1] menu p-2 shadow bg-base-100 rounded-box w-52"
      >
        {themes.map((t) => (
          <li key={t}>
            <button
              onClick={() => setTheme(t)}
              className={theme === t ? 'active' : ''}
            >
              {t.charAt(0).toUpperCase() + t.slice(1)}
            </button>
          </li>
        ))}
      </ul>
    </div>
  );
};

export default ThemeToggle;
