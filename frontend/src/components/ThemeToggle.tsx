import React from 'react';
import { useTheme, Theme } from '../contexts/ThemeContext';

const themes: Theme[] = ['light', 'dark', 'cyberpunk', 'synthwave', 'retro', 'forest'];

export function ThemeToggle() {
  const { theme, setTheme } = useTheme();

  return (
    <div className="dropdown dropdown-end">
      <div tabIndex={0} role="button" className="btn btn-ghost m-1" data-testid="theme-toggle">
        <svg xmlns="http://www.w3.org/2000/svg" className="h-5 w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M7 21a4 4 0 01-4-4V5a2 2 0 012-2h4a2 2 0 012 2v12a4 4 0 01-4 4zm0 0h12a2 2 0 002-2v-4a2 2 0 00-2-2h-2.343M11 7.343l1.657-1.657a2 2 0 012.828 0l2.829 2.829a2 2 0 010 2.828l-8.486 8.485M7 17h.01" />
        </svg>
        <span className="hidden sm:inline">{theme}</span>
      </div>
      <ul tabIndex={0} className="dropdown-content z-[1] menu p-2 shadow-lg bg-base-200 rounded-box w-52">
        {themes.map((t) => (
          <li key={t}>
            <button
              onClick={() => setTheme(t)}
              className={theme === t ? 'active' : ''}
              data-testid={`theme-option-${t}`}
            >
              {t.charAt(0).toUpperCase() + t.slice(1)}
            </button>
          </li>
        ))}
      </ul>
    </div>
  );
}
