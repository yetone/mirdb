import React from 'react';
import { Link } from 'react-router-dom';
import { useTheme } from '../../contexts/ThemeContext';
import { Theme } from '../../types';

const themes: Theme[] = ['light', 'dark', 'cyberpunk', 'synthwave', 'retro', 'valentine'];

export function Navbar() {
  const { theme, setTheme } = useTheme();

  return (
    <header className="navbar bg-base-100/80 backdrop-blur-md sticky top-0 z-50 border-b border-base-content/10" data-testid="navbar">
      <div className="flex-1">
        <Link to="/" className="btn btn-ghost text-xl" data-testid="navbar-logo">
          URLShort
        </Link>
      </div>
      <div className="flex-none gap-2">
        <div className="dropdown dropdown-end">
          <label tabIndex={0} className="btn btn-ghost btn-sm" data-testid="theme-toggle">
            Theme
          </label>
          <ul tabIndex={0} className="dropdown-content z-[1] menu p-2 shadow bg-base-100 rounded-box w-52">
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
        <Link to="/login" className="btn btn-ghost btn-sm" data-testid="navbar-login">
          Login
        </Link>
        <Link to="/register" className="btn btn-primary btn-sm" data-testid="navbar-register">
          Register
        </Link>
      </div>
    </header>
  );
}
