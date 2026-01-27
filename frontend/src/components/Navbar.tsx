import React from 'react';
import { Link } from 'react-router-dom';
import { ThemeToggle } from './ThemeToggle';

interface NavbarProps {
  theme: 'light' | 'dark';
  onThemeToggle: () => void;
}

export const Navbar: React.FC<NavbarProps> = ({ theme, onThemeToggle }) => {
  return (
    <nav className="navbar bg-base-100/80 backdrop-blur-md fixed top-0 z-50 border-b border-base-content/10">
      <div className="navbar-start">
        <Link to="/" className="btn btn-ghost text-xl font-bold">
          URLShortener
        </Link>
      </div>
      <div className="navbar-end gap-2">
        <ThemeToggle theme={theme} onToggle={onThemeToggle} />
        <Link to="/login" className="btn btn-ghost">
          Login
        </Link>
        <Link to="/register" className="btn btn-primary">
          Sign Up
        </Link>
      </div>
    </nav>
  );
};

export default Navbar;
