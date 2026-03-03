import React from 'react';
import { Link } from 'react-router-dom';
import { ThemeToggle } from './ThemeToggle';
import { useAuth } from '../contexts/AuthContext';

export function Navbar() {
  const { isAuthenticated, logout } = useAuth();

  return (
    <div className="navbar bg-base-100/80 backdrop-blur-md fixed top-0 left-0 right-0 z-50 shadow-sm">
      <div className="flex-1">
        <Link to="/" className="btn btn-ghost text-xl font-bold">
          URL Shortener
        </Link>
      </div>
      <div className="flex-none gap-2">
        <ThemeToggle />
        {isAuthenticated ? (
          <>
            <Link to="/dashboard" className="btn btn-ghost">Dashboard</Link>
            <button onClick={logout} className="btn btn-ghost">Logout</button>
          </>
        ) : (
          <>
            <Link to="/login" className="btn btn-ghost">Sign In</Link>
            <Link to="/register" className="btn btn-primary">Get Started</Link>
          </>
        )}
      </div>
    </div>
  );
}
