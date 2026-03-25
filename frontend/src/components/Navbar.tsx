import React from 'react';
import { Link } from 'react-router-dom';
import { useAuth } from '../contexts/AuthContext';
import { ThemeToggle } from './ThemeToggle';

export function Navbar() {
  const { isAuthenticated, user, logout } = useAuth();

  return (
    <nav className="navbar bg-base-100 shadow-lg px-4 py-2" role="navigation" aria-label="Main navigation">
      <div className="flex-1">
        <Link to="/" className="text-xl font-bold text-primary">
          URL Shortener
        </Link>
      </div>
      <div className="flex-none gap-4">
        <ThemeToggle />
        {isAuthenticated ? (
          <div className="flex items-center gap-4">
            <span className="text-sm" data-testid="user-display">
              Welcome, {user?.username}
            </span>
            <Link
              to="/dashboard"
              className="btn btn-primary btn-sm"
              data-testid="dashboard-link"
            >
              Dashboard
            </Link>
            <button
              onClick={logout}
              className="btn btn-outline btn-sm"
              data-testid="logout-button"
            >
              Logout
            </button>
          </div>
        ) : (
          <div className="flex items-center gap-2">
            <Link
              to="/login"
              className="btn btn-ghost btn-sm"
              data-testid="sign-in-link"
              aria-label="Sign in to your account"
            >
              Sign In
            </Link>
            <Link
              to="/register"
              className="btn btn-primary btn-sm"
              data-testid="get-started-link"
            >
              Get Started
            </Link>
          </div>
        )}
      </div>
    </nav>
  );
}
