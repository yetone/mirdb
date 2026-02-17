/**
 * Navigation header component.
 * Owner: First builder (shared)
 *
 * Contains:
 * - Logo/brand
 * - Navigation links
 * - Theme toggle
 * - Login/Register buttons (when unauthenticated)
 * - User menu (when authenticated)
 */

import React from 'react'
import { Link } from 'react-router-dom'
import { useAuth } from '../../contexts/AuthContext'
import { ThemeToggle } from '../ThemeToggle'

export function Navbar() {
  const { isAuthenticated, logout } = useAuth()

  return (
    <header className="navbar bg-base-100/80 backdrop-blur-md sticky top-0 z-50 border-b border-base-content/10">
      <div className="flex-1">
        <Link to="/" className="btn btn-ghost text-xl font-bold">
          URL Shortener
        </Link>
      </div>
      <nav className="flex-none gap-2" aria-label="Main navigation">
        <ThemeToggle />
        {isAuthenticated ? (
          <>
            <Link to="/dashboard" className="btn btn-ghost">
              Dashboard
            </Link>
            <button onClick={logout} className="btn btn-ghost">
              Logout
            </button>
          </>
        ) : (
          <>
            <Link to="/login" className="btn btn-ghost">
              Login
            </Link>
            <Link to="/register" className="btn btn-primary">
              Get Started
            </Link>
          </>
        )}
      </nav>
    </header>
  )
}
