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
import { useTheme } from '../../contexts/ThemeContext'

export function Navbar() {
  const { theme, setTheme, availableThemes } = useTheme()

  return (
    <nav className="navbar bg-base-100/80 backdrop-blur-md border-b border-base-content/10 sticky top-0 z-50" aria-label="Main navigation">
      <div className="navbar-start">
        <Link to="/" className="btn btn-ghost text-xl font-bold">
          URLShort
        </Link>
      </div>

      <div className="navbar-center hidden lg:flex">
        <ul className="menu menu-horizontal px-1">
          <li><Link to="/">Home</Link></li>
          <li><Link to="/login">Login</Link></li>
          <li><Link to="/register">Register</Link></li>
        </ul>
      </div>

      <div className="navbar-end gap-2">
        <select
          className="select select-bordered select-sm"
          value={theme}
          onChange={(e) => setTheme(e.target.value as typeof theme)}
          aria-label="Select theme"
        >
          {availableThemes.map((t) => (
            <option key={t} value={t}>
              {t.charAt(0).toUpperCase() + t.slice(1)}
            </option>
          ))}
        </select>

        <div className="lg:hidden dropdown dropdown-end">
          <label tabIndex={0} className="btn btn-ghost" aria-label="Open navigation menu">
            <svg xmlns="http://www.w3.org/2000/svg" className="h-5 w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" aria-hidden="true">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 6h16M4 12h8m-8 6h16" />
            </svg>
          </label>
          <ul tabIndex={0} className="menu menu-sm dropdown-content mt-3 z-[1] p-2 shadow bg-base-100 rounded-box w-52">
            <li><Link to="/">Home</Link></li>
            <li><Link to="/login">Login</Link></li>
            <li><Link to="/register">Register</Link></li>
          </ul>
        </div>
      </div>
    </nav>
  )
}
