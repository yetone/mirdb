/**
 * Navbar Component
 * Owner: Scenario 2 - Navigation and Routing
 *
 * Displays the navigation bar with:
 * - Logo/brand link (navigates to homepage)
 * - Login link (navigates to /login)
 * - Register/Sign Up link (navigates to /register)
 * - Theme toggle (Scenario 5)
 * - Responsive mobile menu support
 *
 * Requirements: REQ-5 - Provide navigation links to Login and Register pages
 *               REQ-8 - Support theme switching
 * User Stories: US-2 (Navigate to Registration), US-3 (Navigate to Login), US-6 (Toggle Theme)
 */

import { Link } from 'react-router-dom'
import { ThemeToggle } from './ThemeToggle'

export interface NavbarProps {
  /** Optional additional CSS classes */
  className?: string
}

export function Navbar({ className = '' }: NavbarProps) {
  return (
    <nav
      className={`navbar bg-base-100/80 backdrop-blur-md fixed top-0 left-0 right-0 z-50 shadow-sm ${className}`}
      data-testid="navbar"
      aria-label="Main navigation"
      role="navigation"
    >
      <div className="container mx-auto px-4">
        {/* Logo/Brand - Left Side */}
        <div className="flex-1">
          <Link
            to="/"
            className="btn btn-ghost text-xl font-bold bg-gradient-to-r from-primary to-secondary bg-clip-text text-transparent hover:opacity-80 transition-opacity"
            data-testid="navbar-logo"
            aria-label="Go to homepage"
          >
            ShortURL
          </Link>
        </div>

        {/* Desktop Navigation - Right Side */}
        <div className="flex-none hidden md:flex gap-2 items-center">
          <ThemeToggle />
          <Link
            to="/login"
            className="btn btn-ghost"
            data-testid="navbar-login"
          >
            Login
          </Link>
          <Link
            to="/register"
            className="btn btn-primary"
            data-testid="navbar-register"
          >
            Sign Up
          </Link>
        </div>

        {/* Mobile Navigation - Dropdown Menu */}
        <div className="flex-none md:hidden">
          <div className="dropdown dropdown-end">
            <button
              tabIndex={0}
              className="btn btn-ghost"
              aria-label="Open navigation menu"
              aria-haspopup="true"
            >
              <svg
                xmlns="http://www.w3.org/2000/svg"
                fill="none"
                viewBox="0 0 24 24"
                className="inline-block w-6 h-6 stroke-current"
              >
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth="2"
                  d="M4 6h16M4 12h16M4 18h16"
                />
              </svg>
            </button>
            <ul
              tabIndex={0}
              className="dropdown-content menu p-2 shadow-lg bg-base-100 rounded-box w-52 mt-3"
              role="menu"
            >
              <li role="menuitem">
                <Link
                  to="/login"
                  className="justify-between"
                  data-testid="navbar-login-mobile"
                >
                  Login
                </Link>
              </li>
              <li role="menuitem">
                <Link
                  to="/register"
                  className="justify-between"
                  data-testid="navbar-register-mobile"
                >
                  Sign Up
                </Link>
              </li>
            </ul>
          </div>
        </div>
      </div>
    </nav>
  )
}

export default Navbar
