/**
 * Navigation bar component with auth-aware links.
 * Owner: Scenario 7 - Navigation Bar Functionality
 *
 * Shows Login/Register when unauthenticated, Dashboard when authenticated.
 * Includes logo link to homepage and theme toggle.
 */

import { Link } from 'react-router-dom'
import { useAuth } from '../contexts/AuthContext'
import { useTheme } from '../contexts/ThemeContext'

export default function Navbar() {
  const { isAuthenticated } = useAuth()
  const { theme, setTheme } = useTheme()

  const toggleTheme = () => {
    setTheme(theme === 'dark' ? 'light' : 'dark')
  }

  return (
    <nav className="navbar bg-base-100 shadow-lg sticky top-0 z-50" role="navigation">
      <div className="navbar-start">
        <Link
          to="/"
          className="btn btn-ghost text-xl font-bold text-primary"
          aria-label="ShortenR - Go to homepage"
        >
          ShortenR
        </Link>
      </div>

      <div className="navbar-end gap-2">
        <button
          onClick={toggleTheme}
          className="btn btn-ghost btn-circle"
          aria-label="Toggle theme"
          data-testid="theme-toggle"
        >
          {theme === 'dark' ? (
            <svg
              xmlns="http://www.w3.org/2000/svg"
              className="h-5 w-5"
              fill="none"
              viewBox="0 0 24 24"
              stroke="currentColor"
              aria-hidden="true"
            >
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                strokeWidth={2}
                d="M12 3v1m0 16v1m9-9h-1M4 12H3m15.364 6.364l-.707-.707M6.343 6.343l-.707-.707m12.728 0l-.707.707M6.343 17.657l-.707.707M16 12a4 4 0 11-8 0 4 4 0 018 0z"
              />
            </svg>
          ) : (
            <svg
              xmlns="http://www.w3.org/2000/svg"
              className="h-5 w-5"
              fill="none"
              viewBox="0 0 24 24"
              stroke="currentColor"
              aria-hidden="true"
            >
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                strokeWidth={2}
                d="M20.354 15.354A9 9 0 018.646 3.646 9.003 9.003 0 0012 21a9.003 9.003 0 008.354-5.646z"
              />
            </svg>
          )}
        </button>

        {isAuthenticated ? (
          <Link
            to="/dashboard"
            className="btn btn-primary"
            aria-label="Go to Dashboard"
          >
            Dashboard
          </Link>
        ) : (
          <>
            <Link
              to="/login"
              className="btn btn-ghost"
              aria-label="Login to your account"
            >
              Login
            </Link>
            <Link
              to="/register"
              className="btn btn-primary"
              aria-label="Register for a new account"
            >
              Register
            </Link>
          </>
        )}
      </div>
    </nav>
  )
}
