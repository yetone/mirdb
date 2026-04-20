/**
 * Navigation bar component with auth-aware links.
 * Owner: Scenario 3 - Secondary Login CTA Button
 *
 * Shows Login/Register when unauthenticated, Dashboard when authenticated.
 * Includes logo link to homepage and theme toggle.
 */

import { Link } from 'react-router-dom'
import { useAuth } from '../contexts/AuthContext'

export default function Navbar() {
  const { isAuthenticated } = useAuth()

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
