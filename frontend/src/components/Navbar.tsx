/**
 * Navbar Component
 *
 * Navigation bar for the homepage with:
 * - Brand/logo link to home
 * - Navigation links: Login, Register
 * - ThemeToggle for theme switching
 *
 * Uses semantic <nav> element and React Router Link for navigation.
 */
import { Link } from 'react-router-dom'
import { ThemeToggle } from './ThemeToggle'

export function Navbar() {
  return (
    <nav
      data-testid="navbar"
      className="navbar bg-base-100/80 backdrop-blur-sm sticky top-0 z-50 border-b border-base-content/10"
      aria-label="Main navigation"
    >
      <div className="flex-1">
        <Link
          to="/"
          className="btn btn-ghost text-xl font-bold"
        >
          URL Shortener
        </Link>
      </div>
      <div className="flex-none gap-2">
        <Link to="/login" className="btn btn-ghost btn-sm">
          Log In
        </Link>
        <Link to="/register" className="btn btn-primary btn-sm">
          Sign Up
        </Link>
        <ThemeToggle />
      </div>
    </nav>
  )
}
