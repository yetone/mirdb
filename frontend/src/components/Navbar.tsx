import { Link } from 'react-router-dom'
import ThemeToggle from './ThemeToggle'

export default function Navbar() {
  return (
    <nav className="navbar bg-base-100 shadow-lg" role="navigation" aria-label="Main navigation">
      <div className="flex-1">
        <Link to="/" className="btn btn-ghost text-xl" data-testid="site-logo">
          <span className="text-primary font-bold">URL</span>
          <span className="text-secondary">Shortener</span>
        </Link>
      </div>
      <div className="flex-none gap-2">
        <ThemeToggle />
        <Link
          to="/login"
          className="btn btn-ghost"
          data-testid="nav-login"
        >
          Login
        </Link>
        <Link
          to="/register"
          className="btn btn-primary"
          data-testid="nav-register"
        >
          Register
        </Link>
      </div>
    </nav>
  )
}
