import { Link } from 'react-router-dom'
import ThemeToggle from './ThemeToggle'
import { useAuth } from '../contexts/AuthContext'

const Navbar = () => {
  const { isAuthenticated } = useAuth()

  const scrollToSection = (sectionId: string) => {
    const section = document.getElementById(sectionId)
    if (section) {
      section.scrollIntoView({ behavior: 'smooth' })
    }
  }

  return (
    <header
      data-testid="navigation-header"
      className="navbar bg-base-100/80 backdrop-blur-md fixed top-0 left-0 right-0 z-50 shadow-sm"
    >
      <div className="container mx-auto">
        <div className="flex-1">
          <Link
            to="/"
            data-testid="nav-logo"
            className="btn btn-ghost text-xl font-bold"
          >
            <svg
              xmlns="http://www.w3.org/2000/svg"
              fill="none"
              viewBox="0 0 24 24"
              strokeWidth={1.5}
              stroke="currentColor"
              className="w-6 h-6 mr-2 text-primary"
            >
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                d="M13.19 8.688a4.5 4.5 0 011.242 7.244l-4.5 4.5a4.5 4.5 0 01-6.364-6.364l1.757-1.757m13.35-.622l1.757-1.757a4.5 4.5 0 00-6.364-6.364l-4.5 4.5a4.5 4.5 0 001.242 7.244"
              />
            </svg>
            ShortURL
          </Link>
        </div>
        <div className="flex-none gap-2">
          <nav className="hidden md:flex gap-1">
            <button
              data-testid="nav-features"
              onClick={() => scrollToSection('features')}
              className="btn btn-ghost btn-sm"
            >
              Features
            </button>
            <button
              data-testid="nav-how-it-works"
              onClick={() => scrollToSection('how-it-works')}
              className="btn btn-ghost btn-sm"
            >
              How It Works
            </button>
          </nav>
          <ThemeToggle />
          {isAuthenticated ? (
            <Link
              to="/dashboard"
              data-testid="nav-dashboard"
              className="btn btn-primary btn-sm"
            >
              Dashboard
            </Link>
          ) : (
            <>
              <Link
                to="/login"
                data-testid="nav-login"
                className="btn btn-ghost btn-sm"
              >
                Login
              </Link>
              <Link
                to="/register"
                data-testid="nav-register"
                className="btn btn-primary btn-sm"
              >
                Register
              </Link>
            </>
          )}
        </div>
      </div>
    </header>
  )
}

export default Navbar
