import React from 'react'
import { Link } from 'react-router-dom'
import ThemeToggle from './ThemeToggle'

interface NavigationHeaderProps {
  onScrollToSection?: (sectionId: string) => void
}

const NavigationHeader: React.FC<NavigationHeaderProps> = ({ onScrollToSection }) => {
  const handleNavClick = (sectionId: string) => {
    if (onScrollToSection) {
      onScrollToSection(sectionId)
    } else {
      const element = document.getElementById(sectionId)
      if (element) {
        element.scrollIntoView({ behavior: 'smooth' })
      }
    }
  }

  return (
    <header>
      <nav
        className="navbar bg-base-100 shadow-lg sticky top-0 z-50"
        role="navigation"
        aria-label="Main navigation"
        data-testid="navigation-header"
      >
        <div className="flex-1">
          <Link
            to="/"
            className="btn btn-ghost text-xl font-bold"
            data-testid="nav-logo"
            aria-label="URL Shortener - Go to homepage"
          >
            URL Shortener
          </Link>
        </div>
        <div className="flex-none gap-2">
          <button
            className="btn btn-ghost"
            onClick={() => handleNavClick('features')}
            data-testid="nav-features"
          >
            Features
          </button>
          <button
            className="btn btn-ghost"
            onClick={() => handleNavClick('how-it-works')}
            data-testid="nav-how-it-works"
          >
            How It Works
          </button>
          <ThemeToggle />
          <Link
            to="/login"
            className="btn btn-ghost"
            data-testid="nav-login"
          >
            Log In
          </Link>
          <Link
            to="/register"
            className="btn btn-primary"
            data-testid="nav-signup"
          >
            Sign Up
          </Link>
        </div>
      </nav>
    </header>
  )
}

export default NavigationHeader
