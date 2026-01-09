import React, { useState } from 'react'
import { Link } from 'react-router-dom'
import ThemeToggle from './ThemeToggle'
import { HiMenu, HiX } from 'react-icons/hi'

interface NavigationHeaderProps {
  onScrollToSection?: (sectionId: string) => void
}

const NavigationHeader: React.FC<NavigationHeaderProps> = ({ onScrollToSection }) => {
  const [isMobileMenuOpen, setIsMobileMenuOpen] = useState(false)

  const handleNavClick = (sectionId: string) => {
    if (onScrollToSection) {
      onScrollToSection(sectionId)
    } else {
      const element = document.getElementById(sectionId)
      if (element) {
        element.scrollIntoView({ behavior: 'smooth' })
      }
    }
    // Close mobile menu after navigation
    setIsMobileMenuOpen(false)
  }

  const toggleMobileMenu = () => {
    setIsMobileMenuOpen(!isMobileMenuOpen)
  }

  const closeMobileMenu = () => {
    setIsMobileMenuOpen(false)
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

        {/* Desktop Navigation - hidden on mobile */}
        <div className="hidden md:flex flex-none gap-2" data-testid="desktop-nav">
          <button
            className="btn btn-ghost min-h-[44px] min-w-[44px]"
            onClick={() => handleNavClick('features')}
            data-testid="nav-features"
          >
            Features
          </button>
          <button
            className="btn btn-ghost min-h-[44px] min-w-[44px]"
            onClick={() => handleNavClick('how-it-works')}
            data-testid="nav-how-it-works"
          >
            How It Works
          </button>
          <ThemeToggle />
          <Link
            to="/login"
            className="btn btn-ghost min-h-[44px] min-w-[44px]"
            data-testid="nav-login"
          >
            Log In
          </Link>
          <Link
            to="/register"
            className="btn btn-primary min-h-[44px] min-w-[44px]"
            data-testid="nav-signup"
          >
            Sign Up
          </Link>
        </div>

        {/* Mobile Hamburger Menu Button - visible only on mobile */}
        <div className="flex md:hidden items-center gap-2">
          <ThemeToggle />
          <button
            className="btn btn-ghost btn-square min-h-[44px] min-w-[44px]"
            onClick={toggleMobileMenu}
            aria-label={isMobileMenuOpen ? 'Close menu' : 'Open menu'}
            aria-expanded={isMobileMenuOpen}
            data-testid="hamburger-menu"
          >
            {isMobileMenuOpen ? (
              <HiX className="w-6 h-6" aria-hidden="true" />
            ) : (
              <HiMenu className="w-6 h-6" aria-hidden="true" />
            )}
          </button>
        </div>
      </nav>

      {/* Mobile Menu Overlay */}
      {isMobileMenuOpen && (
        <div
          className="fixed inset-0 bg-black/50 z-40 md:hidden"
          onClick={closeMobileMenu}
          aria-hidden="true"
        />
      )}

      {/* Mobile Menu */}
      <div
        className={`fixed top-[64px] left-0 right-0 bg-base-100 shadow-lg z-50 md:hidden transform transition-transform duration-300 ease-in-out ${
          isMobileMenuOpen ? 'translate-y-0' : '-translate-y-full'
        }`}
        data-testid="mobile-menu"
        style={{ display: isMobileMenuOpen ? 'block' : 'none' }}
        role="menu"
        aria-label="Mobile navigation menu"
      >
        <div className="flex flex-col p-4 gap-2">
          <button
            className="btn btn-ghost justify-start min-h-[44px] text-left"
            onClick={() => handleNavClick('features')}
            data-testid="mobile-nav-features"
            role="menuitem"
          >
            Features
          </button>
          <button
            className="btn btn-ghost justify-start min-h-[44px] text-left"
            onClick={() => handleNavClick('how-it-works')}
            data-testid="mobile-nav-how-it-works"
            role="menuitem"
          >
            How It Works
          </button>
          <div className="divider my-2"></div>
          <Link
            to="/login"
            className="btn btn-ghost justify-start min-h-[44px] text-left"
            data-testid="mobile-nav-login"
            onClick={closeMobileMenu}
            role="menuitem"
          >
            Log In
          </Link>
          <Link
            to="/register"
            className="btn btn-primary justify-start min-h-[44px]"
            data-testid="mobile-nav-signup"
            onClick={closeMobileMenu}
            role="menuitem"
          >
            Sign Up
          </Link>
          <button
            className="btn btn-ghost btn-sm mt-2"
            onClick={closeMobileMenu}
            data-testid="mobile-menu-close"
            aria-label="Close menu"
          >
            Close Menu
          </button>
        </div>
      </div>
    </header>
  )
}

export default NavigationHeader
