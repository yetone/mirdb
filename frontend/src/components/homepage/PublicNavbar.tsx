/**
 * Public Navigation Bar Component.
 * Owner: Scenario 2 - Navigation & Routing
 *
 * Expected behavior:
 * - Logo/brand name linking to homepage
 * - Navigation links: Features, About (optional)
 * - Login button (text style)
 * - Sign Up button (primary button style)
 * - Sticky positioning on scroll with subtle shadow
 * - Mobile: hamburger menu with full-screen overlay
 * - Theme toggle integration
 */

import { useState, useEffect } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import { NAV_LINKS } from '../../utils/constants';

interface PublicNavbarProps {
  transparent?: boolean;
}

function PublicNavbar({ transparent = false }: PublicNavbarProps) {
  const navigate = useNavigate();
  const [isScrolled, setIsScrolled] = useState(false);
  const [isMobileMenuOpen, setIsMobileMenuOpen] = useState(false);

  useEffect(() => {
    const handleScroll = () => {
      setIsScrolled(window.scrollY > 10);
    };

    window.addEventListener('scroll', handleScroll);
    return () => window.removeEventListener('scroll', handleScroll);
  }, []);

  const handleLoginClick = () => {
    navigate('/login');
  };

  const handleSignUpClick = () => {
    navigate('/register');
  };

  const toggleMobileMenu = () => {
    setIsMobileMenuOpen(!isMobileMenuOpen);
  };

  const closeMobileMenu = () => {
    setIsMobileMenuOpen(false);
  };

  const navbarClasses = `
    navbar sticky top-0 z-50 transition-all duration-300
    ${isScrolled || !transparent ? 'bg-base-100 shadow-md' : 'bg-transparent'}
  `.trim();

  return (
    <>
      <nav className={navbarClasses} data-testid="public-navbar">
        <div className="container mx-auto max-w-7xl px-4">
          <div className="flex items-center justify-between w-full">
            {/* Logo */}
            <Link
              to="/"
              className="text-xl font-bold text-primary hover:opacity-80 transition-opacity"
              data-testid="navbar-logo"
            >
              ShortURL
            </Link>

            {/* Desktop Navigation */}
            <div className="hidden md:flex items-center gap-6" data-testid="navbar-desktop-nav">
              {NAV_LINKS.map((link) => (
                <a
                  key={link.href}
                  href={link.href}
                  className="text-base-content/70 hover:text-base-content transition-colors"
                  data-testid={`nav-link-${link.label.toLowerCase()}`}
                >
                  {link.label}
                </a>
              ))}
            </div>

            {/* Desktop Auth Buttons */}
            <div className="hidden md:flex items-center gap-4" data-testid="navbar-desktop-auth">
              <button
                onClick={handleLoginClick}
                className="btn btn-ghost"
                data-testid="navbar-login-btn"
              >
                Login
              </button>
              <button
                onClick={handleSignUpClick}
                className="btn btn-primary"
                data-testid="navbar-signup-btn"
              >
                Sign Up
              </button>
            </div>

            {/* Mobile Menu Button */}
            <button
              className="md:hidden btn btn-ghost btn-square"
              onClick={toggleMobileMenu}
              aria-label={isMobileMenuOpen ? 'Close menu' : 'Open menu'}
              data-testid="navbar-mobile-menu-btn"
            >
              {isMobileMenuOpen ? (
                <svg
                  className="w-6 h-6"
                  fill="none"
                  stroke="currentColor"
                  viewBox="0 0 24 24"
                  aria-hidden="true"
                >
                  <path
                    strokeLinecap="round"
                    strokeLinejoin="round"
                    strokeWidth={2}
                    d="M6 18L18 6M6 6l12 12"
                  />
                </svg>
              ) : (
                <svg
                  className="w-6 h-6"
                  fill="none"
                  stroke="currentColor"
                  viewBox="0 0 24 24"
                  aria-hidden="true"
                >
                  <path
                    strokeLinecap="round"
                    strokeLinejoin="round"
                    strokeWidth={2}
                    d="M4 6h16M4 12h16M4 18h16"
                  />
                </svg>
              )}
            </button>
          </div>
        </div>
      </nav>

      {/* Mobile Menu Overlay */}
      {isMobileMenuOpen && (
        <div
          className="md:hidden fixed inset-0 z-40 bg-base-100"
          data-testid="navbar-mobile-menu"
        >
          <div className="flex flex-col h-full pt-20 px-6">
            {/* Mobile Navigation Links */}
            <div className="flex flex-col gap-4">
              {NAV_LINKS.map((link) => (
                <a
                  key={link.href}
                  href={link.href}
                  className="text-lg text-base-content py-2 border-b border-base-200"
                  onClick={closeMobileMenu}
                  data-testid={`mobile-nav-link-${link.label.toLowerCase()}`}
                >
                  {link.label}
                </a>
              ))}
            </div>

            {/* Mobile Auth Buttons */}
            <div className="flex flex-col gap-4 mt-8" data-testid="navbar-mobile-auth">
              <button
                onClick={() => {
                  closeMobileMenu();
                  handleLoginClick();
                }}
                className="btn btn-outline btn-lg w-full"
                data-testid="mobile-login-btn"
              >
                Login
              </button>
              <button
                onClick={() => {
                  closeMobileMenu();
                  handleSignUpClick();
                }}
                className="btn btn-primary btn-lg w-full"
                data-testid="mobile-signup-btn"
              >
                Sign Up
              </button>
            </div>
          </div>
        </div>
      )}
    </>
  );
}

export default PublicNavbar;
