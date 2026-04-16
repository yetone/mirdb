import { useState } from 'react';
import { Link } from 'react-router-dom';
import { useAuth } from '../contexts/AuthContext';
import ThemeToggle from './ThemeToggle';
import { Menu, X } from 'lucide-react';

export default function Navbar() {
  const { isAuthenticated, logout } = useAuth();
  const [isMenuOpen, setIsMenuOpen] = useState(false);

  const toggleMenu = () => setIsMenuOpen(!isMenuOpen);
  const closeMenu = () => setIsMenuOpen(false);

  const NavLinks = () => (
    <>
      {isAuthenticated ? (
        <>
          <Link
            to="/dashboard"
            className="btn btn-ghost"
            onClick={closeMenu}
          >
            Dashboard
          </Link>
          <button onClick={() => { logout(); closeMenu(); }} className="btn btn-ghost">
            Logout
          </button>
        </>
      ) : (
        <>
          <Link to="/login" className="btn btn-ghost" onClick={closeMenu}>
            Login
          </Link>
          <Link to="/register" className="btn btn-primary" onClick={closeMenu}>
            Register
          </Link>
        </>
      )}
    </>
  );

  return (
    <nav className="navbar bg-base-100/80 backdrop-blur-md sticky top-0 z-50 border-b border-base-300">
      <div className="flex-1">
        <Link to="/" className="btn btn-ghost text-xl font-bold">
          ShortURL
        </Link>
      </div>

      {/* Desktop navigation */}
      <div className="hidden sm:flex flex-none gap-2">
        <ThemeToggle />
        <NavLinks />
      </div>

      {/* Mobile navigation */}
      <div className="flex sm:hidden flex-none gap-2">
        <ThemeToggle />
        <button
          className="btn btn-ghost btn-square"
          onClick={toggleMenu}
          aria-label={isMenuOpen ? 'Close menu' : 'Open menu'}
          aria-expanded={isMenuOpen}
          data-testid="mobile-menu-toggle"
        >
          {isMenuOpen ? (
            <X className="w-6 h-6" aria-hidden="true" />
          ) : (
            <Menu className="w-6 h-6" aria-hidden="true" />
          )}
        </button>
      </div>

      {/* Mobile menu dropdown */}
      {isMenuOpen && (
        <div
          className="absolute top-full left-0 right-0 bg-base-100/95 backdrop-blur-md border-b border-base-300 sm:hidden z-50"
          data-testid="mobile-menu"
        >
          <div className="flex flex-col p-4 gap-2">
            <NavLinks />
          </div>
        </div>
      )}
    </nav>
  );
}
