import { Link, useNavigate } from 'react-router-dom';
import { Link2, Menu, X, LogOut, LayoutDashboard } from 'lucide-react';
import { useState } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { useAuth } from '../contexts/AuthContext';
import { ThemeToggle } from './ThemeToggle';
import { FuturisticButton } from './FuturisticButton';

export const Navbar = () => {
  const [isMobileMenuOpen, setIsMobileMenuOpen] = useState(false);
  const { isAuthenticated, logout } = useAuth();
  const navigate = useNavigate();

  const handleLogout = () => {
    logout();
    navigate('/');
  };

  return (
    <nav className="navbar bg-base-100/80 backdrop-blur-md border-b border-base-content/10 sticky top-0 z-50">
      <div className="container mx-auto px-4">
        <div className="flex-1">
          <Link
            to="/"
            className="flex items-center gap-2 text-xl font-bold hover:opacity-80 transition-opacity"
            data-testid="logo-link"
          >
            <Link2 className="h-6 w-6 text-primary" />
            <span className="hidden sm:inline">URL Shortener</span>
          </Link>
        </div>

        {/* Desktop Navigation */}
        <div className="hidden md:flex items-center gap-4">
          <ThemeToggle />
          {isAuthenticated ? (
            <>
              <Link to="/dashboard" className="btn btn-ghost gap-2">
                <LayoutDashboard className="h-4 w-4" />
                Dashboard
              </Link>
              <FuturisticButton onClick={handleLogout} variant="ghost" size="sm">
                <LogOut className="h-4 w-4 mr-2" />
                Logout
              </FuturisticButton>
            </>
          ) : (
            <>
              <Link
                to="/login"
                className="btn btn-ghost"
                data-testid="login-link"
              >
                Login
              </Link>
              <Link to="/register" data-testid="register-button">
                <FuturisticButton variant="primary">
                  Sign Up
                </FuturisticButton>
              </Link>
            </>
          )}
        </div>

        {/* Mobile Menu Button */}
        <div className="flex md:hidden items-center gap-2">
          <ThemeToggle />
          <button
            className="btn btn-ghost btn-circle"
            onClick={() => setIsMobileMenuOpen(!isMobileMenuOpen)}
            aria-label="Toggle menu"
          >
            {isMobileMenuOpen ? <X className="h-6 w-6" /> : <Menu className="h-6 w-6" />}
          </button>
        </div>
      </div>

      {/* Mobile Menu */}
      <AnimatePresence>
        {isMobileMenuOpen && (
          <motion.div
            initial={{ opacity: 0, height: 0 }}
            animate={{ opacity: 1, height: 'auto' }}
            exit={{ opacity: 0, height: 0 }}
            className="md:hidden absolute top-full left-0 right-0 bg-base-100/95 backdrop-blur-md border-b border-base-content/10"
          >
            <div className="container mx-auto px-4 py-4 flex flex-col gap-2">
              {isAuthenticated ? (
                <>
                  <Link
                    to="/dashboard"
                    className="btn btn-ghost justify-start gap-2"
                    onClick={() => setIsMobileMenuOpen(false)}
                  >
                    <LayoutDashboard className="h-4 w-4" />
                    Dashboard
                  </Link>
                  <button
                    onClick={() => {
                      handleLogout();
                      setIsMobileMenuOpen(false);
                    }}
                    className="btn btn-ghost justify-start gap-2"
                  >
                    <LogOut className="h-4 w-4" />
                    Logout
                  </button>
                </>
              ) : (
                <>
                  <Link
                    to="/login"
                    className="btn btn-ghost justify-start"
                    onClick={() => setIsMobileMenuOpen(false)}
                    data-testid="mobile-login-link"
                  >
                    Login
                  </Link>
                  <Link
                    to="/register"
                    className="btn btn-primary justify-start"
                    onClick={() => setIsMobileMenuOpen(false)}
                    data-testid="mobile-register-button"
                  >
                    Sign Up
                  </Link>
                </>
              )}
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </nav>
  );
};

export default Navbar;
