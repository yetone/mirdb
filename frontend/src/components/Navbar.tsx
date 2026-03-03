import { Link } from 'react-router-dom';
import { ThemeToggle } from './ThemeToggle';
import { useAuth } from '../contexts/AuthContext';

export function Navbar() {
  const { isAuthenticated, logout, user } = useAuth();

  return (
    <div className="navbar bg-base-100/50 backdrop-blur-lg fixed top-0 z-50 border-b border-base-300">
      <div className="navbar-start">
        <Link to="/" className="btn btn-ghost text-xl font-bold">
          <span className="text-primary">URL</span>Shortener
        </Link>
      </div>

      <div className="navbar-end gap-1 sm:gap-2">
        <ThemeToggle />
        {isAuthenticated ? (
          <>
            <span className="text-sm hidden sm:inline-block">
              Hello, {user?.username}
            </span>
            <Link to="/dashboard" className="btn btn-ghost btn-sm min-h-[44px] min-w-[44px] touch-manipulation">
              Dashboard
            </Link>
            <button onClick={logout} className="btn btn-outline btn-sm min-h-[44px] min-w-[44px] touch-manipulation">
              Logout
            </button>
          </>
        ) : (
          <>
            <Link to="/login" className="btn btn-ghost btn-sm min-h-[44px] min-w-[44px] touch-manipulation font-semibold">
              Sign In
            </Link>
            <Link to="/register" className="btn btn-primary btn-sm min-h-[44px] min-w-[44px] touch-manipulation font-bold text-white">
              Get Started
            </Link>
          </>
        )}
      </div>
    </div>
  );
}
