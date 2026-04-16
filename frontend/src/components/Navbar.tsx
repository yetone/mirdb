import { Link } from 'react-router-dom';
import { useAuth } from '../contexts/AuthContext';
import ThemeToggle from './ThemeToggle';

export default function Navbar() {
  const { isAuthenticated, logout } = useAuth();

  return (
    <nav className="navbar bg-base-100/80 backdrop-blur-md sticky top-0 z-50 border-b border-base-300">
      <div className="flex-1">
        <Link to="/" className="btn btn-ghost text-xl font-bold">
          ShortURL
        </Link>
      </div>
      <div className="flex-none gap-2">
        <ThemeToggle />
        {isAuthenticated ? (
          <>
            <Link to="/dashboard" className="btn btn-ghost">
              Dashboard
            </Link>
            <button onClick={logout} className="btn btn-ghost">
              Logout
            </button>
          </>
        ) : (
          <>
            <Link to="/login" className="btn btn-ghost">
              Login
            </Link>
            <Link to="/register" className="btn btn-primary">
              Register
            </Link>
          </>
        )}
      </div>
    </nav>
  );
}
