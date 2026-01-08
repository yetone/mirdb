import { Link, useLocation } from 'react-router-dom';

interface NavbarProps {
  variant?: 'default' | 'transparent';
}

export default function Navbar({ variant = 'default' }: NavbarProps) {
  const location = useLocation();
  const isHomePage = location.pathname === '/';

  const navBgClass = variant === 'transparent'
    ? 'bg-transparent'
    : 'bg-base-100 shadow-md';

  return (
    <nav
      className={`navbar ${navBgClass} px-4 lg:px-8`}
      role="navigation"
      aria-label="Main navigation"
    >
      <div className="navbar-start">
        <Link
          to="/"
          className="btn btn-ghost text-xl font-bold"
          aria-label="Go to homepage"
        >
          URLShort
        </Link>
      </div>

      <div className="navbar-center hidden lg:flex">
        {isHomePage && (
          <ul className="menu menu-horizontal px-1">
            <li>
              <a href="#features" className="text-base">Features</a>
            </li>
            <li>
              <a href="#how-it-works" className="text-base">How It Works</a>
            </li>
          </ul>
        )}
      </div>

      <div className="navbar-end gap-2">
        <Link
          to="/login"
          className="btn btn-ghost"
          aria-label="Go to login page"
        >
          Login
        </Link>
        <Link
          to="/register"
          className="btn btn-primary"
          aria-label="Go to registration page"
        >
          Sign Up
        </Link>
      </div>

      {/* Mobile menu */}
      <div className="dropdown dropdown-end lg:hidden">
        <label tabIndex={0} className="btn btn-ghost lg:hidden" aria-label="Open menu">
          <svg xmlns="http://www.w3.org/2000/svg" className="h-5 w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M4 6h16M4 12h8m-8 6h16" />
          </svg>
        </label>
        <ul tabIndex={0} className="menu menu-sm dropdown-content mt-3 z-[1] p-2 shadow bg-base-100 rounded-box w-52">
          {isHomePage && (
            <>
              <li><a href="#features">Features</a></li>
              <li><a href="#how-it-works">How It Works</a></li>
            </>
          )}
          <li><Link to="/login">Login</Link></li>
          <li><Link to="/register">Sign Up</Link></li>
        </ul>
      </div>
    </nav>
  );
}
