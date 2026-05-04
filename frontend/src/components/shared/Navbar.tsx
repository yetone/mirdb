import React from 'react';
import { Link } from 'react-router-dom';
import { useAuth } from '../contexts/AuthContext';

const Navbar: React.FC = () => {
  const { isAuthenticated } = useAuth();

  return (
    <nav className="navbar bg-base-100 shadow-md">
      <div className="flex-1">
        <Link to="/" className="btn btn-ghost text-xl">
          URL Shortener
        </Link>
      </div>
      <div className="flex-none">
        <ul className="menu menu-horizontal px-1">
          {isAuthenticated ? (
            <li>
              <Link to="/dashboard" className="btn btn-primary">
                Go to Dashboard
              </Link>
            </li>
          ) : (
            <>
              <li>
                <Link to="/login" className="btn btn-ghost">
                  Login
                </Link>
              </li>
              <li>
                <Link to="/register" className="btn btn-primary">
                  Get Started Free
                </Link>
              </li>
            </>
          )}
        </ul>
      </div>
    </nav>
  );
};

export default Navbar;