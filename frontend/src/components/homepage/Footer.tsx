/**
 * Footer Section Component
 * Owner: Scenario 15 - Footer Section
 *
 * Homepage footer containing links and product description
 *
 * Related Requirements: Footer Section requirements
 */

import React from 'react';
import { Link } from 'react-router-dom';

const Footer: React.FC = () => {
  return (
    <footer className="footer footer-center p-10 bg-base-200 text-base-content">
      <nav className="grid grid-flow-col gap-4">
        <Link to="/login" className="link link-hover">
          Login
        </Link>
        <Link to="/register" className="link link-hover">
          Register
        </Link>
      </nav>
      <aside>
        <p className="max-w-md text-center">
          URL Shortener - A modern URL shortening service with powerful analytics,
          geographic insights, and link management capabilities.
        </p>
        <p className="mt-4">
          Copyright &copy; {new Date().getFullYear()} - All rights reserved
        </p>
      </aside>
    </footer>
  );
};

export default Footer;
