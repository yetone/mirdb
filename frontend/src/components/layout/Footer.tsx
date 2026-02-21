import React from 'react';
import { Link } from 'react-router-dom';

export function Footer() {
  const currentYear = new Date().getFullYear();

  return (
    <footer className="footer footer-center p-10 bg-base-200 text-base-content" data-testid="footer">
      <nav className="grid grid-flow-col gap-4">
        <Link to="/" className="link link-hover">Home</Link>
        <Link to="/login" className="link link-hover">Login</Link>
        <Link to="/register" className="link link-hover">Register</Link>
      </nav>
      <aside>
        <p data-testid="footer-copyright">
          Copyright © {currentYear} - URLShort. All rights reserved.
        </p>
      </aside>
    </footer>
  );
}
