/**
 * Footer Component.
 * Owner: Scenario 6 - Footer Section
 *
 * Requirements:
 * - Navigation links: Home, Login, Register
 * - Branding/logo
 * - Copyright notice
 *
 * Expected exports:
 * - Footer: React.FC
 */

import { Link } from 'react-router-dom';
import { motion } from 'framer-motion';

export function Footer() {
  const currentYear = new Date().getFullYear();

  return (
    <footer
      className="bg-base-200 border-t border-base-300 py-12"
      data-testid="footer"
      aria-label="Site footer"
    >
      <div className="container mx-auto px-4">
        <div className="flex flex-col md:flex-row items-center justify-between gap-8">
          {/* Branding/Logo */}
          <motion.div
            initial={{ opacity: 0, x: -20 }}
            whileInView={{ opacity: 1, x: 0 }}
            viewport={{ once: true }}
            transition={{ duration: 0.5 }}
            className="flex items-center gap-2"
          >
            <div
              className="text-2xl font-bold text-primary"
              data-testid="footer-logo"
            >
              LinkShort
            </div>
          </motion.div>

          {/* Navigation Links */}
          <motion.div
            initial={{ opacity: 0, y: 20 }}
            whileInView={{ opacity: 1, y: 0 }}
            viewport={{ once: true }}
            transition={{ duration: 0.5, delay: 0.1 }}
          >
            <nav aria-label="Footer navigation">
              <ul className="flex flex-wrap items-center justify-center gap-6">
                <li>
                  <Link
                    to="/"
                    className="text-base-content/70 hover:text-primary transition-colors"
                    data-testid="footer-home-link"
                  >
                    Home
                  </Link>
                </li>
                <li>
                  <Link
                    to="/login"
                    className="text-base-content/70 hover:text-primary transition-colors"
                    data-testid="footer-login-link"
                  >
                    Login
                  </Link>
                </li>
                <li>
                  <Link
                    to="/register"
                    className="text-base-content/70 hover:text-primary transition-colors"
                    data-testid="footer-register-link"
                  >
                    Register
                  </Link>
                </li>
              </ul>
            </nav>
          </motion.div>

          {/* Copyright Notice */}
          <motion.div
            initial={{ opacity: 0, x: 20 }}
            whileInView={{ opacity: 1, x: 0 }}
            viewport={{ once: true }}
            transition={{ duration: 0.5, delay: 0.2 }}
          >
            <p
              className="text-base-content/50 text-sm"
              data-testid="footer-copyright"
            >
              &copy; {currentYear} LinkShort. All rights reserved.
            </p>
          </motion.div>
        </div>
      </div>
    </footer>
  );
}

export default Footer;
