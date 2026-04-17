/**
 * Footer Component
 * Owner: Scenario 6 - Footer Navigation and Content
 *
 * Homepage footer with:
 * - Navigation links (Home, Features, Pricing, About, Contact)
 * - Legal links (Privacy Policy, Terms of Service)
 * - Social media links
 * - Copyright notice with current year
 *
 * Related requirements: REQ-7
 */

import { motion } from 'framer-motion';
import { Twitter, Github, Linkedin, Facebook } from 'lucide-react';
import type { FooterLink } from '@/types/homepage';

const navigationLinks: FooterLink[] = [
  { label: 'Home', href: '/' },
  { label: 'Features', href: '/#features' },
  { label: 'Pricing', href: '/pricing' },
  { label: 'About', href: '/about' },
  { label: 'Contact', href: '/contact' },
];

const legalLinks: FooterLink[] = [
  { label: 'Privacy Policy', href: '/privacy' },
  { label: 'Terms of Service', href: '/terms' },
];

const socialLinks = [
  { label: 'Twitter', href: 'https://twitter.com', icon: Twitter, external: true },
  { label: 'GitHub', href: 'https://github.com', icon: Github, external: true },
  { label: 'LinkedIn', href: 'https://linkedin.com', icon: Linkedin, external: true },
  { label: 'Facebook', href: 'https://facebook.com', icon: Facebook, external: true },
];

const footerVariants = {
  hidden: { opacity: 0, y: 20 },
  visible: {
    opacity: 1,
    y: 0,
    transition: {
      duration: 0.5,
      staggerChildren: 0.1,
    },
  },
};

const itemVariants = {
  hidden: { opacity: 0, y: 10 },
  visible: {
    opacity: 1,
    y: 0,
    transition: { duration: 0.3 },
  },
};

export function Footer() {
  const currentYear = new Date().getFullYear();

  return (
    <motion.footer
      className="bg-base-200 py-12 px-4"
      data-testid="footer"
      variants={footerVariants}
      initial="hidden"
      whileInView="visible"
      viewport={{ once: true, margin: '-50px' }}
      role="contentinfo"
      aria-label="Site footer"
    >
      <div className="container mx-auto max-w-6xl">
        <div className="grid grid-cols-1 md:grid-cols-3 gap-8 mb-8">
          {/* Navigation Links */}
          <motion.div variants={itemVariants}>
            <h3 className="text-lg font-semibold mb-4" data-testid="footer-nav-heading">
              Quick Links
            </h3>
            <nav aria-label="Footer navigation">
              <ul className="space-y-2" data-testid="footer-nav-links">
                {navigationLinks.map((link) => (
                  <li key={link.href}>
                    <a
                      href={link.href}
                      className="link link-hover text-base-content/70 hover:text-primary transition-colors"
                      data-testid={`footer-link-${link.label.toLowerCase()}`}
                    >
                      {link.label}
                    </a>
                  </li>
                ))}
              </ul>
            </nav>
          </motion.div>

          {/* Legal Links */}
          <motion.div variants={itemVariants}>
            <h3 className="text-lg font-semibold mb-4" data-testid="footer-legal-heading">
              Legal
            </h3>
            <nav aria-label="Legal links">
              <ul className="space-y-2" data-testid="footer-legal-links">
                {legalLinks.map((link) => (
                  <li key={link.href}>
                    <a
                      href={link.href}
                      className="link link-hover text-base-content/70 hover:text-primary transition-colors"
                      data-testid={`footer-link-${link.label.toLowerCase().replace(/\s+/g, '-')}`}
                    >
                      {link.label}
                    </a>
                  </li>
                ))}
              </ul>
            </nav>
          </motion.div>

          {/* Social Links */}
          <motion.div variants={itemVariants}>
            <h3 className="text-lg font-semibold mb-4" data-testid="footer-social-heading">
              Connect With Us
            </h3>
            <nav aria-label="Social media links">
              <ul className="flex space-x-4" data-testid="footer-social-links">
                {socialLinks.map((link) => {
                  const Icon = link.icon;
                  return (
                    <li key={link.href}>
                      <a
                        href={link.href}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="btn btn-ghost btn-circle hover:bg-primary/10 transition-colors"
                        aria-label={link.label}
                        data-testid={`footer-social-${link.label.toLowerCase()}`}
                      >
                        <Icon className="w-5 h-5" aria-hidden="true" />
                      </a>
                    </li>
                  );
                })}
              </ul>
            </nav>
          </motion.div>
        </div>

        {/* Divider */}
        <div className="divider my-4" aria-hidden="true" />

        {/* Copyright */}
        <motion.div
          variants={itemVariants}
          className="text-center text-base-content/60"
        >
          <p data-testid="footer-copyright">
            &copy; {currentYear} LinkShort. All rights reserved.
          </p>
        </motion.div>
      </div>
    </motion.footer>
  );
}
