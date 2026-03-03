/**
 * Footer Component
 * Owner: Scenario 11 - Footer Display
 *
 * Displays footer at bottom of homepage with copyright and links.
 *
 * Requirements:
 * - Copyright information with current year
 * - Links: About, Terms of Service, Privacy Policy, Contact
 * - Theme-aware styling
 * - Responsive layout
 */

export function Footer() {
  const currentYear = new Date().getFullYear();

  const footerLinks = [
    { label: 'About', href: '/about' },
    { label: 'Terms of Service', href: '/terms' },
    { label: 'Privacy Policy', href: '/privacy' },
    { label: 'Contact', href: '/contact' },
  ];

  return (
    <footer
      className="w-full py-8 px-4 bg-base-200 border-t border-base-300"
      data-testid="footer"
    >
      <div className="container mx-auto">
        <div className="flex flex-col md:flex-row justify-between items-center gap-4">
          <div
            className="text-base-content/70 text-sm"
            data-testid="footer-copyright"
          >
            &copy; {currentYear} URL Shortener. All rights reserved.
          </div>
          <nav
            className="flex flex-wrap justify-center gap-4 md:gap-6"
            data-testid="footer-links"
            aria-label="Footer navigation"
          >
            {footerLinks.map((link) => (
              <a
                key={link.label}
                href={link.href}
                className="text-base-content/70 hover:text-primary text-sm transition-colors duration-200"
                data-testid={`footer-link-${link.label.toLowerCase().replace(/\s+/g, '-')}`}
              >
                {link.label}
              </a>
            ))}
          </nav>
        </div>
      </div>
    </footer>
  );
}
