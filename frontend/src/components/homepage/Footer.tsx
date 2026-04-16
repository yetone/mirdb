/**
 * Footer Component.
 * Owner: Scenario 10 - Footer Section
 *
 * Displays copyright notice with current year and optional links.
 * Maintains consistent styling with homepage theme.
 */
import { motion } from 'framer-motion';

export interface FooterLink {
  label: string;
  href: string;
}

export interface FooterProps {
  links?: FooterLink[];
}

export function Footer({ links = [] }: FooterProps) {
  const currentYear = new Date().getFullYear();

  return (
    <footer
      data-testid="footer-section"
      className="w-full py-8 px-4 mt-auto border-t border-base-300 bg-base-200/50 backdrop-blur-sm"
    >
      <motion.div
        initial={{ opacity: 0, y: 20 }}
        whileInView={{ opacity: 1, y: 0 }}
        viewport={{ once: true }}
        transition={{ duration: 0.5 }}
        className="max-w-6xl mx-auto"
      >
        <div className="flex flex-col md:flex-row justify-between items-center gap-4">
          <p
            data-testid="footer-copyright"
            className="text-base-content/70 text-sm"
          >
            &copy; {currentYear} ShortURL Service. All rights reserved.
          </p>

          {links.length > 0 && (
            <nav
              data-testid="footer-links"
              className="flex flex-wrap gap-4 justify-center"
              aria-label="Footer navigation"
            >
              {links.map((link, index) => (
                <a
                  key={index}
                  href={link.href}
                  data-testid={`footer-link-${index}`}
                  className="text-sm text-base-content/70 hover:text-primary transition-colors duration-200 focus:outline-none focus:ring-2 focus:ring-primary focus:ring-offset-2 rounded"
                >
                  {link.label}
                </a>
              ))}
            </nav>
          )}
        </div>
      </motion.div>
    </footer>
  );
}

export default Footer;
