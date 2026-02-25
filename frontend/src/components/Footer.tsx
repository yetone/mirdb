/**
 * Footer Component
 * Owner: Scenario 6 - Footer Section Display
 *
 * Site-wide footer with product information and links.
 *
 * Content:
 * - Product name and copyright year
 * - Links: About, Privacy Policy, Terms of Service
 * - Social media icon placeholders
 *
 * Notes:
 * - Reusable across all pages
 * - Respects theme context (light/dark)
 */

// Twitter/X icon
function TwitterIcon() {
  return (
    <svg
      xmlns="http://www.w3.org/2000/svg"
      className="h-6 w-6"
      fill="currentColor"
      viewBox="0 0 24 24"
      aria-hidden="true"
    >
      <path d="M18.244 2.25h3.308l-7.227 8.26 8.502 11.24H16.17l-5.214-6.817L4.99 21.75H1.68l7.73-8.835L1.254 2.25H8.08l4.713 6.231zm-1.161 17.52h1.833L7.084 4.126H5.117z" />
    </svg>
  )
}

// GitHub icon
function GitHubIcon() {
  return (
    <svg
      xmlns="http://www.w3.org/2000/svg"
      className="h-6 w-6"
      fill="currentColor"
      viewBox="0 0 24 24"
      aria-hidden="true"
    >
      <path d="M12 0c-6.626 0-12 5.373-12 12 0 5.302 3.438 9.8 8.207 11.387.599.111.793-.261.793-.577v-2.234c-3.338.726-4.033-1.416-4.033-1.416-.546-1.387-1.333-1.756-1.333-1.756-1.089-.745.083-.729.083-.729 1.205.084 1.839 1.237 1.839 1.237 1.07 1.834 2.807 1.304 3.492.997.107-.775.418-1.305.762-1.604-2.665-.305-5.467-1.334-5.467-5.931 0-1.311.469-2.381 1.236-3.221-.124-.303-.535-1.524.117-3.176 0 0 1.008-.322 3.301 1.23.957-.266 1.983-.399 3.003-.404 1.02.005 2.047.138 3.006.404 2.291-1.552 3.297-1.23 3.297-1.23.653 1.653.242 2.874.118 3.176.77.84 1.235 1.911 1.235 3.221 0 4.609-2.807 5.624-5.479 5.921.43.372.823 1.102.823 2.222v3.293c0 .319.192.694.801.576 4.765-1.589 8.199-6.086 8.199-11.386 0-6.627-5.373-12-12-12z" />
    </svg>
  )
}

// LinkedIn icon
function LinkedInIcon() {
  return (
    <svg
      xmlns="http://www.w3.org/2000/svg"
      className="h-6 w-6"
      fill="currentColor"
      viewBox="0 0 24 24"
      aria-hidden="true"
    >
      <path d="M20.447 20.452h-3.554v-5.569c0-1.328-.027-3.037-1.852-3.037-1.853 0-2.136 1.445-2.136 2.939v5.667H9.351V9h3.414v1.561h.046c.477-.9 1.637-1.85 3.37-1.85 3.601 0 4.267 2.37 4.267 5.455v6.286zM5.337 7.433c-1.144 0-2.063-.926-2.063-2.065 0-1.138.92-2.063 2.063-2.063 1.14 0 2.064.925 2.064 2.063 0 1.139-.925 2.065-2.064 2.065zm1.782 13.019H3.555V9h3.564v11.452zM22.225 0H1.771C.792 0 0 .774 0 1.729v20.542C0 23.227.792 24 1.771 24h20.451C23.2 24 24 23.227 24 22.271V1.729C24 .774 23.2 0 22.222 0h.003z" />
    </svg>
  )
}

interface FooterLink {
  label: string
  href: string
}

const footerLinks: FooterLink[] = [
  { label: 'About', href: '/about' },
  { label: 'Privacy Policy', href: '/privacy' },
  { label: 'Terms of Service', href: '/terms' },
]

interface SocialLink {
  name: string
  href: string
  icon: React.ReactNode
}

const socialLinks: SocialLink[] = [
  { name: 'Twitter', href: 'https://twitter.com', icon: <TwitterIcon /> },
  { name: 'GitHub', href: 'https://github.com', icon: <GitHubIcon /> },
  { name: 'LinkedIn', href: 'https://linkedin.com', icon: <LinkedInIcon /> },
]

export default function Footer() {
  const currentYear = new Date().getFullYear()

  return (
    <footer
      className="footer footer-center p-10 bg-base-200 text-base-content"
      data-testid="footer"
    >
      <div className="container mx-auto max-w-6xl">
        {/* Product name and copyright */}
        <div data-testid="footer-brand">
          <p className="font-bold text-lg" data-testid="footer-product-name">
            URL Shortener
          </p>
          <p className="text-base-content/70" data-testid="footer-copyright">
            © {currentYear} URL Shortener. All rights reserved.
          </p>
        </div>

        {/* Navigation links */}
        <nav aria-label="Footer navigation">
          <ul
            className="flex flex-wrap justify-center gap-4 md:gap-8"
            data-testid="footer-links"
          >
            {footerLinks.map((link) => (
              <li key={link.href}>
                <a
                  href={link.href}
                  className="link link-hover hover:text-primary transition-colors"
                  data-testid={`footer-link-${link.label.toLowerCase().replace(/\s+/g, '-')}`}
                >
                  {link.label}
                </a>
              </li>
            ))}
          </ul>
        </nav>

        {/* Social media icons */}
        <div data-testid="footer-social">
          <div className="flex gap-4">
            {socialLinks.map((social) => (
              <a
                key={social.name}
                href={social.href}
                className="link link-hover hover:text-primary transition-colors"
                aria-label={social.name}
                target="_blank"
                rel="noopener noreferrer"
                data-testid={`footer-social-${social.name.toLowerCase()}`}
              >
                {social.icon}
              </a>
            ))}
          </div>
        </div>
      </div>
    </footer>
  )
}
