import { Link } from 'react-router-dom'

interface FooterLink {
  id: string
  label: string
  href: string
}

const footerLinks: FooterLink[] = [
  { id: 'about', label: 'About', href: '/about' },
  { id: 'privacy', label: 'Privacy Policy', href: '/privacy' },
  { id: 'terms', label: 'Terms of Service', href: '/terms' },
]

export default function Footer() {
  const currentYear = new Date().getFullYear()

  return (
    <footer
      className="footer footer-center p-10 bg-base-300 text-base-content"
      role="contentinfo"
    >
      <nav aria-label="Footer navigation" data-testid="footer-links">
        <div className="flex flex-wrap justify-center gap-4">
          {footerLinks.map((link) => (
            <Link
              key={link.id}
              to={link.href}
              data-testid={`footer-link-${link.id}`}
              className="link link-hover"
            >
              {link.label}
            </Link>
          ))}
        </div>
      </nav>
      <aside data-testid="footer-copyright">
        <p>© {currentYear} URL Shortener. All rights reserved.</p>
      </aside>
    </footer>
  )
}

export { footerLinks }
