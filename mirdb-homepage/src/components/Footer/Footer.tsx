/**
 * Footer Section Component (React).
 * Owner: Scenario 9 - Footer Section
 *
 * Expected features:
 * - GitHub repository link
 * - License information
 * - Acknowledgments section
 * - Copyright notice
 */

export interface FooterProps {
  className?: string;
  githubUrl?: string;
  licenseType?: string;
  licenseUrl?: string;
}

const currentYear = new Date().getFullYear();

export function Footer({
  className = '',
  githubUrl = 'https://github.com/mirdb/mirdb',
  licenseType = 'MIT',
  licenseUrl = 'https://opensource.org/licenses/MIT',
}: FooterProps) {
  return (
    <footer
      className={`bg-surface border-t border-border py-12 ${className}`}
      role="contentinfo"
      aria-label="Site footer"
    >
      <div className="container mx-auto px-4">
        <div className="grid grid-cols-1 md:grid-cols-3 gap-8">
          {/* Project Info */}
          <div className="space-y-4">
            <h3 className="text-lg font-mono font-semibold text-text-primary">
              MirDB
            </h3>
            <p className="text-text-secondary text-sm">
              A persistent key-value store with Memcached protocol compatibility,
              built with an LSM-tree architecture for high write throughput.
            </p>
          </div>

          {/* Links */}
          <div className="space-y-4">
            <h3 className="text-lg font-mono font-semibold text-text-primary">
              Links
            </h3>
            <nav aria-label="Footer navigation">
              <ul className="space-y-2">
                <li>
                  <a
                    href={githubUrl}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="text-text-secondary hover:text-accent transition-colors inline-flex items-center gap-2"
                    aria-label="View MirDB on GitHub (opens in new tab)"
                  >
                    <svg
                      className="w-5 h-5"
                      fill="currentColor"
                      viewBox="0 0 24 24"
                      aria-hidden="true"
                    >
                      <path
                        fillRule="evenodd"
                        clipRule="evenodd"
                        d="M12 2C6.477 2 2 6.484 2 12.017c0 4.425 2.865 8.18 6.839 9.504.5.092.682-.217.682-.483 0-.237-.008-.868-.013-1.703-2.782.605-3.369-1.343-3.369-1.343-.454-1.158-1.11-1.466-1.11-1.466-.908-.62.069-.608.069-.608 1.003.07 1.531 1.032 1.531 1.032.892 1.53 2.341 1.088 2.91.832.092-.647.35-1.088.636-1.338-2.22-.253-4.555-1.113-4.555-4.951 0-1.093.39-1.988 1.029-2.688-.103-.253-.446-1.272.098-2.65 0 0 .84-.27 2.75 1.026A9.564 9.564 0 0112 6.844c.85.004 1.705.115 2.504.337 1.909-1.296 2.747-1.027 2.747-1.027.546 1.379.202 2.398.1 2.651.64.7 1.028 1.595 1.028 2.688 0 3.848-2.339 4.695-4.566 4.943.359.309.678.92.678 1.855 0 1.338-.012 2.419-.012 2.747 0 .268.18.58.688.482A10.019 10.019 0 0022 12.017C22 6.484 17.522 2 12 2z"
                      />
                    </svg>
                    GitHub Repository
                  </a>
                </li>
                <li>
                  <a
                    href="#features"
                    className="text-text-secondary hover:text-accent transition-colors"
                  >
                    Features
                  </a>
                </li>
                <li>
                  <a
                    href="#installation"
                    className="text-text-secondary hover:text-accent transition-colors"
                  >
                    Installation
                  </a>
                </li>
              </ul>
            </nav>
          </div>

          {/* Acknowledgments */}
          <div className="space-y-4">
            <h3 className="text-lg font-mono font-semibold text-text-primary">
              Acknowledgments
            </h3>
            <div className="text-text-secondary text-sm space-y-2" data-testid="acknowledgments">
              <p>
                Built with Rust for performance and reliability.
              </p>
              <p>
                Powered by{' '}
                <a
                  href="https://tokio.rs"
                  target="_blank"
                  rel="noopener noreferrer"
                  className="text-accent hover:underline"
                >
                  Tokio
                </a>{' '}
                async runtime.
              </p>
              <p>
                Special thanks to all contributors.
              </p>
            </div>
          </div>
        </div>

        {/* Bottom Section */}
        <div className="mt-12 pt-8 border-t border-border flex flex-col md:flex-row justify-between items-center gap-4">
          {/* Copyright */}
          <p className="text-text-secondary text-sm">
            © {currentYear} MirDB. All rights reserved.
          </p>

          {/* License */}
          <div className="flex items-center gap-2 text-sm">
            <span className="text-text-secondary">Released under the</span>
            <a
              href={licenseUrl}
              target="_blank"
              rel="noopener noreferrer"
              className="text-accent hover:underline font-medium"
              data-testid="license-link"
            >
              {licenseType} License
            </a>
          </div>
        </div>
      </div>
    </footer>
  );
}

export default Footer;
