/**
 * Demo Section Component.
 * Owner: Scenario 4 - URL Demo Section Functionality
 *
 * Allows unauthenticated users to try the URL shortening demo.
 * Validates URL input and redirects to registration with URL preserved.
 */
import { useState, useCallback } from 'react';
import { useNavigate } from 'react-router-dom';
import { motion, AnimatePresence } from 'framer-motion';
import GlassMorphismCard from '../GlassMorphismCard';
import FuturisticButton from '../FuturisticButton';

/**
 * Validates if a string is a valid URL
 */
function isValidUrl(urlString: string): boolean {
  if (!urlString.trim()) return false;
  try {
    const url = new URL(urlString);
    return url.protocol === 'http:' || url.protocol === 'https:';
  } catch {
    return false;
  }
}

export interface DemoSectionProps {
  className?: string;
}

export function DemoSection({ className = '' }: DemoSectionProps) {
  const navigate = useNavigate();
  const [url, setUrl] = useState('');
  const [error, setError] = useState<string | null>(null);
  const [showLoginPrompt, setShowLoginPrompt] = useState(false);

  const handleUrlChange = useCallback((e: React.ChangeEvent<HTMLInputElement>) => {
    setUrl(e.target.value);
    setError(null);
    setShowLoginPrompt(false);
  }, []);

  const handleTryIt = useCallback(() => {
    // Validate empty URL
    if (!url.trim()) {
      setError('Please enter a URL');
      return;
    }

    // Validate URL format
    if (!isValidUrl(url)) {
      setError('Please enter a valid URL (e.g., https://example.com)');
      return;
    }

    // Valid URL - show login prompt and redirect
    setError(null);
    setShowLoginPrompt(true);

    // Redirect to register with URL preserved after 2 seconds
    setTimeout(() => {
      const encodedUrl = encodeURIComponent(url);
      navigate(`/register?url=${encodedUrl}`);
    }, 2000);
  }, [url, navigate]);

  const handleKeyDown = useCallback((e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter') {
      handleTryIt();
    }
  }, [handleTryIt]);

  const isButtonDisabled = !url.trim();

  return (
    <section
      data-testid="demo-section"
      className={`py-16 px-4 ${className}`}
    >
      <div className="max-w-2xl mx-auto">
        <motion.div
          initial={{ opacity: 0, y: 30 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
          transition={{ duration: 0.6 }}
        >
          <h2
            data-testid="demo-title"
            className="text-3xl md:text-4xl font-bold text-center mb-4"
          >
            Try it yourself
          </h2>
          <p
            data-testid="demo-description"
            className="text-base-content/70 text-center mb-8"
          >
            Paste a long URL below and see how easy it is to shorten
          </p>

          <GlassMorphismCard
            data-testid="demo-card"
            className="p-6 md:p-8"
          >
            <div className="flex flex-col gap-4">
              <div className="flex flex-col sm:flex-row gap-3">
                <input
                  type="url"
                  data-testid="demo-url-input"
                  value={url}
                  onChange={handleUrlChange}
                  onKeyDown={handleKeyDown}
                  placeholder="Paste your URL here..."
                  className={`input input-bordered flex-1 bg-base-200/50 ${
                    error ? 'input-error' : ''
                  }`}
                  aria-label="URL input"
                  aria-describedby={error ? 'demo-error' : undefined}
                  aria-invalid={!!error}
                />
                <FuturisticButton
                  variant="primary"
                  size="md"
                  onClick={handleTryIt}
                  disabled={isButtonDisabled}
                  data-testid="demo-try-button"
                  aria-label="Try It"
                >
                  Try It
                </FuturisticButton>
              </div>

              <AnimatePresence mode="wait">
                {error && (
                  <motion.p
                    id="demo-error"
                    data-testid="demo-error"
                    role="alert"
                    initial={{ opacity: 0, y: -10 }}
                    animate={{ opacity: 1, y: 0 }}
                    exit={{ opacity: 0, y: -10 }}
                    className="text-error text-sm"
                  >
                    {error}
                  </motion.p>
                )}

                {showLoginPrompt && (
                  <motion.div
                    data-testid="demo-login-prompt"
                    initial={{ opacity: 0, y: -10 }}
                    animate={{ opacity: 1, y: 0 }}
                    exit={{ opacity: 0, y: -10 }}
                    className="bg-info/10 border border-info/30 rounded-lg p-4 text-center"
                  >
                    <p className="text-info font-medium mb-1">
                      Registration Required
                    </p>
                    <p className="text-sm text-base-content/70">
                      Create a free account to shorten URLs and track analytics.
                      Redirecting you to registration...
                    </p>
                  </motion.div>
                )}
              </AnimatePresence>
            </div>
          </GlassMorphismCard>
        </motion.div>
      </div>
    </section>
  );
}

export default DemoSection;
