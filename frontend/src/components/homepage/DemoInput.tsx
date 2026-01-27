/**
 * Demo URL Input Component
 * Owner: Scenario 6 - Demo URL Input (Teaser)
 *
 * Requirements: REQ-10
 *
 * Expected functionality:
 * - URL input field (non-functional for unauthenticated users)
 * - "Shorten" button that prompts sign-up
 * - Visual teaser of the shortening experience
 * - On submit: redirect to /register or show modal
 * - Input validation feedback
 * - Accessible form controls
 *
 * Note: This is a "Could" priority feature
 */

import { useState, useCallback } from 'react';
import { motion } from 'framer-motion';
import { useNavigate } from 'react-router-dom';
import { Link2, ArrowRight, AlertCircle } from 'lucide-react';
import { GlassMorphismCard } from '../GlassMorphismCard';
import { FuturisticButton } from '../FuturisticButton';

interface DemoInputProps {
  onSubmit?: (url: string) => void;
}

/**
 * Validates if a string is a valid URL
 */
function isValidUrl(string: string): boolean {
  if (!string || string.trim() === '') {
    return false;
  }
  try {
    const url = new URL(string);
    return url.protocol === 'http:' || url.protocol === 'https:';
  } catch {
    return false;
  }
}

export function DemoInput({ onSubmit }: DemoInputProps) {
  const navigate = useNavigate();
  const [url, setUrl] = useState('');
  const [error, setError] = useState<string | null>(null);
  const [touched, setTouched] = useState(false);

  const validateUrl = useCallback((value: string) => {
    if (!value || value.trim() === '') {
      return 'Please enter a URL';
    }
    if (!isValidUrl(value)) {
      return 'Please enter a valid URL (e.g., https://example.com)';
    }
    return null;
  }, []);

  const handleInputChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const value = e.target.value;
    setUrl(value);
    if (touched) {
      setError(validateUrl(value));
    }
  };

  const handleInputBlur = () => {
    setTouched(true);
    if (url) {
      setError(validateUrl(url));
    }
  };

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    setTouched(true);

    const validationError = validateUrl(url);
    if (validationError) {
      setError(validationError);
      return;
    }

    // Clear any previous error
    setError(null);

    // Call optional callback
    if (onSubmit) {
      onSubmit(url);
    }

    // Redirect to register - this is a teaser for unauthenticated users
    navigate('/register');
  };

  const hasError = touched && error !== null;

  return (
    <section
      data-testid="demo-input-section"
      id="demo"
      className="py-16 px-4 sm:px-6 lg:px-8"
    >
      <div className="max-w-2xl mx-auto">
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
          transition={{ duration: 0.5 }}
        >
          <GlassMorphismCard className="p-8">
            <div className="text-center mb-6">
              <div className="inline-flex items-center justify-center w-12 h-12 rounded-full bg-primary/10 mb-4">
                <Link2 className="w-6 h-6 text-primary" />
              </div>
              <h2 className="text-2xl font-bold mb-2">Try It Now</h2>
              <p className="text-base-content/70">
                Enter a URL below to see how easy it is to create short links
              </p>
            </div>

            <form onSubmit={handleSubmit} noValidate>
              <div className="flex flex-col gap-4">
                <div className="flex flex-col sm:flex-row gap-3">
                  <div className="flex-1">
                    <label htmlFor="demo-url-input" className="sr-only">
                      Enter URL to shorten
                    </label>
                    <input
                      id="demo-url-input"
                      type="url"
                      value={url}
                      onChange={handleInputChange}
                      onBlur={handleInputBlur}
                      placeholder="https://example.com/your-long-url"
                      aria-label="Enter URL to shorten"
                      aria-invalid={hasError}
                      aria-describedby={hasError ? 'demo-url-error' : undefined}
                      data-testid="demo-url-input"
                      className={`input input-bordered w-full ${
                        hasError
                          ? 'input-error border-error focus:border-error'
                          : 'focus:input-primary'
                      }`}
                    />
                  </div>
                  <FuturisticButton
                    type="submit"
                    variant="primary"
                    size="md"
                    data-testid="demo-shorten-button"
                    className="gap-2"
                  >
                    Shorten
                    <ArrowRight className="w-4 h-4" />
                  </FuturisticButton>
                </div>

                {hasError && (
                  <motion.div
                    initial={{ opacity: 0, y: -10 }}
                    animate={{ opacity: 1, y: 0 }}
                    className="flex items-center gap-2 text-error text-sm"
                    id="demo-url-error"
                    role="alert"
                    data-testid="demo-url-error"
                  >
                    <AlertCircle className="w-4 h-4 flex-shrink-0" />
                    <span>{error}</span>
                  </motion.div>
                )}
              </div>
            </form>

            <p className="text-center text-sm text-base-content/50 mt-6">
              Sign up to start creating and tracking your short links
            </p>
          </GlassMorphismCard>
        </motion.div>
      </div>
    </section>
  );
}
