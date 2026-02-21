import React, { useState, FormEvent, ChangeEvent } from 'react';
import { Link } from 'react-router-dom';
import { GlassMorphismCard } from '../common/GlassMorphismCard';
import { FuturisticButton } from '../common/FuturisticButton';

interface UrlDemoSectionProps {
  onRegisterPrompt?: () => void;
}

function isValidUrl(urlString: string): boolean {
  try {
    const url = new URL(urlString);
    return url.protocol === 'http:' || url.protocol === 'https:';
  } catch {
    return false;
  }
}

function generateShortCode(): string {
  const chars = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
  let result = '';
  for (let i = 0; i < 7; i++) {
    result += chars.charAt(Math.floor(Math.random() * chars.length));
  }
  return result;
}

export function UrlDemoSection({ onRegisterPrompt }: UrlDemoSectionProps) {
  const [url, setUrl] = useState('');
  const [error, setError] = useState('');
  const [shortenedUrl, setShortenedUrl] = useState('');
  const [showRegistrationPrompt, setShowRegistrationPrompt] = useState(false);

  const handleUrlChange = (e: ChangeEvent<HTMLInputElement>) => {
    setUrl(e.target.value);
    if (error) {
      setError('');
    }
  };

  const handleSubmit = (e: FormEvent) => {
    e.preventDefault();

    // Reset previous state
    setError('');
    setShortenedUrl('');
    setShowRegistrationPrompt(false);

    // Validate URL
    if (!url.trim()) {
      setError('Please enter a URL');
      return;
    }

    if (!isValidUrl(url)) {
      setError('Please enter a valid URL (must start with http:// or https://)');
      return;
    }

    // Generate demo shortened URL
    const shortCode = generateShortCode();
    setShortenedUrl(`https://short.url/${shortCode}`);
    setShowRegistrationPrompt(true);
  };

  const handleRegisterClick = () => {
    if (onRegisterPrompt) {
      onRegisterPrompt();
    }
  };

  return (
    <section
      className="py-16 px-4"
      data-testid="url-demo-section"
      aria-labelledby="demo-headline"
    >
      <div className="max-w-2xl mx-auto">
        <GlassMorphismCard testId="url-demo-card">
          <div className="text-center mb-8">
            <h2
              id="demo-headline"
              className="text-2xl font-bold mb-2"
              data-testid="demo-headline"
            >
              Try It Out
            </h2>
            <p className="text-base-content/70" data-testid="demo-description">
              See how easy it is to shorten your URLs. Enter a URL below to try our service.
            </p>
          </div>

          <form onSubmit={handleSubmit} data-testid="url-demo-form">
            <div className="flex flex-col sm:flex-row gap-4">
              <input
                type="text"
                value={url}
                onChange={handleUrlChange}
                placeholder="Enter your long URL here..."
                className={`input input-bordered flex-grow ${error ? 'input-error' : ''}`}
                data-testid="url-demo-input"
                aria-label="URL to shorten"
                aria-invalid={!!error}
                aria-describedby={error ? 'url-error' : undefined}
              />
              <FuturisticButton
                type="submit"
                variant="primary"
                data-testid="url-demo-submit"
                aria-label="Shorten URL"
              >
                Shorten
              </FuturisticButton>
            </div>

            {error && (
              <p
                id="url-error"
                className="text-error mt-2 text-sm"
                data-testid="url-demo-error"
                role="alert"
              >
                {error}
              </p>
            )}
          </form>

          {shortenedUrl && (
            <div className="mt-6" data-testid="url-demo-result">
              <div className="bg-base-200/50 rounded-lg p-4 mb-4">
                <p className="text-sm text-base-content/70 mb-1">Your shortened URL:</p>
                <p className="text-lg font-mono text-primary" data-testid="shortened-url">
                  {shortenedUrl}
                </p>
              </div>

              {showRegistrationPrompt && (
                <div
                  className="text-center p-4 border border-primary/20 rounded-lg bg-primary/5"
                  data-testid="registration-prompt"
                >
                  <p className="mb-3 text-base-content/80">
                    This is a demo preview. Register for free to create real shortened URLs with full analytics!
                  </p>
                  <Link
                    to="/register"
                    onClick={handleRegisterClick}
                    className="btn btn-primary min-h-[44px] transition-transform duration-200 hover:scale-105 focus:ring-2 focus:ring-offset-2"
                    data-testid="demo-register-cta"
                    aria-label="Register to create real shortened URLs"
                  >
                    Get Started Free
                  </Link>
                </div>
              )}
            </div>
          )}
        </GlassMorphismCard>
      </div>
    </section>
  );
}
