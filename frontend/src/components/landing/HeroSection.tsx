/**
 * Hero Section Component
 * Owner: Scenario 1 - Hero Section URL Shortening
 *
 * The main hero section of the landing page containing:
 * - Headline and tagline
 * - URL shortening form
 * - Result display with copy functionality
 */

import { HERO_CONTENT } from '../../constants/landingContent';
import { useUrlShortener } from '../../hooks/useUrlShortener';
import { UrlShortenerForm } from './UrlShortenerForm';
import { ShortUrlResult } from './ShortUrlResult';

/**
 * Main hero section component for the landing page
 */
export function HeroSection() {
  const { shortenUrl, isLoading, error, result } = useUrlShortener();

  return (
    <section
      className="min-h-[60vh] flex flex-col items-center justify-center px-4 py-16"
      aria-labelledby="hero-headline"
    >
      <div className="text-center max-w-4xl mx-auto mb-8">
        <h1
          id="hero-headline"
          className="text-4xl sm:text-5xl lg:text-6xl font-bold mb-4 text-base-content"
        >
          {HERO_CONTENT.headline}
        </h1>
        <p className="text-lg sm:text-xl lg:text-2xl text-base-content/70">
          {HERO_CONTENT.tagline}
        </p>
      </div>

      <div className="w-full max-w-2xl mx-auto space-y-6">
        <UrlShortenerForm
          onSubmit={shortenUrl}
          isLoading={isLoading}
          error={error}
        />

        {result && (
          <div className="animate-fade-in" data-testid="result-container">
            <ShortUrlResult shortUrl={result.short_url} />
          </div>
        )}
      </div>
    </section>
  );
}
