import HeroCTA from './HeroCTA';
import { BRAND } from '../../utils/constants';

/**
 * Hero section.
 * Owner: Scenario 1 - Page Branding and Hero Display.
 *
 * Renders the MirDB product name as the page h1, a tagline that explicitly
 * mentions URL shortening and analytics, a longer value-proposition
 * paragraph, and a slot for HeroCTA (owned by Scenario 2).
 */
export default function Hero() {
  return (
    <section
      data-testid="hero"
      aria-labelledby="hero-heading"
      className="hero min-h-[60vh] flex flex-col items-center justify-center text-center px-4 py-16"
    >
      <h1
        id="hero-heading"
        data-testid="hero-heading"
        className="text-4xl sm:text-5xl md:text-6xl font-bold mb-4 tracking-tight"
      >
        {BRAND.name}
      </h1>
      <p
        data-testid="hero-tagline"
        className="text-xl sm:text-2xl mb-6 max-w-2xl font-medium opacity-90"
      >
        URL shortening with powerful analytics and click tracking.
      </p>
      <p
        data-testid="hero-value-prop"
        className="text-base sm:text-lg mb-8 max-w-xl opacity-80 leading-relaxed"
      >
        Transform long links into short, memorable URLs. Track every click with
        detailed analytics, understand your audience, and share insights with
        your team — all from one dashboard.
      </p>
      <HeroCTA />
    </section>
  );
}
