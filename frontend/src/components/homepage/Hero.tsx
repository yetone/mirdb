import HeroCTA from './HeroCTA';
import { BRAND } from '../../utils/constants';

/**
 * Hero section.
 * Owner: Scenario 1 - Page Branding and Hero Display.
 */
export default function Hero() {
  return (
    <section className="hero min-h-[60vh] flex flex-col items-center justify-center text-center px-4">
      <h1 className="text-5xl font-bold mb-4">{BRAND.name}</h1>
      <p className="text-xl mb-6 max-w-2xl">{BRAND.tagline}</p>
      <p className="text-base mb-8 max-w-xl">
        MirDB helps you create short, trackable links and analyze their performance with detailed analytics.
      </p>
      <HeroCTA />
    </section>
  );
}
