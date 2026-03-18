/**
 * Hero Section Component.
 * Owner: Scenario 1 - Hero Section and Branding
 *
 * Displays:
 * - MirDB logo
 * - Product name (h1)
 * - Tagline describing value proposition
 * - Optional CTA button
 */

import { Button } from '../ui/Button';
import { GITHUB_URL } from '../../utils/constants';

export function Hero() {
  return (
    <header
      id="hero"
      className="min-h-screen flex flex-col items-center justify-center px-4 py-16 bg-gradient-to-b from-slate-900 to-slate-800 dark:from-slate-950 dark:to-slate-900"
    >
      <div className="text-center max-w-4xl mx-auto">
        {/* Logo */}
        <img
          src="/assets/logo.svg"
          alt="MirDB Logo"
          className="w-24 h-24 md:w-32 md:h-32 mx-auto mb-8"
        />

        {/* Product Name */}
        <h1 className="text-4xl md:text-6xl font-bold text-white mb-6">
          MirDB
        </h1>

        {/* Tagline */}
        <p className="text-xl md:text-2xl text-slate-300 mb-4">
          Persistent Key-Value Store with Memcached Protocol
        </p>

        {/* Sub-tagline */}
        <p className="text-lg text-slate-400 mb-8 max-w-2xl mx-auto">
          A lightweight, high-performance key-value database built with Rust and Tokio,
          featuring LSM-tree storage and full Memcached protocol compatibility.
        </p>

        {/* CTA Buttons */}
        <div className="flex flex-col sm:flex-row gap-4 justify-center">
          <Button
            variant="primary"
            size="lg"
            onClick={() => window.location.href = '#quickstart'}
          >
            Get Started
          </Button>
          <Button
            variant="outline"
            size="lg"
            onClick={() => window.open(GITHUB_URL, '_blank')}
          >
            View on GitHub
          </Button>
        </div>
      </div>
    </header>
  );
}
