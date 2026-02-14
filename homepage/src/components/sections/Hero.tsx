import React from 'react';
import { Button } from '../common/Button';
import { GITHUB_URL, TAGLINE } from '../../utils/constants';

export const Hero: React.FC = () => {
  return (
    <section
      className="relative min-h-screen flex flex-col items-center justify-center px-4 py-16 bg-gradient-to-br from-blue-50 to-indigo-100 dark:from-gray-900 dark:to-gray-800"
      aria-labelledby="hero-heading"
    >
      <div className="max-w-4xl mx-auto text-center">
        <img
          src="/logo.gif"
          alt="MirDB Logo"
          className="w-32 h-32 mx-auto mb-8 object-contain"
        />

        <h1 id="hero-heading" className="text-4xl md:text-6xl font-bold text-gray-900 dark:text-white mb-4">
          MirDB
        </h1>

        <p className="text-xl md:text-2xl text-gray-600 dark:text-gray-300 mb-8">
          {TAGLINE}
        </p>

        <div className="flex flex-col sm:flex-row gap-4 justify-center">
          <Button href="#quickstart" variant="primary">
            Get Started
          </Button>
          <Button href={GITHUB_URL} variant="secondary">
            View on GitHub
          </Button>
        </div>
      </div>
    </section>
  );
};
