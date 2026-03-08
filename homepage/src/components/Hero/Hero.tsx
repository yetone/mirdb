/**
 * Hero section component for MirDB homepage.
 * Owner: Scenario 2 - Hero Section with Value Proposition
 *
 * Requirements:
 * - Display clear value proposition (REQ-2)
 * - Mention "persistent key-value store"
 * - Mention "Memcached protocol compatibility"
 * - Include primary CTA button linking to Getting Started
 */
import React from 'react'
import { Button } from '../common'

export const Hero: React.FC = () => {
  return (
    <section
      id="hero"
      className="relative min-h-[80vh] flex items-center justify-center bg-gradient-to-br from-slate-900 via-slate-800 to-slate-900"
      aria-labelledby="hero-headline"
    >
      <div className="absolute inset-0 bg-[radial-gradient(ellipse_at_center,_var(--tw-gradient-stops))] from-blue-900/20 via-transparent to-transparent" />

      <div className="relative z-10 max-w-4xl mx-auto px-4 text-center">
        <h1
          id="hero-headline"
          className="text-4xl md:text-5xl lg:text-6xl font-bold mb-6 bg-clip-text text-transparent bg-gradient-to-r from-white to-slate-300"
        >
          A Persistent Key-Value Store with Memcached Protocol
        </h1>

        <p className="text-lg md:text-xl text-slate-300 mb-8 max-w-2xl mx-auto leading-relaxed">
          MirDB combines the simplicity of the Memcached protocol with durable data persistence.
          Built with Rust for performance and reliability, featuring LSM-tree architecture
          and SSTable storage.
        </p>

        <div className="flex flex-col sm:flex-row gap-4 justify-center">
          <Button
            href="#getting-started"
            variant="primary"
            size="lg"
            aria-label="Get started with MirDB"
          >
            Get Started
          </Button>

          <Button
            href="https://github.com/mzry/mirdb"
            variant="secondary"
            size="lg"
            aria-label="View MirDB on GitHub"
          >
            View on GitHub
          </Button>
        </div>
      </div>
    </section>
  )
}
