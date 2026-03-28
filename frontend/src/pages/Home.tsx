/**
 * Homepage component.
 * Owner: Scenario 1 - Hero Section (primary assembler)
 *
 * This component assembles all homepage sections:
 * - HeroSection (Scenario 1)
 * - FeaturesSection (Scenario 2) - placeholder
 * - HowItWorksSection (Scenario 3)
 * - StatsSection (Scenario 4) - placeholder
 * - Footer (Scenario 6)
 */

import React from 'react'
import { HeroSection } from '../components/homepage/HeroSection'
import HowItWorksSection from '../components/homepage/HowItWorksSection'
import { Footer } from '../components/homepage/Footer'

const Home: React.FC = () => {
  return (
    <main className="min-h-screen bg-base-100" data-testid="home-page">
      <HeroSection />

      <section className="py-16 px-4 bg-base-100">
        <div className="container mx-auto">
          <h2 className="text-3xl font-bold text-center mb-12">Features</h2>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
            <div className="card bg-base-200 shadow-xl">
              <div className="card-body">
                <h3 className="card-title">URL Shortening</h3>
                <p>Transform long URLs into short, memorable links instantly.</p>
              </div>
            </div>
            <div className="card bg-base-200 shadow-xl">
              <div className="card-body">
                <h3 className="card-title">Click Analytics</h3>
                <p>Track every click with detailed analytics and insights.</p>
              </div>
            </div>
            <div className="card bg-base-200 shadow-xl">
              <div className="card-body">
                <h3 className="card-title">Real-time Tracking</h3>
                <p>Monitor your links in real-time with live updates.</p>
              </div>
            </div>
          </div>
        </div>
      </section>

      <HowItWorksSection />

      <Footer />
    </main>
  )
}

export default Home
