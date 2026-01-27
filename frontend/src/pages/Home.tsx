import React from 'react';
import { HeroSection, FeaturesSection, AnalyticsPreview } from '../components/homepage';

const Home: React.FC = () => {
  return (
    <main className="min-h-screen bg-base-100">
      <HeroSection />
      <FeaturesSection />
      <AnalyticsPreview />

      {/* CTA Section - Placeholder for Scenario 4 */}
      <section className="py-20 px-4 text-center">
        <h2 className="text-3xl md:text-4xl font-bold mb-4">
          Ready to supercharge your links?
        </h2>
        <button className="btn btn-primary btn-lg">Create Free Account</button>
      </section>
    </main>
  );
};

export default Home;
