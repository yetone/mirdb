import React from 'react';
import { Navbar } from '../components/layout/Navbar';
import { Footer } from '../components/layout/Footer';
import { BackgroundEffect } from '../components/common/BackgroundEffect';
import { HeroSection } from '../components/home/HeroSection';
import { FeaturesSection } from '../components/home/FeaturesSection';

export function Home() {
  return (
    <div className="min-h-screen flex flex-col" data-testid="home-page">
      <BackgroundEffect />
      <Navbar />
      <main className="flex-grow">
        <HeroSection />
        <FeaturesSection />
      </main>
      <Footer />
    </div>
  );
}
