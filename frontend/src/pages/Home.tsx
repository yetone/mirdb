import React from 'react';
import { useNavigate } from 'react-router-dom';
import { Navbar } from '../components/layout/Navbar';
import { Footer } from '../components/layout/Footer';
import { BackgroundEffect } from '../components/common/BackgroundEffect';
import { HeroSection } from '../components/home/HeroSection';
import { FeaturesSection } from '../components/home/FeaturesSection';
import { UrlDemoSection } from '../components/home/UrlDemoSection';
import { FinalCTASection } from '../components/home/FinalCTASection';

export function Home() {
  const navigate = useNavigate();

  const handleRegisterPrompt = () => {
    navigate('/register');
  };

  return (
    <div className="min-h-screen flex flex-col" data-testid="home-page">
      <BackgroundEffect />
      <Navbar />
      <main className="flex-grow">
        <HeroSection />
        <FeaturesSection />
        <UrlDemoSection onRegisterPrompt={handleRegisterPrompt} />
        <FinalCTASection />
      </main>
      <Footer />
    </div>
  );
}
