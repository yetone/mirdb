import { Navbar } from '../components/Navbar';
import { BackgroundEffect } from '../components/BackgroundEffect';
import { HeroSection, FeaturesSection, HowItWorksSection, DashboardPreview, Footer } from '../components/homepage';

export function Home() {
  return (
    <div className="min-h-screen">
      <BackgroundEffect />
      <Navbar />
      <main className="pt-16">
        <HeroSection />
        <FeaturesSection />
        <HowItWorksSection />
        <DashboardPreview />
        <Footer />
      </main>
    </div>
  );
}
