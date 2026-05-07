/**
 * Main homepage component that composes all sections.
 */
import HeroSection from '@/components/HeroSection';
import FeaturesSection from '@/components/FeaturesSection';
import QuickStartSection from '@/components/QuickStartSection';
import ProtocolSection from '@/components/ProtocolSection';
import ArchitectureSection from '@/components/ArchitectureSection';
import ConfigSection from '@/components/ConfigSection';
import DemoSection from '@/components/DemoSection';
import RoadmapSection from '@/components/RoadmapSection';
import StatusSection from '@/components/StatusSection';
import Footer from '@/components/Footer';
import Header from '@/components/Header';

export default function Home() {
  return (
    <>
      <Header />
      {/* pt-16 offsets the fixed header height */}
      <main className="pt-16">
        <HeroSection />
        <FeaturesSection />
        <QuickStartSection />
        <ProtocolSection />
        <ArchitectureSection />
        <ConfigSection />
        <DemoSection />
        <RoadmapSection />
        <StatusSection />
      </main>
      <Footer />
    </>
  );
}
