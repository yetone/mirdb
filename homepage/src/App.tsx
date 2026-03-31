import { HeroSection } from './components/sections/HeroSection';
import { FeaturesSection } from './components/sections/FeaturesSection';
import { QuickStartSection } from './components/sections/QuickStartSection';
import { DemoSection } from './components/sections/DemoSection';
import { StatusSection } from './components/sections/StatusSection';
import { ResourcesSection } from './components/sections/ResourcesSection';
import { Header } from './components/layout/Header';
import { Footer } from './components/layout/Footer';
import { features } from './constants/features';

function App() {
  return (
    <div className="min-h-screen bg-white dark:bg-gray-900 flex flex-col">
      <Header />
      <main className="flex-grow">
        <HeroSection />
        <FeaturesSection features={features} />
        <QuickStartSection />
        <DemoSection />
        <StatusSection />
        <ResourcesSection />
      </main>
      <Footer />
    </div>
  );
}

export default App;
