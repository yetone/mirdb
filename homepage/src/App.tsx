import { FeaturesSection } from './components/sections/FeaturesSection';
import { QuickStartSection } from './components/sections/QuickStartSection';
import { features } from './constants/features';

function App() {
  return (
    <div className="min-h-screen bg-white dark:bg-gray-900">
      <main>
        <FeaturesSection features={features} />
        <QuickStartSection />
      </main>
    </div>
  );
}

export default App;
