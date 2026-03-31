import { FeaturesSection } from './components/sections/FeaturesSection';
import { features } from './constants/features';

function App() {
  return (
    <div className="min-h-screen bg-white dark:bg-gray-900">
      <main>
        <FeaturesSection features={features} />
      </main>
    </div>
  );
}

export default App;
