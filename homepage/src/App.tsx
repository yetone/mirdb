import { Features } from './components/Features/Features';
import { Installation } from './components/Installation/Installation';

function App() {
  return (
    <>
      <main>
        {/* Header component - Scenario 1 */}
        {/* Hero component - Scenario 1 */}
        <section id="features">
          <Features />
        </section>
        <Installation />
        {/* Usage component - Scenario 4 */}
        {/* Footer component - Scenario 6 */}
      </main>
    </>
  );
}

export default App;
