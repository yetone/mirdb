import { Hero, Features, Documentation, Performance, QuickStart, Contributing } from './components/sections';
import { Footer } from './components/layout';

function App() {
  return (
    <>
      <main>
        <Hero />
        <Features />
        <QuickStart />
        <Performance />
        <Documentation />
        <Contributing />
      </main>
      <Footer />
    </>
  );
}

export default App;
