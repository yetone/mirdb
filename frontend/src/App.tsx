import { BrowserRouter, Routes, Route } from 'react-router-dom';
import { Navbar } from './components/Navbar';
import Home from './pages/Home';
import { BackgroundEffect } from './components/BackgroundEffect';
import { FuturisticButton } from './components/FuturisticButton';
import { GlassMorphismCard } from './components/GlassMorphismCard';
import './index.css';

/**
 * Demo page for testing visual effects and animation components
 * Used by E2E tests to verify animation behavior
 */
function AnimationDemoPage() {
  return (
    <main id="main-content" className="min-h-screen p-8">
      <BackgroundEffect />
      <div className="container mx-auto space-y-8">
        <h1 className="text-4xl font-bold text-center mb-8">Animation Demo</h1>

        {/* FuturisticButton demo */}
        <section data-testid="button-demo" className="space-y-4">
          <h2 className="text-2xl font-semibold">Futuristic Buttons</h2>
          <div className="flex flex-wrap gap-4">
            <FuturisticButton variant="primary">Primary Button</FuturisticButton>
            <FuturisticButton variant="secondary">Secondary Button</FuturisticButton>
            <FuturisticButton variant="outline">Outline Button</FuturisticButton>
            <FuturisticButton variant="ghost">Ghost Button</FuturisticButton>
          </div>
        </section>

        {/* GlassMorphismCard demo */}
        <section data-testid="card-demo" className="space-y-4">
          <h2 className="text-2xl font-semibold">Glass Cards</h2>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
            <GlassMorphismCard>
              <h3 className="text-lg font-semibold mb-2">Card 1</h3>
              <p className="text-base-content/70">Hover over this card to see the effect.</p>
            </GlassMorphismCard>
            <GlassMorphismCard glowColor="secondary">
              <h3 className="text-lg font-semibold mb-2">Card 2</h3>
              <p className="text-base-content/70">This card has a secondary glow color.</p>
            </GlassMorphismCard>
            <GlassMorphismCard hoverEffect={false}>
              <h3 className="text-lg font-semibold mb-2">Card 3 (No Hover)</h3>
              <p className="text-base-content/70">This card has hover effects disabled.</p>
            </GlassMorphismCard>
          </div>
        </section>
      </div>
    </main>
  );
}

function LoginPage() {
  return (
    <main className="min-h-screen p-8">
      <div className="container mx-auto">
        <h1 className="text-4xl font-bold">Login</h1>
      </div>
    </main>
  );
}

function RegisterPage() {
  return (
    <main className="min-h-screen p-8">
      <div className="container mx-auto">
        <h1 className="text-4xl font-bold">Register</h1>
      </div>
    </main>
  );
}

function App() {
  return (
    <BrowserRouter>
      <div className="min-h-screen bg-base-100">
        <Routes>
          {/* Home page has its own semantic structure with header, main, footer */}
          <Route path="/" element={<Home />} />
          {/* Other pages use shared Navbar layout */}
          <Route
            path="/login"
            element={
              <>
                <Navbar />
                <LoginPage />
              </>
            }
          />
          <Route
            path="/register"
            element={
              <>
                <Navbar />
                <RegisterPage />
              </>
            }
          />
          <Route
            path="/animation-demo"
            element={
              <>
                <Navbar />
                <AnimationDemoPage />
              </>
            }
          />
        </Routes>
      </div>
    </BrowserRouter>
  );
}

export default App;
