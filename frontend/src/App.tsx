import { BrowserRouter, Routes, Route } from 'react-router-dom';
import { Navbar } from './components/Navbar';
import { HeroSection, FeaturesSection, HowItWorksSection, CTASection, Footer } from './components/landing';
import './index.css';

function HomePage() {
  return (
    <main id="main-content" className="min-h-screen">
      <HeroSection />
      <FeaturesSection />
      <HowItWorksSection />
      <CTASection />
      <Footer />
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
        <Navbar />
        <Routes>
          <Route path="/" element={<HomePage />} />
          <Route path="/login" element={<LoginPage />} />
          <Route path="/register" element={<RegisterPage />} />
        </Routes>
      </div>
    </BrowserRouter>
  );
}

export default App;
