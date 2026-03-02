import { BrowserRouter, Routes, Route } from 'react-router-dom';
import { ThemeProvider } from './contexts/ThemeContext';
import { HeroSection, HowItWorksSection, FeaturesSection, CTASection } from './components/home';
import { HomeNavbar, HomeFooter } from './components/layout';
import { SkipLink } from './components/ui';
import { Login } from './pages/Login';
import { Register } from './pages/Register';

function Home() {
  return (
    <div className="min-h-screen">
      <SkipLink targetId="main-content" />
      <HomeNavbar />
      <main id="main-content">
        <HeroSection />
        <HowItWorksSection />
        <FeaturesSection />
        <CTASection />
      </main>
      <HomeFooter />
    </div>
  );
}

function App() {
  return (
    <BrowserRouter>
      <ThemeProvider>
        <Routes>
          <Route path="/" element={<Home />} />
          <Route path="/login" element={<Login />} />
          <Route path="/register" element={<Register />} />
        </Routes>
      </ThemeProvider>
    </BrowserRouter>
  );
}

export default App;
