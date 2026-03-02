import { BrowserRouter, Routes, Route } from 'react-router-dom';
import { ThemeProvider } from './contexts/ThemeContext';
import { HeroSection, CTASection } from './components/home';
import { HomeNavbar } from './components/layout';
import { Login } from './pages/Login';
import { Register } from './pages/Register';

function Home() {
  return (
    <div className="min-h-screen">
      <HomeNavbar />
      <main id="main-content">
        <HeroSection />
        <CTASection />
      </main>
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
