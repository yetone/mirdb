import { Routes, Route } from 'react-router-dom';
import { Hero } from './components/Hero';
import { Features } from './components/Features';
import { Footer } from './components/Footer';
import { Privacy } from './pages/Privacy';
import { Terms } from './pages/Terms';

function HomePage() {
  return (
    <>
      <main id="main-content" tabIndex={-1}>
        <Hero />
        <Features />
      </main>
      <Footer />
    </>
  );
}

function SignUpPage() {
  return (
    <main id="main-content">
      <h1>Sign Up</h1>
      <p>Welcome to the sign-up page!</p>
    </main>
  );
}

function App() {
  return (
    <Routes>
      <Route path="/" element={<HomePage />} />
      <Route path="/signup" element={<SignUpPage />} />
      <Route path="/privacy" element={<Privacy />} />
      <Route path="/terms" element={<Terms />} />
    </Routes>
  );
}

export default App;
