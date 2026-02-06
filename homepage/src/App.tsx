import { Routes, Route } from 'react-router-dom';
import { Hero } from './components/Hero';
import { Features } from './components/Features';

function HomePage() {
  return (
    <main id="main-content">
      <Hero />
      <Features />
    </main>
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
    </Routes>
  );
}

export default App;
