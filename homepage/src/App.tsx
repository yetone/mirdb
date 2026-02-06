import { Routes, Route } from 'react-router-dom';
import { Home } from './pages/Home';
import { Privacy } from './pages/Privacy';
import { Terms } from './pages/Terms';

/**
 * Sign Up page placeholder.
 * This is a simple placeholder for the sign-up flow destination.
 * A full sign-up implementation is out of scope for the homepage PRD.
 */
function SignUpPage() {
  return (
    <main id="main-content">
      <h1>Sign Up</h1>
      <p>Welcome to the sign-up page! Create your account to get started.</p>
    </main>
  );
}

/**
 * Main application component with routing.
 * Owner: Scenario 10 - User Flow (page composition)
 *
 * Routes:
 * - / : Home page (composed from Hero, Features, SocialProof, Footer)
 * - /signup : Sign up page (destination for primary CTA)
 * - /privacy : Privacy policy page
 * - /terms : Terms of service page
 */
function App() {
  return (
    <Routes>
      <Route path="/" element={<Home />} />
      <Route path="/signup" element={<SignUpPage />} />
      <Route path="/privacy" element={<Privacy />} />
      <Route path="/terms" element={<Terms />} />
    </Routes>
  );
}

export default App;
