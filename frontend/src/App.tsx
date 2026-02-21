import { Routes, Route } from 'react-router-dom';
import HomePage from './pages/HomePage';

function App() {
  return (
    <Routes>
      <Route path="/" element={<HomePage />} />
      <Route path="/register" element={<div>Register Page</div>} />
      <Route path="/login" element={<div>Login Page</div>} />
    </Routes>
  );
}

export default App;
