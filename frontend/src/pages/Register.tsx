import Navbar from '../components/Navbar'
import BackgroundEffect from '../components/BackgroundEffect'

export default function Register() {
  return (
    <BackgroundEffect>
      <Navbar />
      <main className="container mx-auto px-4 py-16">
        <div className="max-w-md mx-auto">
          <h1 className="text-3xl font-bold text-center mb-8">Register</h1>
          <form className="space-y-4">
            <div className="form-control">
              <label className="label">
                <span className="label-text">Name</span>
              </label>
              <input type="text" placeholder="Your name" className="input input-bordered" />
            </div>
            <div className="form-control">
              <label className="label">
                <span className="label-text">Email</span>
              </label>
              <input type="email" placeholder="email@example.com" className="input input-bordered" />
            </div>
            <div className="form-control">
              <label className="label">
                <span className="label-text">Password</span>
              </label>
              <input type="password" placeholder="********" className="input input-bordered" />
            </div>
            <button type="submit" className="btn btn-primary w-full">Register</button>
          </form>
        </div>
      </main>
    </BackgroundEffect>
  )
}
