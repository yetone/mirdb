use std::ops::Drop;
use std::sync::{mpsc, Arc, Mutex};
use std::thread;

use log::info;

struct Worker {
    id: usize,
    thread: Option<thread::JoinHandle<()>>,
}

pub struct ThreadPool {
    workers: Vec<Worker>,
    sender: mpsc::Sender<Message>,
}

pub trait FnBox {
    fn call_box(self: Box<Self>);
}

impl<F: FnOnce()> FnBox for F {
    fn call_box(self: Box<Self>) {
        (*self)()
    }
}

enum Message {
    NewJob(Job),
    Terminate,
}

pub type Job = Box<dyn FnBox + Send + 'static>;

impl Worker {
    fn new(id: usize, receiver: Arc<Mutex<mpsc::Receiver<Message>>>) -> Self {
        let thread = thread::spawn(move || loop {
            let msg = receiver.lock().unwrap().recv().unwrap();
            match msg {
                Message::NewJob(job) => {
                    info!("Worker {} got a job; executing.", id);
                    job.call_box();
                }
                Message::Terminate => {
                    info!("Worker {} was told to terminate.", id);
                    break;
                }
            }
        });
        Worker {
            id,
            thread: Some(thread),
        }
    }
}

impl ThreadPool {
    pub fn new(size: usize) -> Self {
        let mut workers = Vec::with_capacity(size);
        let (sender, receiver) = mpsc::channel();
        let receiver = Arc::new(Mutex::new(receiver));
        for i in 0..size {
            workers.push(Worker::new(i, receiver.clone()));
        }
        ThreadPool { workers, sender }
    }

    pub fn execute<F>(&self, job: F)
    where
        F: FnBox + Send + 'static,
    {
        self.sender.send(Message::NewJob(Box::new(job))).unwrap();
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        info!("Sending terminate message to all workers.");

        for _ in &self.workers {
            self.sender.send(Message::Terminate).unwrap();
        }

        info!("Shutting down all workers.");

        for worker in &mut self.workers {
            info!("Shutting down worker {}", worker.id);

            if let Some(thread) = worker.thread.take() {
                thread.join().unwrap();
            }
        }
    }
}
