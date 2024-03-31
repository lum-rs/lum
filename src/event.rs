use crate::service::BoxedError;
use std::{fmt::Debug, sync::Arc};
use tokio::sync::{
    mpsc::{channel, error::SendError, Receiver, Sender},
    Mutex,
};

pub enum Subscriber<T> {
    Channel(Sender<Arc<T>>),
    Closure(Box<dyn Fn(Arc<T>) -> Result<(), BoxedError> + Send + Sync>),
}

pub enum EventError<T> {
    ChannelSend(SendError<Arc<T>>),
    Closure(BoxedError),
}

pub struct Event<T> {
    pub name: String,
    subscribers: Mutex<Vec<Subscriber<T>>>,
    remove_subscriber_on_error: bool,
}

impl<T> Event<T> {
    pub fn new(name: &str, remove_subscriber_on_error: bool) -> Self {
        Self {
            name: name.to_string(),
            subscribers: Mutex::new(Vec::new()),
            remove_subscriber_on_error,
        }
    }

    pub async fn subscriber_count(&self) -> usize {
        let subscribers = self.subscribers.lock().await;
        subscribers.len()
    }

    pub async fn open_channel(&self, buffer: usize) -> Receiver<Arc<T>> {
        let (sender, receiver) = channel(buffer);
        let mut subscribers = self.subscribers.lock().await;
        subscribers.push(Subscriber::Channel(sender));
        receiver
    }

    pub async fn subscribe(
        &self,
        closure: impl Fn(Arc<T>) -> Result<(), BoxedError> + Send + Sync + 'static,
    ) {
        let mut subscribers = self.subscribers.lock().await;
        subscribers.push(Subscriber::Closure(Box::new(closure)));
    }

    pub async fn dispatch(&self, data: T) -> Result<(), Vec<EventError<T>>> {
        let mut subscribers = self.subscribers.lock().await;
        let data = Arc::new(data);

        let mut errors = Vec::new();
        let mut subscribers_to_remove = Vec::new();

        for (index, subscriber) in subscribers.iter().enumerate() {
            let data = Arc::clone(&data);

            match subscriber {
                Subscriber::Channel(sender) => {
                    let result = sender.send(data).await;

                    if let Err(err) = result {
                        log::error!("Event \"{}\" failed to dispatch data to receiver {}: {}. Receiver will be unregistered from event.", self.name, index, err);
                        errors.push(EventError::ChannelSend(err));
                        subscribers_to_remove.push(index);
                    }
                }
                Subscriber::Closure(closure) => {
                    let result = closure(data);

                    if let Err(err) = result {
                        log::error!("Event \"{}\" failed to dispatch data to closure {}: {}. Closure will be unregistered from event.", self.name, index, err);
                        errors.push(EventError::Closure(err));
                        subscribers_to_remove.push(index);
                    }
                }
            }
        }

        if self.remove_subscriber_on_error {
            for index in subscribers_to_remove.into_iter().rev() {
                subscribers.remove(index);
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }
}

impl<T> Default for Event<T> {
    fn default() -> Self {
        Self::new("Unnamed Event", false)
    }
}

impl<T> Debug for Event<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(format!("Event of type {}", std::any::type_name::<T>()).as_str())
            .finish()
    }
}