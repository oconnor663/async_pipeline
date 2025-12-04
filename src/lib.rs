use futures::channel::mpsc::{Receiver, Sender, channel};
use futures::{SinkExt, Stream, StreamExt, join};
use std::pin::pin;

pub fn pipeline<S: Stream>(stream: S) -> AsyncPipeline<impl Future, S::Item> {
    let (mut sender, receiver) = channel(0);
    AsyncPipeline {
        future: async move {
            let mut stream = pin!(stream);
            while let Some(item) = stream.next().await {
                sender.send(item).await.unwrap();
            }
        },
        outputs: receiver,
    }
}

fn sender_is_ready<T>(sender: &mut Sender<T>) -> impl Future {
    std::future::poll_fn(|cx| sender.poll_ready(cx))
}

pub struct AsyncPipeline<Fut: Future, T> {
    future: Fut,
    outputs: Receiver<T>,
}

impl<Fut: Future, T> AsyncPipeline<Fut, T> {
    pub fn then<F, U>(mut self, mut f: impl AsyncFnMut(T) -> U) -> AsyncPipeline<impl Future, U> {
        let (mut sender, receiver) = channel(0);
        AsyncPipeline {
            future: async {
                join! {
                    self.future,
                    async move {
                        while let Some(input) = self.outputs.next().await {
                            let output = f(input).await;
                            sender.send(output).await.unwrap();
                            sender_is_ready(&mut sender).await;
                        }
                    }
                };
            },
            outputs: receiver,
        }
    }

    pub async fn for_each(mut self, mut f: impl AsyncFnMut(T)) {
        join! {
            self.future,
            async move {
                while let Some(input) = self.outputs.next().await {
                    f(input).await;
                }
            }
        };
    }

    pub async fn collect<C: Default + Extend<T>>(mut self) -> C {
        join! {
            self.future,
            async move {
                let mut collection = C::default();
                while let Some(input) = self.outputs.next().await {
                    collection.extend(std::iter::once(input));
                }
                collection
            }
        }
        .1
    }
}
