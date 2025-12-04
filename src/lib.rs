use futures::channel::mpsc::{Receiver, channel};
use futures::{SinkExt, StreamExt, join};

pub trait AsyncPipeline {
    type Item;

    fn into_future_and_outputs(self) -> (impl Future, Receiver<Self::Item>);

    fn then<F, T>(self, mut f: F) -> Then<impl Future, T>
    where
        Self: Sized,
        F: AsyncFnMut(Self::Item) -> T,
    {
        let (inner_future, mut inputs) = self.into_future_and_outputs();
        let (mut sender, receiver) = channel(0);
        Then {
            future: async {
                join! {
                    inner_future,
                    async move {
                        while let Some(item) = inputs.next().await {
                            let output = f(item).await;
                            sender.send(output).await.unwrap();
                        }
                    }
                }
            },
            outputs: receiver,
        }
    }
}

pub struct Then<Fut, T> {
    future: Fut,
    outputs: Receiver<T>,
}

impl<Fut: Future, T> AsyncPipeline for Then<Fut, T> {
    type Item = T;

    fn into_future_and_outputs(self) -> (impl Future, Receiver<Self::Item>) {
        (self.future, self.outputs)
    }
}
