use futures::channel::mpsc::{Receiver, channel};
use futures::{SinkExt, Stream, StreamExt};
use std::pin::{Pin, pin};

pub struct AsyncPipeline<'a, T> {
    futures: Vec<Pin<Box<dyn Future<Output = ()> + Send + 'a>>>,
    output: Receiver<T>,
}

impl<'a, T> AsyncPipeline<'a, T>
where
    T: Send + 'a,
{
    pub fn new<S: Stream<Item = T> + Send + 'a>(stream: S) -> Self {
        Self::new_buffered(stream, 1)
    }

    pub fn new_buffered<S: Stream<Item = T> + Send + 'a>(stream: S, buf_size: usize) -> Self {
        let (mut sender, receiver) = channel(buf_size);
        let future = Box::pin(async move {
            let mut stream = pin!(stream);
            while let Some(item) = stream.next().await {
                sender.send(item).await.unwrap();
            }
        });
        Self {
            futures: vec![future],
            output: receiver,
        }
    }

    pub fn map(self) -> Self {
        todo!()
    }

    pub fn map_buffered(self) -> Self {
        todo!()
    }

    pub fn map_concurrent(self) -> Self {
        todo!()
    }

    pub fn map_unordered(self) -> Self {
        todo!()
    }

    pub async fn for_each(mut self) {
        while let Some(_) = self.output.next().await {}
    }

    pub async fn collect<C: Default + Extend<T>>(mut self) -> C {
        let mut collection = C::default();
        while let Some(item) = self.output.next().await {
            collection.extend(std::iter::once(item));
        }
        collection
    }
}
