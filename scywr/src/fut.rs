use crate::access::Error;
use crate::session::ScySession;
use futures_util::Future;
use futures_util::FutureExt;
use scylla::frame::value::ValueList;
use scylla::prepared_statement::PreparedStatement;
use scylla::transport::errors::QueryError;
use scylla::QueryResult;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

pub struct ScyQueryFut<'a> {
    fut: Pin<Box<dyn Future<Output = Result<QueryResult, QueryError>> + Send + 'a>>,
}

impl<'a> ScyQueryFut<'a> {
    pub fn new<V>(scy: &'a ScySession, query: Option<&'a PreparedStatement>, values: V) -> Self
    where
        V: ValueList + Send + 'static,
    {
        todo!("ScyQueryFut");
        //let fut = scy.execute(query, values);
        let fut = futures_util::future::ready(Err(QueryError::TimeoutError));
        Self { fut: Box::pin(fut) }
    }
}

impl<'a> Future for ScyQueryFut<'a> {
    type Output = Result<(), Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        use Poll::*;
        match self.fut.poll_unpin(cx) {
            Ready(k) => match k {
                Ok(_) => Ready(Ok(())),
                Err(e) => Ready(Err(e.into())),
            },
            Pending => Pending,
        }
    }
}
