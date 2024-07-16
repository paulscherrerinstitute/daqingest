use std::marker::PhantomData;

pub struct JobChnWorker<JOB> {
    _t1: PhantomData<JOB>,
}

impl<JOB> JobChnWorker<JOB> {}

//pub fn submit_and_await(job)
