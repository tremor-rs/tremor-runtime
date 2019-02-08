use crate::async_sink::SinkDequeueError;
use crate::errors::*;
use crate::pipeline::prelude::*;
use std::collections::HashMap;

static DROP_OUTPUT_ID: usize = 3; // 1 is std, 2 is err, 3 is drop

pub trait WorkerPoolStep {
    fn dequeue(&mut self) -> std::result::Result<Result<Option<f64>>, SinkDequeueError>;
    fn enqueue_send_future(&mut self) -> Result<()>;
    fn has_capacity(&self) -> bool;
    fn pop_payload(&mut self) -> Option<EventData>;
    fn push_payload(&mut self, event: EventData);
    fn event_errors(&self, _event: &EventData) -> Option<String> {
        None
    }
    fn batch_size(&self) -> usize;
    fn payload_len(&self) -> usize;
    fn empty(&mut self);
    fn timeout(&self) -> u64;
    fn maybe_enque(&mut self) -> EventResult {
        match self.dequeue() {
            Err(SinkDequeueError::NotReady) if !self.has_capacity() => {
                if let Some(e_out) = self.pop_payload() {
                    next_id!(DROP_OUTPUT_ID, e_out)
                } else {
                    EventResult::StepError("No capacity in queue".into())
                }
            }
            _ => {
                if self.enqueue_send_future().is_ok() {
                    EventResult::Done
                } else {
                    EventResult::StepError("Could not enqueue event".into())
                }
            }
        }
    }

    fn handle_event(&mut self, event: EventData) -> EventResult {
        if let Some(error) = self.event_errors(&event) {
            return error_result!(event, error);
        }
        self.push_payload(event);
        if self.batch_size() > self.payload_len() {
            if self.payload_len() == 1 {
                EventResult::Timeout {
                    timeout_millis: self.timeout(),
                    result: Box::new(EventResult::Done),
                }
            } else {
                EventResult::Done
            }
        } else {
            self.maybe_enque()
        }
    }

    fn handle_return(r: &Result<Option<f64>>, returns: HashMap<ReturnDest, Vec<u64>>) {
        match &r {
            Ok(r) => {
                for (dst, ids) in returns.iter() {
                    let ret = Return {
                        source: dst.source.to_owned(),
                        chain: dst.chain.to_owned(),
                        ids: ids.to_owned(),
                        v: Ok(*r),
                    };
                    ret.send();
                }
            }
            Err(e) => {
                for (dst, ids) in returns.iter() {
                    let ret = Return {
                        source: dst.source.to_owned(),
                        chain: dst.chain.to_owned(),
                        ids: ids.to_owned(),
                        v: Err(e.clone()),
                    };
                    ret.send();
                }
            }
        };
    }
    fn shutdown(&mut self) {
        // We empty the queue
        self.empty();
        // Ensure the current state is in the quee
        self.maybe_enque();
        // And empty it again
        self.empty();
    }
}
