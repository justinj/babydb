use std::{cell::RefCell, rc::Rc};

use super::{LogEntry, Logger};

#[derive(Debug, Clone)]
pub struct MockLog<E>
where
    E: LogEntry,
{
    entries: Rc<RefCell<Vec<E>>>,
    highest_seen_seqnum: usize,
}

impl<E> Logger<E> for MockLog<E>
where
    E: LogEntry,
{
    fn fname(&self) -> String {
        panic!("unimplemented")
    }

    fn new(_dir: &str, lower_bound: usize) -> anyhow::Result<Self> {
        Ok(MockLog {
            entries: Rc::new(RefCell::new(Vec::new())),
            highest_seen_seqnum: lower_bound,
        })
    }

    fn write(&mut self, e: &E) -> anyhow::Result<()> {
        self.highest_seen_seqnum = std::cmp::max(self.highest_seen_seqnum, e.seqnum());
        (*self.entries).borrow_mut().push(e.clone());
        Ok(())
    }

    fn frontier(&self) -> usize {
        self.highest_seen_seqnum + 1
    }
}
