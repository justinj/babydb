use crate::encoding::{Decode, Encode};

pub(crate) mod file_log;

pub trait LogEntry: std::fmt::Debug + Clone + Encode + Decode {
    fn seqnum(&self) -> usize;
}
