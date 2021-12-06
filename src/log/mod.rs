use std::{cell::RefCell, marker::PhantomData, rc::Rc};

pub trait LogEntry: std::fmt::Debug + Clone {
    fn seqnum(&self) -> usize;
}

struct Frozen<E, L> {
    l: L,
    _marker: PhantomData<E>,
}

impl<E, L> Frozen<E, L>
where
    E: LogEntry,
    L: Logger<E>,
{
    fn frontier(&self) -> usize {
        self.l.frontier()
    }
}

pub trait Logger<E>: std::fmt::Debug + Sized
where
    E: LogEntry,
{
    fn new(dir: &str, lower_bound: usize) -> Self;
    fn write(&mut self, m: E);

    fn frontier(&self) -> usize;
    fn freeze(self) -> Frozen<E, Self>;
}

#[derive(Debug)]
pub struct LogSet<E: LogEntry, L: Logger<E>> {
    active_log: L,
    old: Vec<L>,
    dir: String,
    _marker: PhantomData<E>,
}

impl<E, L> LogSet<E, L>
where
    E: LogEntry,
    L: Logger<E>,
{
    pub fn open_dir(dir: String) -> Self {
        // TODO: what's the right starting seqnum?
        let cur_seqnum = 0;
        LogSet {
            // TODO: use cross-platform path join
            active_log: L::new(format!("{}/wal-{}", dir, cur_seqnum).as_str(), cur_seqnum),
            old: Vec::new(),
            dir,
            _marker: PhantomData,
        }
    }

    pub fn current(&mut self) -> &mut L {
        &mut self.active_log
    }

    pub fn fresh(&mut self) {
        let upper_bound = self.active_log.frontier();
        let old_log = std::mem::replace(
            &mut self.active_log,
            L::new(
                format!("{}/wal-{}", self.dir, upper_bound).as_str(),
                upper_bound,
            ),
        );
        self.old.push(old_log);
    }
}

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
    fn new(_dir: &str, lower_bound: usize) -> Self {
        MockLog {
            entries: Rc::new(RefCell::new(Vec::new())),
            highest_seen_seqnum: lower_bound,
        }
    }

    fn write(&mut self, e: E) {
        self.highest_seen_seqnum = std::cmp::max(self.highest_seen_seqnum, e.seqnum());
        (*self.entries).borrow_mut().push(e);
    }

    fn frontier(&self) -> usize {
        self.highest_seen_seqnum + 1
    }

    fn freeze(self) -> Frozen<E, Self> {
        Frozen {
            l: self,
            _marker: PhantomData,
        }
    }
}
