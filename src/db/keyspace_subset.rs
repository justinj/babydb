// Implements a data structure that represents a union of closed intervals over
// a keyspace.
// --(------)------(---)-----
//   bar    baz    foo zed
// [(bar, baz), (foo, zed)]
#[derive(Debug, Clone)]
pub struct KeyspaceSubset<K>
where
    K: Clone + Ord + std::fmt::Debug,
{
    // Tuples here are disjoint and ordered.
    ranges: Vec<(K, K)>,
}

#[derive(Debug, Clone, Copy)]
enum MergeState<K> {
    NoNo,
    YesNo(K),
    NoYes(K),
    YesYes(K),
}

impl<K> KeyspaceSubset<K>
where
    K: Clone + Ord + std::fmt::Debug,
{
    pub fn new() -> Self {
        KeyspaceSubset { ranges: Vec::new() }
    }

    pub fn new_from_singleton(interval: (K, K)) -> Self {
        KeyspaceSubset {
            ranges: vec![interval],
        }
    }

    pub fn intersects(&self, other: &KeyspaceSubset<K>) -> bool {
        let mut my_idx = 0;
        let mut other_idx = 0;
        while my_idx < self.ranges.len() && other_idx < other.ranges.len() {
            let (a, b) = &self.ranges[my_idx];
            let (c, d) = &other.ranges[my_idx];
            if a <= d && c <= b {
                return true;
            }
            if a < c {
                my_idx += 1;
            } else {
                other_idx += 1;
            }
        }
        false
    }

    pub fn union(&self, other: &KeyspaceSubset<K>) -> KeyspaceSubset<K> {
        let mut my_idx = 0;
        let mut other_idx = 0;

        let mut state = MergeState::NoNo;
        let mut result = Vec::new();

        while my_idx < self.ranges.len() && other_idx < other.ranges.len() {
            let (a, b) = &self.ranges[my_idx];
            let (c, d) = &other.ranges[other_idx];
            match state {
                MergeState::NoNo => {
                    if a < c {
                        state = MergeState::YesNo(a.clone());
                    } else {
                        state = MergeState::NoYes(c.clone());
                    }
                }
                MergeState::YesNo(start) => {
                    if c <= b {
                        state = MergeState::YesYes(start);
                    } else {
                        result.push((start, b.clone()));
                        state = MergeState::NoNo;
                        my_idx += 1;
                    }
                }
                MergeState::NoYes(start) => {
                    if a <= d {
                        state = MergeState::YesYes(start);
                    } else {
                        result.push((start, d.clone()));
                        state = MergeState::NoNo;
                        other_idx += 1;
                    }
                }
                MergeState::YesYes(start) => {
                    if b < d {
                        state = MergeState::NoYes(start);
                        my_idx += 1;
                    } else {
                        state = MergeState::YesNo(start);
                        other_idx += 1;
                    }
                }
            }
        }

        match state {
            MergeState::YesNo(start) => {
                let (_, current_end) = &self.ranges[my_idx];
                result.push((start, current_end.clone()));
                my_idx += 1;
            }
            MergeState::NoYes(start) => {
                let (_, current_end) = &other.ranges[other_idx];
                result.push((start, current_end.clone()));
                other_idx += 1;
            }
            MergeState::NoNo => {}
            MergeState::YesYes(_) => unreachable!(),
        }

        if my_idx < self.ranges.len() {
            result.extend(self.ranges[my_idx..].iter().cloned());
        }
        if other_idx < other.ranges.len() {
            result.extend(other.ranges[other_idx..].iter().cloned());
        }

        KeyspaceSubset { ranges: result }
    }
}

#[test]
fn test_keyspace_subset() {
    let subset1 = KeyspaceSubset::new_from_singleton((1, 3));
    let subset2 = KeyspaceSubset::new_from_singleton((2, 4));
    let subset3 = KeyspaceSubset::new_from_singleton((4, 5));

    assert!(subset1.intersects(&subset2));
    assert!(!subset1.intersects(&subset3));
    assert!(subset2.intersects(&subset3));

    let mut unioned = KeyspaceSubset::new();
    let interval_tests = [
        ((1, 2), vec![(1, 2)]),
        ((3, 4), vec![(1, 2), (3, 4)]),
        ((6, 7), vec![(1, 2), (3, 4), (6, 7)]),
        ((2, 3), vec![(1, 4), (6, 7)]),
    ];
    for (interval, expected) in interval_tests {
        unioned = unioned.union(&KeyspaceSubset::new_from_singleton(interval));
        assert_eq!(unioned.ranges, expected);
    }

    let union_tests = [
        (vec![(1, 2), (4, 5)], vec![(2, 4), (5, 6)], vec![(1, 6)]),
        (
            vec![(1, 3), (100, 110)],
            vec![(5, 6), (1000, 1100)],
            vec![(1, 3), (5, 6), (100, 110), (1000, 1100)],
        ),
    ];
    for (l, r, expected) in union_tests {
        let a = KeyspaceSubset { ranges: l };
        let b = KeyspaceSubset { ranges: r };
        let union = a.union(&b);
        assert_eq!(union.ranges, expected);
    }
}
