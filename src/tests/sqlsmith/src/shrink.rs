use std::panic::{self, AssertUnwindSafe};
use std::sync::{Arc, Mutex};

use risingwave_frontend::binder::Binder;
use risingwave_frontend::planner::Planner;
use risingwave_frontend::session::{OptimizerContextRef, SessionImpl};
use risingwave_sqlparser::ast::{Query, Select, SelectItem, SetExpr, Statement};

/// Returns an iterator of values that are smaller than the initial value.
///
/// The items are ordered in such a way that it maximize the chances to find the smallest counter
/// example.
trait Shrink: Sized + Clone {
    fn shrink(&self) -> Vec<Self>;
}

mod vec_shrink {
    //! copied and adapted from [quickcheck](https://github.com/BurntSushi/quickcheck)
    use super::Shrink;

    impl<A: Shrink> Shrink for Vec<A> {
        fn shrink(&self) -> Vec<Self> {
            VecShrinker::new(self.clone()).into_iter().collect()
        }
    }

    /// Iterator which returns successive attempts to shrink the vector `seed`
    struct VecShrinker<A> {
        seed: Vec<A>,
        /// How much which is removed when trying with smaller vectors
        size: usize,
        /// The end of the removed elements
        offset: usize,
        /// The shrinker for the element at `offset` once shrinking of individual
        /// elements are attempted
        element_shrinker: Vec<A>,
    }

    impl<A: Shrink> VecShrinker<A> {
        fn new(seed: Vec<A>) -> Vec<Vec<A>> {
            let es = match seed.get(0) {
                Some(e) => e.shrink(),
                None => return vec![],
            };
            let size = seed.len();
            VecShrinker {
                seed: seed,
                size: size,
                offset: size,
                element_shrinker: es,
            }
            .into_iter()
            .collect()
        }

        /// Returns the next shrunk element if any, `offset` points to the index
        /// after the returned element after the function returns
        fn next_element(&mut self) -> Option<A> {
            loop {
                let e = self.element_shrinker.drain(0..1).next();
                match e {
                    Some(e) => return Some(e),
                    None => match self.seed.get(self.offset) {
                        Some(e) => {
                            self.element_shrinker = e.shrink();
                            self.offset += 1;
                        }
                        None => return None,
                    },
                }
            }
        }
    }

    impl<A> Iterator for VecShrinker<A>
    where
        A: Shrink,
    {
        type Item = Vec<A>;

        fn next(&mut self) -> Option<Vec<A>> {
            // Try with an empty vector first
            if self.size == self.seed.len() {
                self.size /= 2;
                self.offset = self.size;
                return Some(vec![]);
            }
            if self.size != 0 {
                // Generate a smaller vector by removing the elements between
                // (offset - size) and offset
                let xs1 = self.seed[..(self.offset - self.size)]
                    .iter()
                    .chain(&self.seed[self.offset..])
                    .cloned()
                    .collect();
                self.offset += self.size;
                // Try to reduce the amount removed from the vector once all
                // previous sizes tried
                if self.offset > self.seed.len() {
                    self.size /= 2;
                    self.offset = self.size;
                }
                Some(xs1)
            } else {
                // A smaller vector did not work so try to shrink each element of
                // the vector instead Reuse `offset` as the index determining which
                // element to shrink

                // The first element shrinker is already created so skip the first
                // offset (self.offset == 0 only on first entry to this part of the
                // iterator)
                if self.offset == 0 {
                    self.offset = 1
                }

                match self.next_element() {
                    Some(e) => Some(
                        self.seed[..self.offset - 1]
                            .iter()
                            .cloned()
                            .chain(Some(e).into_iter())
                            .chain(self.seed[self.offset..].iter().cloned())
                            .collect(),
                    ),
                    None => None,
                }
            }
        }
    }
}

impl Shrink for SelectItem {
    fn shrink(&self) -> Vec<Self> {
        // TODO:
        vec![]
    }
}

impl Shrink for Select {
    fn shrink(&self) -> Vec<Self> {
        if self.distinct {
            return vec![Select {
                distinct: false,
                ..self.clone()
            }];
        }

        if self.lateral_views.is_empty() && self.group_by.is_empty() && self.having.is_none() {
            // select ... from ... where ...
            // TODO: shrink from and where
            return self
                .projection
                .shrink()
                .into_iter()
                .map(|projection| Select {
                    projection,
                    ..self.clone()
                })
                .collect();
        }

        return vec![Select {
            distinct: false,
            projection: self.projection.clone(),
            from: self.from.clone(),
            lateral_views: vec![],
            selection: self.selection.clone(),
            group_by: vec![],
            having: None,
        }];

        // TODO: put back more?
    }
}

impl Shrink for SetExpr {
    fn shrink(&self) -> Vec<Self> {
        match self {
            SetExpr::Select(select) => select
                .shrink()
                .into_iter()
                .map(|select| SetExpr::Select(Box::new(select)))
                .collect(),
            SetExpr::Query(query) => query
                .shrink()
                .into_iter()
                .map(|query| SetExpr::Query(Box::new(query)))
                .collect(),
            SetExpr::SetOperation {
                op: _,
                all: _,
                left,
                right,
            } => vec![*left.clone(), *right.clone()],
            SetExpr::Values(_) => vec![],
            SetExpr::Insert(_) => vec![],
        }
    }
}

impl Shrink for Query {
    fn shrink(&self) -> Vec<Self> {
        // Only body is present. Shrink body.
        if self.with.is_none()
            && self.order_by.is_empty()
            && self.limit.is_none()
            && self.offset.is_none()
            && self.fetch.is_none()
        {
            return self
                .body
                .shrink()
                .into_iter()
                .map(|body| Query {
                    body,
                    order_by: vec![],
                    limit: None,
                    offset: None,
                    fetch: None,
                    with: None,
                })
                .collect();
        }

        let mut children = vec![];

        // smallest: only contains body
        children.push(Query {
            body: self.body.clone(),
            order_by: vec![],
            limit: None,
            offset: None,
            fetch: None,
            with: None,
        });

        // put back order_by
        if !self.order_by.is_empty() {
            children.push(Query {
                body: self.body.clone(),
                order_by: self.order_by.clone(),
                limit: None,
                offset: None,
                fetch: None,
                with: None,
            })
        }

        // put back limit & offset
        if self.limit.is_some() || self.offset.is_some() {
            children.push(Query {
                body: self.body.clone(),
                order_by: vec![],
                limit: self.limit.clone(),
                offset: self.offset.clone(),
                fetch: None,
                with: None,
            })
        }

        // TODO: put back more?

        children
    }
}

pub fn shrink_query(
    panic_query: Query,
    panic_msg: &str,
    session: Arc<SessionImpl>,
    context: OptimizerContextRef,
) -> Query {
    let original_hook = panic::take_hook();

    let curr_panic_msg = Arc::new(Mutex::new(None));
    {
        let curr_panic_msg = curr_panic_msg.clone();
        panic::set_hook(Box::new(move |panic_info| {
            if let Some(s) = panic_info.payload().downcast_ref::<&str>() {
                *curr_panic_msg.lock().unwrap() = Some(s.to_string());
            } else {
                *curr_panic_msg.lock().unwrap() = None;
            }
        }));
    }

    let mut curr_query = panic_query;
    // keep shrinking until we cannot find a smaller query with the same error
    for child in curr_query.shrink() {
        if panic::catch_unwind(AssertUnwindSafe(|| {
            plan_query(child.clone(), session.clone(), context.clone());
        }))
        .is_err()
        {
            if let Some(ref msg) = *(curr_panic_msg.lock().unwrap()) {
                if msg == panic_msg {
                    curr_query = child;
                }
            }
        }
    }

    panic::set_hook(original_hook);

    curr_query
}

fn plan_query(query: Query, session: Arc<SessionImpl>, context: OptimizerContextRef) {
    let mut binder = Binder::new(
        session.env().catalog_reader().read_guard(),
        session.database().to_string(),
    );
    let bound = binder
        .bind(Statement::Query(Box::new(query)))
        .unwrap_or_else(|e| panic!("Failed to bind: Reason:\n{}", e));
    let mut planner = Planner::new(context.clone());
    let logical_plan = planner
        .plan(bound)
        .unwrap_or_else(|e| panic!("Failed to generate logical plan: Reason:\n{}", e));
    logical_plan
        .gen_batch_query_plan()
        .unwrap_or_else(|e| panic!("Failed to generate batch plan: Reason:\n{}", e));
}
