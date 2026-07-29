use std::collections::HashSet;

use crate::{LockQuery, LockQueryResult, OperationId, ReferenceLockService, ServiceView};

use super::{
    parse::{bit, Action, Operation},
    Blocked, BlockedReason, SearchBudget,
};

pub(super) struct BudgetExhausted;

#[derive(Default)]
struct Frontier {
    placed: Vec<OperationId>,
    blocked: Vec<Blocked>,
}

pub(super) struct Search<'a> {
    operations: &'a [Operation],
    budget: SearchBudget,
    failed: HashSet<(u32, ServiceView)>,
    configurations: usize,
    placed: Vec<OperationId>,
    deepest: Frontier,
}

impl<'a> Search<'a> {
    pub(super) fn new(operations: &'a [Operation], budget: SearchBudget) -> Self {
        Self {
            operations,
            budget,
            failed: HashSet::new(),
            configurations: 0,
            placed: Vec::new(),
            deepest: Frontier::default(),
        }
    }

    pub(super) const fn configurations(&self) -> usize {
        self.configurations
    }

    pub(super) fn deepest_placed(&self) -> &[OperationId] {
        &self.deepest.placed
    }

    pub(super) fn deepest_blocked(&self) -> &[Blocked] {
        &self.deepest.blocked
    }

    pub(super) fn explore(
        &mut self,
        unplaced: u32,
        state: &ReferenceLockService,
    ) -> Result<bool, BudgetExhausted> {
        if unplaced == 0 {
            return Ok(true);
        }
        self.configurations += 1;
        if self.configurations > self.budget.configurations() {
            return Err(BudgetExhausted);
        }
        let configuration = (unplaced, state.view());
        if self.failed.contains(&configuration) {
            return Ok(false);
        }

        let mut blocked = Vec::new();
        for index in self.candidates(unplaced) {
            let operation = self.operations[index];
            let remainder = unplaced & !bit(index);
            let attempt = match operation.action {
                Action::Mutation { command, outcome } => {
                    self.place_mutation(operation.id, command, outcome, remainder, state)?
                }
                Action::UnknownMutation { command } => {
                    self.place_unknown_mutation(operation.id, command, remainder, state)?
                }
                Action::Query { query, result } => {
                    self.place_query(operation.id, query, result, remainder, state)?
                }
            };
            match attempt {
                Attempt::Linearized => return Ok(true),
                Attempt::Blocked(reason) => blocked.push(Blocked {
                    operation_id: operation.id,
                    reason: *reason,
                }),
            }
        }

        if self.placed.len() >= self.deepest.placed.len() {
            self.deepest = Frontier {
                placed: self.placed.clone(),
                blocked,
            };
        }
        self.failed.insert(configuration);
        Ok(false)
    }

    fn place_mutation(
        &mut self,
        id: OperationId,
        command: crate::Command,
        outcome: crate::ApplyOutcome,
        remainder: u32,
        state: &ReferenceLockService,
    ) -> Result<Attempt, BudgetExhausted> {
        let mut next = state.clone();
        let specified = next.apply(command);
        if specified != outcome {
            return Ok(Attempt::Blocked(Box::new(BlockedReason::OutcomeMismatch {
                expected: specified,
                observed: outcome,
            })));
        }
        self.descend(id, remainder, &next)
    }

    fn place_unknown_mutation(
        &mut self,
        id: OperationId,
        command: crate::Command,
        remainder: u32,
        state: &ReferenceLockService,
    ) -> Result<Attempt, BudgetExhausted> {
        let mut applied = state.clone();
        applied.apply(command);
        if matches!(self.descend(id, remainder, &applied)?, Attempt::Linearized) {
            return Ok(Attempt::Linearized);
        }
        self.descend(id, remainder, state)
    }

    fn place_query(
        &mut self,
        id: OperationId,
        query: LockQuery,
        result: LockQueryResult,
        remainder: u32,
        state: &ReferenceLockService,
    ) -> Result<Attempt, BudgetExhausted> {
        let specified = match query {
            LockQuery::GetLock { resource } => LockQueryResult::Lock(state.status(resource)),
        };
        if specified != result {
            return Ok(Attempt::Blocked(Box::new(BlockedReason::QueryMismatch {
                expected: specified,
                observed: result,
            })));
        }
        self.descend(id, remainder, state)
    }

    fn descend(
        &mut self,
        id: OperationId,
        remainder: u32,
        state: &ReferenceLockService,
    ) -> Result<Attempt, BudgetExhausted> {
        self.placed.push(id);
        let result = self.explore(remainder, state);
        self.placed.pop();
        let linearized = result?;
        Ok(if linearized {
            Attempt::Linearized
        } else {
            Attempt::Blocked(Box::new(BlockedReason::NoContinuation))
        })
    }

    fn candidates(&self, unplaced: u32) -> Vec<usize> {
        (0..self.operations.len())
            .filter(|index| unplaced & bit(*index) != 0)
            .filter(|index| self.operations[*index].must_follow & unplaced == 0)
            .collect()
    }
}

enum Attempt {
    Linearized,
    Blocked(Box<BlockedReason>),
}
