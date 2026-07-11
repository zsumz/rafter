/// Monotonic logical time used by the deterministic simulator.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SimTick(pub u64);

/// Simulator clock advanced explicitly by ticks and message scheduling.
#[derive(Clone, Debug, Default, Eq, Hash, PartialEq)]
pub struct SimClock {
    now: SimTick,
}

impl SimClock {
    /// Returns the current logical tick.
    #[must_use]
    pub fn now(&self) -> SimTick {
        self.now
    }

    /// Advances the clock by one logical tick and returns the new value.
    pub fn advance(&mut self) -> SimTick {
        self.now.0 += 1;
        self.now
    }
}

impl SimTick {
    /// Returns a tick value `ticks` after this one.
    #[must_use]
    pub fn after(self, ticks: u64) -> Self {
        Self(self.0 + ticks)
    }
}

/// Seed for deterministic simulator randomness.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct SimSeed(pub u64);

impl Default for SimSeed {
    fn default() -> Self {
        Self(0x5041_4e47_4541)
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct SimRng {
    state: u64,
}

impl SimRng {
    pub(crate) fn new(seed: SimSeed) -> Self {
        Self { state: seed.0 }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self
            .state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);
        self.state
    }

    pub(crate) fn index(&mut self, upper_bound: usize) -> usize {
        debug_assert!(upper_bound > 0);
        let bounded = self.next_u64() % upper_bound as u64;
        usize::try_from(bounded).unwrap_or(upper_bound - 1)
    }
}
