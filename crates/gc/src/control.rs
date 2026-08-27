use crate::planner::GcPlannerConfig;

const BPS_DENOMINATOR: u128 = 10_000;

/// Disk pressure level used by the GC control plane.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum GcPressureLevel {
    /// Normal operation. Foreground-latency protection may throttle GC.
    Healthy,
    /// Free space is low enough to make GC more eager, but foreground latency still wins.
    Soft,
    /// Disk pressure is high. Reclaim work may run even while foreground latency is degraded.
    Hard,
    /// Disk-full avoidance is urgent and dominates the latency tuner.
    Critical,
}

impl GcPressureLevel {
    /// Returns true when disk-pressure avoidance must override foreground-latency throttling.
    pub fn bypasses_foreground_latency(self) -> bool {
        matches!(self, Self::Hard | Self::Critical)
    }
}

/// Free space sample supplied by the store executor or platform monitor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GcDiskSpace {
    pub free_bytes: u64,
    pub free_ratio_bps: u16,
}

impl GcDiskSpace {
    pub fn new(free_bytes: u64, free_ratio_bps: u16) -> Self {
        Self {
            free_bytes,
            free_ratio_bps: free_ratio_bps.min(BPS_DENOMINATOR as u16),
        }
    }

    pub fn from_capacity(free_bytes: u64, total_bytes: u64) -> Self {
        let free_ratio_bps = if total_bytes == 0 {
            0
        } else {
            free_bytes
                .min(total_bytes)
                .saturating_mul(BPS_DENOMINATOR as u64)
                / total_bytes
        };
        Self::new(free_bytes, free_ratio_bps as u16)
    }
}

/// One free-space threshold. Crossing either the absolute or ratio threshold counts as pressure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GcFreeSpaceThreshold {
    pub free_bytes: u64,
    pub free_ratio_bps: u16,
}

impl GcFreeSpaceThreshold {
    fn crossed(self, disk: GcDiskSpace) -> bool {
        disk.free_bytes <= self.free_bytes || disk.free_ratio_bps <= self.free_ratio_bps
    }
}

/// Free-space thresholds used to classify disk pressure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GcFreeSpacePolicy {
    pub soft: GcFreeSpaceThreshold,
    pub hard: GcFreeSpaceThreshold,
    pub critical: GcFreeSpaceThreshold,
}

impl GcFreeSpacePolicy {
    pub fn pressure_level(self, disk: GcDiskSpace) -> GcPressureLevel {
        let mut pressure = GcPressureLevel::Healthy;
        if self.soft.crossed(disk) {
            pressure = pressure.max(GcPressureLevel::Soft);
        }
        if self.hard.crossed(disk) {
            pressure = pressure.max(GcPressureLevel::Hard);
        }
        if self.critical.crossed(disk) {
            pressure = pressure.max(GcPressureLevel::Critical);
        }
        pressure
    }
}

/// Per pressure GC I/O budget. Step two will consume this through a shared byte limiter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GcIoBudgetPolicy {
    pub healthy_bytes_per_sec: u64,
    pub soft_bytes_per_sec: u64,
    pub hard_bytes_per_sec: u64,
    pub critical_bytes_per_sec: u64,
}

impl GcIoBudgetPolicy {
    pub fn budget_for(self, pressure: GcPressureLevel) -> u64 {
        match pressure {
            GcPressureLevel::Healthy => self.healthy_bytes_per_sec,
            GcPressureLevel::Soft => self.soft_bytes_per_sec,
            GcPressureLevel::Hard => self.hard_bytes_per_sec,
            GcPressureLevel::Critical => self.critical_bytes_per_sec,
        }
    }
}

/// Planner threshold override for one pressure band.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GcPlannerPressureThresholds {
    pub min_reclaim_bytes: u64,
    pub min_garbage_ratio_bps: u16,
}

/// Pressure aware planner threshold policy.
///
/// Thresholds are applied cumulatively as pressure rises, so escalation can only make GC as eager
/// or more eager than the base planner config.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GcPlannerPressurePolicy {
    pub soft: GcPlannerPressureThresholds,
    pub hard: GcPlannerPressureThresholds,
    pub critical: GcPlannerPressureThresholds,
}

impl GcPlannerPressurePolicy {
    pub fn effective_config(
        self,
        pressure: GcPressureLevel,
        base: &GcPlannerConfig,
    ) -> GcPlannerConfig {
        let mut config = base.clone();
        if pressure >= GcPressureLevel::Soft {
            apply_thresholds(&mut config, self.soft);
        }
        if pressure >= GcPressureLevel::Hard {
            apply_thresholds(&mut config, self.hard);
        }
        if pressure >= GcPressureLevel::Critical {
            apply_thresholds(&mut config, self.critical);
        }
        config
    }
}

/// Inputs consumed by the GC control model for one scheduling decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GcControlInputs {
    pub disk_space: GcDiskSpace,
    pub reclaimable_garbage_bytes: u64,
    pub active_gc_backlog_bytes: u64,
    pub foreground_latency_pressured: bool,
}

impl GcControlInputs {
    fn has_gc_work(self) -> bool {
        self.reclaimable_garbage_bytes > 0 || self.active_gc_backlog_bytes > 0
    }
}

/// Pure GC control policy. The store executor supplies measurements, then applies this decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GcControlPolicy {
    pub free_space: GcFreeSpacePolicy,
    pub io_budget: GcIoBudgetPolicy,
    pub planner_thresholds: GcPlannerPressurePolicy,
}

impl GcControlPolicy {
    pub fn decide(
        self,
        inputs: GcControlInputs,
        base_planner_config: &GcPlannerConfig,
    ) -> GcControlDecision {
        let pressure = self.free_space.pressure_level(inputs.disk_space);
        let allow_gc_when_foreground_pressured = pressure.bypasses_foreground_latency();
        let gc_may_run = !inputs.foreground_latency_pressured || allow_gc_when_foreground_pressured;

        GcControlDecision {
            pressure,
            io_budget_bytes_per_sec: self.io_budget.budget_for(pressure),
            effective_planner_config: self
                .planner_thresholds
                .effective_config(pressure, base_planner_config),
            allow_gc_when_foreground_pressured,
            latency_tuner_may_throttle: !allow_gc_when_foreground_pressured,
            gc_may_run,
            request_immediate_run: pressure >= GcPressureLevel::Soft
                && inputs.has_gc_work()
                && gc_may_run,
        }
    }
}

/// Output of the GC control model.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GcControlDecision {
    pub pressure: GcPressureLevel,
    pub io_budget_bytes_per_sec: u64,
    pub effective_planner_config: GcPlannerConfig,
    pub allow_gc_when_foreground_pressured: bool,
    pub latency_tuner_may_throttle: bool,
    pub gc_may_run: bool,
    pub request_immediate_run: bool,
}

fn apply_thresholds(config: &mut GcPlannerConfig, thresholds: GcPlannerPressureThresholds) {
    config.min_reclaim_bytes = config.min_reclaim_bytes.min(thresholds.min_reclaim_bytes);
    config.min_garbage_ratio_bps = config
        .min_garbage_ratio_bps
        .min(thresholds.min_garbage_ratio_bps.min(BPS_DENOMINATOR as u16));
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIB: u64 = 1024 * 1024;

    fn policy() -> GcControlPolicy {
        GcControlPolicy {
            free_space: GcFreeSpacePolicy {
                soft: GcFreeSpaceThreshold {
                    free_bytes: 1024 * MIB,
                    free_ratio_bps: 2_000,
                },
                hard: GcFreeSpaceThreshold {
                    free_bytes: 512 * MIB,
                    free_ratio_bps: 1_000,
                },
                critical: GcFreeSpaceThreshold {
                    free_bytes: 128 * MIB,
                    free_ratio_bps: 500,
                },
            },
            io_budget: GcIoBudgetPolicy {
                healthy_bytes_per_sec: 8 * MIB,
                soft_bytes_per_sec: 16 * MIB,
                hard_bytes_per_sec: 64 * MIB,
                critical_bytes_per_sec: 128 * MIB,
            },
            planner_thresholds: GcPlannerPressurePolicy {
                soft: GcPlannerPressureThresholds {
                    min_reclaim_bytes: 32 * MIB,
                    min_garbage_ratio_bps: 5_000,
                },
                hard: GcPlannerPressureThresholds {
                    min_reclaim_bytes: 8 * MIB,
                    min_garbage_ratio_bps: 3_000,
                },
                critical: GcPlannerPressureThresholds {
                    min_reclaim_bytes: MIB,
                    min_garbage_ratio_bps: 1_000,
                },
            },
        }
    }

    #[test]
    fn healthy_pressure_keeps_base_policy_and_respects_latency() {
        let base = GcPlannerConfig::default();

        let decision = policy().decide(
            GcControlInputs {
                disk_space: GcDiskSpace::new(2 * 1024 * MIB, 4_000),
                reclaimable_garbage_bytes: 64 * MIB,
                active_gc_backlog_bytes: 0,
                foreground_latency_pressured: true,
            },
            &base,
        );

        assert_eq!(decision.pressure, GcPressureLevel::Healthy);
        assert_eq!(decision.io_budget_bytes_per_sec, 8 * MIB);
        assert_eq!(decision.effective_planner_config, base);
        assert!(!decision.allow_gc_when_foreground_pressured);
        assert!(decision.latency_tuner_may_throttle);
        assert!(!decision.gc_may_run);
        assert!(!decision.request_immediate_run);
    }

    #[test]
    fn critical_pressure_overrides_foreground_latency() {
        let decision = policy().decide(
            GcControlInputs {
                disk_space: GcDiskSpace::new(100 * MIB, 400),
                reclaimable_garbage_bytes: 256 * MIB,
                active_gc_backlog_bytes: 0,
                foreground_latency_pressured: true,
            },
            &GcPlannerConfig::default(),
        );

        assert_eq!(decision.pressure, GcPressureLevel::Critical);
        assert_eq!(decision.io_budget_bytes_per_sec, 128 * MIB);
        assert_eq!(decision.effective_planner_config.min_reclaim_bytes, MIB);
        assert_eq!(
            decision.effective_planner_config.min_garbage_ratio_bps,
            1_000
        );
        assert!(decision.allow_gc_when_foreground_pressured);
        assert!(!decision.latency_tuner_may_throttle);
        assert!(decision.gc_may_run);
        assert!(decision.request_immediate_run);
    }

    #[test]
    fn pressure_uses_most_severe_byte_or_ratio_threshold() {
        let free_space = policy().free_space;

        assert_eq!(
            free_space.pressure_level(GcDiskSpace::new(700 * MIB, 400)),
            GcPressureLevel::Critical
        );
        assert_eq!(
            free_space.pressure_level(GcDiskSpace::new(300 * MIB, 3_000)),
            GcPressureLevel::Hard
        );
    }

    #[test]
    fn planner_thresholds_only_get_more_aggressive_as_pressure_rises() {
        let base = GcPlannerConfig {
            min_reclaim_bytes: 64 * MIB,
            min_garbage_ratio_bps: 6_000,
            ..GcPlannerConfig::default()
        };
        let thresholds = GcPlannerPressurePolicy {
            soft: GcPlannerPressureThresholds {
                min_reclaim_bytes: 128 * MIB,
                min_garbage_ratio_bps: 7_000,
            },
            hard: GcPlannerPressureThresholds {
                min_reclaim_bytes: 8 * MIB,
                min_garbage_ratio_bps: 3_000,
            },
            critical: GcPlannerPressureThresholds {
                min_reclaim_bytes: MIB,
                min_garbage_ratio_bps: 1_000,
            },
        };

        let soft = thresholds.effective_config(GcPressureLevel::Soft, &base);
        let hard = thresholds.effective_config(GcPressureLevel::Hard, &base);
        let critical = thresholds.effective_config(GcPressureLevel::Critical, &base);

        assert_eq!(soft.min_reclaim_bytes, 64 * MIB);
        assert_eq!(soft.min_garbage_ratio_bps, 6_000);
        assert_eq!(hard.min_reclaim_bytes, 8 * MIB);
        assert_eq!(hard.min_garbage_ratio_bps, 3_000);
        assert_eq!(critical.min_reclaim_bytes, MIB);
        assert_eq!(critical.min_garbage_ratio_bps, 1_000);
    }
}
