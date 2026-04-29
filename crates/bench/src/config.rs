use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use clap::{Args, Parser, Subcommand, ValueEnum};
use serde::{Deserialize, Serialize};

#[derive(Debug, Parser)]
#[command(name = "redlinedb-bench")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    Run(RunArgs),
    Compare(CompareArgs),
    Certify(CertifyArgs),
    Compat(CompatArgs),
    Recover(RecoverArgs),
    RecoverMatrix(RecoverMatrixArgs),
    Gates(GatesArgs),
    #[command(hide = true)]
    RecoverChild(RecoverChildArgs),
}

#[derive(Debug, Clone, Args)]
pub struct RunArgs {
    #[arg(long, value_enum)]
    pub engine: EngineKind,
    #[arg(long, value_enum)]
    pub workload: WorkloadKind,
    #[arg(long, value_enum, default_value = "strict")]
    pub durability: DurabilityKind,
    #[arg(long, default_value_t = 1)]
    pub threads: usize,
    #[arg(long, default_value_t = 512)]
    pub rows: usize,
    #[arg(long, default_value_t = 2)]
    pub seconds: u64,
    #[arg(long, default_value_t = 16)]
    pub cache_mib: usize,
    #[arg(long, default_value_t = 7)]
    pub seed: u64,
    #[arg(long)]
    pub out: Option<PathBuf>,
}

impl RunArgs {
    pub fn into_run_spec(&self) -> Result<RunSpec> {
        let duration = Duration::from_secs(self.seconds.max(1));
        Ok(RunSpec {
            engine: self.engine,
            workload: self.workload,
            durability: self.durability,
            threads: self.threads.max(1),
            rows: self.rows.max(1),
            duration,
            cache_bytes: self.cache_mib.max(1) * 1024 * 1024,
            seed: self.seed,
            base_dir: std::env::temp_dir().join("redlinedb-bench"),
        })
    }
}

#[derive(Debug, Clone, Args)]
pub struct CompareArgs {
    #[arg(long)]
    pub config: PathBuf,
    #[arg(long)]
    pub out: Option<PathBuf>,
    #[arg(long)]
    pub report: Option<PathBuf>,
    #[arg(long, default_value_t = 7)]
    pub seed: u64,
}

#[derive(Debug, Clone, Args)]
pub struct CertifyArgs {
    #[arg(long)]
    pub config: PathBuf,
    #[arg(long)]
    pub out_dir: PathBuf,
    #[arg(long, default_value_t = 7)]
    pub seed: u64,
    #[arg(long, default_value_t = 1)]
    pub repetitions: usize,
    #[arg(long, default_value_t = 0)]
    pub warmup: usize,
}

#[derive(Debug, Clone, Args)]
pub struct CompatArgs {
    #[arg(long, value_enum, default_value = "both")]
    pub engine: EngineSet,
    #[arg(long)]
    pub test_dir: PathBuf,
    #[arg(long, default_value_t = 7)]
    pub seed: u64,
    #[arg(long)]
    pub out: Option<PathBuf>,
}

#[derive(Debug, Clone, Args)]
pub struct RecoverArgs {
    #[arg(long, value_enum, default_value = "both")]
    pub engine: EngineSet,
    #[arg(long, value_enum, default_value = "single-row-insert")]
    pub workload: WorkloadKind,
    #[arg(long, value_enum, default_value = "strict")]
    pub durability: DurabilityKind,
    #[arg(long, default_value_t = 2)]
    pub seconds: u64,
    #[arg(long, default_value_t = 7)]
    pub seed: u64,
    #[arg(long)]
    pub out: Option<PathBuf>,
}

#[derive(Debug, Clone, Args)]
pub struct RecoverMatrixArgs {
    #[arg(long, value_enum, default_value = "both")]
    pub engine: EngineSet,
    #[arg(long)]
    pub config: PathBuf,
    #[arg(long, default_value_t = 7)]
    pub seed: u64,
    #[arg(long)]
    pub out: Option<PathBuf>,
}

#[derive(Debug, Clone, Args)]
pub struct GatesArgs {
    #[arg(long)]
    pub input: PathBuf,
    #[arg(long)]
    pub out: Option<PathBuf>,
}

#[derive(Debug, Clone, Args)]
pub struct RecoverChildArgs {
    #[arg(long, value_enum)]
    pub engine: EngineKind,
    #[arg(long, value_enum)]
    pub durability: DurabilityKind,
    #[arg(long, value_enum, default_value = "wal")]
    pub scenario: RecoveryScenarioKind,
    #[arg(long)]
    pub db_dir: PathBuf,
    #[arg(long)]
    pub ack_log: PathBuf,
    #[arg(long, default_value_t = 1024)]
    pub rows: usize,
    #[arg(long, default_value_t = 32)]
    pub checkpoint_every_rows: usize,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, ValueEnum, Serialize, Deserialize,
)]
#[serde(rename_all = "kebab-case")]
pub enum EngineKind {
    Redline,
    Sqlite,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, ValueEnum, Serialize, Deserialize,
)]
#[serde(rename_all = "kebab-case")]
pub enum EngineSet {
    Redline,
    Sqlite,
    Both,
}

impl EngineSet {
    pub fn expand(self) -> &'static [EngineKind] {
        match self {
            Self::Redline => &[EngineKind::Redline],
            Self::Sqlite => &[EngineKind::Sqlite],
            Self::Both => &[EngineKind::Redline, EngineKind::Sqlite],
        }
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, ValueEnum, Serialize, Deserialize,
)]
#[serde(rename_all = "kebab-case")]
pub enum WorkloadKind {
    SingleRowInsert,
    BatchedInsert100,
    PointReadPk,
    SecondaryIndexRead,
    SecondaryIndexRange,
    WritersDisjoint,
    HotRowUpdate,
    MixedOltp,
    Mixed95Read5Write,
    Mixed80Read20Write,
    Mixed50Read50Write,
}

impl WorkloadKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::SingleRowInsert => "single-row-insert",
            Self::BatchedInsert100 => "batched-insert-100",
            Self::PointReadPk => "point-read-pk",
            Self::SecondaryIndexRead => "secondary-index-read",
            Self::SecondaryIndexRange => "secondary-index-range",
            Self::WritersDisjoint => "writers-disjoint",
            Self::HotRowUpdate => "hot-row-update",
            Self::MixedOltp => "mixed-oltp",
            Self::Mixed95Read5Write => "mixed-95-5",
            Self::Mixed80Read20Write => "mixed-80-20",
            Self::Mixed50Read50Write => "mixed-50-50",
        }
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, ValueEnum, Serialize, Deserialize,
)]
#[serde(rename_all = "kebab-case")]
pub enum DurabilityKind {
    Strict,
    Normal,
    Unsafe,
}

impl DurabilityKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Strict => "strict",
            Self::Normal => "normal",
            Self::Unsafe => "unsafe",
        }
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, ValueEnum, Serialize, Deserialize,
)]
#[serde(rename_all = "kebab-case")]
pub enum RecoveryScenarioKind {
    Wal,
    Catalog,
    Checkpoint,
}

impl RecoveryScenarioKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Wal => "wal",
            Self::Catalog => "catalog",
            Self::Checkpoint => "checkpoint",
        }
    }
}

#[derive(Debug, Clone)]
pub struct RunSpec {
    pub engine: EngineKind,
    pub workload: WorkloadKind,
    pub durability: DurabilityKind,
    pub threads: usize,
    pub rows: usize,
    pub duration: Duration,
    pub cache_bytes: usize,
    pub seed: u64,
    pub base_dir: PathBuf,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CompareConfig {
    pub out_dir: PathBuf,
    #[serde(default = "default_engines")]
    pub engines: Vec<EngineKind>,
    pub workloads: Vec<WorkloadKind>,
    pub durabilities: Vec<DurabilityKind>,
    pub threads: Vec<usize>,
    #[serde(default = "default_rows")]
    pub rows: usize,
    #[serde(default = "default_seconds")]
    pub seconds: u64,
    #[serde(default = "default_cache_mib")]
    pub cache_mib: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RecoveryMatrixConfig {
    #[serde(default = "default_recovery_durabilities")]
    pub durabilities: Vec<DurabilityKind>,
    pub cases: Vec<RecoveryMatrixCase>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RecoveryMatrixCase {
    pub name: String,
    pub scenario: RecoveryScenarioKind,
    #[serde(default = "default_rows")]
    pub rows: usize,
    #[serde(default = "default_recovery_kill_windows")]
    pub kill_windows_ms: Vec<u64>,
    #[serde(default = "default_checkpoint_every_rows")]
    pub checkpoint_every_rows: usize,
}

impl CompareConfig {
    pub fn load(path: &Path) -> Result<Self> {
        let raw = fs::read_to_string(path)
            .with_context(|| format!("read compare config {}", path.display()))?;
        let config = toml::from_str::<Self>(&raw)
            .with_context(|| format!("parse compare config {}", path.display()))?;
        if config.workloads.is_empty()
            || config.durabilities.is_empty()
            || config.threads.is_empty()
        {
            bail!("compare config must define non-empty workloads, durabilities, and threads");
        }
        Ok(config)
    }

    pub fn run_spec(
        &self,
        engine: &EngineKind,
        workload: &WorkloadKind,
        durability: &DurabilityKind,
        threads: usize,
        seed: u64,
    ) -> Result<RunSpec> {
        Ok(RunSpec {
            engine: *engine,
            workload: *workload,
            durability: *durability,
            threads: threads.max(1),
            rows: self.rows.max(1),
            duration: Duration::from_secs(self.seconds.max(1)),
            cache_bytes: self.cache_mib.max(1) * 1024 * 1024,
            seed,
            base_dir: self.out_dir.join("dbs"),
        })
    }
}

fn default_engines() -> Vec<EngineKind> {
    vec![EngineKind::Redline, EngineKind::Sqlite]
}

fn default_rows() -> usize {
    1024
}

fn default_seconds() -> u64 {
    2
}

fn default_cache_mib() -> usize {
    16
}

fn default_recovery_durabilities() -> Vec<DurabilityKind> {
    vec![DurabilityKind::Strict, DurabilityKind::Normal]
}

fn default_recovery_kill_windows() -> Vec<u64> {
    vec![150, 350, 700]
}

fn default_checkpoint_every_rows() -> usize {
    32
}

impl FromStr for WorkloadKind {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "single-row-insert" => Ok(Self::SingleRowInsert),
            "batched-insert-100" => Ok(Self::BatchedInsert100),
            "point-read-pk" => Ok(Self::PointReadPk),
            "writers-disjoint" => Ok(Self::WritersDisjoint),
            "mixed-oltp" => Ok(Self::MixedOltp),
            _ => bail!("unknown workload {value}"),
        }
    }
}
