use crate::domain::Stripe;
use crate::service::plan::chunk::{
    DeleteChunkPlan, DeleteChunkResult, StatChunkPlan, StatChunkResult,
};
use crate::service::plan::fragment::{
    AppendFragmentPlan, AppendFragmentResult, CreateFragmentPlan, CreateFragmentResult,
    PutFragmentPlan, PutFragmentResult, ReadFragmentPlan, ReadFragmentResult,
};

/// A plan to create all fragments for a strip.
pub struct CreateStripPlan<'a> {
    pub fragments: Vec<CreateFragmentPlan<'a>>,
}

/// The result of executing a `CreateStripPlan`.
pub struct CreateStripResult {
    pub fragments: Vec<CreateFragmentResult>,
}

/// A plan to write initial payloads for all fragments in strip 0.
pub struct PutStripPlan<'a> {
    pub fragments: Vec<PutFragmentPlan<'a>>,
}

/// The result of executing a `PutStripPlan`.
pub struct PutStripResult {
    pub fragments: Vec<PutFragmentResult>,
}

/// A plan to append data to all fragments in a strip.
pub struct AppendStripPlan<'a> {
    pub strip: &'a Stripe,
    pub fragments: Vec<AppendFragmentPlan<'a>>,
}

/// The result of executing an `AppendStripPlan`.
pub struct AppendStripResult<'a> {
    pub strip: &'a Stripe,
    pub fragments: Vec<AppendFragmentResult<'a>>,
}

pub struct ReadStripPlan<'a> {
    pub strip: &'a Stripe,
    pub fragments: Vec<ReadFragmentPlan<'a>>,
}

pub struct ReadStripResult<'a> {
    pub strip: &'a Stripe,
    pub fragments: Vec<ReadFragmentResult<'a>>,
}

pub struct StatStripPlan<'a> {
    pub strip: &'a Stripe,
    pub fragments: Vec<StatChunkPlan<'a>>,
}

pub struct StatStripResult<'a> {
    pub strip: &'a Stripe,
    pub fragments: Vec<StatChunkResult<'a>>,
}

pub struct DeleteStripPlan<'a> {
    pub strip: &'a Stripe,
    pub fragments: Vec<DeleteChunkPlan<'a>>,
}

pub struct DeleteStripResult<'a> {
    pub strip: &'a Stripe,
    pub fragments: Vec<DeleteChunkResult<'a>>,
}
