// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::aws::dynamo::DynamoCommit;
use crate::config::Parse;

/// Configure how to provide [`ObjectStore::copy_if_not_exists`] for [`AmazonS3`].
///
/// [`ObjectStore::copy_if_not_exists`]: crate::ObjectStore::copy_if_not_exists
/// [`AmazonS3`]: super::AmazonS3
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum S3CopyIfNotExists {
    /// Some S3-compatible stores, such as Cloudflare R2, support copy if not exists
    /// semantics through custom headers.
    ///
    /// If set, [`ObjectStore::copy_if_not_exists`] will perform a normal copy operation
    /// with the provided header pair, and expect the store to fail with `412 Precondition Failed`
    /// if the destination file already exists
    ///
    /// Encoded as `header:<HEADER_NAME>:<HEADER_VALUE>` ignoring whitespace
    ///
    /// For example `header: cf-copy-destination-if-none-match: *`, would set
    /// the header `cf-copy-destination-if-none-match` to `*`
    ///
    /// [`ObjectStore::copy_if_not_exists`]: crate::ObjectStore::copy_if_not_exists
    Header(String, String),
    /// The name of a DynamoDB table to use for coordination
    ///
    /// Encoded as `dynamodb:<TABLE_NAME>` ignoring whitespace
    ///
    /// See [`DynamoCommit`] for more information
    ///
    /// This will use the same region, credentials and endpoint as configured for S3
    Dynamo(DynamoCommit),
}

impl std::fmt::Display for S3CopyIfNotExists {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Header(k, v) => write!(f, "header: {}: {}", k, v),
            Self::Dynamo(lock) => write!(f, "dynamo: {}", lock.table_name()),
        }
    }
}

impl S3CopyIfNotExists {
    fn from_str(s: &str) -> Option<Self> {
        let (variant, value) = s.split_once(':')?;
        match variant.trim() {
            "header" => {
                let (k, v) = value.split_once(':')?;
                Some(Self::Header(k.trim().to_string(), v.trim().to_string()))
            }
            "dynamo" => Some(Self::Dynamo(DynamoCommit::new(value.trim().to_string()))),
            _ => None,
        }
    }
}

impl Parse for S3CopyIfNotExists {
    fn parse(v: &str) -> crate::Result<Self> {
        Self::from_str(v).ok_or_else(|| crate::Error::Generic {
            store: "Config",
            source: format!("Failed to parse \"{v}\" as S3CopyIfNotExists").into(),
        })
    }
}

/// Configure how to provide conditional put support for [`AmazonS3`].
///
/// [`AmazonS3`]: super::AmazonS3
#[derive(Debug, Clone)]
#[allow(missing_copy_implementations)]
#[non_exhaustive]
pub enum S3ConditionalPut {
    /// Some S3-compatible stores, such as Cloudflare R2 and minio support conditional
    /// put using the standard [HTTP precondition] headers If-Match and If-None-Match
    ///
    /// Encoded as `etag` ignoring whitespace
    ///
    /// [HTTP precondition]: https://datatracker.ietf.org/doc/html/rfc9110#name-preconditions
    ETagMatch,
}

impl std::fmt::Display for S3ConditionalPut {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ETagMatch => write!(f, "etag"),
        }
    }
}

impl S3ConditionalPut {
    fn from_str(s: &str) -> Option<Self> {
        match s.trim() {
            "etag" => Some(Self::ETagMatch),
            _ => None,
        }
    }
}

impl Parse for S3ConditionalPut {
    fn parse(v: &str) -> crate::Result<Self> {
        Self::from_str(v).ok_or_else(|| crate::Error::Generic {
            store: "Config",
            source: format!("Failed to parse \"{v}\" as S3PutConditional").into(),
        })
    }
}
