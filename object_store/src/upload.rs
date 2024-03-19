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

use crate::{PutResult, Result};
use async_trait::async_trait;
use bytes::Bytes;
use futures::future::BoxFuture;
use tokio::task::JoinSet;

/// An upload part request
pub type UploadPart = BoxFuture<'static, Result<()>>;

/// A trait allowing writing an object in fixed size chunks
///
/// Consecutive chunks of data can be written by calling [`MultipartUpload::put_part`] and polling
/// the returned futures to completion. Multiple futures returned by [`MultipartUpload::put_part`]
/// may be polled in parallel, allowing for concurrent uploads.
///
/// Once all part uploads have been polled to completion, the upload can be completed by
/// calling [`MultipartUpload::complete`]. This will make the entire uploaded object visible
/// as an atomic operation.It is implementation behind behaviour if [`MultipartUpload::complete`]
/// is called before all [`UploadPart`] have been polled to completion.
#[async_trait]
pub trait MultipartUpload: Send + std::fmt::Debug {
    /// Upload the next part
    ///
    /// Most stores require that all parts excluding the last are at least 5 MiB, and some
    /// further require that all parts excluding the last be the same size, e.g. [R2].
    /// Clients wanting to maximise compatibility should therefore perform writes in
    /// fixed size blocks larger than 5 MiB.
    ///
    /// Implementations may invoke this method multiple times and then await on the
    /// returned futures in parallel
    ///
    /// ```no_run
    /// # use futures::StreamExt;
    /// # use object_store::MultipartUpload;
    /// #
    /// # async fn test() {
    /// #
    /// let mut upload: Box<&dyn MultipartUpload> = todo!();
    /// let p1 = upload.put_part(vec![0; 10 * 1024 * 1024].into());
    /// let p2 = upload.put_part(vec![1; 10 * 1024 * 1024].into());
    /// futures::future::try_join(p1, p2).await.unwrap();
    /// upload.complete().await.unwrap();
    /// # }
    /// ```
    ///
    /// [R2]: https://developers.cloudflare.com/r2/objects/multipart-objects/#limitations
    fn put_part(&mut self, data: Bytes) -> UploadPart;

    /// Complete the multipart upload
    ///
    /// It is implementation defined behaviour if this method is called before polling
    /// all [`UploadPart`] returned by [`MultipartUpload::put_part`] to completion. Additionally,
    /// it is implementation defined behaviour to call [`MultipartUpload::complete`]
    /// on an already completed or aborted [`MultipartUpload`].
    async fn complete(&mut self) -> Result<PutResult>;

    /// Abort the multipart upload
    ///
    /// If a [`MultipartUpload`] is dropped without calling [`MultipartUpload::complete`],
    /// some implementations will automatically reap any uploaded parts. However,
    /// this is not always possible, e.g. for S3 and GCS. [`MultipartUpload::abort`] can
    /// therefore be invoked to perform this cleanup.
    ///
    /// Given it is not possible call `abort` in all failure scenarios (e.g. if your program is `SIGKILL`ed due to
    /// OOM), it is recommended to configure your object store with lifecycle rules
    /// to automatically cleanup unused parts older than some threshold.
    /// See [crate::aws] and [crate::gcp] for more information.
    ///
    /// It is implementation defined behaviour to call [`MultipartUpload::abort`]
    /// on an already completed or aborted [`MultipartUpload`]
    async fn abort(&mut self) -> Result<()>;
}

/// A synchronous write API for uploading data in parallel in fixed size chunks
///
/// Uses multiple tokio tasks in a [`JoinSet`] to multiplex upload tasks in parallel
///
/// The design also takes inspiration from [`Sink`] with [`WriteMultipart::wait_for_capacity`]
/// allowing back pressure on producers, prior to buffering the next part. However, unlike
/// [`Sink`] this back pressure is optional, allowing integration with synchronous producers
///
/// [`Sink`]: futures::sink::Sink
#[derive(Debug)]
pub struct WriteMultipart {
    upload: Box<dyn MultipartUpload>,

    buffer: Vec<u8>,

    tasks: JoinSet<Result<()>>,
}

impl WriteMultipart {
    /// Create a new [`WriteMultipart`] that will upload using 5MB chunks
    pub fn new(upload: Box<dyn MultipartUpload>) -> Self {
        Self::new_with_capacity(upload, 5 * 1024 * 1024)
    }

    /// Create a new [`WriteMultipart`] that will upload in fixed `capacity` sized chunks
    pub fn new_with_capacity(upload: Box<dyn MultipartUpload>, capacity: usize) -> Self {
        Self {
            upload,
            buffer: Vec::with_capacity(capacity),
            tasks: Default::default(),
        }
    }

    /// Wait until there are `max_concurrency` or fewer requests in-flight
    pub async fn wait_for_capacity(&mut self, max_concurrency: usize) -> Result<()> {
        while self.tasks.len() > max_concurrency {
            self.tasks.join_next().await.unwrap()??;
        }
        Ok(())
    }

    /// Write data to this [`WriteMultipart`]
    ///
    /// Note this method is synchronous (not `async`) and will immediately start new uploads
    /// as soon as the internal `capacity` is hit, regardless of 
    /// how many outstanding uploads are already in progress.
    /// 
    /// Back pressure can optionally be applied to producers by calling
    /// [`Self::wait_for_capacity`] prior to calling this method
    pub fn write(&mut self, mut buf: &[u8]) {
        while !buf.is_empty() {
            let capacity = self.buffer.capacity();
            let remaining = capacity - self.buffer.len();
            let to_read = buf.len().min(remaining);
            self.buffer.extend_from_slice(&buf[..to_read]);
            if to_read == remaining {
                let part = std::mem::replace(&mut self.buffer, Vec::with_capacity(capacity));
                self.put_part(part.into())
            }
            buf = &buf[to_read..]
        }
    }

    fn put_part(&mut self, part: Bytes) {
        self.tasks.spawn(self.upload.put_part(part));
    }

    /// Abort this upload, attempting to clean up any successfully uploaded parts
    pub async fn abort(mut self) -> Result<()> {
        self.tasks.shutdown().await;
        self.upload.abort().await
    }

    /// Flush final chunk, and await completion of all in-flight requests
    pub async fn finish(mut self) -> Result<PutResult> {
        if !self.buffer.is_empty() {
            let part = std::mem::take(&mut self.buffer);
            self.put_part(part.into())
        }

        self.wait_for_capacity(0).await?;
        self.upload.complete().await
    }
}
